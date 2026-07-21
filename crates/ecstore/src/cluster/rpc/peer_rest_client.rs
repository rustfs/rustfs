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
    TonicInterceptor, gen_tonic_signature_interceptor, heal_control_time_out_client, node_service_time_out_client,
    tier_mutation_control_time_out_client,
};
use crate::cluster::rpc::{set_tonic_canonical_body_digest, verify_tonic_rpc_response_proof};
use crate::error::{Error, Result};
use crate::{
    disk::disk_store::{get_drive_active_check_interval, get_drive_active_check_timeout},
    layout::endpoints::EndpointServerPools,
    runtime::sources as runtime_sources,
    services::metrics_realtime::{CollectMetricsOpts, MetricType},
};
use bytes::Bytes;
use rmp_serde::{Deserializer, Serializer};
use rustfs_madmin::{
    ServerProperties,
    health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysServices},
    metrics::RealtimeMetrics,
    net::NetInfo,
};
use rustfs_protos::proto_gen::node_service::{
    BackgroundHealStatusRequest, CancelDecommissionRequest, ClearDecommissionRequest, DeleteBucketMetadataRequest,
    DeletePolicyRequest, DeleteServiceAccountRequest, DeleteUserRequest, GetCpusRequest, GetLiveEventsRequest, GetMemInfoRequest,
    GetMetricsRequest, GetNetInfoRequest, GetOsInfoRequest, GetPartitionsRequest, GetProcInfoRequest, GetSeLinuxInfoRequest,
    GetSysConfigRequest, GetSysErrorsRequest, HealControlRequest, LoadBucketMetadataRequest, LoadGroupRequest,
    LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest, LoadServiceAccountRequest,
    LoadTransitionTierConfigRequest, LoadUserRequest, LocalStorageInfoRequest, Mss, ReloadPoolMetaRequest,
    ReloadSiteReplicationConfigRequest, ScannerActivityRequest, ScannerActivityResponse, ServerInfoRequest, SignalServiceRequest,
    StartDecommissionRequest, StartProfilingRequest, StopRebalanceRequest, TierMutationAbortRequest, TierMutationCommitRequest,
    TierMutationControlResponse, TierMutationPeerState, TierMutationPrepareRequest, node_service_client::NodeServiceClient,
    tier_mutation_control_service_client::TierMutationControlServiceClient,
};
use rustfs_protos::{TierMutationRpcPhase, evict_failed_connection};
use rustfs_utils::XHost;
use serde::{Deserialize, Serialize as _};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};
use tokio::{net::TcpStream, time::Duration};
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, info, warn};
use uuid::Uuid;

pub const PEER_RESTSIGNAL: &str = "signal";
pub const PEER_RESTSUB_SYS: &str = "sub-sys";
pub const PEER_RESTDRY_RUN: &str = "dry-run";
pub const SERVICE_SIGNAL_REFRESH_CONFIG: u64 = 1;
pub const SERVICE_SIGNAL_RELOAD_DYNAMIC: u64 = 2;
const BACKGROUND_HEAL_STATUS_MAX_MESSAGE_SIZE: usize = 64 * 1024;
const HEAL_CONTROL_FINGERPRINT_MAX_SIZE: usize = 256;
const HEAL_CONTROL_PAYLOAD_MAX_SIZE: usize = 64 * 1024;
const PEER_REST_RECOVERY_MAX_ATTEMPTS: u32 = 60;
const PEER_REST_RECOVERY_MAX_BACKOFF: Duration = Duration::from_secs(30);
const SCANNER_ACTIVITY_MAX_MESSAGE_SIZE: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerPeerActivity {
    pub instance_id: String,
    pub namespace_generation: u64,
    pub maintenance_generation: u64,
}

fn decode_scanner_activity(response: ScannerActivityResponse) -> Result<ScannerPeerActivity> {
    let instance_id = response.instance_id;
    if instance_id.len() != 32
        || !instance_id
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(Error::other("peer returned an invalid scanner activity instance ID"));
    }
    Ok(ScannerPeerActivity {
        instance_id,
        namespace_generation: response.namespace_generation,
        maintenance_generation: response.maintenance_generation,
    })
}

fn validate_heal_control_capability_proof(canonical_ack: &[u8], proof: &[u8]) -> Result<()> {
    verify_tonic_rpc_response_proof(canonical_ack, proof)
        .map_err(|_| Error::other("peer returned an invalid heal control capability proof"))
}

fn validate_heal_control_response_proof(canonical_response: &[u8], proof: &[u8]) -> Result<()> {
    verify_tonic_rpc_response_proof(canonical_response, proof)
        .map_err(|_| Error::other("peer returned an invalid heal control response proof"))
}

#[derive(Clone, Debug)]
pub struct PeerLiveEventsBatch {
    pub events: Vec<u8>,
    pub next_sequence: u64,
    pub truncated: bool,
}

#[derive(Clone, Debug)]
pub struct PeerRestClient {
    pub host: XHost,
    pub grid_host: String,
    offline: Arc<AtomicBool>,
    recovery_running: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerTierMutationState {
    Prepared,
    Committed,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerTierMutationOutcome {
    pub state: PeerTierMutationState,
    pub applied: bool,
}

fn validate_tier_mutation_response_proof(
    version: u32,
    phase: TierMutationRpcPhase,
    mutation_id: Uuid,
    canonical_payload: &[u8],
    response: &TierMutationControlResponse,
) -> Result<()> {
    let canonical_response =
        rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
            version,
            phase,
            mutation_id,
            canonical_payload,
            success: response.success,
            state: response.state,
            applied: response.applied,
            error_info: response.error_info.as_deref(),
        })
        .map_err(|_| Error::other("tier mutation response length cannot be represented"))?;
    verify_tonic_rpc_response_proof(&canonical_response, &response.response_proof)
        .map_err(|_| Error::other("peer returned an invalid tier mutation response proof"))
}

fn decode_tier_mutation_peer_state(state: i32) -> Result<PeerTierMutationState> {
    match TierMutationPeerState::try_from(state).map_err(|_| Error::other("peer returned an invalid tier mutation state"))? {
        TierMutationPeerState::Prepared => Ok(PeerTierMutationState::Prepared),
        TierMutationPeerState::Committed => Ok(PeerTierMutationState::Committed),
        TierMutationPeerState::Aborted => Ok(PeerTierMutationState::Aborted),
        TierMutationPeerState::Unspecified => Err(Error::other("peer returned an unspecified tier mutation state")),
    }
}

fn validate_tier_mutation_payload_len(phase: TierMutationRpcPhase, payload_len: usize) -> Result<()> {
    let limit = match phase {
        TierMutationRpcPhase::Prepare => rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE,
        TierMutationRpcPhase::Commit => rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE,
        TierMutationRpcPhase::Abort => {
            if payload_len == 0 {
                return Ok(());
            }
            return Err(Error::other("tier mutation abort payload must be empty"));
        }
        _ => return Err(Error::other("tier mutation rpc phase is unsupported")),
    };
    if payload_len > limit {
        return Err(Error::other("tier mutation payload exceeds size limit"));
    }
    Ok(())
}

impl PeerRestClient {
    fn recovery_monitor_span(grid_host: &str) -> tracing::Span {
        tracing::info_span!(
            "recovery-monitor",
            component = "ecstore",
            subsystem = "peer_rest_client",
            kind = "peer_rest",
            grid_host = %grid_host
        )
    }

    pub fn new(host: XHost, grid_host: String) -> Self {
        Self {
            host,
            grid_host,
            offline: Arc::new(AtomicBool::new(false)),
            recovery_running: Arc::new(AtomicBool::new(false)),
        }
    }
    pub async fn new_clients(eps: EndpointServerPools) -> (Vec<Option<Self>>, Vec<Option<Self>>) {
        if !runtime_sources::setup_is_dist_erasure().await {
            return (Vec::new(), Vec::new());
        }

        let eps = eps.clone();
        let hosts = eps.hosts_sorted();
        let mut remote = Vec::with_capacity(hosts.len());
        let mut all = vec![None; hosts.len()];
        for (i, hs_host) in hosts.iter().enumerate() {
            if let Some(host) = hs_host
                && let Some(grid_host) = eps.find_grid_hosts_from_peer(host)
            {
                let client = PeerRestClient::new(host.clone(), grid_host);

                all[i] = Some(client.clone());
                remote.push(Some(client));
            }
        }

        if all.len() != remote.len() + 1 {
            warn!("Expected number of all hosts ({}) to be remote +1 ({})", all.len(), remote.len());
        }

        (remote, all)
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        if self.offline.load(Ordering::Acquire) {
            self.mark_offline_and_spawn_recovery();
            return Err(Error::other(format!("peer {} is temporarily offline", self.grid_host)));
        }

        node_service_time_out_client(&self.grid_host, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| {
                let storage_err = Error::other(format!("can not get client, err: {err}"));
                if Self::is_network_like_error(&storage_err) {
                    self.mark_offline_and_spawn_recovery();
                }
                storage_err
            })
    }

    async fn get_heal_control_client(
        &self,
    ) -> Result<
        rustfs_protos::proto_gen::node_service::heal_control_service_client::HealControlServiceClient<
            InterceptedService<Channel, TonicInterceptor>,
        >,
    > {
        if self.offline.load(Ordering::Acquire) {
            self.mark_offline_and_spawn_recovery();
            return Err(Error::other(format!("peer {} is temporarily offline", self.grid_host)));
        }

        heal_control_time_out_client(&self.grid_host, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| {
                let storage_err = Error::other(format!("can not get heal control client, err: {err}"));
                if Self::is_network_like_error(&storage_err) {
                    self.mark_offline_and_spawn_recovery();
                }
                storage_err
            })
    }

    async fn get_tier_mutation_control_client(
        &self,
    ) -> Result<TierMutationControlServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        if self.offline.load(Ordering::Acquire) {
            self.mark_offline_and_spawn_recovery();
            return Err(Error::other(format!("peer {} is temporarily offline", self.grid_host)));
        }

        tier_mutation_control_time_out_client(&self.grid_host, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| {
                let storage_err = Error::other(format!("can not get tier mutation control client, err: {err}"));
                if Self::is_network_like_error(&storage_err) {
                    self.mark_offline_and_spawn_recovery();
                }
                storage_err
            })
    }

    /// Evict the connection to this peer from the global cache.
    /// This should be called when communication with this peer fails.
    pub async fn evict_connection(&self) {
        evict_failed_connection(&self.grid_host).await;
    }

    /// Prepare this client for an immediate fresh-connection retry.
    ///
    /// On a network-like failure `finalize_result` both evicts the channel and
    /// sets the offline gate, after which `get_client` fast-fails with
    /// "temporarily offline" and only the async background recovery monitor
    /// would clear the gate (not within this call). So a plain `evict_connection`
    /// is not enough to make an in-call retry actually re-dial: the gate still
    /// short-circuits it. This drops the cached channel AND clears the gate so
    /// the very next `get_client` re-dials. See rustfs/backlog#1049 (P1-B).
    pub async fn prepare_retry(&self) {
        self.evict_connection().await;
        self.offline.store(false, Ordering::Release);
    }

    fn is_network_like_error(err: &Error) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        [
            "temporarily offline",
            "transport error",
            "unavailable",
            "error trying to connect",
            "connection refused",
            "connection reset",
            "broken pipe",
            "not connected",
            "unexpected eof",
            "timed out",
            "deadline has elapsed",
            "connection closed",
            "connection aborted",
            "tcp connect error",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    fn mark_offline_and_spawn_recovery(&self) {
        self.offline.store(true, Ordering::Release);

        if self
            .recovery_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let grid_host = self.grid_host.clone();
        let offline = Arc::clone(&self.offline);
        let recovery_running = Arc::clone(&self.recovery_running);
        let span = Self::recovery_monitor_span(&grid_host);
        // The offline flag and its recovery are the silent half of
        // rustfs/backlog#888: log the monitor's start and its success so an
        // "offline then back" episode leaves a trace on the observing node.
        warn!(
            event = "peer_connection_marked_offline",
            grid_host = %self.grid_host,
            "peer RPC connection marked offline after a network-like failure; starting background recovery monitor"
        );
        super::spawn_background_monitor(span, async move {
            let mut delay = get_drive_active_check_interval();
            let connect_timeout = get_drive_active_check_timeout();

            for attempt in 1..=PEER_REST_RECOVERY_MAX_ATTEMPTS {
                tokio::time::sleep(delay).await;
                if Self::perform_connectivity_check(&grid_host, connect_timeout).await.is_ok() {
                    offline.store(false, Ordering::Release);
                    recovery_running.store(false, Ordering::Release);
                    info!(
                        event = "peer_connection_recovered",
                        grid_host = %grid_host,
                        attempts = attempt,
                        "peer connectivity restored by background recovery monitor"
                    );
                    return;
                }

                delay = std::cmp::min(delay.saturating_mul(2), PEER_REST_RECOVERY_MAX_BACKOFF);
            }

            warn!(
                grid_host = %grid_host,
                attempts = PEER_REST_RECOVERY_MAX_ATTEMPTS,
                "peer recovery monitor reached max attempts; will retry on next request"
            );
            recovery_running.store(false, Ordering::Release);
        });
    }

    #[cfg(test)]
    fn spawn_recovery_monitor_log_probe_for_test(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let grid_host = self.grid_host.clone();
        let span = Self::recovery_monitor_span(&grid_host);
        super::spawn_background_monitor(span, async move {
            warn!(grid_host = %grid_host, "peer recovery monitor log probe");
            let _ = tx.send(());
        });
        rx
    }

    async fn perform_connectivity_check(addr: &str, timeout_duration: Duration) -> Result<()> {
        let url = url::Url::parse(addr).map_err(|e| Error::other(format!("Invalid URL: {e}")))?;
        let Some(host) = url.host_str() else {
            return Err(Error::other("No host in URL".to_string()));
        };

        let port = url.port_or_known_default().unwrap_or(80);
        match tokio::time::timeout(timeout_duration, TcpStream::connect((host, port))).await {
            Ok(Ok(stream)) => {
                drop(stream);
                Ok(())
            }
            _ => Err(Error::other(format!("Cannot connect to {host}:{port}"))),
        }
    }

    async fn finalize_result<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(err) = &result
            && Self::is_network_like_error(err)
        {
            self.mark_offline_and_spawn_recovery();
            self.evict_connection().await;
        }

        result
    }
}

impl PeerRestClient {
    pub async fn local_storage_info(&self) -> Result<rustfs_madmin::StorageInfo> {
        self.finalize_result(self.local_storage_info_inner().await).await
    }

    async fn local_storage_info_inner(&self) -> Result<rustfs_madmin::StorageInfo> {
        let mut client = self.get_client().await?;
        let request = Request::new(LocalStorageInfoRequest { metrics: true });

        let response = client.local_storage_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.storage_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let storage_info: rustfs_madmin::StorageInfo = Deserialize::deserialize(&mut buf)?;

        Ok(storage_info)
    }

    pub async fn server_info(&self) -> Result<ServerProperties> {
        self.finalize_result(self.server_info_inner().await).await
    }

    async fn server_info_inner(&self) -> Result<ServerProperties> {
        let mut client = self.get_client().await?;
        let request = Request::new(ServerInfoRequest { metrics: true });

        let response = client.server_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.server_properties;

        let mut buf = Deserializer::new(Cursor::new(data));
        let storage_properties: ServerProperties = Deserialize::deserialize(&mut buf)?;

        Ok(storage_properties)
    }

    pub async fn get_cpus(&self) -> Result<Cpus> {
        self.finalize_result(self.get_cpus_inner().await).await
    }

    async fn get_cpus_inner(&self) -> Result<Cpus> {
        let mut client = self.get_client().await?;
        let request = Request::new(GetCpusRequest {});

        let response = client.get_cpus(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.cpus;

        let mut buf = Deserializer::new(Cursor::new(data));
        let cpus: Cpus = Deserialize::deserialize(&mut buf)?;

        Ok(cpus)
    }

    pub async fn get_net_info(&self) -> Result<NetInfo> {
        self.finalize_result(self.get_net_info_inner().await).await
    }

    async fn get_net_info_inner(&self) -> Result<NetInfo> {
        let mut client = self.get_client().await?;
        let request = Request::new(GetNetInfoRequest {});

        let response = client.get_net_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.net_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let net_info: NetInfo = Deserialize::deserialize(&mut buf)?;

        Ok(net_info)
    }

    pub async fn get_partitions(&self) -> Result<Partitions> {
        self.finalize_result(self.get_partitions_inner().await).await
    }

    async fn get_partitions_inner(&self) -> Result<Partitions> {
        let mut client = self.get_client().await?;
        let request = Request::new(GetPartitionsRequest {});

        let response = client.get_partitions(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.partitions;

        let mut buf = Deserializer::new(Cursor::new(data));
        let partitions: Partitions = Deserialize::deserialize(&mut buf)?;

        Ok(partitions)
    }

    pub async fn get_os_info(&self) -> Result<OsInfo> {
        self.finalize_result(self.get_os_info_inner().await).await
    }

    async fn get_os_info_inner(&self) -> Result<OsInfo> {
        let mut client = self.get_client().await?;
        let request = Request::new(GetOsInfoRequest {});

        let response = client.get_os_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        let data = response.os_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let os_info: OsInfo = Deserialize::deserialize(&mut buf)?;

        Ok(os_info)
    }

    pub async fn get_se_linux_info(&self) -> Result<SysServices> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetSeLinuxInfoRequest {});

                let response = client.get_se_linux_info(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.sys_services;

                let mut buf = Deserializer::new(Cursor::new(data));
                let sys_services: SysServices = Deserialize::deserialize(&mut buf)?;

                Ok(sys_services)
            }
            .await,
        )
        .await
    }

    pub async fn get_sys_config(&self) -> Result<SysConfig> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetSysConfigRequest {});

                let response = client.get_sys_config(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.sys_config;

                let mut buf = Deserializer::new(Cursor::new(data));
                let sys_config: SysConfig = Deserialize::deserialize(&mut buf)?;

                Ok(sys_config)
            }
            .await,
        )
        .await
    }

    pub async fn get_sys_errors(&self) -> Result<SysErrors> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetSysErrorsRequest {});

                let response = client.get_sys_errors(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.sys_errors;

                let mut buf = Deserializer::new(Cursor::new(data));
                let sys_errors: SysErrors = Deserialize::deserialize(&mut buf)?;

                Ok(sys_errors)
            }
            .await,
        )
        .await
    }

    pub async fn get_mem_info(&self) -> Result<MemInfo> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetMemInfoRequest {});

                let response = client.get_mem_info(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.mem_info;

                let mut buf = Deserializer::new(Cursor::new(data));
                let mem_info: MemInfo = Deserialize::deserialize(&mut buf)?;

                Ok(mem_info)
            }
            .await,
        )
        .await
    }

    pub async fn get_metrics(&self, t: MetricType, opts: &CollectMetricsOpts) -> Result<RealtimeMetrics> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let mut buf_t = Vec::new();
                t.serialize(&mut Serializer::new(&mut buf_t))?;
                let mut buf_o = Vec::new();
                opts.serialize(&mut Serializer::new(&mut buf_o))?;
                let request = Request::new(GetMetricsRequest {
                    metric_type: buf_t.into(),
                    opts: buf_o.into(),
                });

                let response = client.get_metrics(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.realtime_metrics;

                let mut buf = Deserializer::new(Cursor::new(data));
                let realtime_metrics: RealtimeMetrics = Deserialize::deserialize(&mut buf)?;

                Ok(realtime_metrics)
            }
            .await,
        )
        .await
    }

    pub async fn get_live_events(&self, after_sequence: u64, limit: u32) -> Result<PeerLiveEventsBatch> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetLiveEventsRequest { after_sequence, limit });

                let response = client.get_live_events(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(PeerLiveEventsBatch {
                    events: response.events.to_vec(),
                    next_sequence: response.next_sequence,
                    truncated: response.truncated,
                })
            }
            .await,
        )
        .await
    }

    pub async fn get_proc_info(&self) -> Result<ProcInfo> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(GetProcInfoRequest {});

                let response = client.get_proc_info(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                let data = response.proc_info;

                let mut buf = Deserializer::new(Cursor::new(data));
                let proc_info: ProcInfo = Deserialize::deserialize(&mut buf)?;

                Ok(proc_info)
            }
            .await,
        )
        .await
    }

    pub async fn start_profiling(&self, profiler: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(StartProfilingRequest {
                    profiler: profiler.to_string(),
                });

                let response = client.start_profiling(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn download_profile_data(&self) -> Result<()> {
        warn!("download_profile_data is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn get_bucket_stats(&self) -> Result<()> {
        warn!("get_bucket_stats is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn get_sr_metrics(&self) -> Result<()> {
        warn!("get_sr_metrics is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn get_all_bucket_stats(&self) -> Result<()> {
        warn!("get_all_bucket_stats is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn background_heal_status(&self) -> Result<Option<Vec<u8>>> {
        self.finalize_result(
            async {
                let mut client = self
                    .get_client()
                    .await?
                    .max_decoding_message_size(BACKGROUND_HEAL_STATUS_MAX_MESSAGE_SIZE);
                let response = match client
                    .background_heal_status(Request::new(BackgroundHealStatusRequest::default()))
                    .await
                {
                    Ok(response) => response.into_inner(),
                    Err(status) if status.code() == tonic::Code::Unimplemented => {
                        // RUSTFS_COMPAT_TODO(heal-status-rpc-v1): accept old peers without node heal snapshots. Remove after the minimum supported RustFS peer version implements BackgroundHealStatus.
                        return Ok(None);
                    }
                    Err(status) => return Err(status.into()),
                };
                if !response.success {
                    return Err(Error::other(
                        response
                            .error_info
                            .unwrap_or_else(|| "peer background heal status failed without an error".to_string()),
                    ));
                }
                Ok(Some(response.bg_heal_state.to_vec()))
            }
            .await,
        )
        .await
    }

    pub async fn prepare_tier_mutation(&self, mutation_id: Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationOutcome> {
        self.tier_mutation_control(TierMutationRpcPhase::Prepare, mutation_id, canonical_payload)
            .await
    }

    pub async fn commit_tier_mutation(&self, mutation_id: Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationOutcome> {
        self.tier_mutation_control(TierMutationRpcPhase::Commit, mutation_id, canonical_payload)
            .await
    }

    pub async fn abort_tier_mutation(&self, mutation_id: Uuid) -> Result<PeerTierMutationOutcome> {
        self.tier_mutation_control(TierMutationRpcPhase::Abort, mutation_id, Bytes::new())
            .await
    }

    async fn tier_mutation_control(
        &self,
        phase: TierMutationRpcPhase,
        mutation_id: Uuid,
        canonical_payload: Bytes,
    ) -> Result<PeerTierMutationOutcome> {
        validate_tier_mutation_payload_len(phase, canonical_payload.len())?;
        let version = rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION;
        self.finalize_result(
            async {
                let mut client = self
                    .get_tier_mutation_control_client()
                    .await?
                    .max_encoding_message_size(rustfs_protos::TIER_MUTATION_RPC_MAX_MESSAGE_SIZE)
                    .max_decoding_message_size(rustfs_protos::TIER_MUTATION_RPC_MAX_MESSAGE_SIZE);
                let canonical_body =
                    rustfs_protos::canonical_tier_mutation_rpc_body(version, phase, mutation_id, canonical_payload.as_ref())
                        .map_err(|_| Error::other("tier mutation request length cannot be represented"))?;
                let mutation_id_text = mutation_id.to_string();
                let response = match phase {
                    TierMutationRpcPhase::Prepare => {
                        let mut request = Request::new(TierMutationPrepareRequest {
                            version,
                            mutation_id: mutation_id_text.clone(),
                            canonical_payload: canonical_payload.clone(),
                        });
                        set_tonic_canonical_body_digest(&mut request, &canonical_body)?;
                        client.prepare_tier_mutation(request).await?.into_inner()
                    }
                    TierMutationRpcPhase::Commit => {
                        let mut request = Request::new(TierMutationCommitRequest {
                            version,
                            mutation_id: mutation_id_text.clone(),
                            canonical_payload: canonical_payload.clone(),
                        });
                        set_tonic_canonical_body_digest(&mut request, &canonical_body)?;
                        client.commit_tier_mutation(request).await?.into_inner()
                    }
                    TierMutationRpcPhase::Abort => {
                        let mut request = Request::new(TierMutationAbortRequest {
                            version,
                            mutation_id: mutation_id_text,
                            canonical_payload: canonical_payload.clone(),
                        });
                        set_tonic_canonical_body_digest(&mut request, &canonical_body)?;
                        client.abort_tier_mutation(request).await?.into_inner()
                    }
                    _ => return Err(Error::other("tier mutation rpc phase is unsupported")),
                };
                validate_tier_mutation_response_proof(version, phase, mutation_id, &canonical_payload, &response)?;
                if !response.success {
                    return Err(Error::other(
                        response
                            .error_info
                            .unwrap_or_else(|| "peer tier mutation failed without an error".to_string()),
                    ));
                }
                let state = decode_tier_mutation_peer_state(response.state)?;
                Ok(PeerTierMutationOutcome {
                    state,
                    applied: response.applied,
                })
            }
            .await,
        )
        .await
    }

    pub async fn heal_control(&self, version: u32, topology_fingerprint: String, command: Vec<u8>) -> Result<Vec<u8>> {
        if topology_fingerprint.len() > HEAL_CONTROL_FINGERPRINT_MAX_SIZE {
            return Err(Error::other("heal control topology fingerprint exceeds size limit"));
        }
        if command.len() > HEAL_CONTROL_PAYLOAD_MAX_SIZE {
            return Err(Error::other("heal control command exceeds size limit"));
        }
        let capability_probe = rustfs_protos::is_heal_control_capability_probe(&command);
        self.finalize_result(
            async {
                let mut client = self
                    .get_heal_control_client()
                    .await?
                    .max_encoding_message_size(rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE)
                    .max_decoding_message_size(rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE);
                let canonical_body = rustfs_protos::canonical_heal_control_request_body(version, &topology_fingerprint, &command)
                    .map_err(|_| Error::other("heal control request length cannot be represented"))?;
                let mut request = Request::new(HealControlRequest {
                    version,
                    topology_fingerprint: topology_fingerprint.clone(),
                    command: command.clone().into(),
                });
                request.set_timeout(rustfs_protos::heal_control_execution_timeout());
                set_tonic_canonical_body_digest(&mut request, &canonical_body)?;
                let response = client.heal_control(request).await?.into_inner();
                if !response.success {
                    return Err(Error::other(
                        response
                            .error_info
                            .unwrap_or_else(|| "peer heal control failed without an error".to_string()),
                    ));
                }
                if !capability_probe {
                    let canonical_response = rustfs_protos::canonical_heal_control_response_body(
                        version,
                        &topology_fingerprint,
                        &command,
                        &response.result,
                    )
                    .map_err(|_| Error::other("heal control response length cannot be represented"))?;
                    validate_heal_control_response_proof(&canonical_response, &response.response_proof)?;
                }
                Ok(response.result.to_vec())
            }
            .await,
        )
        .await
    }

    /// Confirms that a peer supports the current heal-control coordination
    /// contract and has the same storage
    /// topology. Every non-success response is an error so old or divergent
    /// peers cannot be mistaken for compatible ones.
    pub async fn probe_heal_control(&self, topology_fingerprint: String) -> Result<()> {
        let nonce = uuid::Uuid::new_v4();
        let probe = rustfs_protos::heal_control_capability_probe(nonce.as_bytes());
        let canonical_ack = rustfs_protos::canonical_heal_control_capability_ack(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &topology_fingerprint,
            &probe,
        )
        .map_err(|_| Error::other("heal control capability acknowledgement length cannot be represented"))?;
        let proof = self
            .heal_control(rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION, topology_fingerprint, probe)
            .await?;
        validate_heal_control_capability_proof(&canonical_ack, &proof)
    }

    pub async fn load_bucket_metadata(&self, bucket: &str, scanner_maintenance_change: bool) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadBucketMetadataRequest {
                    bucket: bucket.to_string(),
                    scanner_maintenance_change,
                });

                let response = client.load_bucket_metadata(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn delete_bucket_metadata(&self, bucket: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(DeleteBucketMetadataRequest {
                    bucket: bucket.to_string(),
                });

                let response = client.delete_bucket_metadata(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn delete_policy(&self, policy: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(DeletePolicyRequest {
                    policy_name: policy.to_string(),
                });

                let response = client.delete_policy(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_policy(&self, policy: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadPolicyRequest {
                    policy_name: policy.to_string(),
                });

                let response = client.load_policy(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_policy_mapping(&self, user_or_group: &str, user_type: u64, is_group: bool) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadPolicyMappingRequest {
                    user_or_group: user_or_group.to_string(),
                    user_type,
                    is_group,
                });

                let response = client.load_policy_mapping(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(DeleteUserRequest {
                    access_key: access_key.to_string(),
                });

                let response = client.delete_user(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(DeleteServiceAccountRequest {
                    access_key: access_key.to_string(),
                });

                let response = client.delete_service_account(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_user(&self, access_key: &str, temp: bool) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadUserRequest {
                    access_key: access_key.to_string(),
                    temp,
                });

                let response = client.load_user(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_service_account(&self, access_key: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadServiceAccountRequest {
                    access_key: access_key.to_string(),
                });

                let response = client.load_service_account(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_group(&self, group: &str) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadGroupRequest {
                    group: group.to_string(),
                });

                let response = client.load_group(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn reload_site_replication_config(&self) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(ReloadSiteReplicationConfigRequest {});

                let response = client.reload_site_replication_config(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn signal_service(&self, sig: u64, sub_sys: &str, dry_run: bool, _exec_at: SystemTime) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let mut vars = HashMap::new();
                vars.insert(PEER_RESTSIGNAL.to_string(), sig.to_string());
                vars.insert(PEER_RESTSUB_SYS.to_string(), sub_sys.to_string());
                vars.insert(PEER_RESTDRY_RUN.to_string(), dry_run.to_string());
                let request = Request::new(SignalServiceRequest {
                    vars: Some(Mss { value: vars }),
                });

                let response = client.signal_service(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }
                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn scanner_activity(&self) -> Result<ScannerPeerActivity> {
        self.finalize_result(
            async {
                let mut client = self
                    .get_client()
                    .await?
                    .max_decoding_message_size(SCANNER_ACTIVITY_MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(SCANNER_ACTIVITY_MAX_MESSAGE_SIZE);
                let response = client
                    .scanner_activity(Request::new(ScannerActivityRequest {}))
                    .await?
                    .into_inner();
                decode_scanner_activity(response)
            }
            .await,
        )
        .await
    }

    pub async fn get_metacache_listing(&self) -> Result<()> {
        warn!("get_metacache_listing is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn update_metacache_listing(&self) -> Result<()> {
        warn!("update_metacache_listing is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(ReloadPoolMetaRequest {});

                let response = client.reload_pool_meta(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn stop_rebalance(&self, expected_rebalance_id: Option<&str>) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(StopRebalanceRequest {
                    expected_rebalance_id: expected_rebalance_id.unwrap_or_default().to_string(),
                });

                let response = client.stop_rebalance(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_rebalance_meta(&self, start_rebalance: bool) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadRebalanceMetaRequest { start_rebalance });

                let response = client.load_rebalance_meta(request).await?.into_inner();

                debug!(
                    event = "peer_rebalance_meta",
                    component = "ecstore",
                    subsystem = "peer_rest_client",
                    action = "load_rebalance_meta",
                    result = "response_received",
                    peer = %self.grid_host,
                    success = response.success,
                    start_rebalance = start_rebalance,
                    "peer rebalance metadata response"
                );
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn start_decommission(&self, pool_indices: Vec<usize>) -> Result<()> {
        self.finalize_result(
            async {
                let pool_indices = pool_indices
                    .into_iter()
                    .map(|idx| {
                        u32::try_from(idx).map_err(|_| Error::other(format!("decommission pool index {idx} exceeds RPC range")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut client = self.get_client().await?;
                let request = Request::new(StartDecommissionRequest { pool_indices });

                let response = client.start_decommission(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn decommission_cancel(&self, pool_index: usize) -> Result<()> {
        self.finalize_result(
            async {
                let pool_index = u32::try_from(pool_index)
                    .map_err(|_| Error::other(format!("decommission pool index {pool_index} exceeds RPC range")))?;
                let mut client = self.get_client().await?;
                let request = Request::new(CancelDecommissionRequest { pool_index });

                let response = client.cancel_decommission(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn clear_decommission(&self, pool_index: usize) -> Result<()> {
        self.finalize_result(
            async {
                let pool_index = u32::try_from(pool_index)
                    .map_err(|_| Error::other(format!("decommission pool index {pool_index} exceeds RPC range")))?;
                let mut client = self.get_client().await?;
                let request = Request::new(ClearDecommissionRequest { pool_index });

                let response = client.clear_decommission(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }

    pub async fn load_transition_tier_config(&self) -> Result<()> {
        self.finalize_result(
            async {
                let mut client = self.get_client().await?;
                let request = Request::new(LoadTransitionTierConfigRequest {});

                let response = client.load_transition_tier_config(request).await?.into_inner();
                if !response.success {
                    if let Some(msg) = response.error_info {
                        return Err(Error::other(msg));
                    }
                    return Err(Error::other(""));
                }

                Ok(())
            }
            .await,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

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
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
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

    fn test_peer_client() -> PeerRestClient {
        PeerRestClient::new(
            XHost {
                name: "127.0.0.1".to_string(),
                port: 9000,
                is_port_set: true,
            },
            "http://127.0.0.1:9000".to_string(),
        )
    }

    #[test]
    fn scanner_activity_requires_restart_safe_peer_identity() {
        let missing_instance = ScannerActivityResponse {
            instance_id: String::new(),
            namespace_generation: 7,
            maintenance_generation: 3,
        };
        assert!(
            decode_scanner_activity(missing_instance)
                .expect_err("an empty instance ID is not restart safe")
                .to_string()
                .contains("instance ID")
        );

        let malformed_instance = ScannerActivityResponse {
            instance_id: "ABCDEF0123456789ABCDEF0123456789".to_string(),
            namespace_generation: 7,
            maintenance_generation: 3,
        };
        assert!(
            decode_scanner_activity(malformed_instance)
                .expect_err("activity instance IDs must use the canonical lowercase hex form")
                .to_string()
                .contains("instance ID")
        );

        let activity = decode_scanner_activity(ScannerActivityResponse {
            instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            namespace_generation: 7,
            maintenance_generation: 3,
        })
        .expect("complete activity responses should be accepted");
        assert_eq!(
            activity,
            ScannerPeerActivity {
                instance_id: "0123456789abcdef0123456789abcdef".to_string(),
                namespace_generation: 7,
                maintenance_generation: 3,
            }
        );
    }

    #[test]
    fn peer_rest_client_marks_network_like_errors() {
        assert!(PeerRestClient::is_network_like_error(&Error::other("transport error")));
        assert!(PeerRestClient::is_network_like_error(&Error::other("connection refused")));
        assert!(!PeerRestClient::is_network_like_error(&Error::NotImplemented));
    }

    #[tokio::test]
    async fn peer_rest_client_fast_fails_when_marked_offline() {
        let client = test_peer_client();
        client.offline.store(true, Ordering::Release);

        let err = client
            .get_client()
            .await
            .expect_err("offline peer should fast-fail before dialing");

        assert!(err.to_string().contains("temporarily offline"));
    }

    #[tokio::test]
    async fn peer_rest_client_rejects_oversized_heal_control_before_dialing() {
        let client = test_peer_client();
        let err = client
            .heal_control(1, "fingerprint".to_string(), vec![0; HEAL_CONTROL_PAYLOAD_MAX_SIZE + 1])
            .await
            .expect_err("oversized heal control payload must fail locally");

        assert!(err.to_string().contains("exceeds size limit"));
        assert!(!client.offline.load(Ordering::Acquire));
    }

    #[test]
    fn heal_control_capability_proof_must_authenticate_exact_ack() {
        runtime_sources::ensure_test_rpc_secret();
        let proof = crate::cluster::rpc::sign_tonic_rpc_response_proof(b"expected").expect("test proof should sign");
        assert!(validate_heal_control_capability_proof(b"expected", &proof).is_ok());
        let err = validate_heal_control_capability_proof(b"different", &proof)
            .expect_err("a proof for a different acknowledgement must fail closed");
        assert!(err.to_string().contains("invalid heal control capability proof"));
    }

    #[test]
    fn heal_control_response_proof_binds_command_and_result() {
        runtime_sources::ensure_test_rpc_secret();
        let canonical = rustfs_protos::canonical_heal_control_response_body(2, "fingerprint", b"query", b"result")
            .expect("small response should encode");
        let proof = crate::cluster::rpc::sign_tonic_rpc_response_proof(&canonical).expect("test proof should sign");
        assert!(validate_heal_control_response_proof(&canonical, &proof).is_ok());

        for tampered in [
            rustfs_protos::canonical_heal_control_response_body(2, "fingerprint", b"cancel", b"result").unwrap(),
            rustfs_protos::canonical_heal_control_response_body(2, "fingerprint", b"query", b"tampered").unwrap(),
        ] {
            let err = validate_heal_control_response_proof(&tampered, &proof)
                .expect_err("proof must not authenticate a different command or result");
            assert!(err.to_string().contains("invalid heal control response proof"));
        }
    }

    struct TierMutationResponseFixture<'a> {
        version: u32,
        phase: TierMutationRpcPhase,
        mutation_id: Uuid,
        canonical_payload: &'a [u8],
        success: bool,
        state: i32,
        applied: bool,
        error_info: Option<&'a str>,
    }

    fn signed_tier_mutation_response(input: TierMutationResponseFixture<'_>) -> TierMutationControlResponse {
        let canonical =
            rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
                version: input.version,
                phase: input.phase,
                mutation_id: input.mutation_id,
                canonical_payload: input.canonical_payload,
                success: input.success,
                state: input.state,
                applied: input.applied,
                error_info: input.error_info,
            })
            .expect("small tier mutation response should encode");
        let response_proof =
            crate::cluster::rpc::sign_tonic_rpc_response_proof(&canonical).expect("tier mutation response should sign");
        TierMutationControlResponse {
            success: input.success,
            state: input.state,
            applied: input.applied,
            error_info: input.error_info.map(str::to_string),
            response_proof: response_proof.into(),
        }
    }

    #[test]
    fn tier_mutation_response_proof_binds_phase_payload_state_and_error() {
        runtime_sources::ensure_test_rpc_secret();
        let mutation_id = Uuid::new_v4();
        let payload = b"tier-mutation-prepare";
        let response = signed_tier_mutation_response(TierMutationResponseFixture {
            version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            phase: TierMutationRpcPhase::Prepare,
            mutation_id,
            canonical_payload: payload,
            success: true,
            state: TierMutationPeerState::Prepared as i32,
            applied: true,
            error_info: None,
        });
        validate_tier_mutation_response_proof(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            payload,
            &response,
        )
        .expect("matching tier mutation response proof should verify");

        for tampered in [
            TierMutationControlResponse {
                success: false,
                error_info: Some("peer failed".to_string()),
                ..response.clone()
            },
            TierMutationControlResponse {
                state: TierMutationPeerState::Committed as i32,
                ..response.clone()
            },
            TierMutationControlResponse {
                applied: false,
                ..response.clone()
            },
        ] {
            let err = validate_tier_mutation_response_proof(
                rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                TierMutationRpcPhase::Prepare,
                mutation_id,
                payload,
                &tampered,
            )
            .expect_err("tampered tier mutation response proof must fail");
            assert!(err.to_string().contains("invalid tier mutation response proof"));
        }

        let err = validate_tier_mutation_response_proof(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            payload,
            &response,
        )
        .expect_err("response proof must bind request phase");
        assert!(err.to_string().contains("invalid tier mutation response proof"));
    }

    #[test]
    fn tier_mutation_peer_state_decode_fails_closed() {
        assert_eq!(
            decode_tier_mutation_peer_state(TierMutationPeerState::Prepared as i32).expect("prepared state should decode"),
            PeerTierMutationState::Prepared
        );
        assert_eq!(
            decode_tier_mutation_peer_state(TierMutationPeerState::Committed as i32).expect("committed state should decode"),
            PeerTierMutationState::Committed
        );
        assert_eq!(
            decode_tier_mutation_peer_state(TierMutationPeerState::Aborted as i32).expect("aborted state should decode"),
            PeerTierMutationState::Aborted
        );
        assert!(decode_tier_mutation_peer_state(TierMutationPeerState::Unspecified as i32).is_err());
        assert!(decode_tier_mutation_peer_state(99).is_err());
    }

    #[test]
    fn tier_mutation_payload_guard_rejects_invalid_lengths() {
        validate_tier_mutation_payload_len(
            TierMutationRpcPhase::Prepare,
            rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE,
        )
        .expect("max prepare payload should fit");
        assert!(
            validate_tier_mutation_payload_len(
                TierMutationRpcPhase::Prepare,
                rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE + 1,
            )
            .is_err()
        );
        validate_tier_mutation_payload_len(
            TierMutationRpcPhase::Commit,
            rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE,
        )
        .expect("max commit payload should fit");
        assert!(
            validate_tier_mutation_payload_len(
                TierMutationRpcPhase::Commit,
                rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE + 1,
            )
            .is_err()
        );
        validate_tier_mutation_payload_len(TierMutationRpcPhase::Abort, 0).expect("empty abort payload should fit");
        assert!(validate_tier_mutation_payload_len(TierMutationRpcPhase::Abort, 1).is_err());
    }

    #[tokio::test]
    async fn peer_rest_client_rejects_oversized_tier_prepare_before_dialing() {
        let client = test_peer_client();
        let err = client
            .prepare_tier_mutation(
                Uuid::new_v4(),
                Bytes::from(vec![0; rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE + 1]),
            )
            .await
            .expect_err("oversized tier prepare should fail before dialing");
        assert!(err.to_string().contains("tier mutation payload exceeds size limit"));
        assert!(!client.offline.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn peer_rest_client_prepare_retry_clears_offline_gate() {
        // finalize_result sets the offline gate on a network error; without
        // clearing it, an in-call retry would fast-fail on the gate instead of
        // re-dialing (rustfs/backlog#1049 P1-B). prepare_retry must clear it.
        let client = test_peer_client();
        client.offline.store(true, Ordering::Release);

        client.prepare_retry().await;

        assert!(
            !client.offline.load(Ordering::Acquire),
            "prepare_retry must clear the offline gate so the next get_client re-dials"
        );
    }

    #[tokio::test]
    async fn peer_rest_client_finalize_result_marks_offline_for_network_errors() {
        let client = test_peer_client();
        let err = client
            .finalize_result::<()>(Err(Error::other("transport error")))
            .await
            .expect_err("network error should still be returned");

        assert!(err.to_string().contains("transport error"));
        assert!(client.offline.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn peer_rest_client_finalize_result_keeps_online_for_business_errors() {
        let client = test_peer_client();
        let err = client
            .finalize_result::<()>(Err(Error::VolumeNotFound))
            .await
            .expect_err("business error should still be returned");

        assert!(matches!(err, Error::VolumeNotFound));
        assert!(!client.offline.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn peer_rest_recovery_probe_logs_keep_request_id_span_context() {
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

        let client = test_peer_client();
        let span = tracing::info_span!("request-span", request_id = "req-peer-rest");
        let _entered = span.enter();
        let done = client.spawn_recovery_monitor_log_probe_for_test();
        done.await.expect("recovery monitor probe should signal completion");

        let log = logs
            .lines()
            .into_iter()
            .find(|value| value.get("message").and_then(Value::as_str) == Some("peer recovery monitor log probe"))
            .expect("expected peer recovery monitor probe log");

        assert_eq!(log["span"]["name"], Value::String("recovery-monitor".to_string()));
        assert_eq!(log["span"]["kind"], Value::String("peer_rest".to_string()));
        let spans = log["spans"].as_array().expect("spans should be present");
        assert!(spans.iter().any(|span| {
            span.get("name").and_then(Value::as_str) == Some("request-span")
                && span.get("request_id").and_then(Value::as_str) == Some("req-peer-rest")
        }));
    }
}
