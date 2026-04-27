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

use crate::error::{Error, Result};
use crate::rpc::client::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client};
use crate::{
    endpoints::EndpointServerPools,
    global::is_dist_erasure,
    metrics_realtime::{CollectMetricsOpts, MetricType},
};
use rmp_serde::{Deserializer, Serializer};
use rustfs_madmin::{
    ServerProperties,
    health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysService},
    metrics::RealtimeMetrics,
    net::NetInfo,
};
use rustfs_protos::evict_failed_connection;
use rustfs_protos::proto_gen::node_service::{
    DeleteBucketMetadataRequest, DeletePolicyRequest, DeleteServiceAccountRequest, DeleteUserRequest, GetCpusRequest,
    GetLiveEventsRequest, GetMemInfoRequest, GetMetricsRequest, GetNetInfoRequest, GetOsInfoRequest, GetPartitionsRequest,
    GetProcInfoRequest, GetSeLinuxInfoRequest, GetSysConfigRequest, GetSysErrorsRequest, LoadBucketMetadataRequest,
    LoadGroupRequest, LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest, LoadServiceAccountRequest,
    LoadTransitionTierConfigRequest, LoadUserRequest, LocalStorageInfoRequest, Mss, ReloadPoolMetaRequest,
    ReloadSiteReplicationConfigRequest, ServerInfoRequest, SignalServiceRequest, StartProfilingRequest, StopRebalanceRequest,
    node_service_client::NodeServiceClient,
};
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
use tracing::warn;

pub const PEER_RESTSIGNAL: &str = "signal";
pub const PEER_RESTSUB_SYS: &str = "sub-sys";
pub const PEER_RESTDRY_RUN: &str = "dry-run";

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

impl PeerRestClient {
    pub fn new(host: XHost, grid_host: String) -> Self {
        Self {
            host,
            grid_host,
            offline: Arc::new(AtomicBool::new(false)),
            recovery_running: Arc::new(AtomicBool::new(false)),
        }
    }
    pub async fn new_clients(eps: EndpointServerPools) -> (Vec<Option<Self>>, Vec<Option<Self>>) {
        if !is_dist_erasure().await {
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

    /// Evict the connection to this peer from the global cache.
    /// This should be called when communication with this peer fails.
    pub async fn evict_connection(&self) {
        evict_failed_connection(&self.grid_host).await;
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
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));

            loop {
                interval.tick().await;
                if Self::perform_connectivity_check(&grid_host).await.is_ok() {
                    offline.store(false, Ordering::Release);
                    recovery_running.store(false, Ordering::Release);
                    return;
                }
            }
        });
    }

    async fn perform_connectivity_check(addr: &str) -> Result<()> {
        let url = url::Url::parse(addr).map_err(|e| Error::other(format!("Invalid URL: {e}")))?;
        let Some(host) = url.host_str() else {
            return Err(Error::other("No host in URL".to_string()));
        };

        let port = url.port_or_known_default().unwrap_or(80);
        match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect((host, port))).await {
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

    pub async fn get_se_linux_info(&self) -> Result<SysService> {
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
        let sys_services: SysService = Deserialize::deserialize(&mut buf)?;

        Ok(sys_services)
    }

    pub async fn get_sys_config(&self) -> Result<SysConfig> {
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

    pub async fn get_sys_errors(&self) -> Result<SysErrors> {
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

    pub async fn get_mem_info(&self) -> Result<MemInfo> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn get_metrics(&self, t: MetricType, opts: &CollectMetricsOpts) -> Result<RealtimeMetrics> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn get_live_events(&self, after_sequence: u64, limit: u32) -> Result<PeerLiveEventsBatch> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn get_proc_info(&self) -> Result<ProcInfo> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn start_profiling(&self, profiler: &str) -> Result<()> {
        self.finalize_result(async {
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
        .await)
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

    pub async fn load_bucket_metadata(&self, bucket: &str) -> Result<()> {
        self.finalize_result(async {
            let mut client = self.get_client().await?;
            let request = Request::new(LoadBucketMetadataRequest {
                bucket: bucket.to_string(),
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
        .await)
        .await
    }

    pub async fn delete_bucket_metadata(&self, bucket: &str) -> Result<()> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn delete_policy(&self, policy: &str) -> Result<()> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn load_policy(&self, policy: &str) -> Result<()> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn load_policy_mapping(&self, user_or_group: &str, user_type: u64, is_group: bool) -> Result<()> {
        self.finalize_result(async {
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
        .await)
        .await
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(DeleteUserRequest {
            access_key: access_key.to_string(),
        });

        let result = client.delete_user(request).await;
        if result.is_err() {
            self.evict_connection().await;
        }
        let response = result?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        Ok(())
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(DeleteServiceAccountRequest {
            access_key: access_key.to_string(),
        });

        let result = client.delete_service_account(request).await;
        if result.is_err() {
            self.evict_connection().await;
        }
        let response = result?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        Ok(())
    }

    pub async fn load_user(&self, access_key: &str, temp: bool) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(LoadUserRequest {
            access_key: access_key.to_string(),
            temp,
        });

        let result = client.load_user(request).await;
        if result.is_err() {
            self.evict_connection().await;
        }
        let response = result?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        Ok(())
    }

    pub async fn load_service_account(&self, access_key: &str) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(LoadServiceAccountRequest {
            access_key: access_key.to_string(),
        });

        let result = client.load_service_account(request).await;
        if result.is_err() {
            self.evict_connection().await;
        }
        let response = result?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        Ok(())
    }

    pub async fn load_group(&self, group: &str) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(LoadGroupRequest {
            group: group.to_string(),
        });

        let result = client.load_group(request).await;
        if result.is_err() {
            self.evict_connection().await;
        }
        let response = result?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }
        Ok(())
    }

    pub async fn reload_site_replication_config(&self) -> Result<()> {
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

    pub async fn signal_service(&self, sig: u64, sub_sys: &str, dry_run: bool, _exec_at: SystemTime) -> Result<()> {
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

    pub async fn get_metacache_listing(&self) -> Result<()> {
        warn!("get_metacache_listing is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn update_metacache_listing(&self) -> Result<()> {
        warn!("update_metacache_listing is not implemented in PeerRestClient");
        Err(Error::NotImplemented)
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
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

    pub async fn stop_rebalance(&self) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(StopRebalanceRequest {});

        let response = client.stop_rebalance(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }

        Ok(())
    }

    pub async fn load_rebalance_meta(&self, start_rebalance: bool) -> Result<()> {
        let mut client = self.get_client().await?;
        let request = Request::new(LoadRebalanceMetaRequest { start_rebalance });

        let response = client.load_rebalance_meta(request).await?.into_inner();

        warn!("load_rebalance_meta response {:?}, grid_host: {:?}", response, &self.grid_host);
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::other(msg));
            }
            return Err(Error::other(""));
        }

        Ok(())
    }

    pub async fn load_transition_tier_config(&self) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
