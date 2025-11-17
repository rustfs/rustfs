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

use crate::StorageAPI;
use crate::admin_server_info::get_commit_id;
use crate::error::{Error, Result};
use crate::global::{GLOBAL_BOOT_TIME, get_global_endpoints};
use crate::metrics_realtime::{CollectMetricsOpts, MetricType};
use crate::rpc::PeerRestClient;
use crate::{endpoints::EndpointServerPools, new_object_layer_fn};
use futures::future::join_all;
use lazy_static::lazy_static;
use rustfs_madmin::health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysService};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::net::NetInfo;
use rustfs_madmin::{ItemState, ServerProperties};
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::time::timeout;
use tracing::{error, warn};

lazy_static! {
    pub static ref GLOBAL_NotificationSys: OnceLock<NotificationSys> = OnceLock::new();
}

pub async fn new_global_notification_sys(eps: EndpointServerPools) -> Result<()> {
    let _ = GLOBAL_NotificationSys
        .set(NotificationSys::new(eps).await)
        .map_err(|_| Error::other("init notification_sys fail"));
    Ok(())
}

pub fn get_global_notification_sys() -> Option<&'static NotificationSys> {
    GLOBAL_NotificationSys.get()
}

pub struct NotificationSys {
    pub peer_clients: Vec<Option<PeerRestClient>>,
    #[allow(dead_code)]
    pub all_peer_clients: Vec<Option<PeerRestClient>>,
}

impl NotificationSys {
    pub async fn new(eps: EndpointServerPools) -> Self {
        let (peer_clients, all_peer_clients) = PeerRestClient::new_clients(eps).await;
        Self {
            peer_clients,
            all_peer_clients,
        }
    }
}

pub struct NotificationPeerErr {
    pub host: String,
    pub err: Option<Error>,
}

impl NotificationSys {
    pub fn rest_client_from_hash(&self, s: &str) -> Option<PeerRestClient> {
        if self.all_peer_clients.is_empty() {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.all_peer_clients.len();
        self.all_peer_clients[idx].clone()
    }

    pub async fn delete_policy(&self, policy_name: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let policy = policy_name.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_policy(&policy).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_policy(&self, policy_name: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let policy = policy_name.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_policy(&policy).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_policy_mapping(&self, user_or_group: &str, user_type: u64, is_group: bool) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let uog = user_or_group.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_policy_mapping(&uog, user_type, is_group).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn delete_user(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_user(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn storage_info<S: StorageAPI>(&self, api: &S) -> rustfs_madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.peer_clients.len());

        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.local_storage_info().await {
                        Ok(info) => Some(info),
                        Err(_) => Some(rustfs_madmin::StorageInfo {
                            disks: get_offline_disks(&client.host.to_string(), &get_global_endpoints()),
                            ..Default::default()
                        }),
                    }
                } else {
                    None
                }
            });
        }

        let mut replies = join_all(futures).await;

        replies.push(Some(api.local_storage_info().await));

        let mut disks = Vec::new();
        for info in replies.into_iter().flatten() {
            disks.extend(info.disks);
        }

        let backend = api.backend_info().await;
        rustfs_madmin::StorageInfo { disks, backend }
    }

    pub async fn server_info(&self) -> Vec<ServerProperties> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        let endpoints = get_global_endpoints();
        let peer_timeout = Duration::from_secs(2);

        for client in self.peer_clients.iter() {
            let endpoints = endpoints.clone();
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    call_peer_with_timeout(
                        peer_timeout,
                        &host,
                        || client.server_info(),
                        || offline_server_properties(&host, &endpoints),
                    )
                    .await
                } else {
                    ServerProperties::default()
                }
            });
        }

        join_all(futures).await
    }

    pub async fn load_user(&self, access_key: &str, temp: bool) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_user(&ak, temp).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_group(&self, group: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let gname = group.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_group(&gname).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_service_account(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_service_account(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_service_account(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn reload_pool_meta(&self) {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().flatten() {
            futures.push(client.reload_pool_meta());
        }

        let results = join_all(futures).await;
        for result in results {
            if let Err(err) = result {
                error!("notification reload_pool_meta err {:?}", err);
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn load_rebalance_meta(&self, start: bool) {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (i, client) in self.peer_clients.iter().flatten().enumerate() {
            warn!(
                "notification load_rebalance_meta start: {}, index: {}, client: {:?}",
                start, i, client.host
            );
            futures.push(client.load_rebalance_meta(start));
        }

        let results = join_all(futures).await;
        for result in results {
            if let Err(err) = result {
                error!("notification load_rebalance_meta err {:?}", err);
            } else {
                warn!("notification load_rebalance_meta success");
            }
        }
    }

    pub async fn stop_rebalance(&self) {
        warn!("notification stop_rebalance start");
        let Some(store) = new_object_layer_fn() else {
            error!("stop_rebalance: not init");
            return;
        };

        // warn!("notification stop_rebalance load_rebalance_meta");
        // self.load_rebalance_meta(false).await;
        // warn!("notification stop_rebalance load_rebalance_meta done");

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().flatten() {
            futures.push(client.stop_rebalance());
        }

        let results = join_all(futures).await;
        for result in results {
            if let Err(err) = result {
                error!("notification stop_rebalance err {:?}", err);
            }
        }

        warn!("notification stop_rebalance stop_rebalance start");
        let _ = store.stop_rebalance().await;
        warn!("notification stop_rebalance stop_rebalance done");
    }

    pub async fn load_bucket_metadata(&self, bucket: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let b = bucket.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_bucket_metadata(&b).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn delete_bucket_metadata(&self, bucket: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let b = bucket.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_bucket_metadata(&b).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn start_profiling(&self, profiler: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let pf = profiler.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.start_profiling(&pf).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_cpus(&self) -> Vec<Cpus> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_cpus().await.unwrap_or_default()
                } else {
                    Cpus::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_net_info(&self) -> Vec<NetInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_net_info().await.unwrap_or_default()
                } else {
                    NetInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_partitions(&self) -> Vec<Partitions> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_partitions().await.unwrap_or_default()
                } else {
                    Partitions::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_os_info(&self) -> Vec<OsInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_os_info().await.unwrap_or_default()
                } else {
                    OsInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_services(&self) -> Vec<SysService> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_se_linux_info().await.unwrap_or_default()
                } else {
                    SysService::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_config(&self) -> Vec<SysConfig> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_sys_config().await.unwrap_or_default()
                } else {
                    SysConfig::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_errors(&self) -> Vec<SysErrors> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_sys_errors().await.unwrap_or_default()
                } else {
                    SysErrors::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_mem_info(&self) -> Vec<MemInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_mem_info().await.unwrap_or_default()
                } else {
                    MemInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_proc_info(&self) -> Vec<ProcInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_proc_info().await.unwrap_or_default()
                } else {
                    ProcInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_metrics(&self, t: MetricType, opts: &CollectMetricsOpts) -> Vec<RealtimeMetrics> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            let t_clone = t;
            let opts_clone = opts;
            futures.push(async move {
                if let Some(client) = client {
                    client.get_metrics(t_clone, opts_clone).await.unwrap_or_default()
                } else {
                    RealtimeMetrics::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn reload_site_replication_config(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.reload_site_replication_config().await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_transition_tier_config(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_transition_tier_config().await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }
}

async fn call_peer_with_timeout<F, Fut>(
    timeout_dur: Duration,
    host_label: &str,
    op: F,
    fallback: impl FnOnce() -> ServerProperties,
) -> ServerProperties
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<ServerProperties>> + Send,
{
    match timeout(timeout_dur, op()).await {
        Ok(Ok(info)) => info,
        Ok(Err(err)) => {
            warn!("peer {host_label} server_info failed: {err}");
            fallback()
        }
        Err(_) => {
            warn!("peer {host_label} server_info timed out after {:?}", timeout_dur);
            fallback()
        }
    }
}

fn offline_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        uptime: SystemTime::now()
            .duration_since(*GLOBAL_BOOT_TIME.get().unwrap())
            .unwrap_or_default()
            .as_secs(),
        version: get_commit_id(),
        endpoint: host.to_string(),
        state: ItemState::Offline.to_string().to_owned(),
        disks: get_offline_disks(host, endpoints),
        ..Default::default()
    }
}

fn get_offline_disks(offline_host: &str, endpoints: &EndpointServerPools) -> Vec<rustfs_madmin::Disk> {
    let mut offline_disks = Vec::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if (offline_host.is_empty() && ep.is_local) || offline_host == ep.host_port() {
                offline_disks.push(rustfs_madmin::Disk {
                    endpoint: ep.to_string(),
                    state: ItemState::Offline.to_string().to_owned(),
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
            }
        }
    }

    offline_disks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_props(endpoint: &str) -> ServerProperties {
        ServerProperties {
            endpoint: endpoint.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn call_peer_with_timeout_returns_value_when_fast() {
        let result = call_peer_with_timeout(
            Duration::from_millis(50),
            "peer-1",
            || async { Ok::<_, Error>(build_props("fast")) },
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fast");
    }

    #[tokio::test]
    async fn call_peer_with_timeout_uses_fallback_on_error() {
        let result = call_peer_with_timeout(
            Duration::from_millis(50),
            "peer-2",
            || async { Err::<ServerProperties, _>(Error::other("boom")) },
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fallback");
    }

    #[tokio::test]
    async fn call_peer_with_timeout_uses_fallback_on_timeout() {
        let result = call_peer_with_timeout(
            Duration::from_millis(5),
            "peer-3",
            || async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok::<_, Error>(build_props("slow"))
            },
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fallback");
    }
}
