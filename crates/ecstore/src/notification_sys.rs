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

use crate::admin_server_info::get_commit_id;
use crate::error::{Error, Result};
use crate::global::{GLOBAL_BOOT_TIME, get_global_endpoints};
use crate::metrics_realtime::{CollectMetricsOpts, MetricType};
use crate::rebalance::RebalSaveOpt;
use crate::rpc::PeerRestClient;
use crate::{endpoints::EndpointServerPools, resolve_object_store_handle};
use futures::future::join_all;
use lazy_static::lazy_static;
use rustfs_madmin::health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysServices};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::net::NetInfo;
use rustfs_madmin::{ItemState, ServerProperties, StorageInfo};
use rustfs_storage_api::StorageAdminApi;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// After this many consecutive admin-call failures, mark the peer as offline.
const CONSECUTIVE_FAILURE_THRESHOLD: u32 = 3;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_NOTIFICATION: &str = "notification";
const EVENT_NOTIFICATION_PEER_PROPAGATION: &str = "notification_peer_propagation";

/// Cached result from the last successful admin call to a peer.
struct PeerAdminCache {
    last_storage_info: Option<StorageInfo>,
    last_server_info: Option<ServerProperties>,
    storage_failures: u32,
    server_failures: u32,
}

impl PeerAdminCache {
    fn new() -> Self {
        Self {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: 0,
        }
    }
}

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
    peer_admin_caches: Vec<Mutex<PeerAdminCache>>,
}

impl NotificationSys {
    pub async fn new(eps: EndpointServerPools) -> Self {
        let (peer_clients, all_peer_clients) = PeerRestClient::new_clients(eps).await;
        let peer_admin_caches = (0..peer_clients.len()).map(|_| Mutex::new(PeerAdminCache::new())).collect();
        Self {
            peer_clients,
            all_peer_clients,
            peer_admin_caches,
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

    pub async fn reload_dynamic_config(&self, sub_sys: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let sub_sys = sub_sys.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client
                        .signal_service(crate::rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC, &sub_sys, false, SystemTime::UNIX_EPOCH)
                        .await
                    {
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

    pub async fn refresh_config_snapshot(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client
                        .signal_service(crate::rpc::SERVICE_SIGNAL_REFRESH_CONFIG, "", false, SystemTime::UNIX_EPOCH)
                        .await
                    {
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

    pub async fn storage_info<S>(&self, api: &S) -> rustfs_madmin::StorageInfo
    where
        S: StorageAdminApi<BackendInfo = rustfs_madmin::BackendInfo, StorageInfo = rustfs_madmin::StorageInfo>,
    {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        let endpoints = get_global_endpoints();
        let peer_timeout = Duration::from_secs(5);

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    match timeout(peer_timeout, client.local_storage_info()).await {
                        Ok(Ok(info)) => {
                            update_storage_info_cache(cache, &host, &info);
                            Some(info)
                        }
                        Ok(Err(err)) => {
                            warn!("peer {} storage_info failed: {}", host, err);
                            handle_peer_failure(cache, &host, &endpoints)
                        }
                        Err(_) => {
                            warn!("peer {} storage_info timed out after {:?}", host, peer_timeout);
                            client.evict_connection().await;
                            handle_peer_failure(cache, &host, &endpoints)
                        }
                    }
                } else {
                    None
                }
            });
        }

        let mut replies = join_all(futures).await;

        replies.push(Some(StorageAdminApi::local_storage_info(api).await));

        let mut disks = Vec::new();
        for info in replies.into_iter().flatten() {
            disks.extend(info.disks);
        }

        let backend = StorageAdminApi::backend_info(api).await;
        rustfs_madmin::StorageInfo { disks, backend }
    }

    pub async fn server_info(&self) -> Vec<ServerProperties> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        let endpoints = get_global_endpoints();
        let peer_timeout = Duration::from_secs(5);

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    match timeout(peer_timeout, client.server_info()).await {
                        Ok(Ok(info)) => {
                            update_server_info_cache(cache, &host, &info);
                            info
                        }
                        Ok(Err(err)) => {
                            warn!("peer {} server_info failed: {}", host, err);
                            handle_server_info_failure(cache, &host, &endpoints)
                        }
                        Err(_) => {
                            warn!("peer {} server_info timed out after {:?}", host, peer_timeout);
                            client.evict_connection().await;
                            handle_server_info_failure(cache, &host, &endpoints)
                        }
                    }
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

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move { client.reload_pool_meta().await.map_err(|err| (host, err)) });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_pool_meta",
                    result = "peer_unreachable",
                    peer_index = idx,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] reload_pool_meta failed: peer is not reachable"));
            }
        }

        for result in join_all(futures).await {
            if let Err((host, err)) = result {
                let failure = format!("peer {host} reload_pool_meta failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_pool_meta",
                    result = "peer_failed",
                    peer = %host,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            }
        }

        aggregate_notification_failures("reload_pool_meta", failures)
    }

    #[tracing::instrument(skip(self))]
    pub async fn load_rebalance_meta(&self, start: bool) -> Result<()> {
        let failures = self.load_rebalance_meta_failures(start).await?;
        aggregate_notification_failures("load_rebalance_meta", failures)
    }

    #[tracing::instrument(skip(self))]
    pub async fn load_rebalance_meta_failures(&self, start: bool) -> Result<Vec<String>> {
        let operation = format!("load_rebalance_meta(start={start})");
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move {
                    let result = client.load_rebalance_meta(start).await;
                    (host, result)
                });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_unreachable",
                    peer_index = idx,
                    start_rebalance = start,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] {operation} failed: peer is not reachable"));
            }
        }

        for (host, result) in join_all(futures).await {
            if let Err(err) = result {
                let failure = format!("peer {host} {operation} failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_failed",
                    peer = %host,
                    start_rebalance = start,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            } else {
                debug!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_success",
                    peer = %host,
                    start_rebalance = start,
                    "notification peer propagation"
                );
            }
        }

        Ok(failures)
    }

    pub async fn stop_rebalance(&self, expected_rebalance_id: Option<&str>) -> Result<()> {
        let failures = self.stop_rebalance_failures(expected_rebalance_id).await?;
        aggregate_notification_failures("stop_rebalance", failures)
    }

    pub async fn stop_rebalance_failures(&self, expected_rebalance_id: Option<&str>) -> Result<Vec<String>> {
        info!(
            event = EVENT_NOTIFICATION_PEER_PROPAGATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_NOTIFICATION,
            action = "stop_rebalance",
            state = "started",
            "notification peer propagation"
        );
        let Some(store) = resolve_object_store_handle() else {
            error!(
                event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                action = "stop_rebalance",
                result = "failed",
                reason = "object_layer_not_initialized",
                "notification peer propagation"
            );
            return Err(Error::other("stop_rebalance: object layer not initialized"));
        };

        let mut failures = Vec::new();

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move {
                    let result = client.stop_rebalance(expected_rebalance_id).await;
                    (host, result)
                });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_unreachable",
                    peer_index = idx,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] stop_rebalance failed: peer is not reachable"));
            }
        }

        for (host, result) in join_all(futures).await {
            if let Err(err) = result {
                let failure = format!("peer {host} stop_rebalance failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_failed",
                    peer = %host,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            } else {
                debug!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_success",
                    peer = %host,
                    "notification peer propagation"
                );
            }
        }

        match store.stop_rebalance_for_id(expected_rebalance_id).await {
            Ok(_) => {
                if let Err(err) = store.save_rebalance_stats(usize::MAX, RebalSaveOpt::StoppedAt).await {
                    error!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "stop_rebalance",
                        result = "local_save_failed",
                        error = %err,
                        "notification peer propagation"
                    );
                    return Err(Error::other(format!(
                        "local stop_rebalance save_rebalance_stats(stopped_at) failed: {err}"
                    )));
                }
            }
            Err(err) => {
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "local_stop_failed",
                    error = %err,
                    "notification peer propagation"
                );
                return Err(Error::other(format!("local stop_rebalance stop failed: {err}")));
            }
        }

        info!(
            event = EVENT_NOTIFICATION_PEER_PROPAGATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_NOTIFICATION,
            action = "stop_rebalance",
            result = if failures.is_empty() { "success" } else { "partial_failure" },
            "notification peer propagation"
        );
        Ok(failures)
    }

    pub async fn load_bucket_metadata(&self, bucket: &str) -> Result<()> {
        let operation = format!("load_bucket_metadata({bucket})");
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.host.to_string();
                let b = bucket.to_string();
                futures.push(async move { client.load_bucket_metadata(&b).await.map_err(|err| (host, err)) });
            } else {
                failures.push(format!("peer[{idx}] {operation} failed: peer is not reachable"));
            }
        }

        for result in join_all(futures).await {
            if let Err((host, err)) = result {
                let failure = format!("peer {host} {operation} failed: {err}");
                error!("notification {operation} err {failure}");
                failures.push(failure);
            }
        }

        aggregate_notification_failures(&operation, failures)
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

    pub async fn get_sys_services(&self) -> Vec<SysServices> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_se_linux_info().await.unwrap_or_default()
                } else {
                    SysServices::default()
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

/// Handle a peer failure for storage_info: return cached data if available,
/// or mark offline only after consecutive failures exceed the threshold.
fn handle_peer_failure(
    cache: Option<&Mutex<PeerAdminCache>>,
    host: &str,
    endpoints: &EndpointServerPools,
) -> Option<StorageInfo> {
    let cache = cache?;

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} storage_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.storage_failures += 1;

    if let Some(ref cached) = c.last_storage_info
        && c.storage_failures < CONSECUTIVE_FAILURE_THRESHOLD
    {
        return Some(cached.clone());
    }

    if c.storage_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        return Some(StorageInfo {
            disks: get_offline_disks(host, endpoints),
            ..Default::default()
        });
    }

    None
}

fn update_storage_info_cache(cache: Option<&Mutex<PeerAdminCache>>, host: &str, info: &StorageInfo) {
    let Some(cache) = cache else {
        return;
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} storage_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.last_storage_info = Some(info.clone());
    c.storage_failures = 0;
}

/// Handle a peer failure for server_info: return cached data if available,
/// or mark offline only after consecutive failures exceed the threshold.
fn handle_server_info_failure(
    cache: Option<&Mutex<PeerAdminCache>>,
    host: &str,
    endpoints: &EndpointServerPools,
) -> ServerProperties {
    let Some(cache) = cache else {
        return initializing_server_properties(host);
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} server_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.server_failures += 1;

    if let Some(ref cached) = c.last_server_info
        && c.server_failures < CONSECUTIVE_FAILURE_THRESHOLD
    {
        return cached.clone();
    }

    if c.server_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        return offline_server_properties(host, endpoints);
    }

    initializing_server_properties(host)
}

fn update_server_info_cache(cache: Option<&Mutex<PeerAdminCache>>, host: &str, info: &ServerProperties) {
    let Some(cache) = cache else {
        return;
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} server_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.last_server_info = Some(info.clone());
    c.server_failures = 0;
}

fn initializing_server_properties(host: &str) -> ServerProperties {
    ServerProperties {
        endpoint: host.to_string(),
        state: ItemState::Initializing.to_string().to_owned(),
        ..Default::default()
    }
}

fn offline_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        uptime: GLOBAL_BOOT_TIME
            .get()
            .and_then(|boot_time| SystemTime::now().duration_since(*boot_time).ok())
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

fn aggregate_notification_failures(operation: &str, failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }

    Err(Error::other(format!(
        "{operation} encountered {} failure(s): {}",
        failures.len(),
        failures.join(" | ")
    )))
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
            std::future::pending::<Result<ServerProperties>>,
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fallback");
    }

    #[test]
    fn aggregate_notification_failures_returns_ok_when_empty() {
        assert!(aggregate_notification_failures("stop_rebalance", Vec::new()).is_ok());
    }

    #[test]
    fn aggregate_notification_failures_returns_joined_error_when_non_empty() {
        let err = aggregate_notification_failures(
            "load_rebalance_meta",
            vec!["peer-1 failed".to_string(), "local save failed".to_string()],
        )
        .expect_err("non-empty failures should return error");

        let msg = err.to_string();
        assert!(msg.contains("load_rebalance_meta"));
        assert!(msg.contains("2 failure(s)"));
        assert!(msg.contains("peer-1 failed"));
        assert!(msg.contains("local save failed"));
    }

    #[test]
    fn load_rebalance_meta_aggregate_failures_return_error() {
        let err = aggregate_notification_failures(
            "load_rebalance_meta(start=true)",
            vec!["peer[0] load_rebalance_meta failed: peer is not reachable".to_string()],
        )
        .expect_err("load_rebalance_meta peer failures must be returned");

        let msg = err.to_string();
        assert!(msg.contains("load_rebalance_meta(start=true)"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[test]
    fn stop_rebalance_aggregate_failures_return_error() {
        let err = aggregate_notification_failures(
            "stop_rebalance",
            vec!["peer[0] stop_rebalance failed: peer is not reachable".to_string()],
        )
        .expect_err("stop_rebalance peer failures must be returned");

        let msg = err.to_string();
        assert!(msg.contains("stop_rebalance"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[tokio::test]
    async fn reload_pool_meta_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
        };

        let err = sys
            .reload_pool_meta()
            .await
            .expect_err("unreachable peers should fail pool metadata reload");

        let msg = err.to_string();
        assert!(msg.contains("reload_pool_meta"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[tokio::test]
    async fn load_bucket_metadata_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
        };

        let err = sys
            .load_bucket_metadata("bucket-a")
            .await
            .expect_err("unreachable peers should fail bucket metadata reload");

        let msg = err.to_string();
        assert!(msg.contains("load_bucket_metadata(bucket-a)"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[tokio::test]
    async fn load_transition_tier_config_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
        };

        let results = sys.load_transition_tier_config().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].host.is_empty());
        assert!(results[0].err.is_some());
        assert!(results[0].err.as_ref().unwrap().to_string().contains("peer is not reachable"));
    }

    // --- Tests for handle_peer_failure / handle_server_info_failure caching ---

    #[test]
    fn handle_peer_failure_first_failure_returns_none_when_no_cache() {
        let cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(result.is_none());
        assert_eq!(cache.lock().unwrap().storage_failures, 1);
    }

    #[test]
    fn handle_peer_failure_returns_cached_data_on_single_failure() {
        let cached_info = StorageInfo {
            disks: vec![rustfs_madmin::Disk {
                endpoint: "disk-0".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(cached_info),
            last_server_info: None,
            storage_failures: 0,
            server_failures: 0,
        });
        let endpoints = EndpointServerPools::default();

        // First failure: should return cached data
        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        let info = result.unwrap();
        assert_eq!(info.disks.len(), 1);
        assert_eq!(info.disks[0].state, "ok");
        assert_eq!(cache.lock().unwrap().storage_failures, 1);
    }

    #[test]
    fn handle_peer_failure_returns_offline_after_threshold_exceeded() {
        let cached_info = StorageInfo {
            disks: vec![rustfs_madmin::Disk {
                endpoint: "disk-0".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(cached_info),
            last_server_info: None,
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
        });
        let endpoints = EndpointServerPools::default();

        // This failure pushes us to the threshold => offline
        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(result.is_some());
        assert_eq!(cache.lock().unwrap().storage_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_returns_cached_on_single_failure() {
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: 0,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints);
        assert_eq!(result.endpoint, "peer-1");
        assert_eq!(result.state, "online");
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn handle_server_info_failure_returns_initializing_before_threshold_without_cache() {
        let cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints);
        assert_eq!(result.endpoint, "peer-1");
        assert_eq!(result.state, ItemState::Initializing.to_string());
        assert!(result.disks.is_empty());
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn handle_server_info_failure_returns_offline_after_threshold() {
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints);
        assert_eq!(result.state, ItemState::Offline.to_string());
        assert_eq!(cache.lock().unwrap().server_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn success_resets_failure_counters_independently() {
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 2,
            server_failures: 2,
        });

        {
            let mut c = cache.lock().unwrap();
            c.last_storage_info = Some(StorageInfo::default());
            c.storage_failures = 0;
        }

        let cache = cache.lock().unwrap();
        assert_eq!(cache.storage_failures, 0);
        assert_eq!(cache.server_failures, 2);
    }

    #[test]
    fn storage_failures_do_not_affect_server_failures() {
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(StorageInfo::default()),
            last_server_info: Some(ServerProperties {
                endpoint: "peer-1".to_string(),
                state: "online".to_string(),
                ..Default::default()
            }),
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
        });
        let endpoints = EndpointServerPools::default();

        let storage_result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(storage_result.is_some());

        let server_result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints);
        assert_eq!(server_result.state, "online");
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn poisoned_admin_cache_mutex_still_returns_fallbacks() {
        let storage_cache = Mutex::new(PeerAdminCache::new());
        let server_cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let _ = std::panic::catch_unwind(|| {
            let _guard = storage_cache.lock().expect("test: poison storage cache mutex");
            panic!("poison storage cache mutex");
        });
        let _ = std::panic::catch_unwind(|| {
            let _guard = server_cache.lock().expect("test: poison server cache mutex");
            panic!("poison server cache mutex");
        });

        let storage_result = handle_peer_failure(Some(&storage_cache), "peer-1", &endpoints);
        assert!(storage_result.is_none());

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints);
        assert_eq!(server_result.endpoint, "peer-1");
        assert_eq!(server_result.state, ItemState::Initializing.to_string());
    }

    #[test]
    fn poisoned_admin_cache_recovers_on_success_and_resets_failures() {
        let storage_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
        });
        let server_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
        });
        let endpoints = EndpointServerPools::default();

        let _ = std::panic::catch_unwind(|| {
            let _guard = storage_cache.lock().expect("test: poison storage cache mutex");
            panic!("poison storage cache mutex");
        });
        let _ = std::panic::catch_unwind(|| {
            let _guard = server_cache.lock().expect("test: poison server cache mutex");
            panic!("poison server cache mutex");
        });

        update_storage_info_cache(
            Some(&storage_cache),
            "peer-1",
            &StorageInfo {
                disks: vec![rustfs_madmin::Disk {
                    endpoint: "disk-0".to_string(),
                    state: "ok".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        update_server_info_cache(
            Some(&server_cache),
            "peer-1",
            &ServerProperties {
                endpoint: "peer-1".to_string(),
                state: "online".to_string(),
                ..Default::default()
            },
        );

        let storage_result = handle_peer_failure(Some(&storage_cache), "peer-1", &endpoints);
        assert!(storage_result.is_some());
        assert_eq!(storage_result.unwrap().disks[0].state, "ok");

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints);
        assert_eq!(server_result.state, "online");
    }
}
