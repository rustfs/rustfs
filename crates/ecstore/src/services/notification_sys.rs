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

use crate::cluster::rpc::{PeerRestClient, ScannerPeerActivity};
use crate::diagnostics::admin_server_info::get_commit_id;
use crate::disk::DiskAPI;
use crate::error::{Error, Result};
use crate::layout::endpoints::EndpointServerPools;
use crate::runtime::sources as runtime_sources;
use crate::services::metrics_realtime::{CollectMetricsOpts, MetricType};
use crate::services::rebalance::RebalSaveOpt;
use crate::storage_api_contracts::admin::StorageAdminApi;
use futures::future::join_all;
use lazy_static::lazy_static;
use rustfs_madmin::health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysServices};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::net::NetInfo;
use rustfs_madmin::{ItemState, ServerProperties, StorageInfo};
use rustfs_utils::XHost;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// After this many consecutive admin-call failures, mark the peer as offline.
const CONSECUTIVE_FAILURE_THRESHOLD: u32 = 3;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_NOTIFICATION: &str = "notification";
const EVENT_NOTIFICATION_PEER_PROPAGATION: &str = "notification_peer_propagation";
const SCANNER_ACTIVITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Cached result from the last successful admin call to a peer.
struct PeerAdminCache {
    last_storage_info: Option<StorageInfo>,
    last_server_info: Option<ServerProperties>,
    storage_failures: u32,
    server_failures: u32,
    /// When the last successful server_info probe landed. Used to stop a stale
    /// cached `online` snapshot from being served indefinitely while a peer is
    /// actually down (rustfs/backlog#1049 P2).
    last_server_success: Option<SystemTime>,
}

impl PeerAdminCache {
    fn new() -> Self {
        Self {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: 0,
            last_server_success: None,
        }
    }
}

/// A cached `online` snapshot older than this is no longer trusted on a probe
/// failure: rather than reporting a stale `online`, the member falls through to
/// the live unknown/degraded/offline classification (rustfs/backlog#1049 P2).
const SERVER_INFO_CACHE_MAX_AGE: Duration = Duration::from_secs(60);

lazy_static! {
    pub static ref GLOBAL_NOTIFICATION_SYS: OnceLock<Arc<NotificationSys>> = OnceLock::new();
}

pub async fn new_global_notification_sys(eps: EndpointServerPools) -> Result<()> {
    let _ = GLOBAL_NOTIFICATION_SYS
        .set(Arc::new(NotificationSys::new(eps).await))
        .map_err(|_| Error::other("init notification_sys fail"));
    Ok(())
}

// Owned handle rather than `&'static` (backlog#1052 S3): per-server contexts
// need to hold their own notification system, which a process-lifetime
// borrow cannot express.
pub fn get_global_notification_sys() -> Option<Arc<NotificationSys>> {
    GLOBAL_NOTIFICATION_SYS.get().cloned()
}

pub struct NotificationSys {
    pub peer_clients: Vec<Option<PeerRestClient>>,
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

    pub fn peer_client_for_grid_host(&self, grid_host: &str) -> Option<PeerRestClient> {
        self.all_peer_clients
            .iter()
            .flatten()
            .find(|client| client.grid_host == grid_host)
            .cloned()
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
                        .signal_service(
                            crate::cluster::rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC,
                            &sub_sys,
                            false,
                            SystemTime::UNIX_EPOCH,
                        )
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
                        .signal_service(crate::cluster::rpc::SERVICE_SIGNAL_REFRESH_CONFIG, "", false, SystemTime::UNIX_EPOCH)
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
        let endpoints = runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into());
        let peer_timeout = Duration::from_secs(5);

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    match timeout(peer_timeout, client.local_storage_info()).await {
                        Ok(Ok(mut info)) => {
                            normalize_and_cache_peer_storage_info(cache, &host, &mut info);
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
        let endpoints = runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into());
        let peer_timeout = Duration::from_secs(5);

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                // `peer_clients` comes from `new_clients`, which only ever pushes
                // `Some(client)` (local hosts are excluded, not slotted as
                // `None`), so this branch is unreachable in practice. Kept as a
                // defensive fallback: report an explicit `unknown` state rather
                // than a blank `default()` entry, so it can never contribute a
                // hollow row to `servers[]` (rustfs/backlog#1049 P3).
                let Some(client) = client else {
                    return ServerProperties {
                        state: ItemState::Unknown.to_string().to_owned(),
                        ..Default::default()
                    };
                };
                let host = client.host.to_string();

                // First attempt. A single evicted or half-open internode channel
                // is enough to fail one probe and, before retrying, would drop
                // the member to unknown/offline for this whole snapshot. So on any
                // first-attempt failure we evict the channel and re-dial once
                // before falling back (rustfs/backlog#1049, P1-B).
                match timeout(peer_timeout, client.server_info()).await {
                    Ok(Ok(info)) => {
                        update_server_info_cache(cache, &host, &info);
                        return info;
                    }
                    Ok(Err(err)) => debug!("peer {host} server_info failed (attempt 1/2): {err}"),
                    Err(_) => debug!("peer {host} server_info timed out (attempt 1/2) after {peer_timeout:?}"),
                }

                // Drop the suspect channel AND clear the offline gate so the
                // retry actually re-dials. A network-like first failure runs
                // through `finalize_result`, which sets the offline gate; a bare
                // `evict_connection` would leave that gate up and the retry would
                // fast-fail with "temporarily offline" instead of reconnecting
                // (rustfs/backlog#1049 P1-B).
                client.prepare_retry().await;

                // Second and final attempt on the fresh channel.
                match timeout(peer_timeout, client.server_info()).await {
                    Ok(Ok(info)) => {
                        update_server_info_cache(cache, &host, &info);
                        info
                    }
                    Ok(Err(err)) => {
                        warn!("peer {host} server_info failed after retry: {err}");
                        let health = peer_disk_health(&host).await;
                        handle_server_info_failure(cache, &host, &endpoints, health.as_ref())
                    }
                    Err(_) => {
                        warn!("peer {host} server_info timed out after retry ({peer_timeout:?})");
                        client.evict_connection().await;
                        let health = peer_disk_health(&host).await;
                        handle_server_info_failure(cache, &host, &endpoints, health.as_ref())
                    }
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
        let Some(store) = runtime_sources::object_store_handle() else {
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
        self.load_bucket_metadata_with_scanner_maintenance(bucket, false).await
    }

    pub async fn load_bucket_metadata_for_scanner_maintenance(&self, bucket: &str) -> Result<()> {
        self.load_bucket_metadata_with_scanner_maintenance(bucket, true).await
    }

    async fn load_bucket_metadata_with_scanner_maintenance(&self, bucket: &str, scanner_maintenance_change: bool) -> Result<()> {
        let operation = format!("load_bucket_metadata({bucket})");
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.host.to_string();
                let b = bucket.to_string();
                futures.push(async move {
                    client
                        .load_bucket_metadata(&b, scanner_maintenance_change)
                        .await
                        .map_err(|err| (host, err))
                });
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

    pub async fn scanner_activity_snapshots(&self) -> Result<Vec<(String, ScannerPeerActivity)>> {
        if self.peer_clients.is_empty() {
            return Err(Error::other("scanner activity probe has no remote peers"));
        }
        if self.all_peer_clients.len() != self.peer_clients.len() + 1 {
            return Err(Error::other(format!(
                "scanner activity peer topology is incomplete: {} remote peers for {} cluster members",
                self.peer_clients.len(),
                self.all_peer_clients.len()
            )));
        }

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().cloned().enumerate() {
            futures.push(async move {
                let client = client.ok_or_else(|| Error::other(format!("scanner activity peer[{idx}] is unreachable")))?;
                let host = client.grid_host.clone();
                scanner_activity_with_timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, &host, client.scanner_activity())
                    .await
                    .map(|activity| (host, activity))
            });
        }

        let mut generations = Vec::with_capacity(futures.len());
        for result in join_all(futures).await {
            generations.push(result?);
        }
        Ok(generations)
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

async fn scanner_activity_with_timeout<F>(timeout_duration: Duration, host: &str, activity: F) -> Result<ScannerPeerActivity>
where
    F: Future<Output = Result<ScannerPeerActivity>>,
{
    timeout(timeout_duration, activity)
        .await
        .map_err(|_| Error::other(format!("scanner activity peer {host} timed out after {timeout_duration:?}")))?
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
        debug!(
            event = "peer_probe_failure",
            peer = host,
            probe = "storage_info",
            consecutive_failures = c.storage_failures,
            threshold = CONSECUTIVE_FAILURE_THRESHOLD,
            "peer storage_info probe failed; returning cached state until the offline threshold is reached"
        );
        return Some(cached.clone());
    }

    if c.storage_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        if c.storage_failures == CONSECUTIVE_FAILURE_THRESHOLD {
            warn!(
                event = "peer_marked_offline",
                peer = host,
                probe = "storage_info",
                consecutive_failures = c.storage_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "reporting peer disks offline after consecutive storage_info failures"
            );
        }
        return Some(StorageInfo {
            disks: synthesized_disks(host, endpoints, ItemState::Offline),
            ..Default::default()
        });
    }

    None
}

fn normalize_and_cache_peer_storage_info(cache: Option<&Mutex<PeerAdminCache>>, host: &str, info: &mut StorageInfo) {
    // `Disk::local` is relative to this aggregator, not to the peer that
    // produced the response.
    for disk in &mut info.disks {
        disk.local = false;
    }

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
    if c.storage_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        info!(
            event = "peer_recovered_online",
            peer = host,
            probe = "storage_info",
            consecutive_failures = c.storage_failures,
            "peer storage_info probe succeeded again; peer disks reported online"
        );
    }
    c.last_storage_info = Some(info.clone());
    c.storage_failures = 0;
}

/// Independent liveness evidence for a peer, gathered from the local disk-health
/// heartbeat rather than the admin RPC path. `any_online` is true when at least
/// one of the peer's drives is still answering the ~15s health check; `disks`
/// carries a per-drive entry (state `"ok"` when online, `"offline"` when the
/// heartbeat marks it faulty) so a `degraded` member's drives are counted for
/// real. See rustfs/backlog#1049 (P0-B).
struct PeerDiskHealth {
    any_online: bool,
    disks: Vec<rustfs_madmin::Disk>,
}

/// Consult the local disk-health state for `host` without issuing any RPC.
///
/// On the aggregating node a peer's drives are remote-disk handles whose
/// `is_online()` is a pure atomic read of the heartbeat tracker (independent of
/// the admin `server_info` RPC that just failed). Returns `None` when the store
/// is not initialized or the host owns no drives in the topology.
async fn peer_disk_health(host: &str) -> Option<PeerDiskHealth> {
    let store = runtime_sources::object_store_handle()?;

    let mut disks = Vec::new();
    let mut any_online = false;
    for sets in store.pools.iter() {
        for set in sets.disk_set.iter() {
            let guard = set.disks.read().await;
            for (idx, slot) in guard.iter().enumerate() {
                let Some(ep) = set.set_endpoints.get(idx) else {
                    continue;
                };
                if !endpoint_host_matches(host, &ep.host_port()) {
                    continue;
                }
                let online = match slot {
                    Some(disk) => disk.is_online().await,
                    None => false,
                };
                any_online |= online;
                // A live drive is counted online via the DriveState "ok" string;
                // a faulty one is counted offline. This keeps a degraded member's
                // drives in the real online/offline buckets.
                disks.push(rustfs_madmin::Disk {
                    endpoint: ep.to_string(),
                    state: if online {
                        rustfs_common::heal_channel::DriveState::Ok.to_string()
                    } else {
                        ItemState::Offline.to_string().to_owned()
                    },
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
            }
        }
    }

    if disks.is_empty() {
        None
    } else {
        Some(PeerDiskHealth { any_online, disks })
    }
}

/// Handle a peer failure for server_info: return cached data if available, or
/// classify the member as `unknown` / `degraded` / `offline` depending on how
/// many consecutive probes have failed and whether the peer's drives are still
/// answering the local disk-health heartbeat.
///
/// - Below the failure threshold with no cached snapshot: `unknown` (probe
///   missed this cycle but the member is not confirmed down).
/// - At/after the threshold with drives still online: `degraded` (the admin RPC
///   is stuck but the node is alive and serving data) — this is what stops a
///   healthy node from rotating through a false `offline` (rustfs/backlog#1049).
/// - At/after the threshold with drives also offline: `offline` (confirmed).
///
/// Synthesized entries always carry one drive per endpoint so the pool's drive
/// totals stay balanced.
fn handle_server_info_failure(
    cache: Option<&Mutex<PeerAdminCache>>,
    host: &str,
    endpoints: &EndpointServerPools,
    peer_health: Option<&PeerDiskHealth>,
) -> ServerProperties {
    let Some(cache) = cache else {
        return unknown_server_properties(host, endpoints);
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
        if cached_snapshot_is_fresh(c.last_server_success) {
            debug!(
                event = "peer_probe_failure",
                peer = host,
                consecutive_failures = c.server_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "peer server_info probe failed; returning cached state until the offline threshold is reached"
            );
            return cached.clone();
        }
        // The cached snapshot is too old to keep reporting as `online`; fall
        // through to the live unknown/degraded/offline classification below
        // instead of masking a down peer with a stale success (P2).
        debug!(
            event = "peer_cache_stale",
            peer = host,
            max_age_secs = SERVER_INFO_CACHE_MAX_AGE.as_secs(),
            "cached server_info snapshot is stale; reclassifying from live signals instead of reporting stale online"
        );
    }

    if c.server_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        // Drives still answering the heartbeat: the node is alive, only its
        // admin surface is unreachable — report `degraded`, not `offline`, so a
        // stuck admin path does not read as an ejected node.
        if let Some(health) = peer_health.filter(|h| h.any_online) {
            if c.server_failures == CONSECUTIVE_FAILURE_THRESHOLD {
                warn!(
                    event = "peer_marked_degraded",
                    peer = host,
                    consecutive_failures = c.server_failures,
                    threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                    "peer admin server_info keeps failing but its drives are online; reporting degraded (not offline)"
                );
            } else {
                debug!(
                    event = "peer_still_degraded",
                    peer = host,
                    consecutive_failures = c.server_failures,
                    "peer admin server_info still failing while its drives remain online"
                );
            }
            return degraded_server_properties(host, &health.disks);
        }

        // Log the transition exactly once (at the crossing) so the console's
        // "node offline" verdict has a matching WARN in the observer's logs
        // (rustfs/backlog#888: nodes were marked offline with no log naming
        // the transition). Later failures while already offline stay at DEBUG
        // to avoid repeating the warning every probe cycle.
        if c.server_failures == CONSECUTIVE_FAILURE_THRESHOLD {
            warn!(
                event = "peer_marked_offline",
                peer = host,
                consecutive_failures = c.server_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "marking peer offline for admin/console reporting after consecutive server_info failures; \
                 a background recovery probe will restore it automatically once reachable"
            );
        } else {
            debug!(
                event = "peer_still_offline",
                peer = host,
                consecutive_failures = c.server_failures,
                "peer server_info probe failed while peer is already reported offline"
            );
        }
        return offline_server_properties(host, endpoints);
    }

    unknown_server_properties(host, endpoints)
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
    if c.server_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        info!(
            event = "peer_recovered_online",
            peer = host,
            consecutive_failures = c.server_failures,
            "peer server_info probe succeeded again; peer is back online for admin/console reporting"
        );
    }
    c.last_server_info = Some(info.clone());
    c.last_server_success = Some(SystemTime::now());
    c.server_failures = 0;
}

/// Whether a cached server_info snapshot is recent enough to still report as
/// `online` on a probe failure. A missing timestamp means no age information is
/// available (e.g. a snapshot set without going through the success path in a
/// test); such a snapshot is treated as fresh to preserve the prior behavior,
/// while a clock that went backwards is treated as stale. See P2 in
/// rustfs/backlog#1049.
fn cached_snapshot_is_fresh(last_success: Option<SystemTime>) -> bool {
    match last_success {
        Some(at) => at.elapsed().map(|age| age < SERVER_INFO_CACHE_MAX_AGE).unwrap_or(false),
        None => true,
    }
}

/// A member that could not be probed this cycle and is not confirmed down.
/// Carries the endpoint's drives (marked `unknown`) so the pool's drive totals
/// stay balanced instead of the member's drives vanishing from the summary.
fn unknown_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        endpoint: host.to_string(),
        state: ItemState::Unknown.to_string().to_owned(),
        disks: synthesized_disks(host, endpoints, ItemState::Unknown),
        ..Default::default()
    }
}

fn offline_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        uptime: runtime_sources::boot_uptime_secs(),
        version: get_commit_id(),
        endpoint: host.to_string(),
        state: ItemState::Offline.to_string().to_owned(),
        disks: synthesized_disks(host, endpoints, ItemState::Offline),
        ..Default::default()
    }
}

/// A member whose admin RPC is unreachable but whose drives are still online.
/// Carries the per-drive health observed from the local heartbeat so the drives
/// land in the real online/offline buckets while the member reads as degraded.
fn degraded_server_properties(host: &str, disks: &[rustfs_madmin::Disk]) -> ServerProperties {
    ServerProperties {
        uptime: runtime_sources::boot_uptime_secs(),
        version: get_commit_id(),
        endpoint: host.to_string(),
        state: ItemState::Degraded.to_string().to_owned(),
        disks: disks.to_vec(),
        ..Default::default()
    }
}

/// Enumerate the drives a host owns from the pool topology, tagged with the
/// given member state. Used to synthesize drive entries for a member whose
/// properties RPC could not be answered, so summary counters stay complete.
fn synthesized_disks(host: &str, endpoints: &EndpointServerPools, state: ItemState) -> Vec<rustfs_madmin::Disk> {
    let mut disks = Vec::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if (host.is_empty() && ep.is_local) || endpoint_host_matches(host, &ep.host_port()) {
                disks.push(rustfs_madmin::Disk {
                    endpoint: ep.to_string(),
                    state: state.to_string().to_owned(),
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
            }
        }
    }

    disks
}

/// Whether `peer_host` refers to the same node as an endpoint whose
/// `host_port()` is `ep_host_port`.
///
/// `PeerRestClient::host` is an `XHost`, which resolves names to an address on
/// construction (`hosts_sorted` -> `XHost::try_from` -> `to_socket_addrs`), so
/// `peer_host` is the resolved `IP:port`. An endpoint's `host_port()`, however,
/// is `url.host():port` — still the raw `hostname:port` on hostname-based
/// deployments. A plain string compare therefore misses on hostname clusters,
/// leaving the synthesized/degraded drive list empty and `unknownDisks` at 0
/// (rustfs/rustfs#4607 follow-up). Compare directly first (fast path / IP
/// deployments), then canonicalize the endpoint side through the same `XHost`
/// resolution and compare again.
fn endpoint_host_matches(peer_host: &str, ep_host_port: &str) -> bool {
    if peer_host == ep_host_port {
        return true;
    }
    XHost::try_from(ep_host_port.to_string())
        .map(|resolved| resolved.to_string() == peer_host)
        .unwrap_or(false)
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
    fn peer_client_for_grid_host_matches_exact_grid_host() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: vec![Some(PeerRestClient::new(
                "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
                "http://127.0.0.1:9000".to_string(),
            ))],
            peer_admin_caches: Vec::new(),
        };

        let client = sys
            .peer_client_for_grid_host("http://127.0.0.1:9000")
            .expect("matching grid host should return peer client");
        assert_eq!(client.grid_host, "http://127.0.0.1:9000");
        assert!(sys.peer_client_for_grid_host("http://node-b:9000").is_none());
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
    async fn scanner_activity_probe_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("unreachable peers must disable scanner idle backoff");

        assert!(err.to_string().contains("scanner activity peer[0] is unreachable"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_rejects_an_empty_peer_set() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_admin_caches: Vec::new(),
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("a missing peer set must disable scanner idle backoff");

        assert!(err.to_string().contains("no remote peers"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_rejects_an_incomplete_peer_topology() {
        let client = PeerRestClient::new(
            "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
            "http://127.0.0.1:9000".to_string(),
        );
        let sys = NotificationSys {
            peer_clients: vec![Some(client)],
            all_peer_clients: vec![None],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("an incomplete peer topology must disable scanner idle backoff");

        assert!(err.to_string().contains("peer topology is incomplete"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_times_out() {
        let err = scanner_activity_with_timeout(
            Duration::from_millis(5),
            "peer-1",
            std::future::pending::<Result<ScannerPeerActivity>>(),
        )
        .await
        .expect_err("a stalled peer must not block scanner scheduling");

        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("peer-1"));
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
            last_server_success: None,
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
    fn normalize_and_cache_peer_storage_info_marks_disks_remote() {
        let cache = Mutex::new(PeerAdminCache::new());
        let mut info = StorageInfo {
            disks: vec![
                rustfs_madmin::Disk {
                    endpoint: "http://node2:9000/data".to_string(),
                    drive_path: "/data".to_string(),
                    local: true,
                    ..Default::default()
                },
                rustfs_madmin::Disk {
                    endpoint: "http://node3:9000/data".to_string(),
                    drive_path: "/data".to_string(),
                    local: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        normalize_and_cache_peer_storage_info(Some(&cache), "peer-1", &mut info);

        assert!(info.disks.iter().all(|disk| !disk.local));
        let cached = cache.lock().expect("peer cache must remain available");
        assert!(
            cached
                .last_storage_info
                .as_ref()
                .expect("successful peer response must be cached")
                .disks
                .iter()
                .all(|disk| !disk.local)
        );
        drop(cached);

        let degraded = handle_peer_failure(Some(&cache), "peer-1", &EndpointServerPools::default())
            .expect("first peer failure must return the cached snapshot");
        assert!(degraded.disks.iter().all(|disk| !disk.local));
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
            last_server_success: None,
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
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.endpoint, "peer-1");
        assert_eq!(result.state, "online");
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn handle_server_info_failure_does_not_serve_stale_cached_online() {
        // A single failure with a cached snapshot would normally return the
        // cached `online`, but if that snapshot is older than the max age we
        // must not keep reporting online — fall through to `unknown` instead of
        // masking a possibly-down peer (rustfs/backlog#1049 P2).
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };
        let stale_at = SystemTime::now()
            .checked_sub(SERVER_INFO_CACHE_MAX_AGE + Duration::from_secs(1))
            .expect("test clock underflow");

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: 0,
            last_server_success: Some(stale_at),
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.state, ItemState::Unknown.to_string());
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn cached_snapshot_freshness_respects_age_and_missing_timestamp() {
        assert!(cached_snapshot_is_fresh(None), "no timestamp is treated as fresh");
        assert!(cached_snapshot_is_fresh(Some(SystemTime::now())), "a just-now success is fresh");
        let stale = SystemTime::now()
            .checked_sub(SERVER_INFO_CACHE_MAX_AGE + Duration::from_secs(1))
            .expect("test clock underflow");
        assert!(!cached_snapshot_is_fresh(Some(stale)), "an old success is stale");
    }

    #[test]
    fn endpoint_host_matches_direct_and_canonicalized() {
        // Direct match (IP deployment): peer host already equals host_port.
        assert!(endpoint_host_matches("10.0.0.12:9000", "10.0.0.12:9000"));
        // Different IPs must not match.
        assert!(!endpoint_host_matches("10.0.0.12:9000", "10.0.0.99:9000"));

        // Hostname deployment: `PeerRestClient::host` is the resolved `IP:port`,
        // the endpoint keeps the raw `hostname:port`. Resolve "localhost" the
        // same way `XHost` does (avoids depending on external DNS) and confirm
        // the canonical compare matches — the regression this fixes is the
        // synthesized/degraded drive list going empty on hostname clusters.
        let resolved = XHost::try_from("localhost:9000".to_string())
            .expect("localhost should resolve")
            .to_string();
        assert!(
            endpoint_host_matches(&resolved, "localhost:9000"),
            "resolved localhost ({resolved}) must match the hostname endpoint"
        );
        // A resolved address that is not localhost must not match.
        assert!(!endpoint_host_matches("203.0.113.1:9000", "localhost:9000"));
    }

    #[test]
    fn handle_server_info_failure_returns_unknown_before_threshold_without_cache() {
        let cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.endpoint, "peer-1");
        // A probe miss below the threshold is "unknown" (not confirmed down,
        // and not the misleading "initializing"): rustfs/backlog#1049.
        assert_eq!(result.state, ItemState::Unknown.to_string());
        // The default (empty) pool has no topology entry for this host, so no
        // drives are synthesized here; the drive-synthesis and counter-balance
        // behavior is exercised by the get_online_offline_disks_stats tests in
        // admin_server_info.
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
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.state, ItemState::Offline.to_string());
        assert_eq!(cache.lock().unwrap().server_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_returns_degraded_when_disks_online_past_threshold() {
        // Past the threshold but the peer's drives still answer the heartbeat:
        // the node is alive, only its admin RPC is stuck — report degraded (with
        // the real per-drive health), not offline (rustfs/backlog#1049 P0-B).
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();
        let health = PeerDiskHealth {
            any_online: true,
            disks: vec![rustfs_madmin::Disk {
                endpoint: "http://peer-1:9000/data".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
        };

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, Some(&health));
        assert_eq!(result.state, ItemState::Degraded.to_string());
        assert_eq!(result.disks.len(), 1);
        assert_eq!(result.disks[0].state, "ok");
        assert_eq!(cache.lock().unwrap().server_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_stays_offline_when_disks_also_offline() {
        // Past the threshold and the heartbeat also reports the drives down:
        // this is a genuine offline, degraded must not mask it.
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();
        let health = PeerDiskHealth {
            any_online: false,
            disks: vec![rustfs_madmin::Disk {
                endpoint: "http://peer-1:9000/data".to_string(),
                state: ItemState::Offline.to_string().to_owned(),
                ..Default::default()
            }],
        };

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, Some(&health));
        assert_eq!(result.state, ItemState::Offline.to_string());
    }

    #[test]
    fn success_resets_failure_counters_independently() {
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 2,
            server_failures: 2,
            last_server_success: None,
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
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let storage_result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(storage_result.is_some());

        let server_result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
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

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints, None);
        assert_eq!(server_result.endpoint, "peer-1");
        assert_eq!(server_result.state, ItemState::Unknown.to_string());
    }

    #[test]
    fn poisoned_admin_cache_recovers_on_success_and_resets_failures() {
        let storage_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
            last_server_success: None,
        });
        let server_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
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

        normalize_and_cache_peer_storage_info(
            Some(&storage_cache),
            "peer-1",
            &mut StorageInfo {
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

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints, None);
        assert_eq!(server_result.state, "online");
    }
}
