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
use crate::rpc::PeerRestClient;
use crate::{endpoints::EndpointServerPools, new_object_layer_fn};
use futures::future::join_all;
use lazy_static::lazy_static;
use madmin::{ItemState, ServerProperties};
use std::sync::OnceLock;
use std::time::SystemTime;
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
    pub fn rest_client_from_hash(&self, _s: &str) -> Option<PeerRestClient> {
        None
    }
    pub async fn delete_policy(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }
    pub async fn load_policy(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }

    pub async fn load_policy_mapping(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }
    pub async fn delete_user(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }

    pub async fn storage_info<S: StorageAPI>(&self, api: &S) -> madmin::StorageInfo {
        let mut futures = Vec::with_capacity(self.peer_clients.len());

        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.local_storage_info().await {
                        Ok(info) => Some(info),
                        Err(_) => Some(madmin::StorageInfo {
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
        madmin::StorageInfo { disks, backend }
    }

    pub async fn server_info(&self) -> Vec<ServerProperties> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());

        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.server_info().await {
                        Ok(info) => info,
                        Err(_) => ServerProperties {
                            uptime: SystemTime::now()
                                .duration_since(*GLOBAL_BOOT_TIME.get().unwrap())
                                .unwrap_or_default()
                                .as_secs(),
                            version: get_commit_id(),
                            endpoint: client.host.to_string(),
                            state: ItemState::Offline.to_string().to_owned(),
                            disks: get_offline_disks(&client.host.to_string(), &get_global_endpoints()),
                            ..Default::default()
                        },
                    }
                } else {
                    ServerProperties::default()
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
}

fn get_offline_disks(offline_host: &str, endpoints: &EndpointServerPools) -> Vec<madmin::Disk> {
    let mut offline_disks = Vec::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if (offline_host.is_empty() && ep.is_local) || offline_host == ep.host_port() {
                offline_disks.push(madmin::Disk {
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
