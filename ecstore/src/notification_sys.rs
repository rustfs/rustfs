use crate::admin_server_info::get_commit_id;
use crate::global::{get_global_endpoints, GLOBAL_BOOT_TIME};
use crate::peer_rest_client::PeerRestClient;
use crate::StorageAPI;
use crate::{endpoints::EndpointServerPools, new_object_layer_fn};
use common::error::{Error, Result};
use futures::future::join_all;
use lazy_static::lazy_static;
use madmin::{ItemState, ServerProperties};
use std::sync::OnceLock;
use std::time::SystemTime;
use tracing::error;

lazy_static! {
    pub static ref GLOBAL_NotificationSys: OnceLock<NotificationSys> = OnceLock::new();
}

pub async fn new_global_notification_sys(eps: EndpointServerPools) -> Result<()> {
    let _ = GLOBAL_NotificationSys
        .set(NotificationSys::new(eps).await)
        .map_err(|_| Error::msg("init notification_sys fail"));
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
        for client in self.peer_clients.iter().flatten() {
            futures.push(client.load_rebalance_meta(start));
        }

        let results = join_all(futures).await;
        for result in results {
            if let Err(err) = result {
                error!("notification load_rebalance_meta err {:?}", err);
            }
        }
    }

    pub async fn stop_rebalance(&self) {
        let Some(store) = new_object_layer_fn() else {
            error!("stop_rebalance: not init");
            return;
        };

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

        let _ = store.stop_rebalance().await;
    }
}

fn get_offline_disks(offline_host: &str, endpoints: &EndpointServerPools) -> Vec<madmin::Disk> {
    let mut offline_disks = Vec::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if (offline_host.is_empty() && ep.is_local) || offline_host == ep.host_port() {
                offline_disks.push(madmin::Disk {
                    endpoint: ep.to_string(),
                    state: madmin::ItemState::Offline.to_string().to_owned(),
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
