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

use super::*;
use crate::global::GLOBAL_LOCAL_DISK_ID_MAP;
use tracing::{debug, error};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK_STARTUP: &str = "disk_startup";
const EVENT_LOCAL_DISK_ID_PREWARM_SKIPPED: &str = "local_disk_id_prewarm_skipped";
const EVENT_LOCK_CLIENT_INITIALIZATION_FAILED: &str = "lock_client_initialization_failed";

async fn remember_local_disk_id(disk: &DiskStore) -> Option<Uuid> {
    let disk_id = disk.get_disk_id().await.ok().flatten()?;
    GLOBAL_LOCAL_DISK_ID_MAP
        .write()
        .await
        .insert(disk_id, disk.endpoint().to_string());
    Some(disk_id)
}

pub async fn find_local_disk(disk_path: &String) -> Option<DiskStore> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;

    if let Some(disk) = disk_map.get(disk_path) {
        disk.as_ref().cloned()
    } else {
        None
    }
}

pub async fn find_local_disk_by_ref(disk_ref: &str) -> Option<DiskStore> {
    if let Some(disk) = find_local_disk(&disk_ref.to_string()).await {
        let _ = remember_local_disk_id(&disk).await;
        return Some(disk);
    }

    let Ok(disk_id) = Uuid::parse_str(disk_ref) else {
        return None;
    };

    if let Some(disk_path) = GLOBAL_LOCAL_DISK_ID_MAP.read().await.get(&disk_id).cloned()
        && let Some(disk) = find_local_disk(&disk_path).await
    {
        return Some(disk);
    }

    for disk in all_local_disk().await {
        if remember_local_disk_id(&disk).await == Some(disk_id) {
            return Some(disk);
        }
    }

    None
}

pub async fn get_disk_via_endpoint(endpoint: &Endpoint) -> Option<DiskStore> {
    let global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.read().await;
    if global_set_drives.is_empty() {
        return GLOBAL_LOCAL_DISK_MAP
            .read()
            .await
            .get(&endpoint.to_string())
            .cloned()
            .unwrap_or(None);
    }
    global_set_drives
        .get(endpoint.pool_idx as usize)
        .and_then(|sets| sets.get(endpoint.set_idx as usize))
        .and_then(|disks| disks.get(endpoint.disk_idx as usize))
        .cloned()
        .unwrap_or(None)
}

pub async fn all_local_disk_path() -> Vec<String> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;
    disk_map.keys().cloned().collect()
}

pub async fn all_local_disk() -> Vec<DiskStore> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;
    disk_map
        .values()
        .filter(|v| v.is_some())
        .map(|v| v.as_ref().unwrap().clone())
        .collect()
}

pub async fn prewarm_local_disk_id_map() {
    for disk in all_local_disk().await {
        if let Err(err) = disk.get_disk_id().await {
            debug!(
                event = EVENT_LOCAL_DISK_ID_PREWARM_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
                disk_endpoint = %disk.endpoint(),
                error = %err,
                "Skipped local disk id prewarm"
            );
            continue;
        }

        let _ = remember_local_disk_id(&disk).await;
    }
}

pub async fn init_local_disks(endpoint_pools: EndpointServerPools) -> Result<()> {
    let opt = &DiskOption {
        cleanup: true,
        health_check: true,
    };

    let mut global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.write().await;
    for pool_eps in endpoint_pools.as_ref().iter() {
        let mut set_count_drives = Vec::with_capacity(pool_eps.set_count);
        for _ in 0..pool_eps.set_count {
            set_count_drives.push(vec![None; pool_eps.drives_per_set]);
        }

        global_set_drives.push(set_count_drives);
    }

    let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;

    for pool_eps in endpoint_pools.as_ref().iter() {
        for ep in pool_eps.endpoints.as_ref().iter() {
            if !ep.is_local {
                continue;
            }

            let disk = new_disk(ep, opt).await?;

            let path = disk.endpoint().to_string();

            global_local_disk_map.insert(path, Some(disk.clone()));

            global_set_drives[ep.pool_idx as usize][ep.set_idx as usize][ep.disk_idx as usize] = Some(disk.clone());
        }
    }

    Ok(())
}

pub fn init_lock_clients(endpoint_pools: EndpointServerPools) {
    let mut unique_endpoints: HashMap<String, &Endpoint> = HashMap::new();

    for pool_eps in endpoint_pools.as_ref().iter() {
        for ep in pool_eps.endpoints.as_ref().iter() {
            unique_endpoints.insert(ep.host_port(), ep);
        }
    }

    let mut clients = HashMap::new();
    let mut first_local_client_set = false;

    for (key, endpoint) in unique_endpoints {
        if endpoint.is_local {
            let local_client = Arc::new(LocalClient::new()) as Arc<dyn LockClient>;

            // Store the first LocalClient globally for use by other modules
            if !first_local_client_set {
                if let Err(e) = crate::global::set_global_lock_client(local_client.clone()) {
                    // If already set, ignore the error (another thread may have set it)
                    debug!(
                        event = EVENT_LOCK_CLIENT_INITIALIZATION_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
                        error = ?e,
                        reason = "global_lock_client_already_set",
                        "Skipped global lock client publication"
                    );
                } else {
                    first_local_client_set = true;
                }
            }

            clients.insert(key, local_client);
        } else {
            clients.insert(key, Arc::new(RemoteClient::new(endpoint.url.to_string())) as Arc<dyn LockClient>);
        }
    }

    // Store the lock clients map globally
    if crate::global::set_global_lock_clients(clients).is_err() {
        error!(
            event = EVENT_LOCK_CLIENT_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK_STARTUP,
            reason = "set_global_lock_clients_failed",
            "Failed to initialize lock clients"
        );
    }
}

pub(super) async fn init_local_peer(endpoint_pools: &EndpointServerPools, host: &String, port: &String) {
    let mut peer_set = Vec::new();
    endpoint_pools.as_ref().iter().for_each(|endpoints| {
        endpoints.endpoints.as_ref().iter().for_each(|endpoint| {
            if endpoint.get_type() == EndpointType::Url && endpoint.is_local && endpoint.url.has_host() {
                peer_set.push(endpoint.url.host_str().unwrap().to_string());
            }
        });
    });

    if peer_set.is_empty() {
        if !host.is_empty() {
            *GLOBAL_LOCAL_NODE_NAME.write().await = format!("{host}:{port}");
            return;
        }

        *GLOBAL_LOCAL_NODE_NAME.write().await = format!("127.0.0.1:{port}");
        return;
    }

    *GLOBAL_LOCAL_NODE_NAME.write().await = peer_set[0].clone();
}

pub async fn get_disk_infos(disks: &[Option<DiskStore>]) -> Vec<Option<DiskInfo>> {
    let opts = &DiskInfoOptions::default();
    let mut res = vec![None; disks.len()];
    for (idx, disk_op) in disks.iter().enumerate() {
        if let Some(disk) = disk_op
            && let Ok(info) = disk.disk_info(opts).await
        {
            res[idx] = Some(info);
        }
    }

    res
}
