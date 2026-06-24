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
use crate::runtime_sources;
use tracing::{debug, error};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DISK_STARTUP: &str = "disk_startup";
const EVENT_LOCAL_DISK_ID_PREWARM_SKIPPED: &str = "local_disk_id_prewarm_skipped";
const EVENT_LOCK_CLIENT_INITIALIZATION_FAILED: &str = "lock_client_initialization_failed";

async fn remember_local_disk_id(disk: &DiskStore) -> Option<Uuid> {
    let disk_id = disk.get_disk_id().await.ok().flatten()?;
    runtime_sources::record_local_disk_id(disk_id, disk.endpoint().to_string()).await;
    Some(disk_id)
}

pub async fn find_local_disk(disk_path: &str) -> Option<DiskStore> {
    runtime_sources::local_disk_by_path(disk_path).await
}

pub async fn find_local_disk_by_ref(disk_ref: &str) -> Option<DiskStore> {
    if let Some(disk) = find_local_disk(disk_ref).await {
        let _ = remember_local_disk_id(&disk).await;
        return Some(disk);
    }

    let Ok(disk_id) = Uuid::parse_str(disk_ref) else {
        return None;
    };

    if let Some(disk_path) = runtime_sources::local_disk_path_by_id(&disk_id).await
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
    runtime_sources::local_disk_for_endpoint(endpoint).await
}

pub async fn all_local_disk_path() -> Vec<String> {
    runtime_sources::local_disk_paths().await
}

pub async fn all_local_disk() -> Vec<DiskStore> {
    runtime_sources::local_disks().await
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

    runtime_sources::initialize_local_disk_maps(endpoint_pools, opt).await
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
                if let Err(e) = runtime_sources::set_primary_lock_client(local_client.clone()) {
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
    if runtime_sources::set_lock_clients(clients).is_err() {
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
            runtime_sources::set_local_node_name(format!("{host}:{port}")).await;
            return;
        }

        runtime_sources::set_local_node_name(format!("127.0.0.1:{port}")).await;
        return;
    }

    runtime_sources::set_local_node_name(peer_set[0].clone()).await;
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
