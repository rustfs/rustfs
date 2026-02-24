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

pub async fn find_local_disk(disk_path: &String) -> Option<DiskStore> {
    let disk_map = GLOBAL_LOCAL_DISK_MAP.read().await;

    if let Some(disk) = disk_map.get(disk_path) {
        disk.as_ref().cloned()
    } else {
        None
    }
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
                    warn!("set_global_lock_client error: {:?}", e);
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
        error!("init_lock_clients: error setting lock clients");
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

pub async fn has_space_for(dis: &[Option<DiskInfo>], size: i64) -> Result<bool> {
    let size = { if size < 0 { DISK_ASSUME_UNKNOWN_SIZE } else { size as u64 * 2 } };

    let mut available = 0;
    let mut total = 0;
    let mut disks_num = 0;

    for disk in dis.iter().flatten() {
        disks_num += 1;
        total += disk.total;
        available += disk.total - disk.used;
    }

    if disks_num < dis.len() / 2 || disks_num == 0 {
        return Err(Error::other(format!(
            "not enough online disks to calculate the available space,need {}, found {}",
            (dis.len() / 2) + 1,
            disks_num,
        )));
    }

    let per_disk = size / disks_num as u64;

    for disk in dis.iter().flatten() {
        if !is_erasure_sd().await && disk.free_inodes < DISK_MIN_INODES && disk.used_inodes > 0 {
            return Ok(false);
        }

        if disk.free <= per_disk {
            return Ok(false);
        }
    }

    if available < size {
        return Ok(false);
    }

    available -= size;

    let want = total as f64 * (1.0 - DISK_FILL_FRACTION);

    Ok(available > want as u64)
}
