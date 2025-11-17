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

use crate::data_usage::{DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, load_data_usage_from_backend};
use crate::error::{Error, Result};
use crate::{
    disk::endpoint::Endpoint,
    global::{GLOBAL_BOOT_TIME, GLOBAL_Endpoints},
    new_object_layer_fn,
    notification_sys::get_global_notification_sys,
    store_api::StorageAPI,
};

use crate::data_usage::load_data_usage_cache;
use rustfs_common::{globals::GLOBAL_Local_Node_Name, heal_channel::DriveState};
use rustfs_madmin::{
    BackendDisks, Disk, ErasureSetInfo, ITEM_INITIALIZING, ITEM_OFFLINE, ITEM_ONLINE, InfoMessage, ServerProperties,
};
use rustfs_protos::{
    models::{PingBody, PingBodyBuilder},
    node_service_time_out_client,
    proto_gen::node_service::{PingRequest, PingResponse},
};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};
use time::OffsetDateTime;
use tokio::time::timeout;
use tonic::Request;
use tracing::warn;

use shadow_rs::shadow;

shadow!(build);

const SERVER_PING_TIMEOUT: Duration = Duration::from_secs(1);

// pub const ITEM_OFFLINE: &str = "offline";
// pub const ITEM_INITIALIZING: &str = "initializing";
// pub const ITEM_ONLINE: &str = "online";

// #[derive(Debug, Default, Serialize, Deserialize)]
// pub struct MemStats {
//     alloc: u64,
//     total_alloc: u64,
//     mallocs: u64,
//     frees: u64,
//     heap_alloc: u64,
// }

// #[derive(Debug, Default, Serialize, Deserialize)]
// pub struct ServerProperties {
//     pub state: String,
//     pub endpoint: String,
//     pub scheme: String,
//     pub uptime: u64,
//     pub version: String,
//     pub commit_id: String,
//     pub network: HashMap<String, String>,
//     pub disks: Vec<madmin::Disk>,
//     pub pool_number: i32,
//     pub pool_numbers: Vec<i32>,
//     pub mem_stats: MemStats,
//     pub max_procs: u64,
//     pub num_cpu: u64,
//     pub runtime_version: String,
//     pub rustfs_env_vars: HashMap<String, String>,
// }

async fn is_server_resolvable(endpoint: &Endpoint) -> Result<()> {
    let addr = format!(
        "{}://{}:{}",
        endpoint.url.scheme(),
        endpoint.url.host_str().unwrap(),
        endpoint.url.port().unwrap()
    );

    let ping_task = async {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello world");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        let decoded_payload = flatbuffers::root::<PingBody>(finished_data);
        assert!(decoded_payload.is_ok());

        let mut client = node_service_time_out_client(&addr)
            .await
            .map_err(|err| Error::other(err.to_string()))?;

        let request = Request::new(PingRequest {
            version: 1,
            body: bytes::Bytes::copy_from_slice(finished_data),
        });

        let response: PingResponse = client.ping(request).await?.into_inner();

        let ping_response_body = flatbuffers::root::<PingBody>(&response.body);
        if let Err(e) = ping_response_body {
            eprintln!("{e}");
        } else {
            println!("ping_resp:body(flatbuffer): {ping_response_body:?}");
        }

        Ok(())
    };

    timeout(SERVER_PING_TIMEOUT, ping_task)
        .await
        .map_err(|_| Error::other("server ping timeout"))?
}

pub async fn get_local_server_property() -> ServerProperties {
    let addr = GLOBAL_Local_Node_Name.read().await.clone();
    let mut pool_numbers = HashSet::new();
    let mut network = HashMap::new();

    let endpoints = match GLOBAL_Endpoints.get() {
        Some(eps) => eps,
        None => return ServerProperties::default(),
    };
    for ep in endpoints.as_ref().iter() {
        for endpoint in ep.endpoints.as_ref().iter() {
            let node_name = match endpoint.url.host_str() {
                Some(s) => s.to_string(),
                None => addr.clone(),
            };
            if endpoint.is_local {
                pool_numbers.insert(endpoint.pool_idx + 1);
                network.insert(node_name, ITEM_ONLINE.to_string());
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(e) = network.entry(node_name) {
                if is_server_resolvable(endpoint).await.is_err() {
                    e.insert(ITEM_OFFLINE.to_string());
                } else {
                    e.insert(ITEM_ONLINE.to_string());
                }
            }
        }
    }

    // todo: mem collect
    // let mem_stats =

    let mut props = ServerProperties {
        endpoint: addr,
        uptime: SystemTime::now()
            .duration_since(*GLOBAL_BOOT_TIME.get().unwrap())
            .unwrap_or_default()
            .as_secs(),
        network,
        version: get_commit_id(),
        ..Default::default()
    };

    for pool_num in pool_numbers.iter() {
        props.pool_numbers.push(*pool_num);
    }
    props.pool_numbers.sort();
    props.pool_number = if props.pool_numbers.len() == 1 {
        props.pool_numbers[0]
    } else {
        i32::MAX
    };

    // let mut sensitive = HashSet::new();
    // sensitive.insert(ENV_ACCESS_KEY.to_string());
    // sensitive.insert(ENV_SECRET_KEY.to_string());
    // sensitive.insert(ENV_ROOT_USER.to_string());
    // sensitive.insert(ENV_ROOT_PASSWORD.to_string());

    if let Some(store) = new_object_layer_fn() {
        let storage_info = store.local_storage_info().await;
        props.state = ITEM_ONLINE.to_string();
        props.disks = storage_info.disks;
    } else {
        props.state = ITEM_INITIALIZING.to_string();
    };

    props
}

pub async fn get_server_info(get_pools: bool) -> InfoMessage {
    let nowt: OffsetDateTime = OffsetDateTime::now_utc();

    warn!("get_server_info start {:?}", nowt);

    let local = get_local_server_property().await;

    let after1 = OffsetDateTime::now_utc();

    warn!("get_local_server_property end {:?}", after1 - nowt);

    let mut servers = {
        if let Some(sys) = get_global_notification_sys() {
            sys.server_info().await
        } else {
            vec![]
        }
    };

    let after2 = OffsetDateTime::now_utc();

    warn!("server_info end {:?}", after2 - after1);
    servers.push(local);

    let mut buckets = rustfs_madmin::Buckets::default();
    let mut objects = rustfs_madmin::Objects::default();
    let mut versions = rustfs_madmin::Versions::default();
    let mut delete_markers = rustfs_madmin::DeleteMarkers::default();
    let mut usage = rustfs_madmin::Usage::default();
    let mut mode = ITEM_INITIALIZING;
    let mut backend = rustfs_madmin::ErasureBackend::default();
    let mut pools: HashMap<i32, HashMap<i32, ErasureSetInfo>> = HashMap::new();

    if let Some(store) = new_object_layer_fn() {
        mode = ITEM_ONLINE;
        match load_data_usage_from_backend(store.clone()).await {
            Ok(res) => {
                buckets.count = res.buckets_count;
                objects.count = res.objects_total_count;
                versions.count = res.versions_total_count;
                delete_markers.count = res.delete_markers_total_count;
                usage.size = res.objects_total_size;
            }
            Err(err) => {
                buckets.error = Some(err.to_string());
                objects.error = Some(err.to_string());
                versions.error = Some(err.to_string());
                delete_markers.error = Some(err.to_string());
                usage.error = Some(err.to_string());
            }
        }

        let after3 = OffsetDateTime::now_utc();

        warn!("load_data_usage_from_backend end {:?}", after3 - after2);

        let backend_info = store.clone().backend_info().await;

        let after4 = OffsetDateTime::now_utc();

        warn!("backend_info end {:?}", after4 - after3);

        let mut all_disks: Vec<Disk> = Vec::new();
        for server in servers.iter() {
            all_disks.extend(server.disks.clone());
        }
        let (online_disks, offline_disks) = get_online_offline_disks_stats(&all_disks);

        let after5 = OffsetDateTime::now_utc();

        warn!("get_online_offline_disks_stats end {:?}", after5 - after4);
        backend = rustfs_madmin::ErasureBackend {
            backend_type: rustfs_madmin::BackendType::ErasureType,
            online_disks: online_disks.sum(),
            offline_disks: offline_disks.sum(),
            standard_sc_parity: backend_info.standard_sc_parity,
            rr_sc_parity: backend_info.rr_sc_parity,
            total_sets: backend_info.total_sets,
            drives_per_set: backend_info.drives_per_set,
        };
        if get_pools {
            pools = get_pools_info(&all_disks).await.unwrap_or_default();
            let after6 = OffsetDateTime::now_utc();

            warn!("get_pools_info end {:?}", after6 - after5);
        }
    }

    let services = rustfs_madmin::Services::default();

    InfoMessage {
        mode: Some(mode.to_string()),
        domain: None,
        region: None,
        sqs_arn: None,
        deployment_id: None,
        buckets: Some(buckets),
        objects: Some(objects),
        versions: Some(versions),
        delete_markers: Some(delete_markers),
        usage: Some(usage),
        backend: Some(backend),
        services: Some(services),
        servers: Some(servers),
        pools: Some(pools),
    }
}

fn get_online_offline_disks_stats(disks_info: &[Disk]) -> (BackendDisks, BackendDisks) {
    let mut online_disks: HashMap<String, usize> = HashMap::new();
    let mut offline_disks: HashMap<String, usize> = HashMap::new();

    for disk in disks_info {
        let ep = &disk.endpoint;
        offline_disks.entry(ep.clone()).or_insert(0);
        online_disks.entry(ep.clone()).or_insert(0);
    }

    for disk in disks_info {
        let ep = &disk.endpoint;
        let state = &disk.state;
        if *state != DriveState::Ok.to_string() && *state != DriveState::Unformatted.to_string() {
            *offline_disks.get_mut(ep).unwrap() += 1;
            continue;
        }
        *online_disks.get_mut(ep).unwrap() += 1;
    }

    let mut root_disk_count = 0;
    for di in disks_info {
        if di.root_disk {
            root_disk_count += 1;
        }
    }

    if disks_info.len() == (root_disk_count + offline_disks.values().sum::<usize>()) {
        return (BackendDisks(online_disks), BackendDisks(offline_disks));
    }

    for disk in disks_info {
        let ep = &disk.endpoint;
        if disk.root_disk {
            *offline_disks.get_mut(ep).unwrap() += 1;
            *online_disks.get_mut(ep).unwrap() -= 1;
        }
    }

    (BackendDisks(online_disks), BackendDisks(offline_disks))
}

async fn get_pools_info(all_disks: &[Disk]) -> Result<HashMap<i32, HashMap<i32, ErasureSetInfo>>> {
    let Some(store) = new_object_layer_fn() else {
        return Err(Error::other("ServerNotInitialized"));
    };

    let mut pools_info: HashMap<i32, HashMap<i32, ErasureSetInfo>> = HashMap::new();
    for d in all_disks {
        let pool_info = pools_info.entry(d.pool_index).or_default();
        let erasure_set = pool_info.entry(d.set_index).or_default();

        if erasure_set.id == 0 {
            erasure_set.id = d.set_index;
            if let Ok(cache) = load_data_usage_cache(
                &store.pools[d.pool_index as usize].disk_set[d.set_index as usize].clone(),
                DATA_USAGE_CACHE_NAME,
            )
            .await
            {
                let data_usage_info = cache.dui(DATA_USAGE_ROOT, &Vec::<String>::new());
                erasure_set.objects_count = data_usage_info.objects_total_count;
                erasure_set.versions_count = data_usage_info.versions_total_count;
                erasure_set.delete_markers_count = data_usage_info.delete_markers_total_count;
                erasure_set.usage = data_usage_info.objects_total_size;
            };
        }

        erasure_set.raw_capacity += d.total_space;
        erasure_set.raw_usage += d.used_space;
        if d.healing {
            erasure_set.heal_disks = 1;
        }
    }
    Ok(pools_info)
}

#[allow(clippy::const_is_empty)]
pub fn get_commit_id() -> String {
    let ver = if !build::TAG.is_empty() {
        build::TAG.to_string()
    } else if !build::SHORT_COMMIT.is_empty() {
        build::SHORT_COMMIT.to_string()
    } else {
        build::PKG_VERSION.to_string()
    };

    format!("{}@{}", build::COMMIT_DATE_3339, ver)
}
