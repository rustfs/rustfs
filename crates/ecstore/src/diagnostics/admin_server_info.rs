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

use crate::cluster::rpc::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client};
use crate::data_usage::{DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, load_data_usage_from_backend_cached};
use crate::error::{Error, Result};
use crate::{
    disk::endpoint::{Endpoint, EndpointType},
    layout::endpoints::EndpointServerPools,
    runtime::sources as runtime_sources,
};

use crate::data_usage::load_data_usage_cache;
use crate::storage_api_contracts::admin::StorageAdminApi;
use rustfs_common::heal_channel::DriveState;
use rustfs_madmin::{
    BackendDisks, Disk, ErasureSetInfo, ITEM_INITIALIZING, ITEM_OFFLINE, ITEM_ONLINE, ITEM_UNKNOWN, InfoMessage, MemStats,
    ServerProperties,
};
use rustfs_protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{PingRequest, PingResponse},
};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::time::timeout;
use tonic::Request;
use tracing::warn;

use shadow_rs::shadow;

shadow!(build);

const SERVER_PING_TIMEOUT: Duration = Duration::from_secs(1);
const DATA_USAGE_UNAVAILABLE_ERROR: &str = "data usage snapshot unavailable";

fn apply_data_usage_result(
    result: Result<rustfs_data_usage::DataUsageInfo>,
    buckets: &mut rustfs_madmin::Buckets,
    objects: &mut rustfs_madmin::Objects,
    versions: &mut rustfs_madmin::Versions,
    delete_markers: &mut rustfs_madmin::DeleteMarkers,
    usage: &mut rustfs_madmin::Usage,
) {
    match result {
        Ok(info) => {
            buckets.count = info.buckets_count;
            objects.count = info.objects_total_count;
            versions.count = info.versions_total_count;
            delete_markers.count = info.delete_markers_total_count;
            usage.size = info.objects_total_size;
        }
        Err(_) => {
            buckets.error = Some(DATA_USAGE_UNAVAILABLE_ERROR.to_string());
            objects.error = Some(DATA_USAGE_UNAVAILABLE_ERROR.to_string());
            versions.error = Some(DATA_USAGE_UNAVAILABLE_ERROR.to_string());
            delete_markers.error = Some(DATA_USAGE_UNAVAILABLE_ERROR.to_string());
            usage.error = Some(DATA_USAGE_UNAVAILABLE_ERROR.to_string());
        }
    }
}

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
        endpoint.url.host_str().expect("URL should have host"),
        // `Url::port()` is None when the URL uses the scheme's default port
        // (e.g. http on 80 / https on 443); fall back to the scheme default.
        endpoint.url.port_or_known_default().expect("URL should have port")
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

        let mut client = node_service_time_out_client(&addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;

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
    let addr = runtime_sources::local_node_name().await;
    let mut pool_numbers = HashSet::new();
    let mut network = HashMap::new();
    let (mem_stats, max_procs, num_cpu) = collect_runtime_server_stats();

    let endpoints = match runtime_sources::endpoint_pools() {
        Some(eps) => eps,
        None => {
            return ServerProperties {
                state: ITEM_INITIALIZING.to_string(),
                endpoint: addr,
                uptime: runtime_sources::boot_uptime_secs(),
                version: get_commit_id(),
                mem_stats,
                max_procs,
                num_cpu,
                ..Default::default()
            };
        }
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

    let mut props = ServerProperties {
        endpoint: addr,
        uptime: runtime_sources::boot_uptime_secs(),
        network,
        version: get_commit_id(),
        mem_stats,
        max_procs,
        num_cpu,
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
    // sensitive.insert(rustfs_config::ENV_RUSTFS_ACCESS_KEY.to_string());
    // sensitive.insert(rustfs_config::ENV_RUSTFS_SECRET_KEY.to_string());
    if let Some(store) = runtime_sources::object_store_handle() {
        let storage_info = StorageAdminApi::local_storage_info(store.as_ref()).await;
        props.state = ITEM_ONLINE.to_string();
        props.disks = storage_info.disks;
    } else {
        props.state = ITEM_INITIALIZING.to_string();
    };

    props
}

fn collect_runtime_server_stats() -> (MemStats, u64, u64) {
    let num_cpu = u64::try_from(num_cpus::get()).unwrap_or(u64::MAX);
    let max_procs = std::thread::available_parallelism()
        .map(|parallelism| u64::try_from(parallelism.get()).unwrap_or(u64::MAX))
        .unwrap_or(num_cpu.max(1));

    (rustfs_madmin::health::collect_mem_stats(), max_procs, num_cpu)
}

pub async fn get_server_info(get_pools: bool) -> InfoMessage {
    let nowt: OffsetDateTime = OffsetDateTime::now_utc();

    warn!("get_server_info start {:?}", nowt);

    let local = get_local_server_property().await;

    let after1 = OffsetDateTime::now_utc();

    warn!("get_local_server_property end {:?}", after1 - nowt);

    let mut servers = {
        if let Some(sys) = runtime_sources::notification_sys() {
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

    if let Some(store) = runtime_sources::object_store_handle() {
        mode = ITEM_ONLINE;
        apply_data_usage_result(
            load_data_usage_from_backend_cached(store.clone()).await,
            &mut buckets,
            &mut objects,
            &mut versions,
            &mut delete_markers,
            &mut usage,
        );

        let after3 = OffsetDateTime::now_utc();

        warn!("load_data_usage_from_backend end {:?}", after3 - after2);

        let backend_info = StorageAdminApi::backend_info(store.as_ref()).await;

        let after4 = OffsetDateTime::now_utc();

        warn!("backend_info end {:?}", after4 - after3);
        if let Some(endpoints) = runtime_sources::endpoint_pools() {
            let added = reconcile_servers_with_endpoint_topology(&mut servers, &endpoints);
            let report = server_topology_completeness_report(&servers, &endpoints);
            if added > 0 || !report.is_complete() {
                warn!(
                    event = "admin_v3_info_topology_incomplete",
                    synthesized_servers = added,
                    expected_drives = report.expected_drives,
                    observed_drives = report.observed_drives,
                    missing_drives = report.missing_drive_ids.len(),
                    duplicate_drives = report.duplicate_drive_ids.len(),
                    "admin v3 server_info reconciled endpoint topology before computing backend counters"
                );
            }
        }

        let mut all_disks: Vec<Disk> = Vec::new();
        for server in servers.iter() {
            all_disks.extend(server.disks.clone());
        }
        let (online_disks, offline_disks, unknown_disks) = get_online_offline_disks_stats(&all_disks);

        let after5 = OffsetDateTime::now_utc();

        warn!("get_online_offline_disks_stats end {:?}", after5 - after4);
        backend = rustfs_madmin::ErasureBackend {
            backend_type: rustfs_madmin::BackendType::ErasureType,
            online_disks: online_disks.sum(),
            offline_disks: offline_disks.sum(),
            unknown_disks: unknown_disks.sum(),
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
        deployment_id: runtime_sources::deployment_id(),
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

/// Classify every drive into online / offline / unknown buckets.
///
/// `unknown` holds drives synthesized for a member whose properties RPC could
/// not be answered this cycle but which is not confirmed offline. Keeping them
/// out of the `offline` bucket means a transient probe miss no longer inflates
/// the offline count for a healthy member, while `online + offline + unknown`
/// still sums to the pool's total drive count (rustfs/backlog#1049).
fn get_online_offline_disks_stats(disks_info: &[Disk]) -> (BackendDisks, BackendDisks, BackendDisks) {
    let mut online_disks: HashMap<String, usize> = HashMap::new();
    let mut offline_disks: HashMap<String, usize> = HashMap::new();
    let mut unknown_disks: HashMap<String, usize> = HashMap::new();

    for disk in disks_info {
        let ep = &disk.endpoint;
        offline_disks.entry(ep.clone()).or_insert(0);
        online_disks.entry(ep.clone()).or_insert(0);
        unknown_disks.entry(ep.clone()).or_insert(0);
    }

    for disk in disks_info {
        let ep = &disk.endpoint;
        let state = &disk.state;
        if *state == ITEM_UNKNOWN {
            *unknown_disks.get_mut(ep).expect("endpoint should be in disk map") += 1;
            continue;
        }
        if *state != DriveState::Ok.to_string() && *state != DriveState::Unformatted.to_string() {
            *offline_disks.get_mut(ep).expect("endpoint should be in disk map") += 1;
            continue;
        }
        *online_disks.get_mut(ep).expect("endpoint should be in disk map") += 1;
    }

    let mut root_disk_count = 0;
    for di in disks_info {
        if di.root_disk {
            root_disk_count += 1;
        }
    }

    // When every non-offline, non-unknown drive is a root mount, leave the
    // online tally as-is instead of demoting all of them (matches the prior
    // behavior; the unknown bucket is simply carried through untouched).
    if disks_info.len() == (root_disk_count + offline_disks.values().sum::<usize>() + unknown_disks.values().sum::<usize>()) {
        return (BackendDisks(online_disks), BackendDisks(offline_disks), BackendDisks(unknown_disks));
    }

    for disk in disks_info {
        let ep = &disk.endpoint;
        if disk.root_disk {
            *offline_disks.get_mut(ep).expect("endpoint should be in disk map") += 1;
            *online_disks.get_mut(ep).expect("endpoint should be in disk map") -= 1;
        }
    }

    (BackendDisks(online_disks), BackendDisks(offline_disks), BackendDisks(unknown_disks))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TopologyCompletenessReport {
    expected_drives: usize,
    observed_drives: usize,
    missing_drive_ids: Vec<String>,
    duplicate_drive_ids: Vec<String>,
}

impl TopologyCompletenessReport {
    fn is_complete(&self) -> bool {
        self.expected_drives == self.observed_drives && self.missing_drive_ids.is_empty() && self.duplicate_drive_ids.is_empty()
    }
}

#[derive(Debug)]
struct TopologyMember {
    display_endpoint: String,
    disks: Vec<Disk>,
}

fn reconcile_servers_with_endpoint_topology(servers: &mut Vec<ServerProperties>, endpoints: &EndpointServerPools) -> usize {
    let (members, aliases) = topology_members(endpoints);
    if members.is_empty() {
        return 0;
    }

    let mut observed = HashSet::with_capacity(members.len());
    for server in servers.iter() {
        collect_observed_topology_members(server, &aliases, &mut observed);
    }

    let mut missing: Vec<_> = members
        .into_iter()
        .filter(|(host, _)| !observed.contains(host))
        .map(|(_, member)| ServerProperties {
            endpoint: member.display_endpoint,
            state: ITEM_UNKNOWN.to_string(),
            disks: member.disks,
            ..Default::default()
        })
        .collect();
    missing.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));

    let added = missing.len();
    servers.extend(missing);
    added
}

fn topology_members(endpoints: &EndpointServerPools) -> (HashMap<String, TopologyMember>, HashMap<String, String>) {
    let mut members: HashMap<String, TopologyMember> = HashMap::new();
    let mut aliases = HashMap::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if ep.get_type() != EndpointType::Url {
                continue;
            }
            let host_port = ep.host_port();
            if host_port.is_empty() {
                continue;
            }
            let display_endpoint = ep.url.host_str().map(str::to_owned).unwrap_or_else(|| host_port.clone());
            aliases.entry(host_port.clone()).or_insert_with(|| host_port.clone());
            aliases.entry(display_endpoint.clone()).or_insert_with(|| host_port.clone());
            aliases.entry(ep.to_string()).or_insert_with(|| host_port.clone());

            members
                .entry(host_port)
                .or_insert_with(|| TopologyMember {
                    display_endpoint,
                    disks: Vec::new(),
                })
                .disks
                .push(Disk {
                    endpoint: ep.to_string(),
                    state: ITEM_UNKNOWN.to_string(),
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
        }
    }

    (members, aliases)
}

fn collect_observed_topology_members(
    server: &ServerProperties,
    aliases: &HashMap<String, String>,
    observed: &mut HashSet<String>,
) {
    if let Some(host) = aliases.get(&server.endpoint) {
        observed.insert(host.clone());
    }

    for disk in &server.disks {
        if let Some(host) = topology_host_from_disk_endpoint(&disk.endpoint, aliases) {
            observed.insert(host);
        }
    }
}

fn topology_host_from_disk_endpoint(endpoint: &str, aliases: &HashMap<String, String>) -> Option<String> {
    if let Some(host) = aliases.get(endpoint) {
        return Some(host.clone());
    }

    Endpoint::try_from(endpoint)
        .ok()
        .and_then(|ep| aliases.get(&ep.host_port()).cloned())
}

fn server_topology_completeness_report(
    servers: &[ServerProperties],
    endpoints: &EndpointServerPools,
) -> TopologyCompletenessReport {
    let expected = expected_topology_drive_ids(endpoints);
    if expected.is_empty() {
        return TopologyCompletenessReport::default();
    }

    let mut observed_counts: HashMap<String, usize> = HashMap::with_capacity(expected.len());
    for server in servers {
        for disk in &server.disks {
            if let Some(id) = server_disk_topology_id(disk)
                && expected.contains(&id)
            {
                *observed_counts.entry(id).or_insert(0) += 1;
            }
        }
    }

    let mut missing_drive_ids = Vec::new();
    let mut duplicate_drive_ids = Vec::new();
    for id in &expected {
        match observed_counts.get(id).copied().unwrap_or(0) {
            0 => missing_drive_ids.push(id.clone()),
            1 => {}
            _ => duplicate_drive_ids.push(id.clone()),
        }
    }
    missing_drive_ids.sort();
    duplicate_drive_ids.sort();

    TopologyCompletenessReport {
        expected_drives: expected.len(),
        observed_drives: observed_counts.values().sum(),
        missing_drive_ids,
        duplicate_drive_ids,
    }
}

fn expected_topology_drive_ids(endpoints: &EndpointServerPools) -> HashSet<String> {
    let mut ids = HashSet::new();
    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if ep.get_type() != EndpointType::Url {
                continue;
            }
            if let Some(id) = endpoint_topology_drive_id(ep) {
                ids.insert(id);
            }
        }
    }
    ids
}

fn endpoint_topology_drive_id(endpoint: &Endpoint) -> Option<String> {
    let host_port = endpoint.host_port();
    if host_port.is_empty() {
        return None;
    }
    Some(format!("{}:{}:{}:{host_port}", endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx))
}

fn server_disk_topology_id(disk: &Disk) -> Option<String> {
    let endpoint = Endpoint::try_from(disk.endpoint.as_str()).ok()?;
    let host_port = endpoint.host_port();
    if host_port.is_empty() {
        return None;
    }
    Some(format!("{}:{}:{}:{host_port}", disk.pool_index, disk.set_index, disk.disk_index))
}

async fn get_pools_info(all_disks: &[Disk]) -> Result<HashMap<i32, HashMap<i32, ErasureSetInfo>>> {
    let Some(store) = runtime_sources::object_store_handle() else {
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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::layout::{
        endpoint::Endpoint,
        endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
    use crate::runtime::sources as runtime_sources;
    use rustfs_madmin::{Disk, ITEM_OFFLINE, ITEM_ONLINE, ITEM_UNKNOWN, ServerProperties};

    use super::{
        DATA_USAGE_UNAVAILABLE_ERROR, apply_data_usage_result, get_local_server_property, get_online_offline_disks_stats,
        get_server_info, reconcile_servers_with_endpoint_topology, server_topology_completeness_report,
    };

    fn disk_with_state(endpoint: &str, state: &str) -> Disk {
        Disk {
            endpoint: endpoint.to_string(),
            state: state.to_string(),
            ..Default::default()
        }
    }

    fn topology_endpoint(host: &str, pool_index: usize, set_index: usize, disk_index: usize) -> Endpoint {
        let mut endpoint =
            Endpoint::try_from(format!("http://{host}:9000/data{disk_index}").as_str()).expect("URL endpoint should parse");
        endpoint.set_pool_index(pool_index);
        endpoint.set_set_index(set_index);
        endpoint.set_disk_index(disk_index);
        endpoint
    }

    fn topology_with_hosts(hosts: &[&str]) -> EndpointServerPools {
        let endpoints: Vec<Endpoint> = hosts
            .iter()
            .enumerate()
            .map(|(disk_index, host)| topology_endpoint(host, 0, 0, disk_index))
            .collect();
        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: hosts.len(),
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }])
    }

    fn server_with_disk(host: &str, disk_index: i32, state: &str) -> ServerProperties {
        ServerProperties {
            endpoint: host.to_string(),
            state: ITEM_ONLINE.to_string(),
            disks: vec![Disk {
                endpoint: format!("http://{host}:9000/data{disk_index}"),
                state: state.to_string(),
                pool_index: 0,
                set_index: 0,
                disk_index,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn disk_stats_split_unknown_into_its_own_bucket() {
        // A member whose properties RPC could not be answered contributes
        // drives tagged `unknown`. They must land in the unknown bucket, not
        // inflate `offline`, and the three buckets must still account for every
        // drive so the summary stays balanced (rustfs/backlog#1049).
        //
        // A live drive reports the DriveState string "ok"; only "ok"/"unformatted"
        // count as online.
        let disks = vec![
            disk_with_state("http://n1:9000/data", "ok"),
            disk_with_state("http://n2:9000/data", "ok"),
            disk_with_state("http://n3:9000/data", ITEM_OFFLINE),
            disk_with_state("http://n4:9000/data", ITEM_UNKNOWN),
        ];

        let (online, offline, unknown) = get_online_offline_disks_stats(&disks);

        assert_eq!(online.sum(), 2, "the two healthy drives are online");
        assert_eq!(offline.sum(), 1, "only the confirmed-offline drive is offline");
        assert_eq!(unknown.sum(), 1, "the unreachable member's drive is unknown, not offline");
        assert_eq!(
            online.sum() + offline.sum() + unknown.sum(),
            disks.len(),
            "online + offline + unknown must equal the total drive count"
        );
    }

    #[test]
    fn topology_reconcile_synthesizes_missing_member_as_unknown() {
        let endpoints = topology_with_hosts(&["rustfs-1", "rustfs-2", "rustfs-3", "rustfs-4"]);
        let mut servers = vec![
            server_with_disk("rustfs-1", 0, "ok"),
            server_with_disk("rustfs-2", 1, "ok"),
            server_with_disk("rustfs-3", 2, "ok"),
        ];

        let added = reconcile_servers_with_endpoint_topology(&mut servers, &endpoints);
        let (_, _, unknown) =
            get_online_offline_disks_stats(&servers.iter().flat_map(|server| server.disks.clone()).collect::<Vec<_>>());

        assert_eq!(added, 1, "the missing fourth topology member must be synthesized");
        assert_eq!(servers.len(), 4, "v3 server list must preserve topology membership length");
        let synthesized = servers
            .iter()
            .find(|server| server.endpoint == "rustfs-4")
            .expect("missing member should be present");
        assert_eq!(synthesized.state, ITEM_UNKNOWN);
        assert_eq!(synthesized.disks.len(), 1);
        assert_eq!(synthesized.disks[0].endpoint, "http://rustfs-4:9000/data3");
        assert_eq!(synthesized.disks[0].disk_index, 3);
        assert_eq!(unknown.sum(), 1, "the synthesized drive must land in unknownDisks");
    }

    #[test]
    fn topology_reconcile_does_not_duplicate_existing_synthesized_rows() {
        let endpoints = topology_with_hosts(&["rustfs-1", "rustfs-2"]);
        let mut servers = vec![
            server_with_disk("rustfs-1", 0, "ok"),
            server_with_disk("rustfs-2", 1, ITEM_UNKNOWN),
        ];
        servers[1].state = ITEM_UNKNOWN.to_string();

        let added = reconcile_servers_with_endpoint_topology(&mut servers, &endpoints);

        assert_eq!(
            added, 0,
            "an existing unknown/degraded/offline row with topology drives already represents the member"
        );
        assert_eq!(servers.len(), 2);
    }

    #[test]
    fn topology_report_detects_duplicate_drive_identity_with_balanced_total() {
        let endpoints = topology_with_hosts(&["rustfs-1", "rustfs-2"]);
        let servers = vec![server_with_disk("rustfs-1", 0, "ok"), server_with_disk("rustfs-1", 0, "ok")];

        let report = server_topology_completeness_report(&servers, &endpoints);

        assert_eq!(report.expected_drives, 2);
        assert_eq!(report.observed_drives, 2, "a plain total-count check would look balanced");
        assert_eq!(report.missing_drive_ids.len(), 1, "rustfs-2's drive identity is absent");
        assert_eq!(report.duplicate_drive_ids.len(), 1, "rustfs-1's drive identity is duplicated");
        assert!(!report.is_complete());
    }

    #[test]
    fn data_usage_errors_are_sanitized_in_server_info() {
        let mut buckets = rustfs_madmin::Buckets::default();
        let mut objects = rustfs_madmin::Objects::default();
        let mut versions = rustfs_madmin::Versions::default();
        let mut delete_markers = rustfs_madmin::DeleteMarkers::default();
        let mut usage = rustfs_madmin::Usage::default();

        apply_data_usage_result(
            Err(crate::error::Error::other("sensitive disk path")),
            &mut buckets,
            &mut objects,
            &mut versions,
            &mut delete_markers,
            &mut usage,
        );

        assert_eq!(buckets.error.as_deref(), Some(DATA_USAGE_UNAVAILABLE_ERROR));
        assert_eq!(objects.error.as_deref(), Some(DATA_USAGE_UNAVAILABLE_ERROR));
        assert_eq!(versions.error.as_deref(), Some(DATA_USAGE_UNAVAILABLE_ERROR));
        assert_eq!(delete_markers.error.as_deref(), Some(DATA_USAGE_UNAVAILABLE_ERROR));
        assert_eq!(usage.error.as_deref(), Some(DATA_USAGE_UNAVAILABLE_ERROR));
    }

    #[test]
    fn data_usage_counts_are_mapped_into_server_info() {
        let mut buckets = rustfs_madmin::Buckets::default();
        let mut objects = rustfs_madmin::Objects::default();
        let mut versions = rustfs_madmin::Versions::default();
        let mut delete_markers = rustfs_madmin::DeleteMarkers::default();
        let mut usage = rustfs_madmin::Usage::default();
        let info = rustfs_data_usage::DataUsageInfo {
            buckets_count: 2,
            objects_total_count: 3,
            versions_total_count: 4,
            delete_markers_total_count: 5,
            objects_total_size: 6,
            ..Default::default()
        };

        apply_data_usage_result(Ok(info), &mut buckets, &mut objects, &mut versions, &mut delete_markers, &mut usage);

        assert_eq!(buckets.count, 2);
        assert_eq!(objects.count, 3);
        assert_eq!(versions.count, 4);
        assert_eq!(delete_markers.count, 5);
        assert_eq!(usage.size, 6);
    }

    #[serial]
    #[tokio::test]
    async fn server_info_includes_global_deployment_id() {
        let expected_deployment_id = runtime_sources::deployment_id();
        let info = get_server_info(false).await;

        assert_eq!(info.deployment_id, expected_deployment_id);
    }

    #[serial]
    #[tokio::test]
    async fn local_server_property_includes_runtime_stats_without_endpoint_pools() {
        let props = get_local_server_property().await;

        assert!(props.num_cpu > 0);
        assert!(props.max_procs > 0);
        assert!(
            props.mem_stats.alloc > 0 || props.mem_stats.total_alloc > 0 || props.mem_stats.heap_alloc > 0,
            "memory stats should not remain fixed placeholders"
        );
    }
}
