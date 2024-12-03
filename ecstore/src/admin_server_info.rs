use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use common::{
    error::{Error, Result},
    globals::GLOBAL_Local_Node_Name,
};
use protos::{
    models::{PingBody, PingBodyBuilder},
    node_service_time_out_client,
    proto_gen::node_service::{PingRequest, PingResponse},
};
use serde::{Deserialize, Serialize};
use tonic::Request;

use crate::{
    disk::endpoint::Endpoint,
    global::GLOBAL_Endpoints,
    new_object_layer_fn,
    store_api::{StorageAPI, StorageDisk},
};

pub const ITEM_OFFLINE: &str = "offline";
pub const ITEM_INITIALIZING: &str = "initializing";
pub const ITEM_ONLINE: &str = "online";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MemStats {
    alloc: u64,
    total_alloc: u64,
    mallocs: u64,
    frees: u64,
    heap_alloc: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ServerProperties {
    pub state: String,
    pub endpoint: String,
    pub scheme: String,
    pub uptime: u64,
    pub version: String,
    pub commit_id: String,
    pub network: HashMap<String, String>,
    pub disks: Vec<StorageDisk>,
    pub pool_number: i32,
    pub pool_numbers: Vec<i32>,
    pub mem_stats: MemStats,
    pub max_procs: u64,
    pub num_cpu: u64,
    pub runtime_version: String,
    pub rustfs_env_vars: HashMap<String, String>,
}

async fn is_server_resolvable(endpoint: &Endpoint) -> Result<()> {
    let addr = format!(
        "{}://{}:{}",
        endpoint.url.scheme(),
        endpoint.url.host_str().unwrap(),
        endpoint.url.port().unwrap()
    );
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");

    let mut builder = PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);

    let finished_data = fbb.finished_data();

    let decoded_payload = flatbuffers::root::<PingBody>(finished_data);
    assert!(decoded_payload.is_ok());

    // 创建客户端
    let mut client = node_service_time_out_client(&addr)
        .await
        .map_err(|err| Error::msg(err.to_string()))?;

    // 构造 PingRequest
    let request = Request::new(PingRequest {
        version: 1,
        body: finished_data.to_vec(),
    });

    // 发送请求并获取响应
    let response: PingResponse = client.ping(request).await?.into_inner();

    // 打印响应
    let ping_response_body = flatbuffers::root::<PingBody>(&response.body);
    if let Err(e) = ping_response_body {
        eprintln!("{}", e);
    } else {
        println!("ping_resp:body(flatbuffer): {:?}", ping_response_body);
    }

    Ok(())
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
        uptime: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        network,
        ..Default::default()
    };

    for pool_num in pool_numbers.iter() {
        props.pool_numbers.push(*pool_num);
    }
    props.pool_numbers.sort();
    props.pool_number = if props.pool_numbers.len() == 1 {
        props.pool_numbers[1]
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
