#![cfg(test)]

use ecstore::disk::VolumeInfo;
use protos::{
    models::{PingBody, PingBodyBuilder},
    node_service_time_out_client,
    proto_gen::node_service::{
        ListVolumesRequest, LocalStorageInfoRequest, MakeVolumeRequest, PingRequest, PingResponse, ReadAllRequest,
    },
};
use rmp_serde::Deserializer;
use serde::Deserialize;
use std::{error::Error, io::Cursor};
use tonic::Request;

const CLUSTER_ADDR: &str = "http://localhost:9000";

#[tokio::test]
async fn ping() -> Result<(), Box<dyn Error>> {
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
    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;

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

#[tokio::test]
async fn make_volume() -> Result<(), Box<dyn Error>> {
    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    let request = Request::new(MakeVolumeRequest {
        disk: "data".to_string(),
        volume: "dandan".to_string(),
    });

    let response = client.make_volume(request).await?.into_inner();
    if response.success {
        println!("success");
    } else {
        println!("failed: {:?}", response.error);
    }
    Ok(())
}

#[tokio::test]
async fn list_volumes() -> Result<(), Box<dyn Error>> {
    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    let request = Request::new(ListVolumesRequest {
        disk: "data".to_string(),
    });

    let response = client.list_volumes(request).await?.into_inner();
    let volume_infos: Vec<VolumeInfo> = response
        .volume_infos
        .into_iter()
        .filter_map(|json_str| serde_json::from_str::<VolumeInfo>(&json_str).ok())
        .collect();

    println!("{:?}", volume_infos);
    Ok(())
}

#[tokio::test]
async fn read_all() -> Result<(), Box<dyn Error>> {
    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    let request = Request::new(ReadAllRequest {
        disk: "data".to_string(),
        volume: "ff".to_string(),
        path: "format.json".to_string(),
    });

    let response = client.read_all(request).await?.into_inner();
    let volume_infos = response.data;

    println!("{}", response.success);
    println!("{:?}", volume_infos);
    Ok(())
}

#[tokio::test]
async fn storage_info() -> Result<(), Box<dyn Error>> {
    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    let request = Request::new(LocalStorageInfoRequest { metrics: true });

    let response = client.local_storage_info(request).await?.into_inner();
    if !response.success {
        println!("{:?}", response.error_info);
        return Ok(());
    }
    let info = response.storage_info;

    let mut buf = Deserializer::new(Cursor::new(info));
    let storage_info: madmin::StorageInfo = Deserialize::deserialize(&mut buf).unwrap();
    println!("{:?}", storage_info);
    Ok(())
}
