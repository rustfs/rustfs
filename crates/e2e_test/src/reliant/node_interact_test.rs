#![cfg(test)]
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

use crate::common::RustFSTestEnvironment;
use crate::storage_api::node_interact::{
    TonicInterceptor, VolumeInfo, WalkDirOptions, gen_tonic_signature_interceptor, node_service_time_out_client,
};
use aws_sdk_s3::primitives::ByteStream;
use rmp_serde::{Deserializer, Serializer};
use rustfs_filemeta::MetaCacheEntry;
use rustfs_protos::proto_gen::node_service::WalkDirRequest;
use rustfs_protos::{
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{ListVolumesRequest, LocalStorageInfoRequest, MakeVolumeRequest, PingRequest, ReadAllRequest},
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::Cursor;
use tonic::Request;
use tonic::codegen::tokio_stream::StreamExt;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const TEST_RPC_SECRET: &str = "rustfs-internode-signature-e2e-secret";

fn signature_interceptor() -> TonicInterceptor {
    TonicInterceptor::Signature(gen_tonic_signature_interceptor())
}

fn rpc_client_error(error: Box<dyn Error>) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

async fn start_server() -> Result<RustFSTestEnvironment, Box<dyn Error + Send + Sync>> {
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    let effective = rustfs_credentials::try_get_rpc_token().expect("RPC secret must resolve in the test process");
    assert_eq!(effective, TEST_RPC_SECRET, "the test process uses an unexpected RPC secret");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_without_cleanup_with_env(&[
        ("RUSTFS_RPC_SECRET", TEST_RPC_SECRET),
        ("RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT", "false"),
        ("RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT", "false"),
        ("RUSTFS_INTERNODE_RPC_REPLAY_SCOPE_STRICT", "false"),
        ("RUST_LOG", "error"),
    ])
    .await?;
    Ok(env)
}

#[tokio::test]
async fn ping() -> TestResult {
    let env = start_server().await?;
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");
    let mut builder = PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);

    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let response = client
        .ping(Request::new(PingRequest {
            version: 1,
            body: bytes::Bytes::copy_from_slice(fbb.finished_data()),
        }))
        .await?
        .into_inner();

    assert_eq!(response.version, 1);
    let body = flatbuffers::root::<PingBody>(&response.body)?;
    assert_eq!(body.payload().expect("ping response must contain a payload").bytes(), b"hello, caller");
    Ok(())
}

#[tokio::test]
async fn make_volume() -> TestResult {
    let env = start_server().await?;
    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let response = client
        .make_volume(Request::new(MakeVolumeRequest {
            disk: env.temp_dir.clone(),
            volume: "node-rpc-volume".to_string(),
        }))
        .await?
        .into_inner();

    assert!(response.success, "make_volume failed: {:?}", response.error);
    assert!(std::path::Path::new(&env.temp_dir).join("node-rpc-volume").is_dir());
    Ok(())
}

#[tokio::test]
async fn list_volumes() -> TestResult {
    let env = start_server().await?;
    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let created = client
        .make_volume(Request::new(MakeVolumeRequest {
            disk: env.temp_dir.clone(),
            volume: "node-rpc-listed-volume".to_string(),
        }))
        .await?
        .into_inner();
    assert!(created.success, "make_volume failed: {:?}", created.error);

    let response = client
        .list_volumes(Request::new(ListVolumesRequest {
            disk: env.temp_dir.clone(),
        }))
        .await?
        .into_inner();
    assert!(response.success, "list_volumes failed: {:?}", response.error);
    let volumes = response
        .volume_infos
        .iter()
        .map(|json| serde_json::from_str::<VolumeInfo>(json))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(volumes.iter().any(|volume| volume.name == "node-rpc-listed-volume"));
    Ok(())
}

#[tokio::test]
async fn walk_dir() -> TestResult {
    let env = start_server().await?;
    let s3 = env.create_s3_client();
    let bucket = "node-rpc-walk-bucket";
    let key = "prefix/object.txt";
    env.create_test_bucket(bucket).await?;
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"walk payload"))
        .send()
        .await?;

    let opts = WalkDirOptions {
        bucket: bucket.to_string(),
        recursive: true,
        ..Default::default()
    };
    let mut encoded = Vec::new();
    opts.serialize(&mut Serializer::new(&mut encoded))?;
    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let mut stream = client
        .walk_dir(Request::new(WalkDirRequest {
            disk: env.temp_dir.clone(),
            walk_dir_options: encoded.into(),
        }))
        .await?
        .into_inner();

    let mut entries = Vec::new();
    while let Some(response) = stream.next().await {
        let response = response?;
        assert!(response.success, "walk_dir failed: {:?}", response.error_info);
        entries.push(serde_json::from_str::<MetaCacheEntry>(&response.meta_cache_entry)?);
    }
    assert!(
        entries.iter().any(|entry| entry.name == key),
        "walk_dir did not return {key}: {entries:?}"
    );
    Ok(())
}

#[tokio::test]
async fn read_all() -> TestResult {
    let env = start_server().await?;
    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let volume = "node-rpc-read-volume";
    let created = client
        .make_volume(Request::new(MakeVolumeRequest {
            disk: env.temp_dir.clone(),
            volume: volume.to_string(),
        }))
        .await?
        .into_inner();
    assert!(created.success, "make_volume failed: {:?}", created.error);
    tokio::fs::write(std::path::Path::new(&env.temp_dir).join(volume).join("payload.bin"), b"read payload").await?;

    let response = client
        .read_all(Request::new(ReadAllRequest {
            disk: env.temp_dir.clone(),
            volume: volume.to_string(),
            path: "payload.bin".to_string(),
        }))
        .await?
        .into_inner();
    assert!(response.success, "read_all failed: {:?}", response.error);
    assert_eq!(response.data.as_ref(), b"read payload");
    Ok(())
}

#[tokio::test]
async fn storage_info() -> TestResult {
    let env = start_server().await?;
    let mut client = node_service_time_out_client(&env.url, signature_interceptor())
        .await
        .map_err(rpc_client_error)?;
    let response = client
        .local_storage_info(Request::new(LocalStorageInfoRequest { metrics: true }))
        .await?
        .into_inner();
    assert!(response.success, "local_storage_info failed: {:?}", response.error_info);

    let mut decoder = Deserializer::new(Cursor::new(response.storage_info));
    let storage_info: rustfs_madmin::StorageInfo = Deserialize::deserialize(&mut decoder)?;
    let expected_disk = std::fs::canonicalize(&env.temp_dir)?;
    assert!(!storage_info.disks.is_empty(), "local_storage_info returned no disks");
    assert!(
        storage_info
            .disks
            .iter()
            .any(|disk| std::path::Path::new(&disk.drive_path) == expected_disk),
        "local_storage_info did not include the configured disk: {:?}",
        storage_info.disks
    );
    Ok(())
}
