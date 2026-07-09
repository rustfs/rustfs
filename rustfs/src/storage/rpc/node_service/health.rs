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

use super::NodeService;
use crate::storage::rpc::encode_msgpack_map;
use crate::storage::storage_api::rpc_consumer::node_service::contract::admin::StorageAdminApi;
use crate::storage::storage_api::rpc_consumer::node_service::get_local_server_property;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use bytes::Bytes;
use rustfs_madmin::health::{
    get_cpus, get_mem_info, get_os_info, get_partitions, get_proc_info, get_sys_config, get_sys_errors, get_sys_services,
};
use rustfs_madmin::net::get_net_info;
use rustfs_protos::proto_gen::node_service::*;
use tonic::{Request, Response, Status};

impl NodeService {
    pub(super) async fn handle_get_proc_info(
        &self,
        _request: Request<GetProcInfoRequest>,
    ) -> Result<Response<GetProcInfoResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_proc_info(&addr);
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetProcInfoResponse {
                success: true,
                proc_info: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetProcInfoResponse {
                success: false,
                proc_info: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_mem_info(
        &self,
        _request: Request<GetMemInfoRequest>,
    ) -> Result<Response<GetMemInfoResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_mem_info(&addr);
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetMemInfoResponse {
                success: true,
                mem_info: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetMemInfoResponse {
                success: false,
                mem_info: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_sys_errors(
        &self,
        _request: Request<GetSysErrorsRequest>,
    ) -> Result<Response<GetSysErrorsResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_sys_errors(&addr);
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetSysErrorsResponse {
                success: true,
                sys_errors: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetSysErrorsResponse {
                success: false,
                sys_errors: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_sys_config(
        &self,
        _request: Request<GetSysConfigRequest>,
    ) -> Result<Response<GetSysConfigResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_sys_config(&addr);
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetSysConfigResponse {
                success: true,
                sys_config: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetSysConfigResponse {
                success: false,
                sys_config: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_se_linux_info(
        &self,
        _request: Request<GetSeLinuxInfoRequest>,
    ) -> Result<Response<GetSeLinuxInfoResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_sys_services(&addr);
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetSeLinuxInfoResponse {
                success: true,
                sys_services: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetSeLinuxInfoResponse {
                success: false,
                sys_services: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_os_info(
        &self,
        _request: Request<GetOsInfoRequest>,
    ) -> Result<Response<GetOsInfoResponse>, Status> {
        let os_info = get_os_info();
        match encode_msgpack_map(&os_info) {
            Ok(buf) => Ok(Response::new(GetOsInfoResponse {
                success: true,
                os_info: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetOsInfoResponse {
                success: false,
                os_info: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_partitions(
        &self,
        _request: Request<GetPartitionsRequest>,
    ) -> Result<Response<GetPartitionsResponse>, Status> {
        let partitions = get_partitions();
        match encode_msgpack_map(&partitions) {
            Ok(buf) => Ok(Response::new(GetPartitionsResponse {
                success: true,
                partitions: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetPartitionsResponse {
                success: false,
                partitions: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_net_info(
        &self,
        _request: Request<GetNetInfoRequest>,
    ) -> Result<Response<GetNetInfoResponse>, Status> {
        let addr = runtime_sources::current_local_node_name().await;
        let info = get_net_info(&addr, "");
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetNetInfoResponse {
                success: true,
                net_info: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetNetInfoResponse {
                success: false,
                net_info: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_get_cpus(&self, _request: Request<GetCpusRequest>) -> Result<Response<GetCpusResponse>, Status> {
        let info = get_cpus();
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(GetCpusResponse {
                success: true,
                cpus: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(GetCpusResponse {
                success: false,
                cpus: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        let info = get_local_server_property().await;
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(ServerInfoResponse {
                success: true,
                server_properties: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ServerInfoResponse {
                success: false,
                server_properties: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_local_storage_info(
        &self,
        _request: Request<LocalStorageInfoRequest>,
    ) -> Result<Response<LocalStorageInfoResponse>, Status> {
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let info = StorageAdminApi::local_storage_info(store.as_ref()).await;
        match encode_msgpack_map(&info) {
            Ok(buf) => Ok(Response::new(LocalStorageInfoResponse {
                success: true,
                storage_info: buf.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some(err.to_string()),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::rpc::encode_msgpack_map;
    use rmp_serde::Deserializer;
    use serde::Deserialize;
    use std::io::Cursor;

    #[test]
    fn local_storage_info_rpc_payload_uses_msgpack_map_encoding() {
        let info = rustfs_madmin::StorageInfo {
            disks: Vec::new(),
            backend: rustfs_madmin::BackendInfo {
                backend_type: rustfs_madmin::BackendByte::Erasure,
                standard_sc_data: vec![2, 2],
                total_sets: vec![1, 1],
                drives_per_set: vec![4, 4],
                ..Default::default()
            },
        };

        let encoded = encode_msgpack_map(&info).expect("storage info should serialize");
        assert_eq!(encoded.first().copied(), Some(0x82));

        let mut decoder = Deserializer::new(Cursor::new(encoded));
        let decoded: rustfs_madmin::StorageInfo = Deserialize::deserialize(&mut decoder).expect("storage info should decode");
        assert_eq!(decoded.backend.drives_per_set, vec![4, 4]);
        assert_eq!(decoded.backend.total_sets, vec![1, 1]);
    }
}
