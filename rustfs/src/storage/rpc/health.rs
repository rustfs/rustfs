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

impl NodeService {
    pub(super) async fn handle_get_proc_info(
        &self,
        _request: Request<GetProcInfoRequest>,
    ) -> Result<Response<GetProcInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_proc_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetProcInfoResponse {
                success: false,
                proc_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetProcInfoResponse {
            success: true,
            proc_info: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_mem_info(
        &self,
        _request: Request<GetMemInfoRequest>,
    ) -> Result<Response<GetMemInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_mem_info(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetMemInfoResponse {
                success: false,
                mem_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetMemInfoResponse {
            success: true,
            mem_info: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_sys_errors(
        &self,
        _request: Request<GetSysErrorsRequest>,
    ) -> Result<Response<GetSysErrorsResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_errors(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSysErrorsResponse {
                success: false,
                sys_errors: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSysErrorsResponse {
            success: true,
            sys_errors: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_sys_config(
        &self,
        _request: Request<GetSysConfigRequest>,
    ) -> Result<Response<GetSysConfigResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_config(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSysConfigResponse {
                success: false,
                sys_config: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSysConfigResponse {
            success: true,
            sys_config: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_se_linux_info(
        &self,
        _request: Request<GetSeLinuxInfoRequest>,
    ) -> Result<Response<GetSeLinuxInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_sys_services(&addr);
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetSeLinuxInfoResponse {
                success: false,
                sys_services: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetSeLinuxInfoResponse {
            success: true,
            sys_services: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_os_info(
        &self,
        _request: Request<GetOsInfoRequest>,
    ) -> Result<Response<GetOsInfoResponse>, Status> {
        let os_info = get_os_info();
        let mut buf = Vec::new();
        if let Err(err) = os_info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetOsInfoResponse {
                success: false,
                os_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetOsInfoResponse {
            success: true,
            os_info: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_partitions(
        &self,
        _request: Request<GetPartitionsRequest>,
    ) -> Result<Response<GetPartitionsResponse>, Status> {
        let partitions = get_partitions();
        let mut buf = Vec::new();
        if let Err(err) = partitions.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetPartitionsResponse {
                success: false,
                partitions: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetPartitionsResponse {
            success: true,
            partitions: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_net_info(
        &self,
        _request: Request<GetNetInfoRequest>,
    ) -> Result<Response<GetNetInfoResponse>, Status> {
        let addr = GLOBAL_LOCAL_NODE_NAME.read().await.clone();
        let info = get_net_info(&addr, "");
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetNetInfoResponse {
                success: false,
                net_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetNetInfoResponse {
            success: true,
            net_info: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_get_cpus(&self, _request: Request<GetCpusRequest>) -> Result<Response<GetCpusResponse>, Status> {
        let info = get_cpus();
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(GetCpusResponse {
                success: false,
                cpus: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(GetCpusResponse {
            success: true,
            cpus: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        let info = get_local_server_property().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(ServerInfoResponse {
                success: false,
                server_properties: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(ServerInfoResponse {
            success: true,
            server_properties: buf.into(),
            error_info: None,
        }))
    }

    pub(super) async fn handle_local_storage_info(
        &self,
        _request: Request<LocalStorageInfoRequest>,
    ) -> Result<Response<LocalStorageInfoResponse>, Status> {
        // let request = request.into_inner();

        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let info = store.local_storage_info().await;
        let mut buf = Vec::new();
        if let Err(err) = info.serialize(&mut Serializer::new(&mut buf)) {
            return Ok(Response::new(LocalStorageInfoResponse {
                success: false,
                storage_info: Bytes::new(),
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LocalStorageInfoResponse {
            success: true,
            storage_info: buf.into(),
            error_info: None,
        }))
    }
}
