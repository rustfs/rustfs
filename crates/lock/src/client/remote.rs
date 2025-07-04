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

use async_trait::async_trait;
use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
use tonic::Request;
use tracing::info;

use crate::{
    error::{LockError, Result},
    lock_args::LockArgs,
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats},
};

/// Remote lock client
#[derive(Debug, Clone)]
pub struct RemoteClient {
    addr: String,
}

impl RemoteClient {
    /// Create new remote client from endpoint string (for trait兼容)
    pub fn new(endpoint: String) -> Self {
        Self { addr: endpoint }
    }
    /// Create new remote client from url::Url（兼容 namespace/distributed 场景）
    pub fn from_url(url: url::Url) -> Self {
        let addr = format!("{}://{}:{}", url.scheme(), url.host_str().unwrap(), url.port().unwrap());
        Self { addr }
    }
}

// 辅助方法：从 LockRequest 创建 LockArgs
impl LockArgs {
    fn from_request(request: &LockRequest, _is_shared: bool) -> Self {
        Self {
            uid: uuid::Uuid::new_v4().to_string(),
            resources: vec![request.resource.clone()],
            owner: request.owner.clone(),
            source: "remote_client".to_string(),
            quorum: 1,
        }
    }
    
    fn from_lock_id(lock_id: &LockId) -> Self {
        Self {
            uid: lock_id.to_string(),
            resources: vec![],
            owner: "remote_client".to_string(),
            source: "remote_client".to_string(),
            quorum: 1,
        }
    }
}

#[async_trait]
impl super::LockClient for RemoteClient {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        info!("remote acquire_exclusive");
        let args = LockArgs::from_request(&request, false);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest { args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))? });
        let resp = client.lock(req).await.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(LockResponse {
            success: resp.success,
            lock_info: None, // 可扩展: 解析resp内容
            error: None,
            wait_time: std::time::Duration::ZERO,
            position_in_queue: None,
        })
    }
    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        info!("remote acquire_shared");
        let args = LockArgs::from_request(&request, true);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest { args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))? });
        let resp = client.r_lock(req).await.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(LockResponse {
            success: resp.success,
            lock_info: None,
            error: None,
            wait_time: std::time::Duration::ZERO,
            position_in_queue: None,
        })
    }
    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release");
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest { args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))? });
        let resp = client.un_lock(req).await.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }
    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote refresh");
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest { args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))? });
        let resp = client.refresh(req).await.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }
    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote force_release");
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest { args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))? });
        let resp = client.force_un_lock(req).await.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }
    async fn check_status(&self, _lock_id: &LockId) -> Result<Option<LockInfo>> {
        // 可扩展: 实现远程状态查询
        Ok(None)
    }
    async fn get_stats(&self) -> Result<LockStats> {
        // 可扩展: 实现远程统计
        Ok(LockStats::default())
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    async fn is_online(&self) -> bool {
        true
    }
    async fn is_local(&self) -> bool {
        false
    }
}

// 同时实现 Locker trait 以兼容现有调用
#[async_trait]
impl crate::Locker for RemoteClient {
    async fn lock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote lock");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .lock(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote unlock");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .un_lock(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote rlock");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .r_lock(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote runlock");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .r_un_lock(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn refresh(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote refresh");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .refresh(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote force_unlock");
        let args = serde_json::to_string(args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client
            .force_un_lock(request)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        if let Some(error_info) = response.error_info {
            return Err(LockError::internal(error_info));
        }

        Ok(response.success)
    }

    async fn close(&self) {}

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        false
    }
}
