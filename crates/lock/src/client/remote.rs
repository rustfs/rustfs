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
use serde::{Deserialize, Serialize};
use tonic::Request;
use tracing::info;

use crate::{
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats},
};

use super::LockClient;

/// RPC lock arguments for gRPC communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: u32,
}

impl LockArgs {
    fn from_request(request: &LockRequest, _is_shared: bool) -> Self {
        Self {
            uid: request.metadata.operation_id.clone().unwrap_or_default(),
            resources: vec![request.resource.clone()],
            owner: request.owner.clone(),
            source: "remote".to_string(),
            quorum: 1,
        }
    }

    fn from_lock_id(lock_id: &LockId) -> Self {
        Self {
            uid: lock_id.as_str().to_string(),
            resources: vec![lock_id.as_str().to_string()],
            owner: "remote".to_string(),
            source: "remote".to_string(),
            quorum: 1,
        }
    }
}

/// Remote lock client implementation
#[derive(Debug, Clone)]
pub struct RemoteClient {
    addr: String,
}

impl RemoteClient {
    pub fn new(endpoint: String) -> Self {
        Self { addr: endpoint }
    }

    pub fn from_url(url: url::Url) -> Self {
        Self { addr: url.to_string() }
    }
}

#[async_trait]
impl LockClient for RemoteClient {
    async fn acquire_exclusive(&self, request: LockRequest) -> Result<LockResponse> {
        info!("remote acquire_exclusive for {}", request.resource);
        let args = LockArgs::from_request(&request, false);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?,
        });
        let resp = client
            .lock(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(LockResponse::success(
            LockInfo {
                id: LockId::new_deterministic(&request.resource),
                resource: request.resource,
                lock_type: request.lock_type,
                status: crate::types::LockStatus::Acquired,
                owner: request.owner,
                acquired_at: std::time::SystemTime::now(),
                expires_at: std::time::SystemTime::now() + request.timeout,
                last_refreshed: std::time::SystemTime::now(),
                metadata: request.metadata,
                priority: request.priority,
                wait_start_time: None,
            },
            std::time::Duration::ZERO,
        ))
    }

    async fn acquire_shared(&self, request: LockRequest) -> Result<LockResponse> {
        info!("remote acquire_shared for {}", request.resource);
        let args = LockArgs::from_request(&request, true);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?,
        });
        let resp = client
            .r_lock(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(LockResponse::success(
            LockInfo {
                id: LockId::new_deterministic(&request.resource),
                resource: request.resource,
                lock_type: request.lock_type,
                status: crate::types::LockStatus::Acquired,
                owner: request.owner,
                acquired_at: std::time::SystemTime::now(),
                expires_at: std::time::SystemTime::now() + request.timeout,
                last_refreshed: std::time::SystemTime::now(),
                metadata: request.metadata,
                priority: request.priority,
                wait_start_time: None,
            },
            std::time::Duration::ZERO,
        ))
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release for {}", lock_id);
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?,
        });
        let resp = client
            .un_lock(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote refresh for {}", lock_id);
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?,
        });
        let resp = client
            .refresh(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote force_release for {}", lock_id);
        let args = LockArgs::from_lock_id(lock_id);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&args).map_err(|e| LockError::internal(format!("Failed to serialize args: {e}")))?,
        });
        let resp = client
            .force_un_lock(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn check_status(&self, _lock_id: &LockId) -> Result<Option<LockInfo>> {
        // TODO: Implement remote status query
        Ok(None)
    }

    async fn get_stats(&self) -> Result<LockStats> {
        // TODO: Implement remote statistics
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
