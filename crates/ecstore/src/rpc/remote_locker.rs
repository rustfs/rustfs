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

use crate::rpc::client::{TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client};
use async_trait::async_trait;
use rustfs_lock::{
    LockClient, LockError, LockInfo, LockRequest, LockResponse, LockStats, LockStatus, LockType, Result,
    types::{LockId, LockMetadata, LockPriority},
};
use rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_protos::proto_gen::node_service::{GenerallyLockRequest, PingRequest};
use tonic::Request;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::{info, warn};

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

    /// Create a minimal LockRequest for unlock operations using only lock_id
    fn create_unlock_request(lock_id: &LockId) -> LockRequest {
        LockRequest {
            lock_id: lock_id.clone(),
            resource: lock_id.resource.clone(),
            lock_type: LockType::Exclusive, // Type doesn't matter for unlock
            owner: String::new(),           // Owner not needed, server uses lock_id
            acquire_timeout: std::time::Duration::from_secs(30),
            ttl: std::time::Duration::from_secs(300),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            deadlock_detection: false,
        }
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))
    }
}

#[async_trait]
impl LockClient for RemoteClient {
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        info!("remote acquire_exclusive for {}", request.resource);
        let mut client = self.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });

        let resp = client
            .lock(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        // Check for explicit error first
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }

        // Check if the lock acquisition was successful
        if resp.success {
            // Try to deserialize lock_info from response
            let lock_info = if let Some(lock_info_json) = resp.lock_info {
                match serde_json::from_str::<LockInfo>(&lock_info_json) {
                    Ok(info) => info,
                    Err(e) => {
                        // If deserialization fails, fall back to constructing from request
                        warn!("Failed to deserialize lock_info from response: {}, using request data", e);
                        LockInfo {
                            id: request.lock_id.clone(),
                            resource: request.resource.clone(),
                            lock_type: request.lock_type,
                            status: LockStatus::Acquired,
                            owner: request.owner.clone(),
                            acquired_at: std::time::SystemTime::now(),
                            expires_at: std::time::SystemTime::now() + request.ttl,
                            last_refreshed: std::time::SystemTime::now(),
                            metadata: request.metadata.clone(),
                            priority: request.priority,
                            wait_start_time: None,
                        }
                    }
                }
            } else {
                // If lock_info is not provided, construct from request
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                }
            };

            Ok(LockResponse::success(lock_info, std::time::Duration::ZERO))
        } else {
            // Lock acquisition failed
            Ok(LockResponse::failure(
                "Lock acquisition failed on remote server".to_string(),
                std::time::Duration::ZERO,
            ))
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release for {}", lock_id);

        let unlock_request = Self::create_unlock_request(lock_id);
        let request_string = serde_json::to_string(&unlock_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;
        let mut client = self.get_client().await?;

        // Try UnLock first (for exclusive locks)
        let req = Request::new(GenerallyLockRequest {
            args: request_string.clone(),
        });
        let resp = client.un_lock(req).await;

        let success = if resp.is_err() {
            // If that fails, try RUnLock (for shared locks)
            let req = Request::new(GenerallyLockRequest { args: request_string });
            let resp = client
                .r_un_lock(req)
                .await
                .map_err(|e| LockError::internal(e.to_string()))?
                .into_inner();
            if let Some(error_info) = resp.error_info {
                return Err(LockError::internal(error_info));
            }
            resp.success
        } else {
            let resp = resp.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

            if let Some(error_info) = resp.error_info {
                return Err(LockError::internal(error_info));
            }
            resp.success
        };

        Ok(success)
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote refresh for {}", lock_id);
        let refresh_request = Self::create_unlock_request(lock_id);
        let mut client = self.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&refresh_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
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
        let force_request = Self::create_unlock_request(lock_id);
        let mut client = self.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&force_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
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

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        info!("remote check_status for {}", lock_id);

        // Since there's no direct status query in the gRPC service,
        // we attempt a non-blocking lock acquisition to check if the resource is available
        let status_request = Self::create_unlock_request(lock_id);
        let mut client = self.get_client().await?;

        // Try to acquire a very short-lived lock to test availability
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&status_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });

        // Try exclusive lock first with very short timeout
        let resp = client.lock(req).await;

        match resp {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    // If we successfully acquired the lock, the resource was free
                    // Immediately release it
                    let release_req = Request::new(GenerallyLockRequest {
                        args: serde_json::to_string(&status_request)
                            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
                    });
                    let _ = client.un_lock(release_req).await; // Best effort release

                    // Return None since no one was holding the lock
                    Ok(None)
                } else {
                    // Lock acquisition failed, meaning someone is holding it
                    // We can't determine the exact details remotely, so return a generic status
                    Ok(Some(LockInfo {
                        id: lock_id.clone(),
                        resource: lock_id.resource.clone(),
                        lock_type: LockType::Exclusive, // We can't know the exact type
                        status: LockStatus::Acquired,
                        owner: "unknown".to_string(), // Remote client can't determine owner
                        acquired_at: std::time::SystemTime::now(),
                        expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                        last_refreshed: std::time::SystemTime::now(),
                        metadata: LockMetadata::default(),
                        priority: LockPriority::Normal,
                        wait_start_time: None,
                    }))
                }
            }
            Err(_) => {
                // Communication error or lock is held
                Ok(Some(LockInfo {
                    id: lock_id.clone(),
                    resource: lock_id.resource.clone(),
                    lock_type: LockType::Exclusive,
                    status: LockStatus::Acquired,
                    owner: "unknown".to_string(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: LockMetadata::default(),
                    priority: LockPriority::Normal,
                    wait_start_time: None,
                }))
            }
        }
    }

    async fn get_stats(&self) -> Result<LockStats> {
        info!("remote get_stats from {}", self.addr);

        // Since there's no direct statistics endpoint in the gRPC service,
        // we return basic stats indicating this is a remote client
        let stats = LockStats {
            last_updated: std::time::SystemTime::now(),
            ..Default::default()
        };

        // We could potentially enhance this by:
        // 1. Keeping local counters of operations performed
        // 2. Adding a stats gRPC method to the service
        // 3. Querying server health endpoints

        // For now, return minimal stats indicating remote connectivity
        Ok(stats)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        // Use Ping interface to test if remote service is online
        let mut client = match self.get_client().await {
            Ok(client) => client,
            Err(_) => {
                info!("remote client {} connection failed", self.addr);
                return false;
            }
        };

        let ping_req = Request::new(PingRequest {
            version: 1,
            body: bytes::Bytes::new(),
        });

        match client.ping(ping_req).await {
            Ok(_) => {
                info!("remote client {} is online", self.addr);
                true
            }
            Err(_) => {
                info!("remote client {} ping failed", self.addr);
                false
            }
        }
    }

    async fn is_local(&self) -> bool {
        false
    }
}
