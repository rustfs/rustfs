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
use rustfs_protos::proto_gen::node_service::{BatchGenerallyLockRequest, GenerallyLockRequest, PingRequest};
use rustfs_protos::{evict_failed_connection, proto_gen::node_service::node_service_client::NodeServiceClient};
use std::time::Duration;
use tokio::time::timeout;
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
            suppress_contention_logs: false,
        }
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<Channel, TonicInterceptor>>> {
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))
    }

    async fn evict_connection(&self, op: &'static str, reason: &str) {
        warn!(
            addr = %self.addr,
            op,
            reason,
            "Evicting cached remote lock connection after RPC failure"
        );
        evict_failed_connection(&self.addr).await;
    }

    fn rpc_timeout(timeout_duration: Duration) -> Duration {
        if timeout_duration.is_zero() {
            Duration::from_millis(1)
        } else {
            timeout_duration
        }
    }

    async fn execute_rpc<T, F>(
        &self,
        op: &'static str,
        timeout_duration: Duration,
        future: F,
    ) -> std::result::Result<T, LockError>
    where
        F: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let timeout_duration = Self::rpc_timeout(timeout_duration);
        match timeout(timeout_duration, future).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(err)) => {
                let reason = err.to_string();
                self.evict_connection(op, &reason).await;
                Err(LockError::internal(format!("{op} RPC failed: {reason}")))
            }
            Err(_) => {
                let reason = format!("RPC timed out after {:?}", timeout_duration);
                self.evict_connection(op, &reason).await;
                Err(LockError::timeout(format!("remote lock RPC {op} on {}", self.addr), timeout_duration))
            }
        }
    }

    fn timeout_failure_response(request: &LockRequest) -> LockResponse {
        LockResponse::failure("Lock acquisition timeout", request.acquire_timeout)
    }

    fn rpc_failure_response(_request: &LockRequest, err: &LockError) -> LockResponse {
        LockResponse::failure(format!("Remote lock RPC failed: {err}"), Duration::ZERO)
    }

    fn timeout_failure_batch(requests: &[LockRequest]) -> Vec<LockResponse> {
        requests.iter().map(Self::timeout_failure_response).collect()
    }

    fn rpc_failure_batch(requests: &[LockRequest], err: &LockError) -> Vec<LockResponse> {
        requests
            .iter()
            .map(|request| Self::rpc_failure_response(request, err))
            .collect()
    }

    fn batch_rpc_timeout(requests: &[LockRequest]) -> Duration {
        requests
            .iter()
            .map(|request| request.acquire_timeout)
            .max()
            .map(Self::rpc_timeout)
            .unwrap_or_else(|| Duration::from_millis(1))
    }

    fn build_lock_info(request: &LockRequest, lock_info_json: Option<String>) -> LockInfo {
        if let Some(lock_info_json) = lock_info_json {
            match serde_json::from_str::<LockInfo>(&lock_info_json) {
                Ok(info) => info,
                Err(e) => {
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

        let resp = match self.execute_rpc("lock", request.acquire_timeout, client.lock(req)).await {
            Ok(resp) => resp.into_inner(),
            Err(LockError::Timeout { .. }) => return Ok(Self::timeout_failure_response(request)),
            Err(err) => return Ok(Self::rpc_failure_response(request, &err)),
        };

        // Check if the lock acquisition was successful
        if resp.success {
            Ok(LockResponse::success(
                Self::build_lock_info(request, resp.lock_info),
                std::time::Duration::ZERO,
            ))
        } else {
            // Lock acquisition failed
            Ok(LockResponse::failure(
                resp.error_info
                    .unwrap_or_else(|| "Lock acquisition failed on remote server".to_string()),
                std::time::Duration::ZERO,
            ))
        }
    }

    async fn acquire_locks_batch(&self, requests: &[LockRequest]) -> Result<Vec<LockResponse>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.get_client().await?;
        let req = Request::new(BatchGenerallyLockRequest {
            args: requests
                .iter()
                .map(|request| {
                    serde_json::to_string(request).map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))
                })
                .collect::<Result<Vec<_>>>()?,
        });

        let resp = match self
            .execute_rpc("lock_batch", Self::batch_rpc_timeout(requests), client.lock_batch(req))
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(LockError::Timeout { .. }) => return Ok(Self::timeout_failure_batch(requests)),
            Err(err) => return Ok(Self::rpc_failure_batch(requests, &err)),
        };

        Ok(requests
            .iter()
            .enumerate()
            .map(|(idx, request)| match resp.results.get(idx) {
                Some(result) if result.success => {
                    LockResponse::success(Self::build_lock_info(request, result.lock_info.clone()), std::time::Duration::ZERO)
                }
                Some(result) => LockResponse::failure(
                    result
                        .error_info
                        .clone()
                        .unwrap_or_else(|| "Lock acquisition failed on remote server".to_string()),
                    std::time::Duration::ZERO,
                ),
                None => LockResponse::failure(
                    format!("Lock batch response missing entry for request index {idx}"),
                    std::time::Duration::ZERO,
                ),
            })
            .collect())
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release for {}", lock_id);

        let unlock_request = Self::create_unlock_request(lock_id);
        let request_string = serde_json::to_string(&unlock_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;
        let mut client = self.get_client().await?;
        let req = Request::new(GenerallyLockRequest { args: request_string });
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

    async fn release_locks_batch(&self, lock_ids: &[LockId]) -> Result<Vec<bool>> {
        let mut client = self.get_client().await?;
        let req = Request::new(BatchGenerallyLockRequest {
            args: lock_ids
                .iter()
                .map(|lock_id| {
                    serde_json::to_string(&Self::create_unlock_request(lock_id))
                        .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))
                })
                .collect::<Result<Vec<_>>>()?,
        });

        let resp = client
            .un_lock_batch(req)
            .await
            .map_err(|e| LockError::internal(e.to_string()))?
            .into_inner();

        Ok(lock_ids
            .iter()
            .enumerate()
            .map(|(idx, _)| resp.results.get(idx).map(|result| result.success).unwrap_or(false))
            .collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::GLOBAL_CONN_MAP;
    use rustfs_lock::{ObjectKey, types::LockPriority};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tonic::transport::Endpoint as TonicEndpoint;

    async fn spawn_hanging_listener() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _stream = stream;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        (addr, task)
    }

    async fn cache_lazy_channel(addr: &str) {
        let channel = TonicEndpoint::from_shared(addr.to_string()).unwrap().connect_lazy();
        GLOBAL_CONN_MAP.write().await.insert(addr.to_string(), channel);
    }

    fn ensure_test_rpc_secret() {
        let _ = rustfs_credentials::GLOBAL_RUSTFS_RPC_SECRET.set("test-rpc-secret".to_string());
    }

    fn test_lock_request(timeout_duration: Duration) -> LockRequest {
        LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner-a")
            .with_acquire_timeout(timeout_duration)
            .with_priority(LockPriority::Normal)
    }

    #[tokio::test]
    async fn test_remote_client_acquire_lock_respects_request_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let (addr, accept_task) = spawn_hanging_listener().await;
        cache_lazy_channel(&addr).await;
        assert!(GLOBAL_CONN_MAP.read().await.contains_key(&addr));

        let client = RemoteClient::new(addr.clone());
        let request = test_lock_request(Duration::from_millis(50));
        let started_at = tokio::time::Instant::now();

        let response = client.acquire_lock(&request).await.unwrap();

        assert!(
            started_at.elapsed() < Duration::from_secs(1),
            "remote lock RPC should honor request timeout"
        );
        assert!(!response.success, "timed out lock acquisition should fail");
        assert_eq!(response.error.as_deref(), Some("Lock acquisition timeout"));
        assert!(
            !GLOBAL_CONN_MAP.read().await.contains_key(&addr),
            "timeout should evict cached connection"
        );

        accept_task.abort();
    }

    #[tokio::test]
    async fn test_remote_client_acquire_locks_batch_respects_request_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let (addr, accept_task) = spawn_hanging_listener().await;
        cache_lazy_channel(&addr).await;
        assert!(GLOBAL_CONN_MAP.read().await.contains_key(&addr));

        let client = RemoteClient::new(addr.clone());
        let requests = vec![test_lock_request(Duration::from_millis(50))];
        let started_at = tokio::time::Instant::now();

        let responses = client.acquire_locks_batch(&requests).await.unwrap();

        assert!(
            started_at.elapsed() < Duration::from_secs(1),
            "remote batch lock RPC should honor request timeout"
        );
        assert_eq!(responses.len(), 1);
        assert!(!responses[0].success, "timed out batch lock acquisition should fail");
        assert_eq!(responses[0].error.as_deref(), Some("Lock acquisition timeout"));
        assert!(
            !GLOBAL_CONN_MAP.read().await.contains_key(&addr),
            "batch timeout should evict cached connection"
        );

        accept_task.abort();
    }

    #[test]
    fn test_remote_client_zero_timeout_is_clamped() {
        assert_eq!(RemoteClient::rpc_timeout(Duration::ZERO), Duration::from_millis(1));
        assert_eq!(RemoteClient::rpc_timeout(Duration::from_millis(25)), Duration::from_millis(25));
    }
}
