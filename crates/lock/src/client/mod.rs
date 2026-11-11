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

pub mod local;
// pub mod remote;

use crate::{LockError, LockId, LockInfo, LockLeaseInfo, LockRequest, LockResponse, LockStats, Result};
use async_trait::async_trait;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// A batch peer's result for one request, in the original request order.
#[derive(Clone, Debug)]
pub enum BatchAcquireOutcome {
    /// The peer confirmed a grant. Missing legacy metadata uses the requested ID.
    Granted {
        lock_info: Option<Box<LockInfo>>,
        wait_time: Duration,
    },
    /// This attempt cannot grant later, and another attempt may succeed.
    RetryableNoGrant { error: LockError },
    /// This attempt cannot grant later, and this peer must not be retried.
    TerminalNoGrant { error: LockError },
    /// A grant may exist or arrive later. Stop using this peer for this key.
    Ambiguous {
        error: LockError,
        /// The client already retains exact-ID cleanup until the attempt settles.
        /// Otherwise the coordinator must retain uncertain cleanup ownership.
        cleanup_owned_by_client: bool,
    },
}

impl BatchAcquireOutcome {
    fn from_legacy_response(response: LockResponse) -> Self {
        if response.success {
            Self::Granted {
                lock_info: response.lock_info.map(Box::new),
                wait_time: response.wait_time,
            }
        } else {
            Self::Ambiguous {
                error: LockError::internal(response.error.unwrap_or_else(|| "Legacy batch acquisition failed".to_owned())),
                cleanup_owned_by_client: false,
            }
        }
    }
}

/// Outcome of cleaning up a lock whose acquisition has already completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockCleanupOutcome {
    /// This request released the granted lock.
    Released,
    /// The authoritative backend no longer holds this exact lock ID.
    AlreadyAbsent,
    /// The backend did not confirm release or authoritative absence.
    Unconfirmed,
}

/// Lock client trait
#[async_trait]
pub trait LockClient: Send + Sync + std::fmt::Debug {
    /// Acquire lock (generic method)
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse>;

    /// Acquire multiple locks. Default implementation fans out to single-lock requests.
    async fn acquire_locks_batch(&self, requests: &[LockRequest]) -> Result<Vec<LockResponse>> {
        Ok(join_all(requests.iter().map(|request| self.acquire_lock(request)))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?)
    }

    /// Acquire a batch within the caller's absolute deadline, preserving per-key outcomes.
    ///
    /// Legacy clients cannot prove that failed or missing responses did not grant.
    /// Their calls retain completion ownership in the coordinator if the caller
    /// stops waiting. Implementations may narrow each request's backend wait;
    /// they must never reset the deadline after connection or queue delays.
    async fn acquire_locks_batch_until(&self, requests: &[LockRequest], deadline: Instant) -> Vec<BatchAcquireOutcome> {
        if Instant::now() >= deadline {
            return requests
                .iter()
                .map(|request| BatchAcquireOutcome::RetryableNoGrant {
                    error: LockError::timeout(request.resource.to_string(), request.acquire_timeout),
                })
                .collect();
        }
        match self.acquire_locks_batch(requests).await {
            Ok(responses) => {
                let mut responses = responses.into_iter();
                requests
                    .iter()
                    .map(|_| match responses.next() {
                        Some(response) => BatchAcquireOutcome::from_legacy_response(response),
                        None => BatchAcquireOutcome::Ambiguous {
                            error: LockError::internal("Legacy peer returned an incomplete batch response"),
                            cleanup_owned_by_client: false,
                        },
                    })
                    .collect()
            }
            Err(error) => requests
                .iter()
                .map(|_| BatchAcquireOutcome::Ambiguous {
                    error: error.clone(),
                    cleanup_owned_by_client: false,
                })
                .collect(),
        }
    }

    /// Release lock
    async fn release(&self, lock_id: &LockId) -> Result<bool>;

    /// Idempotently clean up a previously confirmed grant for this exact lock ID.
    ///
    /// Call only after acquisition has completed successfully. An absent lock
    /// cannot settle an acquisition that may still grant it later; that
    /// acquisition must retain its own completion cleanup. This operation must
    /// never release another owner's lock on the same resource.
    ///
    /// The default preserves conservative behavior for clients without an
    /// authoritative absence result. Cancellation or an error leaves cleanup
    /// unconfirmed, so the caller must retain its retry ownership.
    ///
    /// # Errors
    /// Returns backend or transport errors when cleanup cannot be confirmed.
    async fn cleanup_grant(&self, lock_id: &LockId) -> Result<LockCleanupOutcome> {
        self.release(lock_id).await.map(|released| {
            if released {
                LockCleanupOutcome::Released
            } else {
                LockCleanupOutcome::Unconfirmed
            }
        })
    }

    /// Release multiple locks. Default implementation fans out to single-lock releases.
    async fn release_locks_batch(&self, lock_ids: &[LockId]) -> Result<Vec<bool>> {
        Ok(join_all(lock_ids.iter().map(|lock_id| self.release(lock_id)))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?)
    }

    /// Refresh lock
    async fn refresh(&self, lock_id: &LockId) -> Result<bool>;

    /// Force release lock
    async fn force_release(&self, lock_id: &LockId) -> Result<bool>;

    /// Check lock status
    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>>;

    /// Return authoritative lease information when this client owns lease state.
    ///
    /// Clients that do not manage renewable leases return an empty snapshot.
    async fn list_lock_leases(&self) -> Vec<LockLeaseInfo> {
        Vec::new()
    }

    /// Get statistics
    async fn get_stats(&self) -> Result<LockStats>;

    /// Close client
    async fn close(&self) -> Result<()>;

    /// Check if client is online
    async fn is_online(&self) -> bool;

    /// Check if client is local
    async fn is_local(&self) -> bool;
}

/// Client factory
pub struct ClientFactory;

impl ClientFactory {
    /// Create local client
    pub fn create_local() -> Arc<dyn LockClient> {
        Arc::new(local::LocalClient::new())
    }

    // /// Create remote client
    // pub fn create_remote(endpoint: String) -> Arc<dyn LockClient> {
    //     Arc::new(remote::RemoteClient::new(endpoint))
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_failure_text_never_proves_no_grant() {
        for message in [
            "Lock acquisition timeout",
            "Lock conflict: resource held by holder",
            "Remote lock RPC timed out",
        ] {
            assert!(matches!(
                BatchAcquireOutcome::from_legacy_response(LockResponse::failure(message, Duration::ZERO)),
                BatchAcquireOutcome::Ambiguous {
                    cleanup_owned_by_client: false,
                    ..
                }
            ));
        }
    }

    #[test]
    fn legacy_success_without_metadata_remains_a_grant() {
        assert!(matches!(
            BatchAcquireOutcome::from_legacy_response(LockResponse {
                success: true,
                lock_info: None,
                error: None,
                wait_time: Duration::ZERO,
                position_in_queue: None,
            }),
            BatchAcquireOutcome::Granted { lock_info: None, .. }
        ));
    }
}
