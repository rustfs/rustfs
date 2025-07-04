#![allow(dead_code)]
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

use crate::core::local::LocalLockManager;
use crate::error::Result;
use async_trait::async_trait;
use lock_args::LockArgs;
use once_cell::sync::Lazy;
use client::remote::RemoteClient;
use std::sync::Arc;
use std::sync::LazyLock;
use tokio::sync::RwLock;
pub mod lock_args;

// Refactored architecture modules
pub mod client;
pub mod config;
pub mod core;
pub mod deadlock_detector;
pub mod error;
pub mod namespace;

pub mod types;
pub mod utils;

// Re-export commonly used types
pub use config::LockConfig;
pub use core::{LockHandle, LockManager, LockManagerImpl};
pub use error::{LockError, Result as LockResult};
pub use namespace::{NamespaceLockManager, NsLockMap};
pub use types::{LockId, LockInfo, LockRequest, LockResponse, LockStats, LockType};

// Backward compatibility constants and type aliases
pub const MAX_DELETE_LIST: usize = 1000;

// Global local lock service instance for distributed lock modules
pub static GLOBAL_LOCAL_SERVER: Lazy<Arc<RwLock<core::local::LocalLockMap>>> =
    Lazy::new(|| Arc::new(RwLock::new(core::local::LocalLockMap::new())));

type LockClient = dyn Locker;

#[async_trait]
pub trait Locker {
    async fn lock(&mut self, args: &LockArgs) -> Result<bool>;
    async fn unlock(&mut self, args: &LockArgs) -> Result<bool>;
    async fn rlock(&mut self, args: &LockArgs) -> Result<bool>;
    async fn runlock(&mut self, args: &LockArgs) -> Result<bool>;
    async fn refresh(&mut self, args: &LockArgs) -> Result<bool>;
    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool>;
    async fn close(&self);
    async fn is_online(&self) -> bool;
    async fn is_local(&self) -> bool;
}

#[derive(Debug, Clone)]
pub enum LockApi {
    Local,
    Remote(RemoteClient),
}

#[async_trait]
impl Locker for LockApi {
    async fn lock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => {
                let resource = args
                    .resources
                    .first()
                    .ok_or_else(|| crate::error::LockError::internal("No resource specified"))?;
                let timeout = std::time::Duration::from_secs(30);
                GLOBAL_LOCAL_SERVER
                    .write()
                    .await
                    .lock(resource, &args.owner, timeout)
                    .await
                    .map_err(|e| crate::error::LockError::internal(format!("Local lock failed: {e}")))
            }
            LockApi::Remote(r) => r.lock(args).await,
        }
    }

    async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => {
                let resource = args
                    .resources
                    .first()
                    .ok_or_else(|| crate::error::LockError::internal("No resource specified"))?;
                GLOBAL_LOCAL_SERVER
                    .write()
                    .await
                    .unlock(resource, &args.owner)
                    .await
                    .map(|_| true)
                    .map_err(|e| crate::error::LockError::internal(format!("Local unlock failed: {e}")))
            }
            LockApi::Remote(r) => r.unlock(args).await,
        }
    }

    async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => {
                let resource = args
                    .resources
                    .first()
                    .ok_or_else(|| crate::error::LockError::internal("No resource specified"))?;
                let timeout = std::time::Duration::from_secs(30);
                GLOBAL_LOCAL_SERVER
                    .write()
                    .await
                    .rlock(resource, &args.owner, timeout)
                    .await
                    .map_err(|e| crate::error::LockError::internal(format!("Local rlock failed: {e}")))
            }
            LockApi::Remote(r) => r.rlock(args).await,
        }
    }

    async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => {
                let resource = args
                    .resources
                    .first()
                    .ok_or_else(|| crate::error::LockError::internal("No resource specified"))?;
                GLOBAL_LOCAL_SERVER
                    .write()
                    .await
                    .runlock(resource, &args.owner)
                    .await
                    .map(|_| true)
                    .map_err(|e| crate::error::LockError::internal(format!("Local runlock failed: {e}")))
            }
            LockApi::Remote(r) => r.runlock(args).await,
        }
    }

    async fn refresh(&mut self, _args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => Ok(true), // Local locks don't need refresh
            LockApi::Remote(r) => r.refresh(_args).await,
        }
    }

    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => {
                let resource = args
                    .resources
                    .first()
                    .ok_or_else(|| crate::error::LockError::internal("No resource specified"))?;
                GLOBAL_LOCAL_SERVER
                    .write()
                    .await
                    .unlock(resource, &args.owner)
                    .await
                    .map(|_| true)
                    .map_err(|e| crate::error::LockError::internal(format!("Local force unlock failed: {e}")))
            }
            LockApi::Remote(r) => r.force_unlock(args).await,
        }
    }

    async fn close(&self) {
        match self {
            LockApi::Local => (), // Local locks don't need to be closed
            LockApi::Remote(r) => r.close().await,
        }
    }

    async fn is_online(&self) -> bool {
        match self {
            LockApi::Local => true, // Local locks are always online
            LockApi::Remote(r) => r.is_online().await,
        }
    }

    async fn is_local(&self) -> bool {
        match self {
            LockApi::Local => true,
            LockApi::Remote(r) => r.is_local().await,
        }
    }
}

pub fn new_lock_api(is_local: bool, url: Option<url::Url>) -> LockApi {
    if is_local {
        LockApi::Local
    } else {
        let url = url.expect("URL must be provided for remote lock API");
        LockApi::Remote(RemoteClient::from_url(url))
    }
}

pub fn create_lock_manager(config: LockConfig) -> LockResult<LockManagerImpl> {
    LockManagerImpl::new(config)
}

pub fn create_local_client() -> Arc<dyn client::LockClient> {
    Arc::new(client::local::LocalClient::new())
}

pub fn create_remote_client(endpoint: String) -> Arc<dyn client::LockClient> {
    Arc::new(client::remote::RemoteClient::new(endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_api() {
        let local_api = new_lock_api(true, None);
        assert!(matches!(local_api, LockApi::Local));

        let url = url::Url::parse("http://localhost:8080").unwrap();
        let remote_api = new_lock_api(false, Some(url));
        assert!(matches!(remote_api, LockApi::Remote(_)));
    }

    #[tokio::test]
    async fn test_backward_compatibility() {
        let client = create_local_client();
        assert!(client.is_local().await);
    }
}
