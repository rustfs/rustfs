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

use async_trait::async_trait;
use local_locker::LocalLocker;
use lock_args::LockArgs;
use remote_client::RemoteClient;
use std::io::Result;
use std::sync::Arc;
use std::sync::LazyLock;
use tokio::sync::RwLock;

pub mod drwmutex;
pub mod local_locker;
pub mod lock_args;
pub mod lrwmutex;
pub mod namespace_lock;
pub mod remote_client;

pub static GLOBAL_LOCAL_SERVER: LazyLock<Arc<RwLock<LocalLocker>>> = LazyLock::new(|| Arc::new(RwLock::new(LocalLocker::new())));

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
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.lock(args).await,
            LockApi::Remote(r) => r.lock(args).await,
        }
    }

    async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.unlock(args).await,
            LockApi::Remote(r) => r.unlock(args).await,
        }
    }

    async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.rlock(args).await,
            LockApi::Remote(r) => r.rlock(args).await,
        }
    }

    async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.runlock(args).await,
            LockApi::Remote(r) => r.runlock(args).await,
        }
    }

    async fn refresh(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.refresh(args).await,
            LockApi::Remote(r) => r.refresh(args).await,
        }
    }

    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.force_unlock(args).await,
            LockApi::Remote(r) => r.force_unlock(args).await,
        }
    }

    async fn close(&self) {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.read().await.close().await,
            LockApi::Remote(r) => r.close().await,
        }
    }

    async fn is_online(&self) -> bool {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.read().await.is_online().await,
            LockApi::Remote(r) => r.is_online().await,
        }
    }

    async fn is_local(&self) -> bool {
        match self {
            LockApi::Local => GLOBAL_LOCAL_SERVER.write().await.is_local().await,
            LockApi::Remote(r) => r.is_local().await,
        }
    }
}

pub fn new_lock_api(is_local: bool, url: Option<url::Url>) -> LockApi {
    if is_local {
        return LockApi::Local;
    }

    LockApi::Remote(RemoteClient::new(url.unwrap()))
}
