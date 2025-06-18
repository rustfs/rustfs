use async_trait::async_trait;
use rustfs_notify::store::{Key, Store, StoreError, StoreResult};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

pub mod mqtt;
pub mod webhook;

pub const STORE_PREFIX: &str = "rustfs";

// Target 公共 trait，对应 Go 的 Target 接口
#[async_trait]
pub trait Target: Send + Sync {
    fn name(&self) -> String;
    async fn send_from_store(&self, key: Key) -> StoreResult<()>;
    async fn is_active(&self) -> StoreResult<bool>;
    async fn close(&self) -> StoreResult<()>;
}

// TargetID 结构体，用于唯一标识目标
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetID {
    pub id: String,
    pub name: String,
}

impl TargetID {
    pub fn new(id: &str, name: &str) -> Self {
        Self {
            id: id.to_owned(),
            name: name.to_owned(),
        }
    }
}

impl ToString for TargetID {
    fn to_string(&self) -> String {
        format!("{}:{}", self.name, self.id)
    }
}

// TargetStore 接口
pub trait TargetStore {
    fn store<T>(&self) -> Option<Arc<dyn Store<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static;
}

pub type Logger = fn(ctx: Option<&str>, err: StoreError, id: &str, err_kind: &[&dyn std::fmt::Display]);
