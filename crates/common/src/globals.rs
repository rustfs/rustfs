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

#![allow(non_upper_case_globals)] // FIXME

use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub static GLOBAL_Local_Node_Name: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Rustfs_Host: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Rustfs_Port: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("9000".to_string()));
pub static GLOBAL_Rustfs_Addr: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_Conn_Map: LazyLock<RwLock<HashMap<String, Channel>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

pub async fn set_global_addr(addr: &str) {
    *GLOBAL_Rustfs_Addr.write().await = addr.to_string();
}

/// Evict a stale/dead connection from the global connection cache.
/// This is critical for cluster recovery when a node dies unexpectedly (e.g., power-off).
/// By removing the cached connection, subsequent requests will establish a fresh connection.
pub async fn evict_connection(addr: &str) {
    let removed = GLOBAL_Conn_Map.write().await.remove(addr);
    if removed.is_some() {
        tracing::warn!("Evicted stale connection from cache: {}", addr);
    }
}

/// Check if a connection exists in the cache for the given address.
pub async fn has_cached_connection(addr: &str) -> bool {
    GLOBAL_Conn_Map.read().await.contains_key(addr)
}

/// Clear all cached connections. Useful for full cluster reset/recovery.
pub async fn clear_all_connections() {
    let mut map = GLOBAL_Conn_Map.write().await;
    let count = map.len();
    map.clear();
    if count > 0 {
        tracing::warn!("Cleared {} cached connections from global map", count);
    }
}
