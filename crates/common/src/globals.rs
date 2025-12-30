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

pub static GLOBAL_LOCAL_NODE_NAME: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_RUSTFS_HOST: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_RUSTFS_PORT: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("9000".to_string()));
pub static GLOBAL_RUSTFS_ADDR: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new("".to_string()));
pub static GLOBAL_CONN_MAP: LazyLock<RwLock<HashMap<String, Channel>>> = LazyLock::new(|| RwLock::new(HashMap::new()));
pub static GLOBAL_ROOT_CERT: LazyLock<RwLock<Option<Vec<u8>>>> = LazyLock::new(|| RwLock::new(None));
pub static GLOBAL_MTLS_IDENTITY: LazyLock<RwLock<Option<MtlsIdentityPem>>> = LazyLock::new(|| RwLock::new(None));

/// Set the global RustFS address used for gRPC connections.
///
/// # Arguments
/// * `addr` - A string slice representing the RustFS address (e.g., "https://node1:9000").
pub async fn set_global_addr(addr: &str) {
    *GLOBAL_RUSTFS_ADDR.write().await = addr.to_string();
}

/// Set the global root CA certificate for outbound gRPC clients.
/// This certificate is used to validate server TLS certificates.
/// When set to None, clients use the system default root CAs.
///
/// # Arguments
/// * `cert` - A vector of bytes representing the PEM-encoded root CA certificate.
pub async fn set_global_root_cert(cert: Vec<u8>) {
    *GLOBAL_ROOT_CERT.write().await = Some(cert);
}

/// Set the global mTLS identity (cert+key PEM) for outbound gRPC clients.
/// When set, clients will present this identity to servers requesting/requiring mTLS.
/// When None, clients proceed with standard server-authenticated TLS.
///
/// # Arguments
/// * `identity` - An optional MtlsIdentityPem struct containing the cert and key PEM.
pub async fn set_global_mtls_identity(identity: Option<MtlsIdentityPem>) {
    *GLOBAL_MTLS_IDENTITY.write().await = identity;
}

/// Evict a stale/dead connection from the global connection cache.
/// This is critical for cluster recovery when a node dies unexpectedly (e.g., power-off).
/// By removing the cached connection, subsequent requests will establish a fresh connection.
///
/// # Arguments
/// * `addr` - The address of the connection to evict.
pub async fn evict_connection(addr: &str) {
    let removed = GLOBAL_CONN_MAP.write().await.remove(addr);
    if removed.is_some() {
        tracing::warn!("Evicted stale connection from cache: {}", addr);
    }
}

/// Check if a connection exists in the cache for the given address.
///
/// # Arguments
/// * `addr` - The address to check.
///
/// # Returns
/// * `bool` - True if a cached connection exists, false otherwise.
pub async fn has_cached_connection(addr: &str) -> bool {
    GLOBAL_CONN_MAP.read().await.contains_key(addr)
}

/// Clear all cached connections. Useful for full cluster reset/recovery.
pub async fn clear_all_connections() {
    let mut map = GLOBAL_CONN_MAP.write().await;
    let count = map.len();
    map.clear();
    if count > 0 {
        tracing::warn!("Cleared {} cached connections from global map", count);
    }
}
/// Optional client identity (cert+key PEM) for outbound mTLS.
///
/// When present, gRPC clients will present this identity to servers requesting/requiring mTLS.
/// When absent, clients proceed with standard server-authenticated TLS.
#[derive(Clone, Debug)]
pub struct MtlsIdentityPem {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}
