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

use std::collections::HashMap;
use std::sync::Arc;
use rustfs_lock::client::{LockClient, local::LocalClient, remote::RemoteClient};
use crate::disk::endpoint::Endpoint;
use crate::error::Result;

/// Create unique lock clients from endpoints
/// This function creates one client per unique host:port combination
/// to avoid duplicate connections to the same server
pub async fn create_unique_clients(endpoints: &[Endpoint]) -> Result<Vec<Arc<dyn LockClient>>> {
    let mut unique_endpoints: HashMap<String, &Endpoint> = HashMap::new();
    
    // Collect unique endpoints based on host:port
    for endpoint in endpoints {
        if endpoint.is_local {
            // For local endpoints, use "local" as the key
            unique_endpoints.insert("local".to_string(), endpoint);
        } else {
            // For remote endpoints, use host:port as the key
            let host_port = format!(
                "{}:{}", 
                endpoint.url.host_str().unwrap_or("localhost"), 
                endpoint.url.port().unwrap_or(9000)
            );
            unique_endpoints.insert(host_port, endpoint);
        }
    }
    
    let mut clients = Vec::new();
    
    // Create clients for unique endpoints
    for (_key, endpoint) in unique_endpoints {
        if endpoint.is_local {
            // For local endpoints, create a local lock client
            let local_client = LocalClient::new();
            clients.push(Arc::new(local_client) as Arc<dyn LockClient>);
        } else {
            // For remote endpoints, create a remote lock client
            let remote_client = RemoteClient::new(endpoint.url.to_string());
            clients.push(Arc::new(remote_client) as Arc<dyn LockClient>);
        }
    }
    
    Ok(clients)
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[tokio::test]
    async fn test_create_unique_clients_local() {
        let endpoints = vec![
            Endpoint {
                url: Url::parse("http://localhost:9000").unwrap(),
                is_local: true,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            },
            Endpoint {
                url: Url::parse("http://localhost:9000").unwrap(),
                is_local: true,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 1,
            },
        ];

        let clients = create_unique_clients(&endpoints).await.unwrap();
        // Should only create one client for local endpoints
        assert_eq!(clients.len(), 1);
        assert!(clients[0].is_local().await);
    }

    #[tokio::test]
    async fn test_create_unique_clients_mixed() {
        let endpoints = vec![
            Endpoint {
                url: Url::parse("http://localhost:9000").unwrap(),
                is_local: true,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            },
            Endpoint {
                url: Url::parse("http://remote1:9000").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 1,
            },
            Endpoint {
                url: Url::parse("http://remote1:9000").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 2,
            },
            Endpoint {
                url: Url::parse("http://remote2:9000").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 3,
            },
        ];

        let clients = create_unique_clients(&endpoints).await.unwrap();
        // Should create 3 clients: 1 local + 2 unique remote
        assert_eq!(clients.len(), 3);
        
        // Check that we have one local client
        let local_count = clients.iter().filter(|c| futures::executor::block_on(c.is_local())).count();
        assert_eq!(local_count, 1);
        
        // Check that we have two remote clients
        let remote_count = clients.iter().filter(|c| !futures::executor::block_on(c.is_local())).count();
        assert_eq!(remote_count, 2);
    }
}