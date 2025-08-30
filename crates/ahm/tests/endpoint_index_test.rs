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

//! 测试Endpoint索引设置是否正确

use rustfs_ecstore::endpoints::{EndpointServerPools, PoolEndpoints, Endpoints};
use rustfs_ecstore::disk::endpoint::Endpoint;
use tempfile::TempDir;
use std::net::SocketAddr;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_endpoint_index_settings() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    
    // 创建测试磁盘路径
    let disk_paths: Vec<_> = (0..4)
        .map(|i| temp_dir.path().join(format!("disk{}", i)))
        .collect();

    for path in &disk_paths {
        tokio::fs::create_dir_all(path).await?;
    }

    // 构建endpoints
    let mut endpoints: Vec<Endpoint> = disk_paths.iter()
        .map(|p| Endpoint::try_from(p.to_string_lossy().as_ref()).unwrap())
        .collect();
    
    // 正确设置Endpoint索引
    for (i, endpoint) in endpoints.iter_mut().enumerate() {
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);  // 注意：disk_index是usize类型
        println!("Endpoint {}: pool_idx={}, set_idx={}, disk_idx={}", 
            i, endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx);
    }

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: endpoints.len(),
        endpoints: Endpoints::from(endpoints.clone()),
        cmd_line: "test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
    
    // 验证所有Endpoint的索引都在有效范围内
    for (i, ep) in endpoints.iter().enumerate() {
        assert_eq!(ep.pool_idx, 0, "Endpoint {} pool_idx should be 0", i);
        assert_eq!(ep.set_idx, 0, "Endpoint {} set_idx should be 0", i);
        assert_eq!(ep.disk_idx, i as i32, "Endpoint {} disk_idx should be {}", i, i);
        println!("Endpoint {} indices are valid: pool={}, set={}, disk={}", 
            i, ep.pool_idx, ep.set_idx, ep.disk_idx);
    }
    
    // 测试ECStore初始化
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await?;
    
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let ecstore = rustfs_ecstore::store::ECStore::new(server_addr, endpoint_pools).await?;
    
    println!("ECStore initialized successfully with {} pools", ecstore.pools.len());
    
    Ok(())
}