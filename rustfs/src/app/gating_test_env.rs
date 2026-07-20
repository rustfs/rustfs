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

//! Shared test fixture for gating tests (delete_objects_stat_gating_test,
//! put_prelookup_gating_test).
//!
//! Both test modules exercise bucket-metadata-dependent logic against a real
//! 4-disk `ECStore`. Because `ECStore::new()` adopts the process-level
//! bootstrap `InstanceContext` (a single `OnceLock`), calling
//! `init_bucket_metadata_sys` from two separate test modules would panic on
//! the second call. This module provides a single shared setup so only one
//! `ECStore` and one metadata-sys initialization exist per test binary.

use super::storage_api::test::bucket::metadata_sys;
use super::storage_api::test::contract::bucket::{BucketOperations, BucketOptions};
use super::storage_api::test::{ECStore, Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;
use tokio::fs;
use tokio_util::sync::CancellationToken;

static SHARED_GATING_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, TempDir)> = OnceLock::new();

/// Return a shared 4-disk `ECStore` with bucket metadata initialized.
///
/// The first caller creates the store and initializes the metadata system;
/// subsequent callers get the same `Arc<ECStore>`. Safe to call from
/// `#[serial]` tests that share the process bootstrap context.
pub(crate) async fn shared_gating_ecstore() -> Arc<ECStore> {
    if let Some((_paths, store, _)) = SHARED_GATING_ENV.get() {
        return store.clone();
    }

    let temp_dir = TempDir::new().expect("create temp dir for gating test env");
    let temp_path = temp_dir.path().to_path_buf();

    let disk_paths = vec![
        temp_path.join("disk1"),
        temp_path.join("disk2"),
        temp_path.join("disk3"),
        temp_path.join("disk4"),
    ];
    for disk_path in &disk_paths {
        fs::create_dir_all(disk_path).await.unwrap();
    }

    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);
        endpoints.push(endpoint);
    }

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "gating-test-env".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
    super::storage_api::test::runtime::init_local_disks(endpoint_pools.clone())
        .await
        .unwrap();
    crate::storage::storage_api::new_global_notification_sys(endpoint_pools.clone())
        .await
        .expect("initialize notification system for gating test env");

    let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap();

    let buckets_list = ecstore
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let buckets = buckets_list.into_iter().map(|v| v.name).collect();
    metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

    let _ = SHARED_GATING_ENV.set((disk_paths, ecstore.clone(), temp_dir));
    ecstore
}
