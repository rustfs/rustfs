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
use super::storage_api::test::bucket::quota::BucketQuota;
use super::storage_api::test::bucket::quota::checker::QuotaChecker;
use super::storage_api::test::contract::bucket::MakeBucketOptions;
use super::storage_api::test::contract::bucket::{BucketOperations, BucketOptions};
use super::storage_api::test::{ECStore, Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
use super::{context::AppContext, object_traffic_health::ObjectTrafficHealth};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;
use tokio::fs;
use tokio_util::sync::CancellationToken;

static SHARED_GATING_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, TempDir)> = OnceLock::new();
static SHARED_GATING_INIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Return a shared 4-disk `ECStore` with bucket metadata initialized.
///
/// The first caller creates the store and initializes the metadata system;
/// subsequent callers get the same `Arc<ECStore>`. Safe to call from
/// `#[serial]` tests that share the process bootstrap context.
pub(crate) async fn shared_gating_ecstore() -> Arc<ECStore> {
    if let Some((_paths, store, _)) = SHARED_GATING_ENV.get() {
        return store.clone();
    }
    let _init_guard = SHARED_GATING_INIT.lock().await;
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
    let topology_fingerprint =
        crate::storage::storage_api::heal_control_startup_consumer::heal_topology_fingerprint(&endpoint_pools)
            .expect("single-node gating topology should hash");
    crate::storage::storage_api::start_remote_version_state_fleet_probe(topology_fingerprint);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while crate::storage::storage_api::ecstore_notification::acquire_cross_pool_fence_fleet_proof().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("single-node cross-pool fence capability proof should publish");

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

pub(crate) async fn durable_quota_test_bucket(prefix: &str, limit: u64) -> (Arc<ECStore>, String) {
    let store = shared_gating_ecstore().await;
    crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
    let bucket = format!("{prefix:.30}-{}", uuid::Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create durable quota test bucket");
    super::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 0).await;
    let metadata_sys =
        crate::app::storage_api::test::get_global_bucket_metadata_sys().expect("test app context should expose bucket metadata");
    QuotaChecker::new(metadata_sys)
        .set_quota_config(&bucket, BucketQuota::new(Some(limit)))
        .await
        .expect("configure durable quota test bucket");
    (store, bucket)
}

pub(crate) async fn shared_gating_ambient() -> Arc<AppContext> {
    let store = shared_gating_ecstore().await;
    if let Some(ambient) = crate::runtime_sources::current_app_context() {
        return ambient;
    }

    let _init_guard = SHARED_GATING_INIT.lock().await;
    if crate::runtime_sources::current_app_context().is_none() {
        super::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
    }
    crate::runtime_sources::current_app_context().expect("object traffic test context must be installed")
}

pub(crate) fn app_context_from_current_environment(ambient: &AppContext) -> AppContext {
    AppContext::new(ambient.object_store(), ambient.iam(), ambient.kms())
}

pub(crate) async fn app_context_with_object_traffic_health(object_traffic_health: Arc<ObjectTrafficHealth>) -> Arc<AppContext> {
    let ambient = shared_gating_ambient().await;
    Arc::new(app_context_from_current_environment(&ambient).with_test_object_traffic_health(object_traffic_health))
}

/// Like [`shared_gating_ecstore`], but also returns the backing disk paths so
/// tests can remove on-disk shards and simulate the object data vanishing
/// mid-stream.
pub(crate) async fn shared_gating_ecstore_and_disk_paths() -> (Vec<PathBuf>, Arc<ECStore>) {
    let _ = shared_gating_ecstore().await;
    let (disk_paths, store, _) = SHARED_GATING_ENV.get().expect("gating env must be initialized");
    (disk_paths.clone(), store.clone())
}

/// Build an isolated two-pool store for tests that move object data between
/// pools while a production read is in flight.
pub(crate) async fn isolated_multi_pool_ecstore() -> (TempDir, Vec<Vec<PathBuf>>, Arc<ECStore>) {
    let temp_dir = TempDir::new().expect("create temp dir for multi-pool gating test");
    let mut pool_disk_paths = Vec::with_capacity(2);
    let mut pools = Vec::with_capacity(2);
    for pool_index in 0..2 {
        let mut disk_paths = Vec::with_capacity(4);
        let mut endpoints = Vec::with_capacity(4);
        for disk_index in 0..4 {
            let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
            fs::create_dir_all(&disk_path)
                .await
                .expect("create multi-pool gating test disk");
            let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("test disk path must be utf8"))
                .expect("multi-pool test endpoint must parse");
            endpoint.set_pool_index(pool_index);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
            disk_paths.push(disk_path);
        }
        pool_disk_paths.push(disk_paths);
        pools.push(PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: format!("multi-pool-gating-{pool_index}"),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        });
    }

    let endpoint_pools = EndpointServerPools(pools);
    let instance_ctx = super::storage_api::test::runtime::new_instance_ctx();
    super::storage_api::test::runtime::init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
        .await
        .expect("initialize isolated multi-pool disks");
    let store = ECStore::new_with_instance_ctx(
        "127.0.0.1:0".parse().expect("multi-pool test address must parse"),
        endpoint_pools,
        CancellationToken::new(),
        instance_ctx,
    )
    .await
    .expect("initialize isolated multi-pool store");
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

    (temp_dir, pool_disk_paths, store)
}
