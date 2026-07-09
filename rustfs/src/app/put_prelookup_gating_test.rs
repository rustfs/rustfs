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

//! Regression coverage for rustfs/backlog#1009: the PUT path skips its
//! pre-PUT `get_object_info` only when `put_prelookup_worm_gate` proves the
//! existing-object WORM validation is a no-op. These tests pin the gate's
//! truth table against a real 4-disk `ECStore` with the bucket metadata sys
//! initialized, mirroring the HP-8 delete-gating fixture:
//!
//! - a bucket created with Object Lock keeps the prelookup (gate = true);
//! - a plain bucket takes the skip path (gate = false);
//! - an unknown bucket (metadata lookup error) fails closed (gate = true),
//!   so a degraded metadata subsystem can never silently drop the WORM check.

use super::object_usecase::put_prelookup_worm_gate;
use super::storage_api::test::bucket::metadata_sys;
use super::storage_api::test::contract::bucket::{BucketOperations, BucketOptions, MakeBucketOptions};
use super::storage_api::test::{ECStore, Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
use serial_test::serial;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static PUT_GATING_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, TempDir)> = OnceLock::new();

async fn setup_put_gating_env() -> Arc<ECStore> {
    if let Some((_paths, store, _)) = PUT_GATING_ENV.get() {
        return store.clone();
    }

    let temp_dir = TempDir::new().expect("create temp dir for put prelookup gating test");
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
        cmd_line: "put-prelookup-gating-test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
    super::storage_api::test::runtime::init_local_disks(endpoint_pools.clone())
        .await
        .unwrap();

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

    let _ = PUT_GATING_ENV.set((disk_paths, ecstore.clone(), temp_dir));
    ecstore
}

#[tokio::test]
#[serial]
async fn worm_gate_keeps_prelookup_for_object_lock_bucket() {
    let ecstore = setup_put_gating_env().await;
    let bucket = format!("put-gate-lock-{}", Uuid::new_v4());

    ecstore
        .make_bucket(
            &bucket,
            &MakeBucketOptions {
                lock_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("create object-lock bucket");

    assert!(
        put_prelookup_worm_gate(&bucket).await,
        "an object-lock bucket must keep the pre-PUT lookup"
    );
}

#[tokio::test]
#[serial]
async fn worm_gate_allows_skip_for_plain_bucket() {
    let ecstore = setup_put_gating_env().await;
    let bucket = format!("put-gate-plain-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create plain bucket");

    assert!(
        !put_prelookup_worm_gate(&bucket).await,
        "a bucket without object locking must take the prelookup-skip path"
    );
}

#[tokio::test]
#[serial]
async fn worm_gate_fails_closed_when_bucket_metadata_is_unavailable() {
    let _ecstore = setup_put_gating_env().await;
    let missing_bucket = format!("put-gate-missing-{}", Uuid::new_v4());

    assert!(
        put_prelookup_worm_gate(&missing_bucket).await,
        "a bucket-metadata lookup failure must fail closed and keep the pre-PUT lookup"
    );
}
