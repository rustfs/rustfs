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

use rustfs_common::heal_channel::{HealOpts, HealScanMode};
use rustfs_ecstore::{
    bucket::metadata_sys,
    disk::endpoint::Endpoint,
    endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    store::ECStore,
    store_api::{BucketOperations, BucketOptions, HealOperations, MakeBucketOptions, ObjectIO, ObjectOptions, PutObjReader},
};
use rustfs_object_capacity::capacity_manager::{HybridStrategyConfig, create_isolated_manager};
use serial_test::serial;
use std::{
    collections::HashSet,
    fs as stdfs,
    path::Path,
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
};
use tempfile::TempDir;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static CAPACITY_DIRTY_SCOPE_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>, TempDir)> = OnceLock::new();
static CAPACITY_DIRTY_SCOPE_INIT: Once = Once::new();

fn init_capacity_dirty_scope_tracing() {
    CAPACITY_DIRTY_SCOPE_INIT.call_once(|| {});
}

async fn setup_capacity_dirty_scope_env() -> (Vec<PathBuf>, Arc<ECStore>) {
    init_capacity_dirty_scope_tracing();

    if let Some((paths, store, _)) = CAPACITY_DIRTY_SCOPE_ENV.get() {
        return (paths.clone(), store.clone());
    }

    let temp_dir = TempDir::new().expect("create temp dir for capacity dirty scope test");
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
        cmd_line: "capacity-dirty-scope-test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };

    let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);
    rustfs_ecstore::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

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

    let _ = CAPACITY_DIRTY_SCOPE_ENV.set((disk_paths.clone(), ecstore.clone(), temp_dir));
    (disk_paths, ecstore)
}

fn find_part_file(root: &Path, part_name: &str) -> Option<PathBuf> {
    let entries = stdfs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_part_file(&path, part_name) {
                return Some(found);
            }
            continue;
        }

        if path.file_name().and_then(|name| name.to_str()) == Some(part_name) {
            return Some(path);
        }
    }

    None
}

#[tokio::test]
#[serial]
async fn data_movement_put_object_marks_dirty_disks_for_capacity_manager() {
    let (disk_paths, ecstore) = setup_capacity_dirty_scope_env().await;
    let bucket_name = format!("dirty-scope-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket_name, &MakeBucketOptions::default())
        .await
        .expect("create test bucket");

    let manager = create_isolated_manager(HybridStrategyConfig::default());
    let _ = manager.get_dirty_disks().await;

    let payload = b"data-movement-dirty-scope".to_vec();
    let mut reader = PutObjReader::from_vec(payload);
    let opts = ObjectOptions {
        data_movement: true,
        src_pool_idx: 0,
        ..Default::default()
    };

    ecstore
        .put_object(&bucket_name, "object.bin", &mut reader, &opts)
        .await
        .expect("data movement put_object should succeed");

    let dirty_disks = manager.get_dirty_disks().await;
    assert_eq!(dirty_disks.len(), disk_paths.len());

    let actual_paths: HashSet<_> = dirty_disks
        .into_iter()
        .map(|disk| stdfs::canonicalize(&disk.drive_path).unwrap().to_string_lossy().into_owned())
        .collect();
    let expected_paths: HashSet<_> = disk_paths
        .iter()
        .map(|path| stdfs::canonicalize(path).unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(actual_paths, expected_paths);
}

#[tokio::test]
#[serial]
async fn heal_object_marks_missing_shard_disk_dirty_for_capacity_manager() {
    let (disk_paths, ecstore) = setup_capacity_dirty_scope_env().await;
    let bucket_name = format!("dirty-heal-{}", Uuid::new_v4());

    ecstore
        .make_bucket(&bucket_name, &MakeBucketOptions::default())
        .await
        .expect("create test bucket");

    let manager = create_isolated_manager(HybridStrategyConfig::default());
    let _ = manager.get_dirty_disks().await;

    let payload_len = 3 * 1024 * 1024 + 137;
    let payload: Vec<u8> = (0..payload_len).map(|idx| (idx % 251) as u8).collect();
    let mut reader = PutObjReader::from_vec(payload);
    let object_name = "test/heal.bin";
    let put_info = ecstore
        .put_object(&bucket_name, object_name, &mut reader, &ObjectOptions::default())
        .await
        .expect("put object for heal test");
    assert!(put_info.data_blocks > 1, "expected multi-shard object for heal test");

    let _ = manager.get_dirty_disks().await;

    let object_root = disk_paths[0].join(&bucket_name).join("test").join("heal.bin");
    let missing_part = find_part_file(&object_root, "part.1").expect("part file on first disk");
    fs::remove_file(&missing_part).await.expect("remove shard to force heal");

    let heal_opts = HealOpts {
        recursive: false,
        dry_run: false,
        remove: false,
        recreate: true,
        scan_mode: HealScanMode::Deep,
        update_parity: true,
        no_lock: false,
        pool: None,
        set: None,
    };

    let (_result, error) = ecstore
        .heal_object(&bucket_name, object_name, "", &heal_opts)
        .await
        .expect("heal_object call should succeed");

    let dirty_disks = manager.get_dirty_disks().await;
    let actual_paths: HashSet<_> = dirty_disks
        .into_iter()
        .map(|disk| stdfs::canonicalize(&disk.drive_path).unwrap().to_string_lossy().into_owned())
        .collect();
    let expected_missing_disk = stdfs::canonicalize(&disk_paths[0]).unwrap().to_string_lossy().into_owned();

    assert!(
        error.is_none() || actual_paths.contains(&expected_missing_disk),
        "heal returned {error:?} and did not mark the repaired shard disk dirty: {actual_paths:?}"
    );
}
