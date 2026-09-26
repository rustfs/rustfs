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

//! Replacement healing must restore internal records only on their owning sets.
#![recursion_limit = "256"]

use http::HeaderMap;
use rustfs_heal::heal::{
    storage::{ECStoreHealStorage, HealObjectOptions, HealPutObjReader, HealStorageAPI},
    task::{HealOptions, HealPriority, HealRequest, HealTask, HealType},
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

mod storage_api;
use storage_api::endpoint_index::{ECStore, Endpoint, EndpointServerPools, Endpoints, PoolEndpoints, init_local_disks};
use storage_api::integration::{BucketOperations as _, MakeBucketOptions, NamespaceLocking as _, ObjectIO as _};
use storage_api::pool_metadata::{HEALING_MARKER_PATH, POOL_META_NAME, RUSTFS_META_BUCKET, init_bucket_metadata_sys};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_pool_metadata_follows_real_two_set_placement() {
    let temp = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut endpoints = Vec::new();
    for set in 0..2 {
        for disk in 0..4 {
            let path = temp.path().join(format!("set{set}-disk{disk}"));
            std::fs::create_dir_all(&path).unwrap();
            let mut endpoint = Endpoint::try_from(path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(0);
            endpoint.set_set_index(set);
            endpoint.set_disk_index(disk);
            paths.push(path);
            endpoints.push(endpoint);
        }
    }
    let endpoint_pools = EndpointServerPools::from(vec![PoolEndpoints {
        legacy: false,
        set_count: 2,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "heal-pool-metadata-two-sets".to_string(),
        platform: "test".to_string(),
    }]);
    init_local_disks(endpoint_pools.clone()).await.unwrap();
    let shutdown = CancellationToken::new();
    let store = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, shutdown.clone())
        .await
        .unwrap();
    let mut pool_meta = store.pool_meta.read().await.clone();
    pool_meta.dont_save = false;
    pool_meta.save(store.pools.clone()).await.unwrap();
    init_bucket_metadata_sys(store.clone(), vec![]).await;
    let storage = Arc::new(ECStoreHealStorage::new(store.clone()));
    let bucket = "metadata-placement";
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    for opts in [
        HealOpts::default(),
        HealOpts {
            pool: Some(0),
            ..Default::default()
        },
        HealOpts {
            set: Some(0),
            ..Default::default()
        },
        HealOpts {
            pool: Some(1),
            set: Some(0),
            ..Default::default()
        },
        HealOpts {
            pool: Some(0),
            set: Some(2),
            ..Default::default()
        },
    ] {
        assert!(
            storage.replacement_pool_metadata_required(&opts).is_err(),
            "invalid scope must fail closed"
        );
        assert!(
            storage
                .heal_replacement_bucket_metadata(bucket, &opts, &[paths[0].to_string_lossy().into_owned()])
                .await
                .is_err(),
            "bucket metadata repair must reject an incomplete or invalid replacement scope"
        );
    }

    for targets in [vec![], vec![paths[4].to_string_lossy().into_owned()]] {
        assert!(
            storage
                .heal_replacement_bucket_metadata(
                    bucket,
                    &HealOpts {
                        pool: Some(0),
                        set: Some(0),
                        ..Default::default()
                    },
                    &targets,
                )
                .await
                .is_err(),
            "empty or wrong-set targets must be rejected before repairing metadata"
        );
    }

    let mut owning_set = None;
    for set in 0..2 {
        let opts = HealOpts {
            recreate: true,
            scan_mode: HealScanMode::Deep,
            pool: Some(0),
            set: Some(set),
            ..Default::default()
        };
        let target_path = &paths[set * 4];
        let metadata_path = target_path.join(RUSTFS_META_BUCKET).join(POOL_META_NAME);
        let owns_metadata = metadata_path.join("xl.meta").exists();
        assert_eq!(storage.replacement_pool_metadata_required(&opts).unwrap(), owns_metadata);
        if owns_metadata {
            assert!(owning_set.replace(set).is_none(), "only one set owns pool.bin");
            std::fs::remove_dir_all(&metadata_path).unwrap();
        }
        let bucket_records: Vec<_> = [".metadata.bin", ".bucket-incarnation"]
            .into_iter()
            .map(|file| {
                let path = target_path.join(RUSTFS_META_BUCKET).join("buckets").join(bucket).join(file);
                let existed = path.join("xl.meta").exists();
                if existed {
                    std::fs::remove_dir_all(&path).expect("remove bucket metadata shard from replacement");
                }
                (path, existed)
            })
            .collect();
        // Select a user key using the real placement algorithm, independently
        // of the metadata-scope decision under test.
        let key = (0..1000)
            .map(|n| format!("object-{n}"))
            .find(|key| Arc::ptr_eq(&store.pools[0].get_disks_by_key(key), &store.pools[0].disk_set[set]))
            .unwrap();
        let mut versions = Vec::new();
        for seed in [7, 29] {
            let bytes: Vec<u8> = (0..(256 * 1024 + 137)).map(|i| ((i + seed) % 251) as u8).collect();
            let mut reader = HealPutObjReader::from_vec(bytes.clone());
            let info = store
                .put_object(
                    bucket,
                    &key,
                    &mut reader,
                    &HealObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            versions.push((info.version_id.unwrap().to_string(), bytes));
        }
        // Wait for detached PUT owners before deleting their committed shards.
        {
            let lock = store.new_ns_lock(bucket, &key).await.unwrap();
            let _guard = lock.get_write_lock(Duration::from_secs(30)).await.unwrap();
        }
        let object_path = target_path.join(bucket).join(&key);
        let original_parts: Vec<_> = walkdir::WalkDir::new(&object_path)
            .into_iter()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_str().unwrap().starts_with("part."))
            .map(|entry| {
                (
                    entry.path().strip_prefix(&object_path).unwrap().to_path_buf(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect();
        assert_eq!(original_parts.len(), 2, "both versions must have physical data shards");
        std::fs::remove_dir_all(&object_path).unwrap();
        let target = target_path.to_str().unwrap().to_string();
        let set_id = format!("pool_0_set_{set}");
        let mut request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec![bucket.to_string()],
                set_disk_id: set_id.clone(),
            },
            HealOptions {
                recreate_missing: true,
                scan_mode: HealScanMode::Deep,
                pool_index: Some(0),
                set_index: Some(set),
                timeout: Some(Duration::from_secs(60)),
                ..Default::default()
            },
            HealPriority::Normal,
        );
        request.heal_endpoints = vec![target];
        let task = HealTask::from_request(request, storage.clone());
        tokio::time::timeout(Duration::from_secs(60), task.execute())
            .await
            .unwrap()
            .expect("both owning and non-owning replacement sets must complete");
        assert!(
            !target_path.join(RUSTFS_META_BUCKET).join(HEALING_MARKER_PATH).exists(),
            "completed repair must clear its healing marker"
        );
        assert_eq!(metadata_path.join("xl.meta").exists(), owns_metadata);
        for (path, owned) in bucket_records {
            assert_eq!(
                path.join("xl.meta").exists(),
                owned,
                "replacement must restore each owned bucket record without creating a wrong-set copy: {}",
                path.display()
            );
        }
        for (relative, bytes) in original_parts {
            assert_eq!(std::fs::read(object_path.join(relative)).unwrap(), bytes, "reconstructed shard differs");
        }
        for (version, bytes) in versions {
            let mut reader = store
                .get_object_reader(
                    bucket,
                    &key,
                    None,
                    HeaderMap::new(),
                    &HealObjectOptions {
                        version_id: Some(version),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut actual = Vec::new();
            tokio::io::copy(&mut reader, &mut actual).await.unwrap();
            assert_eq!(actual, bytes, "current and historical versions must survive healing");
        }
    }

    let set = owning_set.expect("fixture must contain a persisted pool.bin");
    // Absence in the owning set must never be mistaken for out-of-scope data.
    for path in &paths[set * 4..set * 4 + 4] {
        std::fs::remove_dir_all(path.join(RUSTFS_META_BUCKET).join(POOL_META_NAME)).unwrap();
    }
    let opts = HealOpts {
        recreate: true,
        pool: Some(0),
        set: Some(set),
        ..Default::default()
    };
    assert!(storage.replacement_pool_metadata_required(&opts).unwrap());
    let set_id = format!("pool_0_set_{set}");
    let mut request = HealRequest::new(
        HealType::ErasureSet {
            buckets: vec![],
            set_disk_id: set_id.clone(),
        },
        HealOptions {
            recreate_missing: true,
            pool_index: Some(0),
            set_index: Some(set),
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.heal_endpoints = vec![paths[set * 4].to_str().unwrap().to_string()];
    let task = HealTask::from_request(request, storage);
    assert!(
        tokio::time::timeout(Duration::from_secs(60), task.execute())
            .await
            .unwrap()
            .is_err()
    );
    shutdown.cancel();
}
