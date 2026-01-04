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

#![cfg(test)]

use rustfs_ahm::scanner::data_scanner::Scanner;
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::GLOBAL_Endpoints;
use rustfs_ecstore::bucket::metadata_sys::{BucketMetadataSys, GLOBAL_BucketMetadataSys};
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::{ObjectIO, PutObjReader, StorageAPI};
use std::sync::{Arc, Once};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Level;

/// Build a minimal single-node ECStore over a temp directory and populate objects.
async fn create_store_with_objects(count: usize) -> (TempDir, std::sync::Arc<ECStore>) {
    let temp_dir = TempDir::new().expect("temp dir");
    let root = temp_dir.path().to_string_lossy().to_string();

    // Create endpoints from the temp dir
    let (endpoint_pools, _setup) = EndpointServerPools::from_volumes("127.0.0.1:0", vec![root])
        .await
        .expect("endpoint pools");

    // Seed globals required by metadata sys if not already set
    if GLOBAL_Endpoints.get().is_none() {
        let _ = GLOBAL_Endpoints.set(endpoint_pools.clone());
    }

    let store = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, CancellationToken::new())
        .await
        .expect("create store");

    if rustfs_ecstore::global::new_object_layer_fn().is_none() {
        rustfs_ecstore::global::set_object_layer(store.clone()).await;
    }

    // Initialize metadata system before bucket operations
    if GLOBAL_BucketMetadataSys.get().is_none() {
        let mut sys = BucketMetadataSys::new(store.clone());
        sys.init(Vec::new()).await;
        let _ = GLOBAL_BucketMetadataSys.set(Arc::new(RwLock::new(sys)));
    }

    store
        .make_bucket("fallback-bucket", &rustfs_ecstore::store_api::MakeBucketOptions::default())
        .await
        .expect("make bucket");

    for i in 0..count {
        let key = format!("obj-{i:04}");
        let data = format!("payload-{i}");
        let mut reader = PutObjReader::from_vec(data.into_bytes());
        store
            .put_object("fallback-bucket", &key, &mut reader, &rustfs_ecstore::store_api::ObjectOptions::default())
            .await
            .expect("put object");
    }

    (temp_dir, store)
}

static INIT: Once = Once::new();

fn init_tracing(filter_level: Level) {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_max_level(filter_level)
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_thread_names(true)
            .try_init();
    });
}

#[tokio::test]
async fn fallback_builds_full_counts_over_100_objects() {
    init_tracing(Level::ERROR);
    let (_tmp, store) = create_store_with_objects(1000).await;
    let scanner = Scanner::new(None, None);

    // Directly call the fallback builder to ensure pagination works.
    let usage: DataUsageInfo = scanner.build_data_usage_from_ecstore(&store).await.expect("fallback usage");

    let bucket = usage.buckets_usage.get("fallback-bucket").expect("bucket usage present");

    assert!(
        usage.objects_total_count >= 1000,
        "total objects should be >=1000, got {}",
        usage.objects_total_count
    );
    assert!(
        bucket.objects_count >= 1000,
        "bucket objects should be >=1000, got {}",
        bucket.objects_count
    );
}
