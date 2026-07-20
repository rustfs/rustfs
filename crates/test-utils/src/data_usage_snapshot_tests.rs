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

use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
use std::time::{Duration, SystemTime};

use crate::TestECStoreEnv;
use crate::ecstore_test_compat::fixture::{
    BUCKET_META_PREFIX, delete_config, load_data_usage_from_backend, load_data_usage_from_backend_cached, read_config,
    remove_bucket_usage_from_backend, save_config, store_data_usage_in_backend,
};

fn snapshot(bucket: &str, last_update: SystemTime) -> DataUsageInfo {
    let mut info = DataUsageInfo {
        last_update: Some(last_update),
        ..Default::default()
    };
    info.buckets_usage.insert(
        bucket.to_string(),
        BucketUsageInfo {
            objects_count: 3,
            size: 42,
            ..Default::default()
        },
    );
    info.bucket_sizes.insert(bucket.to_string(), 42);
    info.buckets_count = 1;
    info.calculate_totals();
    info
}

#[tokio::test]
async fn data_usage_snapshot_recovery_wires_primary_backup_and_negative_cache() {
    let temp_root = tempfile::tempdir().expect("create test temp root");
    let env = TestECStoreEnv::builder()
        .base_dir(temp_root.path())
        .init_bucket_metadata(false)
        .build()
        .await;
    let primary = format!("{BUCKET_META_PREFIX}/.usage.json");
    let backup = format!("{primary}.bkp");
    let last_update = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let expected = snapshot("bucket-a", last_update);

    store_data_usage_in_backend(expected.clone(), env.ecstore.clone())
        .await
        .expect("seed primary snapshot");
    let valid = read_config(env.ecstore.clone(), &primary).await.expect("read seeded primary");
    save_config(env.ecstore.clone(), &backup, valid.clone())
        .await
        .expect("seed backup snapshot");

    let corrupt = b"{not json".to_vec();
    save_config(env.ecstore.clone(), &primary, corrupt.clone())
        .await
        .expect("corrupt primary snapshot");
    let recovered = load_data_usage_from_backend(env.ecstore.clone())
        .await
        .expect("public loader must fall back to backup");
    assert!(recovered.buckets_usage.contains_key("bucket-a"));

    store_data_usage_in_backend(expected, env.ecstore.clone())
        .await
        .expect("equal backup timestamp must repair primary");
    let repaired = read_config(env.ecstore.clone(), &primary)
        .await
        .expect("read repaired primary");
    serde_json::from_slice::<DataUsageInfo>(&repaired).expect("primary must contain a valid snapshot after repair");

    save_config(env.ecstore.clone(), &primary, corrupt.clone())
        .await
        .expect("corrupt primary before destructive update");
    remove_bucket_usage_from_backend(env.ecstore.clone(), "bucket-a")
        .await
        .expect_err("destructive updates must not promote a backup snapshot");
    assert_eq!(
        read_config(env.ecstore.clone(), &primary)
            .await
            .expect("read corrupt primary"),
        corrupt
    );

    delete_config(env.ecstore.clone(), &backup)
        .await
        .expect("remove backup before empty-primary check");
    save_config(env.ecstore.clone(), &primary, Vec::new())
        .await
        .expect("write empty primary snapshot");
    load_data_usage_from_backend(env.ecstore.clone())
        .await
        .expect_err("an empty primary with no backup must not look missing");

    delete_config(env.ecstore.clone(), &primary)
        .await
        .expect("remove primary before empty-backup check");
    save_config(env.ecstore.clone(), &backup, Vec::new())
        .await
        .expect("write empty backup snapshot");
    load_data_usage_from_backend(env.ecstore.clone())
        .await
        .expect_err("an empty backup must not look missing");

    delete_config(env.ecstore.clone(), &backup)
        .await
        .expect("remove backup before negative-cache check");
    save_config(env.ecstore.clone(), &primary, corrupt.clone())
        .await
        .expect("restore corrupt primary before negative-cache check");
    load_data_usage_from_backend_cached(env.ecstore.clone())
        .await
        .expect_err("corrupt primary without backup must fail");
    save_config(env.ecstore.clone(), &primary, valid)
        .await
        .expect("repair primary without invalidating the in-process cache");
    load_data_usage_from_backend(env.ecstore.clone())
        .await
        .expect("direct load must observe repaired primary");
    load_data_usage_from_backend_cached(env.ecstore.clone())
        .await
        .expect_err("cached loader must reuse the recent failure until its TTL expires");
}
