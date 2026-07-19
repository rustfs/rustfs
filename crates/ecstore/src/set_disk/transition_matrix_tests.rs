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

use super::*;
use crate::bucket::lifecycle::lifecycle::{TRANSITION_PENDING, TransitionOptions};
use crate::ecstore_validation_blackbox::make_local_set_disks;
use crate::services::tier::test_util::register_mock_tier;
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
use tokio::io::AsyncReadExt;

async fn prime_metadata_generation(set_disks: &SetDisks, bucket: &str, object: &str) -> GetObjectMetadataCacheKey {
    set_disks
        .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
        .await
        .expect("object metadata should resolve");
    let generation = set_disks
        .get_object_metadata_cache_generation(bucket, object)
        .expect("metadata generation should be active");
    let key = GetObjectMetadataCacheKey::new(bucket, object, generation);
    assert!(
        set_disks.get_object_metadata_cache.get(&key).await.is_some(),
        "metadata read should publish the generation under test"
    );
    key
}

async fn assert_generation_reclaimed(set_disks: &SetDisks, key: &GetObjectMetadataCacheKey) {
    set_disks.get_object_metadata_cache.run_pending_tasks().await;
    assert!(
        set_disks.get_object_metadata_cache.get(key).await.is_none(),
        "metadata mutation must physically reclaim the prior generation"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn transition_and_restore_reclaim_prior_metadata_generations() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "transition-restore-generation-bucket";
    let object = "object.bin";
    let payload = vec![0x5au8; 1024 * 1024];
    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(payload.clone());
    let original = set_disks
        .put_object(bucket, object, &mut reader, &ObjectOptions::default())
        .await
        .expect("source object should be written");
    let source_generation = prime_metadata_generation(&set_disks, bucket, object).await;

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&set_disks.instance_ctx().tier_config_mgr(), &tier_name).await;
    let transition_opts = ObjectOptions {
        transition: TransitionOptions {
            status: TRANSITION_PENDING.to_string(),
            tier: tier_name,
            etag: original.etag.clone().expect("source ETag should be present"),
            ..Default::default()
        },
        version_id: original.version_id.map(|version| version.to_string()),
        mod_time: original.mod_time,
        ..Default::default()
    };
    set_disks
        .transition_object(bucket, object, &transition_opts)
        .await
        .expect("transition should succeed");
    assert_generation_reclaimed(&set_disks, &source_generation).await;

    let transitioned_generation = prime_metadata_generation(&set_disks, bucket, object).await;
    let mut restore_opts = ObjectOptions::default();
    restore_opts.transition.restore_request.days = Some(1);
    Arc::clone(&set_disks)
        .restore_transitioned_object(bucket, object, &restore_opts)
        .await
        .expect("restore should succeed");
    assert_generation_reclaimed(&set_disks, &transitioned_generation).await;
    assert_eq!(backend.get_count().await, 1, "restore should read the remote candidate exactly once");

    let mut restored = Vec::new();
    set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("restored object should be readable")
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("restored body should drain");
    assert_eq!(restored, payload);
    assert_eq!(backend.get_count().await, 1, "restored GET should use the local copy");
}
