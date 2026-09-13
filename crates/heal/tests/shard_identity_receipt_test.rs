// Copyright 2026 RustFS Team
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

use rustfs_heal::heal::{
    outcome::HealObjectDisposition,
    storage::{ECStoreHealStorage, HealStorageAPI},
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use rustfs_test_utils::TestECStoreEnv;
use serial_test::serial;

mod storage_api;
use storage_api::integration::{DiskAPI, ObjectIO, ObjectOptions, PutObjReader, ReadOptions};

#[tokio::test]
#[serial]
async fn receipt_requires_identity_verification_even_for_normal_sweeps() {
    let root = tempfile::tempdir().expect("receipt fixture");
    let env = TestECStoreEnv::builder()
        .base_dir(root.path())
        .prefix("shard_identity_receipt")
        .build()
        .await;
    let bucket = "shard-identity-receipt";
    env.make_bucket(bucket, false).await;
    let set = env.ecstore.pools[0].get_disks(0);
    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
    let storage = ECStoreHealStorage::new(env.ecstore.clone());
    for corrupt_count in [1usize, 3] {
        let object = format!("target-{corrupt_count}");
        let donor = format!("donor-{corrupt_count}");
        for (name, byte) in [(&object, 0x3c), (&donor, 0xa9)] {
            env.ecstore
                .put_object(
                    bucket,
                    name,
                    &mut PutObjReader::from_vec(vec![byte; 1024 * 1024 + 123]),
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("commit all fixture shards");
        }
        let mut target_meta = Vec::new();
        let mut donor_meta = Vec::new();
        for disk in &disks {
            target_meta.push(
                disk.read_version("", bucket, &object, "", &ReadOptions::default())
                    .await
                    .expect("target metadata"),
            );
            donor_meta.push(
                disk.read_version("", bucket, &donor, "", &ReadOptions::default())
                    .await
                    .expect("donor metadata"),
            );
        }
        let mut retained = Vec::new();
        for (slot, target) in target_meta.iter().enumerate() {
            let target_path = env.disk_paths[slot]
                .join(bucket)
                .join(&object)
                .join(target.data_dir.expect("target directory").to_string())
                .join("part.1");
            let original = tokio::fs::read(&target_path).await.expect("original shard");
            if target.erasure.index <= corrupt_count {
                let donor_slot = donor_meta
                    .iter()
                    .position(|part| part.erasure.index == target.erasure.index)
                    .expect("same donor coding index");
                let donor_path = env.disk_paths[donor_slot]
                    .join(bucket)
                    .join(&donor)
                    .join(donor_meta[donor_slot].data_dir.expect("donor directory").to_string())
                    .join("part.1");
                let replacement = tokio::fs::read(donor_path).await.expect("complete donor shard");
                assert_eq!(replacement.len(), original.len());
                tokio::fs::write(&target_path, replacement)
                    .await
                    .expect("replace intact shard");
            } else {
                retained.push((target_path, original));
            }
        }
        let result = storage
            .heal_object_with_receipt(
                bucket,
                &object,
                None,
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Normal,
                    ..Default::default()
                },
            )
            .await;
        if corrupt_count == 1 {
            let result = result.expect("recoverable repair");
            assert!(result.error.is_none());
            assert_eq!(
                result.receipt.expect("verified repair receipt").disposition,
                HealObjectDisposition::Repaired,
                "presence alone must not produce VerifiedHealthy"
            );
        } else if let Ok(result) = result {
            assert!(result.error.is_some(), "below quorum cannot be a success");
            assert!(result.receipt.is_none(), "no completion receipt without authoritative sources");
        }
        for (path, original) in retained {
            assert_eq!(tokio::fs::read(path).await.expect("retained correct shard"), original);
        }
    }
}
