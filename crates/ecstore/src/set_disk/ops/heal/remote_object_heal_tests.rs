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

use crate::disk::{DiskAPI as _, RUSTFS_META_TMP_BUCKET, RUSTFS_META_TMP_DELETED_BUCKET, ReadOptions};
use crate::object_api::{ObjectOptions, PutObjReader};
use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_for_pool_with_default_parity_isolated;
use crate::storage_api_contracts::object::ObjectIO as _;
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};

#[tokio::test]
async fn heal_restores_transitioned_object_missing_from_one_drive() {
    assert_remote_heal(true).await;
}

#[tokio::test]
async fn heal_restores_transitioned_object_without_a_data_directory() {
    assert_remote_heal(false).await;
}

async fn assert_remote_heal(keep_data_dir: bool) {
    let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "remote-heal";
    let object = "transitioned";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    set.put_object(
        bucket,
        object,
        &mut PutObjReader::from_vec(vec![0x42; 1024 * 1024 + 17]),
        &ObjectOptions {
            no_lock: true,
            ..Default::default()
        },
    )
    .await
    .expect("source object");
    for (disk, dir) in disks.iter().zip(&dirs) {
        let mut info = disk
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata");
        info.transition_status = rustfs_filemeta::TRANSITION_COMPLETE.to_owned();
        info.transition_tier = "WARM".to_owned();
        info.transitioned_objname = "remote/transitioned".to_owned();
        let data_dir = info.data_dir.expect("external shard directory");
        if !keep_data_dir {
            info.data_dir = None;
        }
        disk.write_metadata("", bucket, object, info)
            .await
            .expect("publish remote metadata");
        tokio::fs::remove_dir_all(dir.path().join(bucket).join(object).join(data_dir.to_string()))
            .await
            .expect("tier transition releases local payload");
    }
    // A write that reached only the quorum drives leaves no object directory on the last one.
    tokio::fs::remove_dir_all(dirs[0].path().join(bucket).join(object))
        .await
        .expect("drop object from one drive");

    let (item, error) = set
        .heal_object(
            bucket,
            object,
            "",
            &HealOpts {
                no_lock: true,
                scan_mode: HealScanMode::Normal,
                ..Default::default()
            },
        )
        .await
        .expect("remote object heal");
    assert!(error.is_none(), "{error:?}");
    assert_eq!(item.drives_healed(), Some(1));

    let healed = disks[0]
        .read_version("", bucket, object, "", &ReadOptions::default())
        .await
        .expect("healed drive publishes metadata");
    assert!(healed.is_remote());
    assert_eq!(healed.data_dir.is_some_and(|id| !id.is_nil()), keep_data_dir);
    assert_eq!(healed.transitioned_objname, "remote/transitioned");

    for dir in &dirs {
        let tmp = dir.path().join(RUSTFS_META_TMP_BUCKET);
        let trash = dir.path().join(RUSTFS_META_TMP_DELETED_BUCKET);
        let mut entries = tokio::fs::read_dir(&tmp).await.expect("tmp volume");
        while let Some(entry) = entries.next_entry().await.expect("tmp entry") {
            assert_eq!(entry.path(), trash, "heal must not leave staging residue in tmp");
        }
    }
}
