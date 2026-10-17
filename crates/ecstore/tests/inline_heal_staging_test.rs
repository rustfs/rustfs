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

#![cfg(all(feature = "test-util", not(windows)))]

use bytes::Bytes;
use rustfs_ecstore::api::disk::{
    DiskAPI, DiskError, DiskOption, Endpoint, LocalPublicationPause, LocalPublicationStage, ReadOptions, new_disk,
};
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::{process::Command, time::Duration};
use time::OffsetDateTime;
use uuid::Uuid;

const TMP_BUCKET: &str = ".rustfs.sys/tmp";
const CHILD: &str = "RUSTFS_INLINE_HEAL_STAGING_CHILD";

fn metadata(object: &str, version: Uuid, data_dir: Uuid, data: Bytes, inline: bool) -> FileInfo {
    let mut info = FileInfo::new(object, 1, 0);
    info.erasure.index = 1;
    info.version_id = Some(version);
    info.data_dir = Some(data_dir);
    info.size = i64::try_from(data.len()).expect("fixture size");
    info.parts = vec![ObjectPartInfo {
        number: 1,
        size: data.len(),
        actual_size: info.size,
        ..Default::default()
    }];
    info.mod_time = Some(OffsetDateTime::now_utc());
    if inline {
        info.data = Some(data);
        info.set_inline_data();
    }
    info
}

#[test]
fn inline_heal_stages_backup_in_production_durability_modes() {
    if std::env::var_os(CHILD).is_none() {
        // Durability is cached process-wide; use fresh production-library processes.
        for mode in ["none", "relaxed"] {
            let result = Command::new(std::env::current_exe().expect("integration test binary"))
                .args([
                    "--exact",
                    "inline_heal_stages_backup_in_production_durability_modes",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("RUSTFS_DURABILITY_MODE", mode)
                .status()
                .expect("run production-library durability fixture");
            assert!(result.success(), "inline staging failed in {mode} mode");
        }
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("fixture runtime")
        .block_on(async {
            for version in [Uuid::nil(), Uuid::new_v4()] {
                for old_inline in [true, false] {
                    let root = tempfile::tempdir().expect("fixture directory");
                    let endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 fixture path")).expect("endpoint");
                    let disk = new_disk(&endpoint, &DiskOption::default()).await.expect("local disk");
                    disk.make_volume("bucket").await.expect("bucket");
                    match disk.make_volume(TMP_BUCKET).await {
                        Ok(()) | Err(DiskError::VolumeExists) => {}
                        Err(error) => panic!("temporary bucket: {error:?}"),
                    }
                    let old_dir = Uuid::new_v4();
                    let old_body = Bytes::from_static(b"old bytes must remain recoverable");
                    if !old_inline {
                        disk.write_all(TMP_BUCKET, &format!("seed/{old_dir}/part.1"), old_body.clone())
                            .await
                            .expect("stage external source");
                    }
                    disk.rename_data(
                        TMP_BUCKET,
                        "seed",
                        metadata("object", version, old_dir, old_body, old_inline),
                        "bucket",
                        "object",
                    )
                    .await
                    .expect("seed old version");
                    let object_dir = root.path().join("bucket/object");
                    let old_metadata = tokio::fs::read(object_dir.join("xl.meta")).await.expect("old metadata");
                    let new_body = Bytes::from_static(b"repaired inline bytes");
                    let mut repaired = metadata("object", version, Uuid::new_v4(), new_body.clone(), true);
                    repaired.set_healing();
                    let mut pause =
                        LocalPublicationPause::install(&disk, "bucket", "object/xl.meta", LocalPublicationStage::PreparedRename)
                            .expect("pause physical metadata publication");
                    let task_disk = disk.clone();
                    let task =
                        tokio::spawn(
                            async move { task_disk.rename_data(TMP_BUCKET, "heal", repaired, "bucket", "object").await },
                        );
                    tokio::time::timeout(Duration::from_secs(10), pause.entered())
                        .await
                        .expect("publication must reach its physical boundary")
                        .expect("publication pause");
                    let backup = root.path().join(TMP_BUCKET).join("heal/xl.meta.bkp");
                    assert_eq!(
                        tokio::fs::read(&backup)
                            .await
                            .expect("production heal must stage rollback bytes"),
                        old_metadata
                    );
                    assert_eq!(
                        tokio::fs::read(object_dir.join("xl.meta"))
                            .await
                            .expect("unpublished metadata"),
                        old_metadata
                    );
                    drop(pause);
                    let response = tokio::time::timeout(Duration::from_secs(10), task)
                        .await
                        .expect("publication must finish")
                        .expect("publication task")
                        .expect("healing commit");
                    assert!(response.rollback_data_dir.is_none(), "inline heal must not publish a live backup");
                    assert_eq!(response.cleanup_data_dir.is_some(), !old_inline);
                    assert!(!backup.exists(), "committed staging backup must be retired");
                    for entry in std::fs::read_dir(&object_dir).expect("object directory") {
                        let entry = entry.expect("object entry");
                        if entry.file_type().expect("entry type").is_dir() {
                            assert!(!entry.path().join("xl.meta.bkp").exists(), "no live rollback backup may be published");
                        }
                    }
                    let current = disk
                        .read_version(
                            "",
                            "bucket",
                            "object",
                            &version.to_string(),
                            &ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read healed physical target");
                    assert_eq!(current.data, Some(new_body));
                    assert_eq!(current.version_id.unwrap_or_default(), version);
                }
            }
        });
}
