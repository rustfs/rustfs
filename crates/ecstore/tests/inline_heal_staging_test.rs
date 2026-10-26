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

#![cfg(all(feature = "test-util", unix))]

mod storage_api;

use bytes::Bytes;
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::{io::ErrorKind, process::Command, time::Duration};
use storage_api::inline_heal_staging::{
    DeleteOptions, DiskAPI, DiskError, DiskOption, Endpoint, LocalPublicationPause, LocalPublicationStage, ReadOptions,
    local_durability_modes, new_disk, set_local_dir_fsync_failure, set_local_rename_data_fail_after_metadata_commit,
};
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
    run_inline_heal_fixture("inline_heal_stages_backup_in_production_durability_modes", false);
}

#[test]
fn inline_heal_staged_backup_restores_metadata_after_commit_failure_in_production_durability_modes() {
    run_inline_heal_fixture(
        "inline_heal_staged_backup_restores_metadata_after_commit_failure_in_production_durability_modes",
        true,
    );
}

fn run_inline_heal_fixture(test_name: &str, fail_publication: bool) {
    if std::env::var_os(CHILD).is_none() {
        // Durability is cached process-wide; use fresh production-library processes.
        for mode in ["none", "relaxed", "strict"] {
            let result = Command::new(std::env::current_exe().expect("integration test binary"))
                .args(["--exact", test_name, "--nocapture"])
                .env(CHILD, mode)
                .env("RUSTFS_DURABILITY_MODE", mode)
                .env("RUSTFS_NEW_BUCKET_DURABILITY_MODE", mode)
                .env("RUSTFS_EXPERIMENTAL_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE", "false")
                .env("RUSTFS_EXPERIMENTAL_FILE_FDATASYNC_GROUP_COMMIT_ENABLE", "false")
                .status()
                .expect("run production-library durability fixture");
            assert!(result.success(), "{test_name} failed in {mode} mode");
        }
        return;
    }
    let expected_mode = std::env::var(CHILD).expect("child durability mode");
    assert_eq!(
        local_durability_modes("bucket"),
        (expected_mode.as_str(), expected_mode.as_str()),
        "the cached process and destination durability must match the selected matrix row"
    );
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("fixture runtime")
        .block_on(async {
            for version in [Uuid::nil(), Uuid::new_v4()] {
                for old_inline in [true, false] {
                    let root = tempfile::tempdir().expect("fixture directory");
                    let object = format!("inline-heal-{}", Uuid::new_v4());
                    let endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 fixture path")).expect("endpoint");
                    let disk = new_disk(&endpoint, &DiskOption::default()).await.expect("local disk");
                    disk.make_volume("bucket").await.expect("bucket");
                    match disk.make_volume(TMP_BUCKET).await {
                        Ok(()) | Err(DiskError::VolumeExists) => {}
                        Err(error) => panic!("temporary bucket: {error:?}"),
                    }
                    for volume in ["bucket", TMP_BUCKET] {
                        assert_eq!(
                            local_durability_modes(volume),
                            (expected_mode.as_str(), expected_mode.as_str()),
                            "creating volumes must not change effective durability for {volume}"
                        );
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
                        metadata(&object, version, old_dir, old_body.clone(), old_inline),
                        "bucket",
                        &object,
                    )
                    .await
                    .expect("seed old version");
                    let object_dir = root.path().join("bucket").join(&object);
                    let old_metadata = tokio::fs::read(object_dir.join("xl.meta")).await.expect("old metadata");
                    let new_body = Bytes::from_static(b"repaired inline bytes");
                    let mut repaired = metadata(&object, version, Uuid::new_v4(), new_body.clone(), true);
                    repaired.set_healing();
                    let mut pause = LocalPublicationPause::install(
                        &disk,
                        "bucket",
                        &format!("{object}/xl.meta"),
                        LocalPublicationStage::PreparedRename,
                    )
                    .expect("pause physical metadata publication");
                    let task_disk = disk.clone();
                    let task_object = object.clone();
                    let task = tokio::spawn(async move {
                        task_disk
                            .rename_data(TMP_BUCKET, "heal", repaired, "bucket", &task_object)
                            .await
                    });
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
                    if fail_publication {
                        if expected_mode == "strict" {
                            set_local_dir_fsync_failure(&disk, "bucket", &object, ErrorKind::PermissionDenied)
                                .expect("inject a destination directory fsync boundary error");
                        } else {
                            set_local_rename_data_fail_after_metadata_commit(&object);
                        }
                    }
                    drop(pause);
                    let result = tokio::time::timeout(Duration::from_secs(10), task)
                        .await
                        .expect("publication must finish")
                        .expect("publication task");
                    if fail_publication {
                        let error = result.expect_err("publication failure must not be acknowledged");
                        let expected_kind = if expected_mode == "strict" {
                            ErrorKind::PermissionDenied
                        } else {
                            ErrorKind::Other
                        };
                        assert!(
                            matches!(error, DiskError::Io(ref error) if error.kind() == expected_kind),
                            "{expected_mode}: original publication error must be preserved: {error:?}"
                        );
                        assert_eq!(
                            tokio::fs::read(object_dir.join("xl.meta")).await.expect("restored metadata"),
                            old_metadata,
                            "failed publication must restore the exact previous metadata"
                        );
                    } else {
                        let response = result.expect("healing commit");
                        assert!(response.rollback_data_dir.is_none(), "inline heal must not publish a live backup");
                        assert_eq!(response.cleanup_data_dir.is_some(), !old_inline);
                    }
                    assert!(!backup.exists(), "finished publication must retire its staging backup");
                    if !old_inline {
                        assert_eq!(
                            tokio::fs::read(object_dir.join(old_dir.to_string()).join("part.1"))
                                .await
                                .expect("old external shard must remain until its storage owner cleans it"),
                            old_body
                        );
                    }
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
                            &object,
                            &version.to_string(),
                            &ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read healed physical target");
                    if fail_publication {
                        if old_inline {
                            assert_eq!(current.data, Some(old_body));
                        } else {
                            assert_eq!(current.data_dir, Some(old_dir));
                        }
                    } else {
                        assert_eq!(current.data, Some(new_body));
                    }
                    assert_eq!(current.version_id.unwrap_or_default(), version);
                    eprintln!(
                        "inline heal row passed: mode={expected_mode}, version={}, old_inline={old_inline}, failed_publication={fail_publication}",
                        if version.is_nil() { "null" } else { "UUID" }
                    );
                }
            }
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recursive_tmp_cleanup_prunes_nested_stage_without_relocking() {
    for key in ["stage", "stage/child", "stage/deep/child"] {
        for layout in ["absent", "empty-parent", "present"] {
            for has_sibling in [false, true] {
                let root = tempfile::tempdir().expect("nested cleanup fixture");
                let endpoint = Endpoint::try_from(root.path().to_str().expect("UTF-8 fixture path")).expect("endpoint");
                let disk = new_disk(&endpoint, &DiskOption::default()).await.expect("local disk");
                let temporary = root.path().join(TMP_BUCKET);
                let target = temporary.join(key);
                tokio::fs::create_dir_all(&temporary).await.expect("TMP bucket");
                if layout != "absent" {
                    tokio::fs::create_dir_all(target.parent().expect("target parent"))
                        .await
                        .expect("prepare cleanup ancestors");
                }
                if layout == "present" {
                    disk.write_all(TMP_BUCKET, &format!("{key}/payload"), Bytes::from_static(b"removed child"))
                        .await
                        .expect("seed cleanup target");
                }
                let sibling = if key == "stage" { "other/payload" } else { "stage/sibling" };
                if has_sibling {
                    disk.write_all(TMP_BUCKET, sibling, Bytes::from_static(b"preserved sibling"))
                        .await
                        .expect("seed sibling");
                }
                for _ in 0..2 {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        disk.delete(
                            TMP_BUCKET,
                            key,
                            DeleteOptions {
                                recursive: true,
                                ..Default::default()
                            },
                        ),
                    )
                    .await
                    .expect("nested TMP cleanup must not reacquire its staging lease")
                    .expect("recursive TMP cleanup");
                }
                assert!(!target.exists(), "cleanup target must be gone: {key}");
                assert!(temporary.is_dir(), "TMP bucket must remain");
                if has_sibling {
                    assert_eq!(
                        tokio::fs::read(temporary.join(sibling)).await.expect("sibling survives"),
                        b"preserved sibling"
                    );
                }
                assert_eq!(
                    temporary.join("stage").exists(),
                    has_sibling && key != "stage",
                    "empty staging ancestors must be pruned: {key}"
                );
            }
        }
    }
}
