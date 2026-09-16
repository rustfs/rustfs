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

use crate::disk::{DiskAPI as _, ReadOptions};
use crate::object_api::{ObjectOptions, PutObjReader, ShardIntegrityWriteMode};
use crate::set_disk::CompletePart;
use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_for_pool_with_default_parity_isolated;
use crate::storage_api_contracts::multipart::MultipartOperations as _;
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealScanMode};
use tokio::io::AsyncReadExt;

fn rollout_vars(requested: bool, confirmed: bool) -> [(&'static str, Option<&'static str>); 2] {
    [
        (rustfs_config::ENV_SHARD_INTEGRITY_WRITE, Some(if requested { "true" } else { "false" })),
        (
            rustfs_config::ENV_SHARD_INTEGRITY_FLEET_CONFIRMED,
            Some(if confirmed { "true" } else { "false" }),
        ),
    ]
}

#[tokio::test]
async fn remote_metadata_heal_does_not_certify_unread_tier_payload() {
    let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "remote-integrity";
    let object = "transitioned";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    set.put_object(
        bucket,
        object,
        &mut PutObjReader::from_vec(vec![0x31; 1024 * 1024 + 17]),
        &ObjectOptions {
            no_lock: true,
            shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
            ..Default::default()
        },
    )
    .await
    .expect("protected source");
    for (disk, dir) in disks.iter().zip(&dirs) {
        let mut info = disk
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("source metadata");
        assert!(info.parts[0].integrity.is_some());
        info.transition_status = rustfs_filemeta::TRANSITION_COMPLETE.to_owned();
        info.transition_tier = "WARM".to_owned();
        info.transitioned_objname = "remote/transitioned".to_owned();
        let data_dir = info.data_dir.expect("external shard directory");
        disk.write_metadata("", bucket, object, info)
            .await
            .expect("publish remote metadata");
        tokio::fs::remove_dir_all(dir.path().join(bucket).join(object).join(data_dir.to_string()))
            .await
            .expect("tier transition releases local payload and proof indexes");
    }
    let (item, error) = set
        .heal_object(
            bucket,
            object,
            "",
            &HealOpts {
                no_lock: true,
                scan_mode: HealScanMode::Deep,
                ..Default::default()
            },
        )
        .await
        .expect("remote metadata scan");
    assert!(error.is_none(), "{error:?}");
    assert_eq!(item.drives_healed(), Some(0));
    assert!(!item.integrity_verified, "local metadata presence cannot certify unread remote bytes");
}

#[tokio::test]
#[serial_test::serial(shard_integrity_rollout)]
async fn rollout_requires_both_flags_for_new_puts() {
    let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "integrity-rollout";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };

    for (requested, confirmed, protected) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, true),
    ] {
        for size in [4096, 1024 * 1024 + 19] {
            let object = format!("put-{requested}-{confirmed}-{size}");
            let expected = vec![0x53; size];
            temp_env::async_with_vars(rollout_vars(requested, confirmed), async {
                set.put_object(bucket, &object, &mut PutObjReader::from_vec(expected.clone()), &opts)
                    .await
                    .expect("write at rollout gate");
            })
            .await;
            for disk in &disks {
                let meta = disk
                    .read_version(
                        "",
                        bucket,
                        &object,
                        "",
                        &ReadOptions {
                            read_data: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("published metadata");
                assert_eq!(meta.parts[0].integrity.is_some(), protected);
                assert_eq!(
                    rustfs_utils::http::contains_key_str(
                        &meta.metadata,
                        rustfs_filemeta::shard_integrity::SUFFIX_SHARD_INTEGRITY
                    ),
                    protected,
                    "the public gate must control the committed extension"
                );
                assert_eq!(meta.erasure.get_checksum_info(1).algorithm, rustfs_utils::HashAlgorithm::HighwayHash256S);
            }
            let mut reader = temp_env::async_with_vars(
                rollout_vars(false, false),
                set.get_object_reader(bucket, &object, None, Default::default(), &opts),
            )
            .await
            .expect("read after disabling new protection");
            let mut actual = Vec::new();
            reader.stream.read_to_end(&mut actual).await.expect("complete exact body");
            assert_eq!(actual, expected);
        }
    }
}

#[tokio::test]
async fn legacy_shard_repair_and_explicit_version_metadata_recovery_remain_available() {
    let (dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "legacy-recovery";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    let opts = ObjectOptions {
        no_lock: true,
        versioned: true,
        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
        ..Default::default()
    };
    let expected = vec![0x67; 1024 * 1024 + 37];
    for case in ["missing-part", "bad-bitrot", "missing-version-metadata"] {
        let object = case;
        let info = set
            .put_object(bucket, object, &mut PutObjReader::from_vec(expected.clone()), &opts)
            .await
            .expect("write legacy fixture");
        let version = info.version_id.expect("versioned fixture").to_string();
        let meta = disks[0]
            .read_version("", bucket, object, &version, &ReadOptions::default())
            .await
            .expect("legacy metadata");
        assert!(meta.parts.iter().all(|part| part.integrity.is_none()));
        let data_dir = meta.data_dir.expect("external part directory").to_string();
        let mut original = Vec::new();
        for dir in &dirs {
            original.push(
                tokio::fs::read(dir.path().join(bucket).join(object).join(&data_dir).join("part.1"))
                    .await
                    .expect("original framed shard"),
            );
        }
        match case {
            "missing-part" => {
                tokio::fs::remove_file(dirs[0].path().join(bucket).join(object).join(&data_dir).join("part.1"))
                    .await
                    .expect("remove one shard");
            }
            "bad-bitrot" => {
                let mut damaged = original[0].clone();
                damaged[32] ^= 1;
                tokio::fs::write(dirs[0].path().join(bucket).join(object).join(&data_dir).join("part.1"), damaged)
                    .await
                    .expect("corrupt one stored block");
            }
            _ => {
                for dir in dirs.iter().skip(1) {
                    tokio::fs::remove_file(dir.path().join(bucket).join(object).join("xl.meta"))
                        .await
                        .expect("retain one consistent version metadata copy");
                }
            }
        }
        let (result, error) = set
            .heal_object(
                bucket,
                object,
                &version,
                &HealOpts {
                    no_lock: true,
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("legacy recovery must run");
        assert!(error.is_none(), "{case}: {error:?}");
        assert!(!result.integrity_verified, "traditional recovery cannot certify original identity");
        if case != "missing-version-metadata" {
            assert_eq!(result.drives_healed(), Some(1), "physical repair must remain observable");
        }
        for (index, disk) in disks.iter().enumerate() {
            let restored = disk
                .read_version("", bucket, object, &version, &ReadOptions::default())
                .await
                .expect("metadata must be restored for the explicit version");
            assert_eq!(restored.version_id, info.version_id);
            assert!(restored.parts.iter().all(|part| part.integrity.is_none()));
            let bytes = tokio::fs::read(
                dirs[index]
                    .path()
                    .join(bucket)
                    .join(object)
                    .join(restored.data_dir.expect("restored data directory").to_string())
                    .join("part.1"),
            )
            .await
            .expect("restored payload");
            assert_eq!(bytes, original[index], "recover the original framed bytes on every drive");
        }
        let mut reader = set
            .get_object_reader(
                bucket,
                object,
                None,
                Default::default(),
                &ObjectOptions {
                    version_id: Some(version),
                    ..opts.clone()
                },
            )
            .await
            .expect("read recovered historical version");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("complete recovered body");
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
#[serial_test::serial(shard_integrity_rollout)]
async fn multipart_mode_survives_rollout_switch_changes() {
    let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "multipart-rollout";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };
    for protected in [false, true] {
        let object = format!("upload-{protected}");
        let upload =
            temp_env::async_with_vars(rollout_vars(protected, protected), set.new_multipart_upload(bucket, &object, &opts))
                .await
                .expect("initiate upload");
        let expected = vec![0x72; 128 * 1024 + 7];
        let complete = temp_env::async_with_vars(rollout_vars(!protected, !protected), async {
            let part = set
                .put_object_part(
                    bucket,
                    &object,
                    &upload.upload_id,
                    1,
                    &mut PutObjReader::from_vec(expected.clone()),
                    &opts,
                )
                .await
                .expect("upload after switch change");
            set.clone()
                .complete_multipart_upload(
                    bucket,
                    &object,
                    &upload.upload_id,
                    vec![CompletePart {
                        part_num: part.part_num,
                        etag: part.etag,
                        ..Default::default()
                    }],
                    &opts,
                )
                .await
                .expect("complete using persisted upload mode")
        })
        .await;
        assert_eq!(
            complete.shard_integrity_write_mode(),
            if protected {
                ShardIntegrityWriteMode::Protected
            } else {
                ShardIntegrityWriteMode::Legacy
            }
        );
        let meta = disks[0]
            .read_version("", bucket, &object, "", &ReadOptions::default())
            .await
            .expect("complete metadata");
        assert_eq!(meta.parts[0].integrity.is_some(), protected);
        assert!(!rustfs_utils::http::contains_key_str(
            &meta.metadata,
            rustfs_filemeta::shard_integrity::SUFFIX_UPLOAD_INTEGRITY
        ));
        let mut reader = set
            .get_object_reader(bucket, &object, None, Default::default(), &opts)
            .await
            .expect("completed upload");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("full multipart body");
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
#[serial_test::serial(shard_integrity_rollout)]
async fn physical_copy_inherits_source_mode_instead_of_current_switch() {
    let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(4, 0, 2).await;
    let bucket = "rewrite-rollout";
    for disk in &disks {
        disk.make_volume(bucket).await.expect("fixture bucket");
    }
    for protected in [false, true] {
        let mode = if protected {
            ShardIntegrityWriteMode::Protected
        } else {
            ShardIntegrityWriteMode::Legacy
        };
        let object = format!("source-{protected}");
        let expected = vec![0x39; 1024 * 1024 + 11];
        let seed_opts = ObjectOptions {
            no_lock: true,
            shard_integrity_write_mode: Some(mode),
            ..Default::default()
        };
        let mut source = set
            .put_object(bucket, &object, &mut PutObjReader::from_vec(expected.clone()), &seed_opts)
            .await
            .expect("source fixture");
        source.metadata_only = false;
        source.put_object_reader = Some(PutObjReader::from_vec(expected.clone()));
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let copied = temp_env::async_with_vars(
            rollout_vars(!protected, !protected),
            set.copy_object(bucket, &object, bucket, &object, &mut source, &opts, &opts),
        )
        .await
        .expect("materialized self-copy");
        assert_eq!(
            copied.shard_integrity_write_mode(),
            mode,
            "a legacy rewrite is not a trusted migration and a protected rewrite cannot downgrade"
        );
        let meta = disks[0]
            .read_version("", bucket, &object, "", &ReadOptions::default())
            .await
            .expect("rewritten metadata");
        assert_eq!(meta.parts[0].integrity.is_some(), protected);
        let mut reader = set
            .get_object_reader(bucket, &object, None, Default::default(), &opts)
            .await
            .expect("read physical rewrite");
        let mut actual = Vec::new();
        reader.stream.read_to_end(&mut actual).await.expect("complete rewritten body");
        assert_eq!(actual, expected);
    }
}
