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

use super::super::{
    Cursor, DiskStore, EVENT_SET_DISK_WRITE, Error, FileInfo, HashAlgorithm, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_SET_DISK,
    Result, join_all, warn,
};
use crate::disk::{CHECK_PART_SUCCESS, Disk, DiskAPI, RUSTFS_META_TMP_BUCKET};
use crate::set_disk::coding;
#[cfg(test)]
use bytes::Bytes;
use rustfs_filemeta::{ErasureInfo, ObjectPartInfo};

/// Null out any disk whose shard writer failed (or was never created) so its
/// truncated/absent shard is not committed by the final rename, and return the
/// number of disks that still carry a fully-written shard.
pub(in crate::set_disk::ops) fn drop_failed_writer_disks<D, W>(disks: &mut [Option<D>], writers: &[Option<W>]) -> usize {
    let mut committed = 0usize;
    for (slot, writer) in disks.iter_mut().zip(writers.iter()) {
        if writer.is_none() {
            *slot = None;
        } else if slot.is_some() {
            committed += 1;
        }
    }
    committed
}

pub(super) enum WrittenBitrotShards<'a> {
    Inline(&'a [FileInfo]),
    Staged {
        directory: &'a str,
        layout: &'a ErasureInfo,
        part_size: usize,
    },
}

#[derive(Clone, Copy)]
pub(in crate::set_disk::ops) struct BitrotSelfVerifyTarget<'a> {
    pub(in crate::set_disk::ops) operation: &'static str,
    pub(in crate::set_disk::ops) bucket: &'a str,
    pub(in crate::set_disk::ops) object: &'a str,
    pub(in crate::set_disk::ops) part_number: Option<usize>,
    pub(in crate::set_disk::ops) logical_shard_size: usize,
    pub(in crate::set_disk::ops) shard_size: usize,
    pub(in crate::set_disk::ops) write_quorum: usize,
}

pub(in crate::set_disk::ops) async fn verify_written_bitrot_shards(
    disks: &[Option<DiskStore>],
    source: WrittenBitrotShards<'_>,
    target: BitrotSelfVerifyTarget<'_>,
) -> Result<usize> {
    let encoded_shard_size =
        coding::bitrot_shard_file_size(target.logical_shard_size, target.shard_size, HashAlgorithm::HighwayHash256S);

    let tasks = disks.iter().enumerate().filter_map(|(index, disk)| {
        let disk = disk.as_ref()?;
        let verify = source.verify(index, disk, &target, encoded_shard_size);
        Some(async move { (index, disk, verify.await) })
    });
    let results = join_all(tasks).await;

    let mut verified_shards = 0usize;
    for (index, disk, result) in results {
        if let Err(err) = result {
            warn!(
                event = EVENT_SET_DISK_WRITE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                operation = target.operation,
                bucket = target.bucket,
                object = target.object,
                part_number = target.part_number,
                shard_index = index,
                disk = ?disk,
                logical_shard_size = target.logical_shard_size,
                encoded_shard_size,
                error = ?err,
                "Set disk no-parity bitrot self-verify failed"
            );
            return Err(Error::other(format!(
                "{} no-parity bitrot self-verify failed for shard {index}: {err}",
                target.operation
            )));
        }

        verified_shards += 1;
    }

    if verified_shards < target.write_quorum {
        return Err(Error::other(format!(
            "{} no-parity bitrot self-verify quorum unavailable: {verified_shards} shard(s) verified, need {}",
            target.operation, target.write_quorum
        )));
    }

    Ok(verified_shards)
}

impl WrittenBitrotShards<'_> {
    async fn verify(&self, index: usize, disk: &Disk, target: &BitrotSelfVerifyTarget<'_>, encoded_size: usize) -> Result<()> {
        let algo = HashAlgorithm::HighwayHash256S;
        match self {
            Self::Inline(parts) => {
                let data = parts
                    .get(index)
                    .and_then(|part| part.data.as_ref())
                    .ok_or_else(|| Error::other(format!("{} inline shard {index} is missing bitrot data", target.operation)))?;
                coding::bitrot_verify(
                    Cursor::new(data.as_ref()),
                    encoded_size,
                    target.logical_shard_size,
                    algo,
                    target.shard_size,
                )
                .await?;
            }
            Self::Staged {
                directory,
                layout,
                part_size,
            } => {
                let number = target.part_number.unwrap_or(1);
                match disk {
                    Disk::Local(local) => {
                        let path = format!("{directory}/part.{number}");
                        let reader = local.read_file(RUSTFS_META_TMP_BUCKET, &path).await?;
                        coding::bitrot_verify(reader, encoded_size, target.logical_shard_size, algo, target.shard_size).await?;
                    }
                    Disk::Remote(peer) => {
                        let fi = staged_part_file_info(layout, index + 1, number, *part_size, target.shard_size)?;
                        let response = peer.verify_staged_part(directory, &fi, encoded_size).await?;
                        require_verified_part(&response.results)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// VerifyFile resolves `directory/part.N` using this part's write layout. Only
/// the geometry and part size are needed to verify a staged, non-inline shard.
fn staged_part_file_info(
    layout: &ErasureInfo,
    index: usize,
    number: usize,
    part_size: usize,
    frame_size: usize,
) -> Result<FileInfo> {
    let mut fi = FileInfo {
        size: i64::try_from(part_size).map_err(Error::other)?,
        erasure: ErasureInfo {
            algorithm: layout.algorithm.clone(),
            data_blocks: layout.data_blocks,
            parity_blocks: layout.parity_blocks,
            block_size: layout.block_size,
            index,
            distribution: layout.distribution.clone(),
            ..Default::default()
        },
        parts: vec![ObjectPartInfo {
            number,
            size: part_size,
            ..Default::default()
        }],
        ..Default::default()
    };
    if frame_size != coding::calc_shard_size(layout.block_size, layout.data_blocks) {
        crate::object_api::insert_ec_read_quantum(&mut fi.metadata, frame_size);
    }
    Ok(fi)
}

fn require_verified_part(results: &[usize]) -> Result<()> {
    match results {
        [CHECK_PART_SUCCESS] => Ok(()),
        [crate::disk::CHECK_PART_DISK_NOT_FOUND] => Err(Error::DiskNotFound),
        [crate::disk::CHECK_PART_VOLUME_NOT_FOUND] => Err(Error::VolumeNotFound),
        [crate::disk::CHECK_PART_FILE_NOT_FOUND] => Err(Error::FileNotFound),
        [crate::disk::CHECK_PART_FILE_CORRUPT] => Err(Error::FileCorrupt),
        _ => Err(Error::Unexpected),
    }
}

#[cfg(test)]
mod tests {
    use super::super::object::hermetic_set_disks_support::{
        hermetic_set_disks_for_pool_with_default_parity, hermetic_set_disks_for_pool_with_default_parity_isolated,
    };
    use super::*;
    use crate::storage_api_contracts::object::ObjectIO as _;

    async fn encode_streaming_shard(data: &[u8], shard_size: usize) -> Bytes {
        let mut writer = coding::BitrotWriter::new(Cursor::new(Vec::new()), shard_size, HashAlgorithm::HighwayHash256S);
        for chunk in data.chunks(shard_size) {
            writer.write(chunk).await.expect("streaming shard should encode");
        }
        Bytes::from(writer.into_inner().into_inner())
    }

    #[test]
    fn excludes_failed_writers_and_counts_committed() {
        let mut disks = vec![Some(0u8), Some(1), Some(2), Some(3)];
        let writers = vec![Some(()), Some(()), None, Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 3);
        assert_eq!(disks, vec![Some(0), Some(1), None, Some(3)]);
    }

    #[test]
    fn offline_disk_stays_excluded_and_uncounted() {
        let mut disks = vec![Some(0u8), None, Some(2)];
        let writers = vec![Some(()), None, Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 2);
        assert_eq!(disks, vec![Some(0), None, Some(2)]);
    }

    #[test]
    fn all_writers_ok_keeps_every_disk() {
        let mut disks = vec![Some(0u8), Some(1), Some(2)];
        let writers = vec![Some(()), Some(()), Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 3);
        assert_eq!(disks, vec![Some(0), Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn no_parity_self_verify_rejects_issue_5173_final_block_mismatch() {
        let (_temp_dirs, disks, _set_disks) = hermetic_set_disks_for_pool_with_default_parity(1, 0, 0).await;
        let disk = disks[0].clone();
        let shard_size = 1_048_576usize;
        let logical_shard_size = 8_250_370usize;
        let payload = vec![0x5a; logical_shard_size];
        let encoded = encode_streaming_shard(&payload, shard_size).await;
        let layout = &FileInfo::new("object", 1, 0).erasure;
        let staged = || WrittenBitrotShards::Staged {
            directory: "issue-5173",
            layout,
            part_size: payload.len(),
        };
        let path = "issue-5173/part.1";

        assert_eq!(encoded.len(), 8_250_626);
        disk.write_all(RUSTFS_META_TMP_BUCKET, path, encoded.clone())
            .await
            .expect("healthy temp shard should be written");
        let verified = verify_written_bitrot_shards(
            &[Some(disk.clone())],
            staged(),
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "object",
                part_number: None,
                logical_shard_size,
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect("healthy temp shard should verify");
        assert_eq!(verified, 1);

        for trailing in [vec![0xa5], vec![0xa5; 17]] {
            let mut oversized = encoded.to_vec();
            oversized.extend_from_slice(&trailing);
            disk.write_all(RUSTFS_META_TMP_BUCKET, path, Bytes::from(oversized))
                .await
                .expect("oversized temp shard should be staged");
            let err = verify_written_bitrot_shards(
                &[Some(disk.clone())],
                staged(),
                BitrotSelfVerifyTarget {
                    operation: "put_object",
                    bucket: "bucket",
                    object: "object",
                    part_number: None,
                    logical_shard_size,
                    shard_size,
                    write_quorum: 1,
                },
            )
            .await
            .expect_err("disk shard with trailing bytes must not be committed");
            assert!(err.to_string().contains("trailing data"));
        }

        let mut corrupt = encoded.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;
        disk.write_all(RUSTFS_META_TMP_BUCKET, path, Bytes::from(corrupt))
            .await
            .expect("corrupt temp shard should be staged");

        let err = verify_written_bitrot_shards(
            &[Some(disk)],
            staged(),
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "object",
                part_number: None,
                logical_shard_size,
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect_err("corrupt no-parity temp shard must not be committed");
        assert!(err.to_string().contains("bitrot self-verify failed"));
    }

    #[test]
    fn staged_part_requires_exactly_one_success() {
        require_verified_part(&[CHECK_PART_SUCCESS]).expect("one verified shard");
        for results in [
            vec![],
            vec![CHECK_PART_SUCCESS; 2],
            vec![crate::disk::CHECK_PART_UNKNOWN],
            vec![usize::MAX],
        ] {
            assert!(
                matches!(require_verified_part(&results), Err(Error::Unexpected)),
                "unexpected peer result {results:?}"
            );
        }
        assert!(matches!(
            require_verified_part(&[crate::disk::CHECK_PART_DISK_NOT_FOUND]),
            Err(Error::DiskNotFound)
        ));
        assert!(matches!(
            require_verified_part(&[crate::disk::CHECK_PART_VOLUME_NOT_FOUND]),
            Err(Error::VolumeNotFound)
        ));
        assert!(matches!(
            require_verified_part(&[crate::disk::CHECK_PART_FILE_NOT_FOUND]),
            Err(Error::FileNotFound)
        ));
        assert!(matches!(
            require_verified_part(&[crate::disk::CHECK_PART_FILE_CORRUPT]),
            Err(Error::FileCorrupt)
        ));
    }

    #[tokio::test]
    async fn tcp_and_nixl_verify_staged_shards_at_the_owner() {
        let (_dirs, disks, _) = hermetic_set_disks_for_pool_with_default_parity(2, 0, 0).await;
        let owner = disks[1].clone();
        let frame_size = 64 * 1024;
        let mut layout = FileInfo::new("bucket/key", 2, 0).erasure;
        layout.block_size = 256 * 1024;
        let part_size = layout.block_size + 5;
        let logical_shard_size = layout.block_size / 2 + 3;
        let bytes = encode_streaming_shard(&vec![0x43; logical_shard_size], frame_size).await;
        crate::cluster::rpc::remote_disk::with_staged_part_peer(owner.clone(), |tcp, nixl| async move {
            for peer in [tcp, nixl] {
                let set = [
                    Some(disks[0].clone()),
                    Some(std::sync::Arc::new(Disk::Remote(Box::new(peer)))),
                ];
                for (directory, part_number) in [("staging/data", None), ("upload-stage", Some(7))] {
                    let path = format!("{directory}/part.{}", part_number.unwrap_or(1));
                    let verify = || {
                        verify_written_bitrot_shards(
                            &set,
                            WrittenBitrotShards::Staged {
                                directory,
                                layout: &layout,
                                part_size,
                            },
                            BitrotSelfVerifyTarget {
                                operation: "test",
                                bucket: "bucket",
                                object: "key",
                                part_number,
                                logical_shard_size,
                                shard_size: frame_size,
                                write_quorum: 2,
                            },
                        )
                    };
                    for disk in &disks {
                        disk.write_all(RUSTFS_META_TMP_BUCKET, &path, bytes.clone())
                            .await
                            .expect("stage shard");
                    }
                    assert_eq!(verify().await.expect("peer verified its local bytes over gRPC"), 2);
                    let mut corrupt = bytes.to_vec();
                    *corrupt.last_mut().expect("non-empty shard") ^= 1;
                    let mut oversized = bytes.to_vec();
                    oversized.push(0);
                    for damaged in [Bytes::from(corrupt), bytes.slice(..bytes.len() - 1), Bytes::from(oversized)] {
                        owner
                            .write_all(RUSTFS_META_TMP_BUCKET, &path, damaged)
                            .await
                            .expect("damage peer shard");
                        assert!(
                            verify()
                                .await
                                .expect_err("invalid peer shard")
                                .to_string()
                                .contains("failed for shard 1")
                        );
                    }
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn put_object_remote_verification_failure_does_not_publish_object() {
        use crate::cluster::rpc::remote_disk::with_lost_staged_shard_peer;
        use crate::set_disk::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::object::ObjectOperations as _;

        let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(2, 0, 0).await;
        let bucket = "peer-verify-put";
        let key = "object";
        let opts = ObjectOptions::default();
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create bucket");
        }
        with_lost_staged_shard_peer(disks[1].clone(), |peer| async {
            set.disks.write().await[1] = Some(std::sync::Arc::new(Disk::Remote(Box::new(peer))));
            let mut reader = PutObjReader::from_vec(vec![0x42; 2 * 1024 * 1024]);
            let error = set
                .put_object(bucket, key, &mut reader, &opts)
                .await
                .expect_err("remote verification must fail the PUT");
            assert!(error.to_string().contains("bitrot self-verify failed for shard"), "{error}");
            assert!(error.to_string().ends_with(&Error::FileNotFound.to_string()), "{error}");
            set.disks.write().await[1] = Some(disks[1].clone());
        })
        .await;
        let error = set
            .get_object_info(bucket, key, &opts)
            .await
            .expect_err("failed PUT must leave the object absent");
        assert!(crate::error::is_err_object_not_found(&error), "{error}");
        for disk in disks {
            assert!(matches!(
                disk.read_version("", bucket, key, "", &Default::default()).await,
                Err(crate::disk::error::DiskError::FileNotFound)
            ));
        }
    }

    #[tokio::test]
    async fn upload_part_remote_verification_failure_does_not_publish_part() {
        use crate::cluster::rpc::remote_disk::with_lost_staged_shard_peer;
        use crate::set_disk::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::multipart::MultipartOperations as _;

        let (_dirs, disks, set) = hermetic_set_disks_for_pool_with_default_parity_isolated(2, 0, 0).await;
        let bucket = "peer-verify-multipart";
        let key = "object";
        let opts = ObjectOptions::default();
        for disk in &disks {
            disk.make_volume(bucket).await.expect("create bucket");
        }
        let upload_id = set
            .new_multipart_upload(bucket, key, &opts)
            .await
            .expect("create multipart upload")
            .upload_id;
        with_lost_staged_shard_peer(disks[1].clone(), |peer| async {
            set.disks.write().await[1] = Some(std::sync::Arc::new(Disk::Remote(Box::new(peer))));
            let mut reader = PutObjReader::from_vec(vec![0x53; 1024 * 1024]);
            let error = set
                .put_object_part(bucket, key, &upload_id, 7, &mut reader, &opts)
                .await
                .expect_err("remote verification must fail UploadPart");
            assert!(error.to_string().contains("bitrot self-verify failed for shard"), "{error}");
            assert!(error.to_string().ends_with(&Error::FileNotFound.to_string()), "{error}");
            set.disks.write().await[1] = Some(disks[1].clone());
        })
        .await;
        let listed = set
            .list_object_parts(bucket, key, &upload_id, None, 10, &opts)
            .await
            .expect("upload must remain listable");
        assert!(listed.parts.is_empty(), "failed UploadPart published a part");
    }

    #[tokio::test]
    async fn no_parity_inline_self_verify_rejects_trailing_bytes() {
        let (_temp_dirs, disks, _set_disks) = hermetic_set_disks_for_pool_with_default_parity(1, 0, 0).await;
        let shard_size = 16usize;
        let payload = b"inline bitrot payload";
        let encoded = encode_streaming_shard(payload, shard_size).await;
        let disks = disks.into_iter().map(Some).collect::<Vec<_>>();

        for trailing in [vec![0xa5], vec![0xa5; 17]] {
            let mut oversized = encoded.to_vec();
            oversized.extend_from_slice(&trailing);
            let parts = [FileInfo {
                data: Some(Bytes::from(oversized)),
                ..Default::default()
            }];
            let err = verify_written_bitrot_shards(
                &disks,
                WrittenBitrotShards::Inline(&parts),
                BitrotSelfVerifyTarget {
                    operation: "put_object",
                    bucket: "bucket",
                    object: "inline-object",
                    part_number: None,
                    logical_shard_size: payload.len(),
                    shard_size,
                    write_quorum: 1,
                },
            )
            .await
            .expect_err("inline shard with trailing bytes must not be committed");
            assert!(err.to_string().contains("trailing data"));
        }
    }

    #[tokio::test]
    async fn no_parity_put_round_trip_large_stream() {
        let (_temp_dirs, disks, set_disks) = hermetic_set_disks_for_pool_with_default_parity(1, 0, 0).await;
        let bucket = "no-parity-put-round-trip";
        for disk in &disks {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let payload = vec![0x5a; 13 * 1024 * 1024 + 727_213];
        let mut reader = crate::set_disk::PutObjReader::from_vec(payload);
        set_disks
            .put_object(bucket, "object", &mut reader, &crate::set_disk::ObjectOptions::default())
            .await
            .expect("single-disk no-parity PUT should pass self-verify");
    }

    #[tokio::test]
    async fn no_parity_self_verify_rejects_missing_inline_data_missing_disk_shard_and_under_quorum() {
        let (_temp_dirs, disks, _set_disks) = hermetic_set_disks_for_pool_with_default_parity(1, 0, 0).await;
        let shard_size = 16usize;
        let payload = b"self verify negative path";
        let encoded = encode_streaming_shard(payload, shard_size).await;
        let disks_opt = disks.iter().cloned().map(Some).collect::<Vec<_>>();

        // 1. Inline shard with `data: None` fails closed.
        let empty_parts = [FileInfo {
            data: None,
            ..Default::default()
        }];
        let err = verify_written_bitrot_shards(
            &disks_opt,
            WrittenBitrotShards::Inline(&empty_parts),
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "missing-inline-data",
                part_number: None,
                logical_shard_size: payload.len(),
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect_err("inline shard missing data must fail");
        assert!(err.to_string().contains("missing bitrot data"));

        // 2. Disk shard missing on disk (FileNotFound) fails closed with shard error context.
        let err = verify_written_bitrot_shards(
            &disks_opt,
            WrittenBitrotShards::Staged {
                directory: "missing",
                layout: &FileInfo::new("object", 1, 0).erasure,
                part_size: payload.len(),
            },
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "missing-disk-file",
                part_number: Some(3),
                logical_shard_size: payload.len(),
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect_err("missing disk shard must fail");
        assert!(err.to_string().contains("no-parity bitrot self-verify failed for shard 0"));

        // 3. Verified shard count below write_quorum fails closed even when available shard is valid.
        let valid_parts = [
            FileInfo {
                data: Some(encoded),
                ..Default::default()
            },
            FileInfo::default(),
        ];
        let two_slots = vec![disks_opt[0].clone(), None];
        let err = verify_written_bitrot_shards(
            &two_slots,
            WrittenBitrotShards::Inline(&valid_parts),
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "under-quorum",
                part_number: None,
                logical_shard_size: payload.len(),
                shard_size,
                write_quorum: 2,
            },
        )
        .await
        .expect_err("under-quorum verification must fail");
        assert!(err.to_string().contains("quorum unavailable: 1 shard(s) verified, need 2"));
    }
}
