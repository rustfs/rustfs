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

use super::super::*;

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

#[derive(Clone, Copy)]
pub(in crate::set_disk::ops) struct BitrotSelfVerifyTarget<'a> {
    pub(in crate::set_disk::ops) operation: &'static str,
    pub(in crate::set_disk::ops) bucket: &'a str,
    pub(in crate::set_disk::ops) object: &'a str,
    pub(in crate::set_disk::ops) part_number: Option<usize>,
    pub(in crate::set_disk::ops) volume: &'a str,
    pub(in crate::set_disk::ops) path: &'a str,
    pub(in crate::set_disk::ops) logical_shard_size: usize,
    pub(in crate::set_disk::ops) shard_size: usize,
    pub(in crate::set_disk::ops) write_quorum: usize,
}

pub(in crate::set_disk::ops) async fn verify_written_bitrot_shards(
    disks: &[Option<DiskStore>],
    inline_parts: Option<&[FileInfo]>,
    target: BitrotSelfVerifyTarget<'_>,
) -> Result<usize> {
    let algo = HashAlgorithm::HighwayHash256S;
    let encoded_shard_size = coding::bitrot_shard_file_size(target.logical_shard_size, target.shard_size, algo.clone());

    let results = if let Some(parts) = inline_parts {
        let tasks = disks.iter().enumerate().filter_map(|(index, disk)| {
            let disk = disk.as_ref()?.clone();
            let data = parts.get(index).and_then(|part| part.data.as_ref()).cloned();
            let algo = algo.clone();
            Some(async move {
                let result = match data {
                    Some(data) => coding::bitrot_verify(
                        Cursor::new(data),
                        encoded_shard_size,
                        target.logical_shard_size,
                        algo,
                        target.shard_size,
                    )
                    .await
                    .map_err(Error::from),
                    None => Err(Error::other(format!("{} inline shard {index} is missing bitrot data", target.operation))),
                };
                (index, disk, result)
            })
        });
        join_all(tasks).await
    } else {
        let tasks = disks.iter().enumerate().filter_map(|(index, disk)| {
            let disk = disk.as_ref()?.clone();
            let algo = algo.clone();
            Some(async move {
                let result = match disk.read_file(target.volume, target.path).await {
                    Ok(reader) => {
                        coding::bitrot_verify(reader, encoded_shard_size, target.logical_shard_size, algo, target.shard_size)
                            .await
                            .map_err(Error::from)
                    }
                    Err(err) => Err(Error::from(err)),
                };
                (index, disk, result)
            })
        });
        join_all(tasks).await
    };

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

#[cfg(test)]
mod tests {
    use super::super::object::hermetic_set_disks_support::hermetic_set_disks_for_pool_with_default_parity;
    use super::*;
    use crate::disk::DiskAPI as _;

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
        let path = "issue-5173/part.1";

        assert_eq!(encoded.len(), 8_250_626);
        disk.write_all(RUSTFS_META_TMP_BUCKET, path, encoded.clone())
            .await
            .expect("healthy temp shard should be written");
        let verified = verify_written_bitrot_shards(
            &[Some(disk.clone())],
            None,
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "object",
                part_number: None,
                volume: RUSTFS_META_TMP_BUCKET,
                path,
                logical_shard_size,
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect("healthy temp shard should verify");
        assert_eq!(verified, 1);

        let mut corrupt = encoded.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;
        disk.write_all(RUSTFS_META_TMP_BUCKET, path, Bytes::from(corrupt))
            .await
            .expect("corrupt temp shard should be staged");

        let err = verify_written_bitrot_shards(
            &[Some(disk)],
            None,
            BitrotSelfVerifyTarget {
                operation: "put_object",
                bucket: "bucket",
                object: "object",
                part_number: None,
                volume: RUSTFS_META_TMP_BUCKET,
                path,
                logical_shard_size,
                shard_size,
                write_quorum: 1,
            },
        )
        .await
        .expect_err("corrupt no-parity temp shard must not be committed");
        assert!(err.to_string().contains("bitrot self-verify failed"));
    }
}
