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

use super::{FileInfo, SetDisks, coding, create_bitrot_reader, file_info_is_valid_for_metadata};
use crate::disk::error::{DiskError, Result};
use futures::future::join_all;
use md5::{Digest, Md5};

impl SetDisks {
    /// Unbound frames need independent parity evidence before their bytes can
    /// be released. Never mint identities from an inconsistent surviving set.
    /// Missing members cannot supply that evidence; preserve them for recovery.
    pub(super) async fn verify_unbound_payload(&self, bucket: &str, object: &str, latest: &FileInfo) -> Result<()> {
        if latest.deleted || latest.is_remote() || latest.size == 0 || latest.uses_bound_bitrot()? {
            return Ok(());
        }
        latest.validate_for_metadata_read()?;
        if latest.erasure.parity_blocks == 0
            || latest.is_compressed()
            || latest
                .metadata
                .keys()
                .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
        {
            return Err(DiskError::FileCorrupt);
        }
        let disks = self.get_disks_internal().await;
        let version = latest.version_id.map(|id| id.to_string()).unwrap_or_default();
        let (mut files, errors) = Self::read_all_fileinfo(&disks, "", bucket, object, &version, true, true, false)
            .await
            .map_err(|_| DiskError::FileCorrupt)?;
        let identity = Self::file_info_quorum_hash(latest);
        for ((file, error), disk) in files.iter_mut().zip(&errors).zip(&disks) {
            if error.is_some() || disk.is_none() || !file_info_is_valid_for_metadata(file) {
                return Err(DiskError::ErasureReadQuorum);
            }
            Self::hydrate_selected_fileinfo_part_checksums(file).map_err(|_| DiskError::FileCorrupt)?;
            if Self::file_info_quorum_hash(file) != identity {
                return Err(DiskError::FileCorrupt);
            }
        }
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files, latest);
        let erasure = coding::Erasure::try_new_with_options(
            latest.erasure.data_blocks,
            latest.erasure.parity_blocks,
            latest.erasure.block_size,
            latest.uses_legacy_checksum,
        )
        .map_err(DiskError::from)?;
        for part in &latest.parts {
            // A complete foreign codeword is parity-consistent too. The legacy
            // plaintext ETag supplies an independent anchor to the target meta.
            let expected = if part.etag.is_empty() && latest.parts.len() == 1 {
                latest.metadata.get("etag").map(String::as_str).unwrap_or_default()
            } else {
                part.etag.as_str()
            }
            .trim_matches('"');
            if expected.len() != 32 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(DiskError::FileCorrupt);
            }
            let mut digest = Md5::new();
            let mut payload_remaining = part.size;
            let size = i64::try_from(part.size).map_err(|_| DiskError::FileCorrupt)?;
            let mut remaining = usize::try_from(erasure.shard_file_size(size)).map_err(|_| DiskError::FileCorrupt)?;
            let algorithm = latest.bitrot_algorithm(part.number)?;
            let mut readers = Vec::with_capacity(disks.len());
            for (file, disk) in files.iter().zip(&disks) {
                let data_dir = file.data_dir.ok_or(DiskError::FileCorrupt)?;
                let path = format!("{object}/{data_dir}/part.{}", part.number);
                let reader = create_bitrot_reader(
                    file.data.as_deref(),
                    disk.as_ref(),
                    bucket,
                    &path,
                    0,
                    remaining,
                    erasure.shard_size(),
                    algorithm.clone(),
                    false,
                    false,
                )
                .await?
                .ok_or(DiskError::ErasureReadQuorum)?;
                readers.push(reader);
            }
            let mut shards = vec![Some(Vec::with_capacity(erasure.shard_size())); readers.len()];
            while remaining > 0 {
                let want = remaining.min(erasure.shard_size());
                let results = join_all(readers.iter_mut().zip(&mut shards).map(|(reader, shard)| async move {
                    let bytes = shard.as_mut().ok_or(DiskError::FileCorrupt)?;
                    bytes.resize(want, 0);
                    reader.read(bytes).await.map_err(|_| DiskError::FileCorrupt)?;
                    Ok::<_, DiskError>(())
                }))
                .await;
                for result in results {
                    result?;
                }
                let verifier = erasure.clone();
                shards = tokio::task::spawn_blocking(move || {
                    if verifier.verify_data_and_parity(&shards).map_err(|_| DiskError::FileCorrupt)? {
                        Ok(shards)
                    } else {
                        Err(DiskError::FileCorrupt)
                    }
                })
                .await
                .map_err(|_| DiskError::FileCorrupt)??;
                let mut stripe_remaining = payload_remaining.min(erasure.block_size);
                let stripe_payload = stripe_remaining;
                for shard in shards.iter().take(erasure.data_shards) {
                    let shard = shard.as_ref().ok_or(DiskError::FileCorrupt)?;
                    let take = stripe_remaining.min(shard.len());
                    digest.update(&shard[..take]);
                    stripe_remaining -= take;
                }
                if stripe_remaining != 0 {
                    return Err(DiskError::FileCorrupt);
                }
                payload_remaining -= stripe_payload;
                remaining -= want;
            }
            if payload_remaining != 0 || !rustfs_utils::hex(digest.finalize()).eq_ignore_ascii_case(expected) {
                return Err(DiskError::FileCorrupt);
            }
        }
        Ok(())
    }
}
