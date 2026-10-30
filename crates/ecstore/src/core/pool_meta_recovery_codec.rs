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

//! Integrity-only codec for conditional recovery of an inline `pool.bin` head.
//! Reconstructing a payload here never grants authority to publish it.

use crate::disk::RUSTFS_META_BUCKET;
use crate::erasure::coding::encode::IntegrityEncodeMode;
use crate::erasure::coding::{BitrotReader, BitrotWriterWrapper, Erasure, calc_shard_size};
use crate::io_support::shard_integrity::{PartProofReader, ShardVerifier};
use bytes::Bytes;
use rustfs_filemeta::{
    BLOCK_SIZE_V2, ChecksumAlgo, ChecksumInfo, ERASURE_ALGORITHM, FileInfo, FileInfoOpts, FileMeta, get_file_info,
};
use rustfs_utils::HashAlgorithm;
use rustfs_utils::http::{SUFFIX_ACTUAL_SIZE, SUFFIX_INLINE_DATA, get_consistent_str, insert_str, remove_str};
use std::io::{self, Cursor};
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

const OBJECT: &str = "pool.bin";
const MAX_SHARDS: usize = 16;

#[derive(Debug, thiserror::Error)]
pub(super) enum PoolMetaCodecError {
    #[error("unsupported inline pool metadata layout")]
    UnsupportedLayout,
    #[error("pool metadata shard does not match its physical disk")]
    ShardPlacement,
    #[error("pool metadata replicas disagree on persistent attributes")]
    InconsistentMetadata,
    #[error("pool metadata payload does not match its metadata or shards")]
    PayloadMismatch,
    #[error("invalid inline pool metadata")]
    Metadata(#[source] rustfs_filemeta::Error),
    #[error("inline pool metadata integrity verification failed")]
    Integrity(#[source] io::Error),
}

impl From<rustfs_filemeta::Error> for PoolMetaCodecError {
    fn from(value: rustfs_filemeta::Error) -> Self {
        Self::Metadata(value)
    }
}

impl From<io::Error> for PoolMetaCodecError {
    fn from(value: io::Error) -> Self {
        Self::Integrity(value)
    }
}

type Result<T> = std::result::Result<T, PoolMetaCodecError>;

/// Rejects layouts outside the narrow, current inline pool metadata writer.
pub(super) fn parse_inline_info(raw: &Bytes) -> Result<FileInfo> {
    if FileMeta::read_format_versions(raw)? != (1, 3, 3, 3) {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    let meta = FileMeta::load(raw)?;
    if meta.versions.len() != 1 {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    if meta.versions[0]
        .parse_version_meta()?
        .object
        .is_none_or(|object| object.bitrot_checksum_algo != ChecksumAlgo::HighwayHash)
    {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    let info = get_file_info(
        raw,
        RUSTFS_META_BUCKET,
        OBJECT,
        "",
        FileInfoOpts {
            data: true,
            include_free_versions: true,
            include_part_checksums: true,
        },
    )?;
    let size = usize::try_from(info.size).map_err(|_| PoolMetaCodecError::UnsupportedLayout)?;
    let erasure = &info.erasure;
    let shards = erasure.data_blocks.checked_add(erasure.parity_blocks);
    if !info.is_valid()
        || info.deleted
        || info.mark_deleted
        || info.version_id.is_some_and(|id| !id.is_nil())
        || info.versioned
        || info.uses_legacy_checksum
        || info.data_dir.is_none_or(|id| id.is_nil())
        || info.mod_time.is_none_or(|modified| modified <= OffsetDateTime::UNIX_EPOCH)
        || info.checksum.is_some()
        || erasure.algorithm != ERASURE_ALGORITHM
        || erasure.block_size != BLOCK_SIZE_V2
        || erasure.data_blocks == 0
        || erasure.data_blocks != erasure.parity_blocks
        || shards.is_none_or(|count| !(2..=MAX_SHARDS).contains(&count))
        || size == 0
        || size > BLOCK_SIZE_V2
        || info.parts.len() != 1
        || info.data.is_none()
        || get_consistent_str(&info.metadata, SUFFIX_INLINE_DATA) != Some("true")
    {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    let part = &info.parts[0];
    let etag = info.metadata.get("etag").ok_or(PoolMetaCodecError::UnsupportedLayout)?;
    if part.number != 1
        || part.size != size
        || part.actual_size != info.size
        || part.etag != *etag
        || part.index.is_some()
        || part.checksums.is_some()
        || part.error.is_some()
        || etag.len() != 32
        || !etag
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || get_consistent_str(&info.metadata, SUFFIX_ACTUAL_SIZE).and_then(|actual| actual.parse::<usize>().ok()) != Some(size)
        || !erasure.checksums.is_empty()
    {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    Ok(info)
}

fn validate_placement(info: &FileInfo, physical_index: usize, disk_count: usize) -> Result<()> {
    if info.erasure.distribution.len() != disk_count
        || info.erasure.data_blocks + info.erasure.parity_blocks != disk_count
        || info.erasure.distribution.get(physical_index).copied() != Some(info.erasure.index)
    {
        return Err(PoolMetaCodecError::ShardPlacement);
    }
    Ok(())
}

fn erasure(info: &FileInfo) -> Result<Erasure> {
    Erasure::try_new(info.erasure.data_blocks, info.erasure.parity_blocks, info.erasure.block_size)
        .map_err(|_| PoolMetaCodecError::UnsupportedLayout)
}

fn md5(payload: &[u8]) -> String {
    hex_simd::encode_to_string(HashAlgorithm::Md5.hash_encode(payload).as_ref(), hex_simd::AsciiCase::Lower)
}

async fn verified_shard(info: &FileInfo) -> Result<Vec<u8>> {
    let size = usize::try_from(info.size).map_err(|_| PoolMetaCodecError::UnsupportedLayout)?;
    let shard_size = calc_shard_size(size, info.erasure.data_blocks);
    let data = info.data.as_ref().ok_or(PoolMetaCodecError::UnsupportedLayout)?;
    if data.len() != shard_size + HashAlgorithm::HighwayHash256S.size() {
        return Err(PoolMetaCodecError::PayloadMismatch);
    }
    let mut reader = BitrotReader::new(Cursor::new(data.clone()), shard_size, HashAlgorithm::HighwayHash256S, false);
    if let Some(integrity) = &info.parts[0].integrity {
        let proof = PartProofReader::new(integrity.clone(), std::slice::from_ref(info), &[None], RUSTFS_META_BUCKET, OBJECT)?;
        reader.set_integrity(ShardVerifier::new(proof, info.erasure.index - 1, 0, None)?)?;
    }
    let mut shard = vec![0; shard_size];
    if reader.read(&mut shard).await? != shard_size {
        return Err(PoolMetaCodecError::PayloadMismatch);
    }
    Ok(shard)
}

fn generation_key(info: &FileInfo) -> FileInfo {
    let mut key = info.clone();
    key.erasure.index = 0;
    key.data = None;
    key
}

/// Decodes every reconstructable generation without making a quorum decision.
///
/// All physical disks must be present and every supplied shard must verify.
/// Groups below the erasure reconstruction threshold are left undecoded; the
/// caller must match every such shard against an independently known payload
/// with [`validate_inline_pool_meta_shard`] before proposing any mutation.
pub(super) async fn decode_inline_pool_meta_replicas(raw: &[Bytes]) -> Result<Vec<Bytes>> {
    if !(2..=MAX_SHARDS).contains(&raw.len()) {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    struct Group {
        key: FileInfo,
        present: usize,
        shards: Vec<Option<Vec<u8>>>,
    }
    let mut groups: Vec<Group> = Vec::new();
    for (physical_index, bytes) in raw.iter().enumerate() {
        let info = parse_inline_info(bytes)?;
        validate_placement(&info, physical_index, raw.len())?;
        let shard = verified_shard(&info).await?;
        let key = generation_key(&info);
        let group_index = if let Some(index) = groups.iter().position(|group| group.key == key) {
            index
        } else {
            groups.push(Group {
                key,
                present: 0,
                shards: vec![None; raw.len()],
            });
            groups.len() - 1
        };
        let group = &mut groups[group_index];
        let slot = &mut group.shards[info.erasure.index - 1];
        if slot.is_some() {
            return Err(PoolMetaCodecError::ShardPlacement);
        }
        *slot = Some(shard);
        group.present += 1;
    }
    let mut decoded = Vec::with_capacity(groups.len());
    for mut group in groups {
        if group.present < group.key.erasure.data_blocks {
            continue;
        }
        let codec = erasure(&group.key)?;
        codec.decode_data(&mut group.shards)?;
        let size = usize::try_from(group.key.size).map_err(|_| PoolMetaCodecError::UnsupportedLayout)?;
        let mut payload = Vec::with_capacity(size);
        for shard in group.shards.iter().take(group.key.erasure.data_blocks) {
            let shard = shard.as_ref().ok_or(PoolMetaCodecError::PayloadMismatch)?;
            let remaining = size.saturating_sub(payload.len());
            payload.extend_from_slice(&shard[..remaining.min(shard.len())]);
        }
        if payload.len() != size || group.key.metadata.get("etag") != Some(&md5(&payload)) {
            return Err(PoolMetaCodecError::PayloadMismatch);
        }
        let encoded = codec.encode_data(&payload)?;
        if group
            .shards
            .iter()
            .zip(&encoded)
            .any(|(actual, expected)| actual.as_ref().is_some_and(|actual| actual.as_slice() != expected.as_ref()))
        {
            return Err(PoolMetaCodecError::PayloadMismatch);
        }
        decoded.push(Bytes::from(payload));
    }
    Ok(decoded)
}

/// Verifies a minority shard against a payload whose authority was established elsewhere.
pub(super) async fn validate_inline_pool_meta_shard(raw: &Bytes, physical_index: usize, payload: &[u8]) -> Result<()> {
    let info = parse_inline_info(raw)?;
    validate_placement(&info, physical_index, info.erasure.distribution.len())?;
    if usize::try_from(info.size).ok() != Some(payload.len()) || info.metadata.get("etag") != Some(&md5(payload)) {
        return Err(PoolMetaCodecError::PayloadMismatch);
    }
    let actual = verified_shard(&info).await?;
    let encoded = erasure(&info)?.encode_data(payload)?;
    if encoded
        .get(info.erasure.index - 1)
        .is_none_or(|expected| expected.as_ref() != actual.as_slice())
    {
        return Err(PoolMetaCodecError::PayloadMismatch);
    }
    Ok(())
}

fn persistent_attributes(info: &FileInfo) -> FileInfo {
    let mut attributes = generation_key(info);
    attributes.data_dir = None;
    attributes.mod_time = None;
    attributes.size = 0;
    attributes.parts.clear();
    attributes.erasure.checksums.clear();
    attributes.metadata.remove("etag");
    remove_str(&mut attributes.metadata, SUFFIX_ACTUAL_SIZE);
    rustfs_filemeta::shard_integrity::clear_integrity_metadata(&mut attributes.metadata);
    attributes
}

/// Builds disk-specific inline heads while retaining each disk's existing FileMeta.
/// The caller supplies the independently authorized canonical COMMIT payload.
pub(super) async fn encode_inline_pool_meta_replicas(
    raw: &[Bytes],
    canonical: &[u8],
    modified: OffsetDateTime,
) -> Result<Vec<Bytes>> {
    if !(2..=MAX_SHARDS).contains(&raw.len()) || canonical.is_empty() || canonical.len() > BLOCK_SIZE_V2 {
        return Err(PoolMetaCodecError::UnsupportedLayout);
    }
    let mut infos = Vec::with_capacity(raw.len());
    for (physical_index, bytes) in raw.iter().enumerate() {
        let info = parse_inline_info(bytes)?;
        validate_placement(&info, physical_index, raw.len())?;
        verified_shard(&info).await?;
        if info.mod_time.is_none_or(|previous| modified <= previous) {
            return Err(PoolMetaCodecError::UnsupportedLayout);
        }
        if let Some(first) = infos.first()
            && persistent_attributes(first) != persistent_attributes(&info)
        {
            return Err(PoolMetaCodecError::InconsistentMetadata);
        }
        infos.push(info);
    }
    let mut base = infos.first().ok_or(PoolMetaCodecError::UnsupportedLayout)?.clone();
    let protected = infos.iter().any(|info| info.parts[0].integrity.is_some());
    let codec = Arc::new(erasure(&base)?);
    let mut writers: Vec<Option<BitrotWriterWrapper>> = std::iter::repeat_with(|| None).take(raw.len()).collect();
    let (_, written, shards, integrity) = Arc::clone(&codec)
        .encode_with_shard_integrity(
            Cursor::new(Bytes::copy_from_slice(canonical)),
            &mut writers,
            raw.len(),
            1,
            IntegrityEncodeMode::Inline(canonical.len()),
            protected,
        )
        .await?;
    if written != canonical.len() {
        return Err(PoolMetaCodecError::PayloadMismatch);
    }
    let shards = shards.ok_or(PoolMetaCodecError::PayloadMismatch)?;
    let etag = md5(canonical);
    base.data_dir = Some(Uuid::new_v4());
    base.mod_time = Some(modified);
    base.size = i64::try_from(canonical.len()).map_err(|_| PoolMetaCodecError::UnsupportedLayout)?;
    base.metadata.insert("etag".to_owned(), etag.clone());
    insert_str(&mut base.metadata, SUFFIX_ACTUAL_SIZE, canonical.len().to_string());
    base.parts.clear();
    base.add_object_part(1, etag, canonical.len(), Some(modified), base.size, None, None);
    rustfs_filemeta::shard_integrity::clear_integrity_metadata(&mut base.metadata);
    if let Some(integrity) = integrity {
        integrity.set_inline_metadata(&mut base)?;
        base.parts[0].integrity = Some(integrity.part);
    }
    base.persist_shard_integrity()?;
    base.erasure.checksums = vec![ChecksumInfo {
        part_number: 1,
        algorithm: HashAlgorithm::HighwayHash256S,
        hash: Bytes::new(),
    }];
    base.set_inline_data();
    let mut replacement = Vec::with_capacity(raw.len());
    for (bytes, original) in raw.iter().zip(infos) {
        let mut info = base.clone();
        info.erasure.index = original.erasure.index;
        info.data = Some(
            shards
                .get(info.erasure.index - 1)
                .ok_or(PoolMetaCodecError::ShardPlacement)?
                .clone(),
        );
        let mut meta = FileMeta::load(bytes)?;
        meta.add_version(info)?;
        replacement.push(Bytes::from(meta.marshal_msg()?));
    }
    Ok(replacement)
}

#[cfg(test)]
pub(super) async fn fixture_inline_pool_meta_replicas(payload: &[u8], modified: OffsetDateTime) -> Vec<Bytes> {
    use crate::erasure::coding::{BitrotWriterWrapper, CustomWriter};
    let mut base = FileInfo::new(".rustfs.sys/pool.bin", 2, 2);
    base.volume = RUSTFS_META_BUCKET.to_owned();
    base.name = OBJECT.to_owned();
    base.data_dir = Some(Uuid::new_v4());
    base.mod_time = Some(modified);
    base.size = i64::try_from(payload.len()).expect("test payload should fit i64");
    let etag = md5(payload);
    base.metadata.insert("etag".to_owned(), etag.clone());
    base.metadata
        .insert("content-type".to_owned(), "application/octet-stream".to_owned());
    insert_str(&mut base.metadata, SUFFIX_ACTUAL_SIZE, payload.len().to_string());
    base.set_inline_data();
    base.add_object_part(1, etag, payload.len(), Some(modified), base.size, None, None);
    let codec = erasure(&base).expect("fixture layout should construct");
    let shards = codec.encode_data(payload).expect("fixture erasure encoding should succeed");
    let mut raw = Vec::with_capacity(4);
    for coding_index in &base.erasure.distribution {
        let mut writer =
            BitrotWriterWrapper::new(CustomWriter::new_inline_buffer(), codec.shard_size(), HashAlgorithm::HighwayHash256S);
        writer
            .write(&shards[*coding_index - 1])
            .await
            .expect("fixture bitrot writer should write");
        writer.shutdown().await.expect("fixture bitrot writer should finish");
        let mut info = base.clone();
        info.erasure.index = *coding_index;
        info.data = Some(Bytes::from(writer.into_inline_data().expect("fixture writer should retain inline bytes")));
        let mut meta = FileMeta::new();
        meta.add_version(info).expect("test inline FileInfo should serialize");
        raw.push(Bytes::from(meta.marshal_msg().expect("test xl.meta should serialize")));
    }
    raw
}

#[cfg(test)]
pub(super) async fn fixture_split_inline_pool_meta_replicas(
    prepare: &[u8],
    commit: &[u8],
    modified: OffsetDateTime,
) -> Vec<Bytes> {
    let mut raw = fixture_inline_pool_meta_replicas(prepare, modified).await;
    let committed = fixture_inline_pool_meta_replicas(commit, modified + time::Duration::seconds(1)).await;
    raw[2..].clone_from_slice(&committed[2..]);
    raw
}

#[cfg(test)]
mod tests {
    use super::*;
    use fixture_inline_pool_meta_replicas as fixtures;

    fn time(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp should be valid")
    }

    fn metadata(info: FileInfo) -> Bytes {
        let mut meta = FileMeta::new();
        meta.add_version(info).expect("test inline FileInfo should serialize");
        Bytes::from(meta.marshal_msg().expect("test xl.meta should serialize"))
    }

    #[tokio::test]
    async fn pool_meta_codec_reconstructs_both_halves_without_treating_them_as_authority() {
        let raw = fixture_split_inline_pool_meta_replicas(b"prepare with predecessor", b"commit", time(10)).await;
        let decoded = decode_inline_pool_meta_replicas(&raw)
            .await
            .expect("two shards should reconstruct each payload");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].as_ref(), b"prepare with predecessor");
        assert_eq!(decoded[1].as_ref(), b"commit");
    }

    #[tokio::test]
    async fn pool_meta_codec_requires_known_payload_for_single_minority_shard() {
        let previous = fixtures(b"previous", time(10)).await;
        let commit = fixtures(b"commit", time(11)).await;
        let raw = vec![previous[0].clone(), commit[1].clone(), commit[2].clone(), commit[3].clone()];
        let decoded = decode_inline_pool_meta_replicas(&raw)
            .await
            .expect("three commit shards should reconstruct");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].as_ref(), b"commit");
        validate_inline_pool_meta_shard(&raw[0], 0, b"previous")
            .await
            .expect("known predecessor should verify");
        assert!(matches!(
            validate_inline_pool_meta_shard(&raw[0], 0, b"commit").await,
            Err(PoolMetaCodecError::PayloadMismatch)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_rejects_bitrot_even_with_other_reconstructable_shards() {
        let mut raw = fixtures(b"commit", time(10)).await;
        let mut info = parse_inline_info(&raw[0]).expect("fixture should parse");
        let mut data = info.data.take().expect("fixture should be inline").to_vec();
        data[HashAlgorithm::HighwayHash256S.size()] ^= 1;
        info.data = Some(Bytes::from(data));
        raw[0] = metadata(info);
        assert!(matches!(
            decode_inline_pool_meta_replicas(&raw).await,
            Err(PoolMetaCodecError::Integrity(_))
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_rejects_trailing_inline_bytes() {
        let mut raw = fixtures(b"commit", time(10)).await;
        let mut info = parse_inline_info(&raw[0]).expect("fixture should parse");
        let mut data = info.data.take().expect("fixture should be inline").to_vec();
        data.push(0);
        info.data = Some(Bytes::from(data));
        raw[0] = metadata(info);
        assert!(matches!(
            decode_inline_pool_meta_replicas(&raw).await,
            Err(PoolMetaCodecError::PayloadMismatch)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_rejects_duplicate_physical_shard_mapping() {
        let mut raw = fixtures(b"commit", time(10)).await;
        raw.swap(0, 1);
        assert!(matches!(
            decode_inline_pool_meta_replicas(&raw).await,
            Err(PoolMetaCodecError::ShardPlacement)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_rejects_payload_md5_disagreement_after_valid_bitrot() {
        let mut raw = fixtures(b"commit", time(10)).await;
        for bytes in &mut raw {
            let mut info = parse_inline_info(bytes).expect("fixture should parse");
            info.metadata
                .insert("etag".to_owned(), "00000000000000000000000000000000".to_owned());
            info.parts[0].etag = "00000000000000000000000000000000".to_owned();
            *bytes = metadata(info);
        }
        assert!(matches!(
            decode_inline_pool_meta_replicas(&raw).await,
            Err(PoolMetaCodecError::PayloadMismatch)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_reencodes_split_heads_to_one_canonical_generation() {
        let prepare = fixtures(b"prepare with predecessor", time(10)).await;
        let commit = fixtures(b"commit", time(11)).await;
        let raw = vec![prepare[0].clone(), prepare[1].clone(), commit[2].clone(), commit[3].clone()];
        let repaired = encode_inline_pool_meta_replicas(&raw, b"commit", time(12))
            .await
            .expect("split heads should encode");
        let decoded = decode_inline_pool_meta_replicas(&repaired)
            .await
            .expect("repaired set should decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].as_ref(), b"commit");
        let info = parse_inline_info(&repaired[0]).expect("repaired metadata should parse");
        assert_eq!(info.mod_time, Some(time(12)));
        assert_eq!(info.metadata.get("content-type").map(String::as_str), Some("application/octet-stream"));
        for (index, bytes) in repaired.iter().enumerate() {
            validate_inline_pool_meta_shard(bytes, index, b"commit")
                .await
                .expect("every repaired disk should match commit");
        }
    }

    #[tokio::test]
    async fn pool_meta_codec_preserves_required_persistent_metadata_agreement() {
        let mut raw = fixtures(b"commit", time(10)).await;
        let mut info = parse_inline_info(&raw[0]).expect("fixture should parse");
        info.metadata.insert("content-type".to_owned(), "text/plain".to_owned());
        raw[0] = metadata(info);
        assert!(matches!(
            encode_inline_pool_meta_replicas(&raw, b"commit", time(11)).await,
            Err(PoolMetaCodecError::InconsistentMetadata)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_requires_strictly_newer_common_modification_time() {
        let raw = fixtures(b"commit", time(10)).await;
        assert!(matches!(
            encode_inline_pool_meta_replicas(&raw, b"commit", time(10)).await,
            Err(PoolMetaCodecError::UnsupportedLayout)
        ));
    }

    #[tokio::test]
    async fn pool_meta_codec_rejects_noninline_heads_and_wrong_disk_count() {
        let mut raw = fixtures(b"commit", time(10)).await;
        assert!(matches!(
            decode_inline_pool_meta_replicas(&raw[..3]).await,
            Err(PoolMetaCodecError::ShardPlacement)
        ));
        let mut info = parse_inline_info(&raw[0]).expect("fixture should parse");
        info.data = None;
        raw[0] = metadata(info);
        assert!(matches!(parse_inline_info(&raw[0]), Err(PoolMetaCodecError::UnsupportedLayout)));
    }
}
