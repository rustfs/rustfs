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

//! Optional, quorum-owned SHA-256 commitments. The existing part payload and
//! HighwayHash framing are unchanged. A proof file is only an untrusted index;
//! authority comes from the root stored in the selected object's metadata.

use crate::{Error, FileInfo, Result};
use rustfs_utils::http::{contains_key_str, get_consistent_str, insert_str, remove_str};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use uuid::Uuid;

pub const SUFFIX_SHARD_INTEGRITY: &str = "shard-integrity-v1";
pub const SUFFIX_INLINE_INTEGRITY: &str = "shard-integrity-inline-v1";
pub const SUFFIX_UPLOAD_INTEGRITY: &str = "shard-integrity-upload-v1";
pub const MAX_PARTS: usize = 10_000;
pub const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;
pub const MAX_INLINE_PROOF_BYTES: usize = 64 * 1024;
pub const INDEX_HEADER_SIZE: usize = 64;
const TABLE_HEADER_SIZE: usize = 32;
const PART_RECORD_SIZE: usize = 64;
const TABLE_MAGIC: &[u8; 8] = b"RFSI\x01\0\0\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IntegrityLayout {
    pub data: u16,
    pub parity: u16,
    pub block_size: u32,
    pub legacy: bool,
}

impl IntegrityLayout {
    pub fn new(data: usize, parity: usize, block_size: usize, legacy: bool) -> Result<Self> {
        let layout = Self {
            data: data.try_into().map_err(|_| Error::FileCorrupt)?,
            parity: parity.try_into().map_err(|_| Error::FileCorrupt)?,
            block_size: block_size.try_into().map_err(|_| Error::FileCorrupt)?,
            legacy,
        };
        layout.validate()?;
        Ok(layout)
    }

    pub fn validate(&self) -> Result<()> {
        if self.data == 0
            || self.data < self.parity
            || self.shards() > crate::fileinfo::MAX_ERASURE_SHARDS
            || self.block_size == 0
            || self.block_size > 64 * 1024 * 1024
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    pub fn shards(&self) -> usize {
        usize::from(self.data) + usize::from(self.parity)
    }

    fn bytes(&self) -> [u8; 9] {
        let mut out = [0; 9];
        out[..2].copy_from_slice(&self.data.to_le_bytes());
        out[2..4].copy_from_slice(&self.parity.to_le_bytes());
        out[4..8].copy_from_slice(&self.block_size.to_le_bytes());
        out[8] = u8::from(self.legacy);
        out
    }
}

/// One immutable UploadPart/PUT generation. A metadata-only copy retains this
/// identity; overwriting even an identical part creates a new generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartIntegrity {
    pub layout: IntegrityLayout,
    pub number: u32,
    pub generation: Uuid,
    pub size: u64,
    pub stripes: u32,
    pub root: [u8; 32],
}

impl PartIntegrity {
    pub fn new(layout: IntegrityLayout, number: usize) -> Result<Self> {
        let part = Self {
            layout,
            number: number.try_into().map_err(|_| Error::FileCorrupt)?,
            generation: Uuid::new_v4(),
            size: 0,
            stripes: 0,
            root: [0; 32],
        };
        part.validate()?;
        Ok(part)
    }

    pub fn validate(&self) -> Result<()> {
        self.layout.validate()?;
        if self.number == 0
            || usize::try_from(self.number).map_err(|_| Error::FileCorrupt)? > MAX_PARTS
            || self.generation.is_nil()
            || self.size > i64::MAX.unsigned_abs()
            || self.size.div_ceil(u64::from(self.layout.block_size)) != u64::from(self.stripes)
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    pub fn file_name(&self) -> String {
        format!("part.{}.integrity.{}", self.number, self.generation)
    }

    pub fn height(&self) -> u32 {
        u32::BITS - self.stripes.max(1).saturating_sub(1).leading_zeros()
    }

    pub fn record_size(&self) -> usize {
        // At most 16 digests and 32 siblings, independent of untrusted sizes.
        32 * (self.layout.shards() + usize::try_from(self.height()).unwrap_or(32))
    }

    pub fn index_size(&self) -> Result<usize> {
        usize::try_from(self.stripes)
            .ok()
            .and_then(|n| n.checked_mul(self.record_size()))
            .and_then(|n| n.checked_add(INDEX_HEADER_SIZE))
            .ok_or(Error::FileCorrupt)
    }

    pub fn record_offset(&self, stripe: u32) -> Result<usize> {
        if stripe >= self.stripes {
            return Err(Error::FileCorrupt);
        }
        usize::try_from(stripe)
            .ok()
            .and_then(|n| n.checked_mul(self.record_size()))
            .and_then(|n| n.checked_add(INDEX_HEADER_SIZE))
            .ok_or(Error::FileCorrupt)
    }

    pub fn index_header(&self) -> [u8; INDEX_HEADER_SIZE] {
        let mut out = [0; INDEX_HEADER_SIZE];
        out[..8].copy_from_slice(b"RFSP\x01\0\0\0");
        out[8..17].copy_from_slice(&self.layout.bytes());
        out[20..24].copy_from_slice(&self.number.to_le_bytes());
        out[24..40].copy_from_slice(self.generation.as_bytes());
        out[40..48].copy_from_slice(&self.size.to_le_bytes());
        out[48..52].copy_from_slice(&self.stripes.to_le_bytes());
        out
    }

    fn hash_context(&self, domain: &[u8]) -> Sha256 {
        let mut hash = Sha256::new();
        hash.update(domain);
        hash.update(self.layout.bytes());
        hash.update(self.number.to_le_bytes());
        hash.update(self.generation.as_bytes());
        hash
    }

    pub fn shard_digest(&self, stripe: u32, coding_index: usize, payload: &[u8]) -> Result<[u8; 32]> {
        if coding_index >= self.layout.shards() {
            return Err(Error::FileCorrupt);
        }
        let mut hash = self.hash_context(b"rustfs/shard-integrity/v1/payload\0");
        hash.update(stripe.to_le_bytes());
        hash.update(u16::try_from(coding_index).map_err(|_| Error::FileCorrupt)?.to_le_bytes());
        hash.update(u64::try_from(payload.len()).map_err(|_| Error::FileCorrupt)?.to_le_bytes());
        hash.update(payload);
        Ok(hash.finalize().into())
    }

    pub fn leaf_digest(&self, stripe: u32, digests: &[u8]) -> Result<[u8; 32]> {
        if digests.len() != self.layout.shards() * 32 {
            return Err(Error::FileCorrupt);
        }
        let mut hash = self.hash_context(b"rustfs/shard-integrity/v1/leaf\0");
        hash.update(stripe.to_le_bytes());
        hash.update(digests);
        Ok(hash.finalize().into())
    }

    pub fn padding_digest(&self, stripe: u32) -> [u8; 32] {
        let mut hash = self.hash_context(b"rustfs/shard-integrity/v1/padding\0");
        hash.update(stripe.to_le_bytes());
        hash.finalize().into()
    }

    pub fn root_digest(&self, tree_root: &[u8; 32]) -> [u8; 32] {
        let mut hash = self.hash_context(b"rustfs/shard-integrity/v1/root\0");
        hash.update(self.size.to_le_bytes());
        hash.update(self.stripes.to_le_bytes());
        hash.update(tree_root);
        hash.finalize().into()
    }

    pub fn verify_record(&self, stripe: u32, record: &[u8]) -> Result<()> {
        self.record_offset(stripe)?;
        if record.len() != self.record_size() {
            return Err(Error::FileCorrupt);
        }
        let (digests, siblings) = record.split_at(self.layout.shards() * 32);
        let mut digest = self.leaf_digest(stripe, digests)?;
        let mut position = stripe;
        for sibling in siblings.as_chunks::<32>().0 {
            digest = if position & 1 == 0 {
                node_digest(&digest, sibling)
            } else {
                node_digest(sibling, &digest)
            };
            position >>= 1;
        }
        if self.root_digest(&digest) != self.root {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    pub fn verify_shard(&self, stripe: u32, coding_index: usize, payload: &[u8], record: &[u8]) -> Result<()> {
        self.verify_record(stripe, record)?;
        let digest = self.shard_digest(stripe, coding_index, payload)?;
        if record.get(coding_index * 32..(coding_index + 1) * 32) != Some(digest.as_slice()) {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }
}

pub fn node_digest(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rustfs/shard-integrity/v1/node\0");
    hash.update(left);
    hash.update(right);
    hash.finalize().into()
}

pub fn encode_descriptor(parts: &[PartIntegrity]) -> Result<String> {
    let Some(first) = parts.first() else { return Err(Error::FileCorrupt) };
    if parts.len() > MAX_PARTS {
        return Err(Error::FileCorrupt);
    }
    let mut out = vec![0; TABLE_HEADER_SIZE];
    out[..8].copy_from_slice(TABLE_MAGIC);
    out[8..17].copy_from_slice(&first.layout.bytes());
    out[28..32].copy_from_slice(&u32::try_from(parts.len()).map_err(|_| Error::FileCorrupt)?.to_le_bytes());
    let mut previous = 0;
    for part in parts {
        part.validate()?;
        if part.layout != first.layout || part.number <= previous {
            return Err(Error::FileCorrupt);
        }
        previous = part.number;
        out.extend_from_slice(&part.number.to_le_bytes());
        out.extend_from_slice(part.generation.as_bytes());
        out.extend_from_slice(&part.size.to_le_bytes());
        out.extend_from_slice(&part.stripes.to_le_bytes());
        out.extend_from_slice(&part.root);
    }
    Ok(base64_simd::STANDARD.encode_to_string(out))
}

pub fn decode_descriptor(value: &str) -> Result<Vec<PartIntegrity>> {
    if value.len() > MAX_DESCRIPTOR_BYTES {
        return Err(Error::FileCorrupt);
    }
    let bytes = base64_simd::STANDARD.decode_to_vec(value).map_err(|_| Error::FileCorrupt)?;
    // Reject non-canonical encodings as well as unsupported versions/reserved bits.
    if bytes.len() < TABLE_HEADER_SIZE
        || &bytes[..8] != TABLE_MAGIC
        || bytes[16] > 1
        || bytes[17..28].iter().any(|v| *v != 0)
        || base64_simd::STANDARD.encode_to_string(&bytes) != value
    {
        return Err(Error::FileCorrupt);
    }
    let u32_at = |start| -> Result<u32> {
        Ok(u32::from_le_bytes(
            bytes
                .get(start..start + 4)
                .ok_or(Error::FileCorrupt)?
                .try_into()
                .map_err(|_| Error::FileCorrupt)?,
        ))
    };
    let count = usize::try_from(u32_at(28)?).map_err(|_| Error::FileCorrupt)?;
    if count == 0 || count > MAX_PARTS || bytes.len() != TABLE_HEADER_SIZE + count * PART_RECORD_SIZE {
        return Err(Error::FileCorrupt);
    }
    let layout = IntegrityLayout {
        data: u16::from_le_bytes([bytes[8], bytes[9]]),
        parity: u16::from_le_bytes([bytes[10], bytes[11]]),
        block_size: u32_at(12)?,
        legacy: bytes[16] == 1,
    };
    layout.validate()?;
    let mut parts = Vec::with_capacity(count);
    let mut previous = 0;
    for record in bytes[TABLE_HEADER_SIZE..].as_chunks::<PART_RECORD_SIZE>().0 {
        let part = PartIntegrity {
            layout,
            number: u32::from_le_bytes(record[..4].try_into().map_err(|_| Error::FileCorrupt)?),
            generation: Uuid::from_slice(&record[4..20])
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or(Error::FileCorrupt)?,
            size: u64::from_le_bytes(record[20..28].try_into().map_err(|_| Error::FileCorrupt)?),
            stripes: u32::from_le_bytes(record[28..32].try_into().map_err(|_| Error::FileCorrupt)?),
            root: record[32..64].try_into().map_err(|_| Error::FileCorrupt)?,
        };
        part.validate()?;
        if part.number <= previous {
            return Err(Error::FileCorrupt);
        }
        previous = part.number;
        parts.push(part);
    }
    Ok(parts)
}

pub fn descriptor_from_metadata(metadata: &HashMap<String, String>) -> Result<Option<Vec<PartIntegrity>>> {
    if !contains_key_str(metadata, SUFFIX_SHARD_INTEGRITY) {
        // An orphaned inline proof must not silently turn a protected object into legacy.
        if contains_key_str(metadata, SUFFIX_INLINE_INTEGRITY) {
            return Err(Error::FileCorrupt);
        }
        return Ok(None);
    }
    decode_descriptor(get_consistent_str(metadata, SUFFIX_SHARD_INTEGRITY).ok_or(Error::FileCorrupt)?).map(Some)
}

pub fn clear_integrity_metadata(metadata: &mut HashMap<String, String>) {
    for suffix in [SUFFIX_SHARD_INTEGRITY, SUFFIX_INLINE_INTEGRITY, SUFFIX_UPLOAD_INTEGRITY] {
        remove_str(metadata, suffix);
    }
}

impl FileInfo {
    /// Decode once at the metadata boundary and retain each part's validated
    /// commitment. Reading part n must not reparse a 10,000-part descriptor.
    pub fn hydrate_shard_integrity(&mut self) -> Result<()> {
        if !self.parts.is_empty() && contains_key_str(&self.metadata, SUFFIX_UPLOAD_INTEGRITY) {
            return Err(Error::FileCorrupt);
        }
        let Some(parts) = descriptor_from_metadata(&self.metadata)? else {
            if self.parts.iter().any(|part| part.integrity.is_some()) {
                return Err(Error::FileCorrupt);
            }
            return Ok(());
        };
        let layout = IntegrityLayout::new(
            self.erasure.data_blocks,
            self.erasure.parity_blocks,
            self.erasure.block_size,
            self.uses_legacy_checksum,
        )?;
        if parts.len() != self.parts.len() {
            return Err(Error::FileCorrupt);
        }
        for (expected, actual) in parts.into_iter().zip(&mut self.parts) {
            if expected.layout != layout
                || usize::try_from(expected.number).map_err(|_| Error::FileCorrupt)? != actual.number
                || usize::try_from(expected.size).map_err(|_| Error::FileCorrupt)? != actual.size
                || actual.integrity.as_ref().is_some_and(|value| value != &expected)
            {
                return Err(Error::FileCorrupt);
            }
            actual.integrity = Some(expected);
        }
        Ok(())
    }

    pub fn persist_shard_integrity(&mut self) -> Result<()> {
        let parts: Vec<_> = self.parts.iter().filter_map(|part| part.integrity.clone()).collect();
        if parts.is_empty() {
            clear_integrity_metadata(&mut self.metadata);
            return Ok(());
        }
        if parts.len() != self.parts.len() {
            return Err(Error::FileCorrupt);
        }
        insert_str(&mut self.metadata, SUFFIX_SHARD_INTEGRITY, encode_descriptor(&parts)?);
        self.hydrate_shard_integrity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectPartInfo;

    fn part(number: usize) -> PartIntegrity {
        let mut part =
            PartIntegrity::new(IntegrityLayout::new(12, 4, 1024 * 1024, false).expect("layout"), number).expect("part");
        part.size = 1024 * 1024 + 123;
        part.stripes = 2;
        part.root = [7; 32];
        part
    }

    #[test]
    fn integrity_descriptor_is_bounded_canonical_and_complete() {
        let parts: Vec<_> = (1..=MAX_PARTS).map(part).collect();
        let encoded = encode_descriptor(&parts).expect("maximum multipart descriptor");
        assert_eq!(encoded.len(), 853_376);
        assert_eq!(decode_descriptor(&encoded).expect("decode maximum table"), parts);
        assert!(encode_descriptor(&[part(2), part(1)]).is_err());
        assert!(encode_descriptor(&[part(1), part(1)]).is_err());
        assert!(decode_descriptor(&format!("{encoded} ")).is_err());
        assert!(decode_descriptor(&"A".repeat(MAX_DESCRIPTOR_BYTES + 1)).is_err());
        let mut bytes = base64_simd::STANDARD
            .decode_to_vec(encode_descriptor(&[part(1)]).expect("encode"))
            .expect("base64");
        for offset in [4, 17, 28, 36] {
            let mut invalid = bytes.clone();
            if offset == 36 {
                invalid[36..52].fill(0);
            } else {
                invalid[offset] = 255;
            }
            assert!(
                decode_descriptor(&base64_simd::STANDARD.encode_to_string(invalid)).is_err(),
                "offset {offset}"
            );
        }
        bytes.push(0);
        assert!(decode_descriptor(&base64_simd::STANDARD.encode_to_string(bytes)).is_err());
    }

    #[test]
    fn integrity_metadata_distinguishes_absence_corruption_and_part_mismatch() {
        let mut file = FileInfo::new("object", 12, 4);
        file.add_object_part(1, String::new(), 1024 * 1024 + 123, None, 0, None, None);
        assert!(descriptor_from_metadata(&file.metadata).expect("legacy absence").is_none());
        file.parts[0].integrity = Some(part(1));
        file.persist_shard_integrity().expect("persist");
        let mut decoded = file.clone();
        decoded.parts[0].integrity = None;
        decoded.hydrate_shard_integrity().expect("hydrate old peer response");
        assert_eq!(decoded.parts[0].integrity, file.parts[0].integrity);
        decoded.parts[0].size += 1;
        assert!(decoded.hydrate_shard_integrity().is_err());
        file.metadata
            .insert(format!("x-minio-internal-{SUFFIX_SHARD_INTEGRITY}"), "invalid".to_owned());
        assert!(descriptor_from_metadata(&file.metadata).is_err());
    }

    #[test]
    fn integrity_descriptor_matches_independent_v1_vector() {
        // Header, UUID byte order, dimensions and root frozen independently
        // with Python struct/hashlib, rather than a Rust round trip.
        let encoded = "UkZTSQEAAAACAAIACAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAABEiM0RVRneImaq7zN3u/xAAAAAAAAAAAgAAACH+n+LW6++vtoSsUQbqfbsZYr4G+/GrIZa8s/oaPC/G";
        let parts = decode_descriptor(encoded).expect("known descriptor");
        assert_eq!(parts.len(), 1);
        let part = &parts[0];
        assert_eq!(part.layout, IntegrityLayout::new(2, 2, 8, false).expect("layout"));
        assert_eq!(part.generation.to_string(), "00112233-4455-4677-8899-aabbccddeeff");
        assert_eq!((part.number, part.size, part.stripes), (1, 16, 2));
        assert_eq!(
            part.root,
            [
                33, 254, 159, 226, 214, 235, 239, 175, 182, 132, 172, 81, 6, 234, 125, 187, 25, 98, 190, 6, 251, 241, 171, 33,
                150, 188, 179, 250, 26, 60, 47, 198
            ]
        );
        assert_eq!(encode_descriptor(&parts).expect("encode known descriptor"), encoded);
    }

    #[derive(Serialize, Deserialize)]
    struct ReleasePart {
        etag: String,
        number: usize,
        size: usize,
        actual_size: i64,
        mod_time: Option<time::OffsetDateTime>,
        index: Option<bytes::Bytes>,
        checksums: Option<HashMap<String, String>>,
        error: Option<String>,
    }

    #[test]
    fn integrity_part_extension_is_readable_by_release_decoder_and_reads_old_arrays() {
        let old = ReleasePart {
            etag: "same-etag".to_owned(),
            number: 1,
            size: 5,
            actual_size: 5,
            mod_time: None,
            index: None,
            checksums: None,
            error: None,
        };
        let tuple = rmp_serde::to_vec(&old).expect("release positional writer");
        assert_eq!(tuple[0], 0x98, "release wrote eight positional fields");
        let mut new = ObjectPartInfo::unmarshal(&tuple).expect("read release part");
        assert!(new.integrity.is_none());
        new.integrity = Some(part(1));
        let bytes = new.marshal_msg().expect("new writer");
        assert_eq!(bytes[0], 0x89, "extension uses nine named fields");
        let decoded: ReleasePart = rmp_serde::from_slice(&bytes).expect("release decoder ignores unknown named field");
        assert_eq!(decoded.etag, old.etag);
        assert_eq!(decoded.size, 5);
        assert_eq!(ObjectPartInfo::unmarshal(&bytes).expect("new decoder").integrity, new.integrity);
    }
}
