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

use crate::{Error, ReplicationState, ReplicationStatusType, Result, TRANSITION_COMPLETE, VersionPurgeStatusType};
use bytes::Bytes;
use rmp_serde::Serializer;
use rustfs_utils::HashAlgorithm;
use rustfs_utils::http::{
    SUFFIX_COMPRESSION, SUFFIX_DATA_MOVED, SUFFIX_HEALING, SUFFIX_INLINE_DATA, SUFFIX_TIER_FV_ID, SUFFIX_TIER_FV_MARKER,
    SUFFIX_TIER_SKIP_FV_ID, contains_key_str, get_str, insert_str,
};
use s3s::dto::{RestoreStatus, Timestamp};
use s3s::header::X_AMZ_RESTORE;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use time::{format_description::FormatItem, macros::format_description};
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1024 * 1024; // 1M

// Additional constants from Go version
pub const NULL_VERSION_ID: &str = "null";
// pub const RUSTFS_ERASURE_UPGRADED: &str = "x-rustfs-internal-erasure-upgraded";

pub const TIER_FV_ID: &str = "tier-free-versionID";
pub const TIER_FV_MARKER: &str = "tier-free-marker";
pub const TIER_SKIP_FV_ID: &str = "tier-skip-fvid";

const ERR_RESTORE_HDR_MALFORMED: &str = "x-amz-restore header malformed";

const RFC1123: &[FormatItem<'_>] =
    format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct ObjectPartInfo {
    pub etag: String,
    pub number: usize,
    pub size: usize,
    pub actual_size: i64, // Original data size
    pub mod_time: Option<OffsetDateTime>,
    // Index holds the index of the part in the erasure coding
    pub index: Option<Bytes>,
    // Checksums holds checksums of the part
    pub checksums: Option<HashMap<String, String>>,
    pub error: Option<String>,
}

impl ObjectPartInfo {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: ObjectPartInfo = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
// ChecksumInfo - carries checksums of individual scattered parts per disk.
pub struct ChecksumInfo {
    pub part_number: usize,
    pub algorithm: HashAlgorithm,
    pub hash: Bytes,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum ErasureAlgo {
    #[default]
    Invalid = 0,
    ReedSolomon = 1,
}

impl ErasureAlgo {
    pub fn valid(&self) -> bool {
        *self > ErasureAlgo::Invalid
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ErasureAlgo::Invalid => 0,
            ErasureAlgo::ReedSolomon => 1,
        }
    }

    pub fn from_u8(u: u8) -> Self {
        match u {
            1 => ErasureAlgo::ReedSolomon,
            _ => ErasureAlgo::Invalid,
        }
    }
}

impl std::fmt::Display for ErasureAlgo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErasureAlgo::Invalid => write!(f, "Invalid"),
            ErasureAlgo::ReedSolomon => write!(f, "{ERASURE_ALGORITHM}"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
// ErasureInfo holds erasure coding and bitrot related information.
pub struct ErasureInfo {
    // Algorithm is the String representation of erasure-coding-algorithm
    pub algorithm: String,
    // DataBlocks is the number of data blocks for erasure-coding
    pub data_blocks: usize,
    // ParityBlocks is the number of parity blocks for erasure-coding
    pub parity_blocks: usize,
    // BlockSize is the size of one erasure-coded block
    pub block_size: usize,
    // Index is the index of the current disk
    pub index: usize,
    // Distribution is the distribution of the data and parity blocks
    pub distribution: Vec<usize>,
    // Checksums holds all bitrot checksums of all erasure encoded blocks
    pub checksums: Vec<ChecksumInfo>,
}

pub fn calc_shard_size(block_size: usize, data_shards: usize) -> usize {
    (block_size.div_ceil(data_shards) + 1) & !1
}

impl ErasureInfo {
    pub fn get_checksum_info(&self, part_number: usize) -> ChecksumInfo {
        for sum in &self.checksums {
            if sum.part_number == part_number {
                return sum.clone();
            }
        }

        ChecksumInfo {
            algorithm: HashAlgorithm::HighwayHash256S,
            ..Default::default()
        }
    }

    /// Calculate the size of each shard.
    pub fn shard_size(&self) -> usize {
        calc_shard_size(self.block_size, self.data_blocks)
    }
    /// Calculate the total erasure file size for a given original size.
    // Returns the final erasure size from the original size
    pub fn shard_file_size(&self, total_length: i64) -> i64 {
        if total_length == 0 {
            return 0;
        }

        if total_length < 0 {
            return total_length;
        }

        let total_length = total_length as usize;

        let num_shards = total_length / self.block_size;
        let last_block_size = total_length % self.block_size;
        let last_shard_size = calc_shard_size(last_block_size, self.data_blocks);
        (num_shards * self.shard_size() + last_shard_size) as i64
    }

    /// Check if this ErasureInfo equals another ErasureInfo
    pub fn equals(&self, other: &ErasureInfo) -> bool {
        if self.algorithm != other.algorithm {
            return false;
        }

        if self.data_blocks != other.data_blocks {
            return false;
        }

        if self.parity_blocks != other.parity_blocks {
            return false;
        }

        if self.block_size != other.block_size {
            return false;
        }

        if self.distribution.len() != other.distribution.len() {
            return false;
        }
        for (i, v) in self.distribution.iter().enumerate() {
            if v != &other.distribution[i] {
                return false;
            }
        }
        true
    }
}

// #[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct FileInfo {
    pub volume: String,
    pub name: String,
    pub version_id: Option<Uuid>,
    pub is_latest: bool,
    pub deleted: bool,
    pub transition_status: String,
    pub transitioned_objname: String,
    pub transition_tier: String,
    pub transition_version_id: Option<Uuid>,
    pub expire_restored: bool,
    pub data_dir: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    // File mode bits
    pub mode: Option<u32>,
    // WrittenByVersion is the unix time stamp of the version that created this version of the object
    pub written_by_version: Option<u64>,
    pub metadata: HashMap<String, String>,
    pub parts: Vec<ObjectPartInfo>,
    pub erasure: ErasureInfo,
    // MarkDeleted marks this version as deleted
    pub mark_deleted: bool,
    // ReplicationState - Internal replication state to be passed back in ObjectInfo
    pub replication_state_internal: Option<ReplicationState>,
    pub data: Option<Bytes>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub fresh: bool,
    pub idx: usize,
    // Combined checksum when object was uploaded
    pub checksum: Option<Bytes>,
    pub versioned: bool,
    /// True when version meta was parsed via rmp_serde fallback (legacy format).
    pub uses_legacy_checksum: bool,
}

/// Validates that an erasure `distribution` is a permutation of `1..=n`.
///
/// A well-formed distribution has exactly `n` entries and each 1-based slot
/// index in `1..=n` appears exactly once. Corrupt or adversarial `xl.meta`
/// can carry values of `0` or greater than `n`, which are later used as
/// `distribution[k] - 1` indices into fixed-size vectors and would trigger a
/// `usize` underflow / out-of-bounds panic. Rejecting such distributions here
/// lets the metadata surface as a clean quorum/corruption error instead.
pub(crate) fn is_valid_distribution(distribution: &[usize], n: usize) -> bool {
    if n == 0 || distribution.len() != n {
        return false;
    }

    let mut seen = vec![false; n];
    for &block_idx in distribution {
        // Valid 1-based slots are `1..=n`; anything else (including `0`) is invalid.
        if block_idx < 1 || block_idx > n {
            return false;
        }
        let slot = block_idx - 1;
        if seen[slot] {
            // Duplicate slot: not a permutation.
            return false;
        }
        seen[slot] = true;
    }

    true
}

impl FileInfo {
    pub fn new(object: &str, data_blocks: usize, parity_blocks: usize) -> Self {
        let indices = {
            let cardinality = data_blocks + parity_blocks;
            let mut nums = vec![0; cardinality];
            let key_crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(object.as_bytes());
                hasher.finalize() as u32
            };

            let start = key_crc as usize % cardinality;
            for i in 1..=cardinality {
                nums[i - 1] = 1 + ((start + i) % cardinality);
            }

            nums
        };
        Self {
            erasure: ErasureInfo {
                algorithm: String::from(ERASURE_ALGORITHM),
                data_blocks,
                parity_blocks,
                block_size: BLOCK_SIZE_V2,
                distribution: indices,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn is_valid(&self) -> bool {
        if self.deleted {
            return true;
        }

        let data_blocks = self.erasure.data_blocks;
        let parity_blocks = self.erasure.parity_blocks;

        (data_blocks >= parity_blocks)
            && (data_blocks > 0)
            && (self.erasure.index > 0
                && self.erasure.index <= data_blocks + parity_blocks
                && is_valid_distribution(&self.erasure.distribution, data_blocks + parity_blocks))
    }

    pub fn get_etag(&self) -> Option<String> {
        self.metadata.get("etag").cloned()
    }

    pub fn write_quorum(&self, quorum: usize) -> usize {
        if self.deleted {
            return quorum;
        }

        if self.erasure.data_blocks == self.erasure.parity_blocks {
            return self.erasure.data_blocks + 1;
        }

        self.erasure.data_blocks
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut Serializer::new(&mut buf))?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: FileInfo = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_object_part(
        &mut self,
        num: usize,
        etag: String,
        part_size: usize,
        mod_time: Option<OffsetDateTime>,
        actual_size: i64,
        index: Option<Bytes>,
        checksums: Option<HashMap<String, String>>,
    ) {
        let part = ObjectPartInfo {
            etag,
            number: num,
            size: part_size,
            mod_time,
            actual_size,
            index,
            checksums,
            error: None,
        };

        for p in self.parts.iter_mut() {
            if p.number == num {
                *p = part;
                return;
            }
        }

        self.parts.push(part);

        self.parts.sort_by_key(|a| a.number);
    }

    // to_part_offset gets the part index where offset is located, returns part index and offset
    pub fn to_part_offset(&self, offset: usize) -> Result<(usize, usize)> {
        if offset == 0 {
            return Ok((0, 0));
        }

        let mut part_offset = offset;
        for (i, part) in self.parts.iter().enumerate() {
            let part_index = i;
            if part_offset < part.size {
                return Ok((part_index, part_offset));
            }

            part_offset -= part.size
        }

        Err(Error::other("part not found"))
    }

    pub fn set_healing(&mut self) {
        insert_str(&mut self.metadata, SUFFIX_HEALING, "true".to_string());
    }

    pub fn set_tier_free_version_id(&mut self, version_id: &str) {
        insert_str(&mut self.metadata, SUFFIX_TIER_FV_ID, version_id.to_string());
    }

    pub fn tier_free_version_id(&self) -> String {
        get_str(&self.metadata, SUFFIX_TIER_FV_ID).unwrap_or_default()
    }

    pub fn set_tier_free_version(&mut self) {
        insert_str(&mut self.metadata, SUFFIX_TIER_FV_MARKER, "".to_string());
    }

    pub fn set_skip_tier_free_version(&mut self) {
        insert_str(&mut self.metadata, SUFFIX_TIER_SKIP_FV_ID, "".to_string());
    }

    pub fn skip_tier_free_version(&self) -> bool {
        contains_key_str(&self.metadata, SUFFIX_TIER_SKIP_FV_ID)
    }

    pub fn tier_free_version(&self) -> bool {
        contains_key_str(&self.metadata, SUFFIX_TIER_FV_MARKER)
    }

    pub fn set_inline_data(&mut self) {
        insert_str(&mut self.metadata, SUFFIX_INLINE_DATA, "true".to_string());
    }

    pub fn set_data_moved(&mut self) {
        insert_str(&mut self.metadata, SUFFIX_DATA_MOVED, "true".to_string());
    }

    pub fn inline_data(&self) -> bool {
        contains_key_str(&self.metadata, SUFFIX_INLINE_DATA) && !self.is_remote()
    }

    /// Check if the object is compressed
    pub fn is_compressed(&self) -> bool {
        contains_key_str(&self.metadata, SUFFIX_COMPRESSION)
    }

    /// Check if the object is remote (transitioned to another tier)
    pub fn is_remote(&self) -> bool {
        if self.transition_status != TRANSITION_COMPLETE {
            return false;
        }
        !is_restored_object_on_disk(&self.metadata)
    }

    /// Get the data directory for this object
    pub fn get_data_dir(&self) -> String {
        if self.deleted {
            return "delete-marker".to_string();
        }
        self.data_dir.map_or_else(|| "".to_string(), |dir| dir.to_string())
    }

    /// Read quorum returns expected read quorum for this FileInfo
    pub fn read_quorum(&self, dquorum: usize) -> usize {
        if self.deleted {
            return dquorum;
        }
        self.erasure.data_blocks
    }

    /// Create a shallow copy with minimal information for READ MRF checks
    pub fn shallow_copy(&self) -> Self {
        Self {
            volume: self.volume.clone(),
            name: self.name.clone(),
            version_id: self.version_id,
            deleted: self.deleted,
            erasure: self.erasure.clone(),
            ..Default::default()
        }
    }

    /// Check if this FileInfo equals another FileInfo
    pub fn equals(&self, other: &FileInfo) -> bool {
        // Check if both are compressed or both are not compressed
        if self.is_compressed() != other.is_compressed() {
            tracing::warn!("equals: is_compressed is not equal, object_name={}", self.name);
            return false;
        }

        // Check transition info
        if !self.transition_info_equals(other) {
            tracing::warn!("equals: transition_info_equals is not equal, object_name={}", self.name);
            return false;
        }

        // Check mod time
        if self.mod_time != other.mod_time {
            tracing::warn!("equals: mod_time is not equal, object_name={}", self.name);
            return false;
        }

        // Check erasure info
        if !self.erasure.equals(&other.erasure) {
            tracing::warn!("equals: erasure is not equal, object_name={}", self.name);
            return false;
        }
        true
    }

    /// Check if transition related information are equal
    pub fn transition_info_equals(&self, other: &FileInfo) -> bool {
        self.transition_status == other.transition_status
            && self.transition_tier == other.transition_tier
            && self.transitioned_objname == other.transitioned_objname
            && self.transition_version_id == other.transition_version_id
    }

    /// Check if metadata maps are equal
    pub fn metadata_equals(&self, other: &FileInfo) -> bool {
        if self.metadata.len() != other.metadata.len() {
            return false;
        }
        for (k, v) in &self.metadata {
            if other.metadata.get(k) != Some(v) {
                return false;
            }
        }
        true
    }

    /// Check if replication related fields are equal
    pub fn replication_info_equals(&self, other: &FileInfo) -> bool {
        self.mark_deleted == other.mark_deleted && self.replication_state_internal == other.replication_state_internal
    }

    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.replication_state_internal
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }
    pub fn replication_status(&self) -> ReplicationStatusType {
        self.replication_state_internal
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }
    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        if self.deleted {
            self.replication_state_internal
                .as_ref()
                .map(|v| v.composite_replication_status())
                .unwrap_or(ReplicationStatusType::Empty)
        } else {
            ReplicationStatusType::Empty
        }
    }

    pub fn shard_file_size(&self, total_length: i64) -> i64 {
        self.erasure.shard_file_size(total_length)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileInfoVersions {
    // Name of the volume.
    pub volume: String,

    // Name of the file.
    pub name: String,

    // Represents the latest mod time of the
    // latest version.
    pub latest_mod_time: Option<OffsetDateTime>,

    pub versions: Vec<FileInfo>,
    pub free_versions: Vec<FileInfo>,
}

impl FileInfoVersions {
    pub fn find_version_index(&self, vid: Uuid) -> Option<usize> {
        self.versions.iter().position(|v| v.version_id == Some(vid))
    }

    /// Calculate the total size of all versions for this object
    pub fn size(&self) -> i64 {
        self.versions.iter().map(|v| v.size).sum()
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct RawFileInfo {
    pub buf: Vec<u8>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FilesInfo {
    pub files: Vec<FileInfo>,
    pub is_truncated: bool,
}

pub trait RestoreStatusOps {
    fn expiry(&self) -> Option<OffsetDateTime>;
    fn on_going(&self) -> bool;
    fn on_disk(&self) -> bool;
    fn to_string(&self) -> String;
    fn to_string2(&self) -> String;
}

impl RestoreStatusOps for RestoreStatus {
    fn expiry(&self) -> Option<OffsetDateTime> {
        if self.on_going() {
            return None;
        }
        self.restore_expiry_date.clone().map(OffsetDateTime::from)
    }

    fn on_going(&self) -> bool {
        if let Some(on_going) = self.is_restore_in_progress {
            return on_going;
        }
        false
    }

    fn on_disk(&self) -> bool {
        let expiry = self.expiry();
        if let Some(expiry0) = expiry
            && OffsetDateTime::now_utc().unix_timestamp() < expiry0.unix_timestamp()
        {
            return true;
        }
        false
    }

    fn to_string(&self) -> String {
        if self.on_going() {
            return "ongoing-request=\"true\"".to_string();
        }
        format!(
            "ongoing-request=\"false\", expiry-date=\"{}\"",
            OffsetDateTime::from(self.restore_expiry_date.clone().unwrap())
                .format(&Rfc3339)
                .unwrap()
        )
    }

    fn to_string2(&self) -> String {
        if self.on_going() {
            return "ongoing-request=\"true\"".to_string();
        }
        format!(
            "ongoing-request=\"false\", expiry-date=\"{}\"",
            OffsetDateTime::from(self.restore_expiry_date.clone().unwrap())
                .format(&RFC1123)
                .unwrap()
        )
    }
}

pub fn parse_restore_obj_status(restore_hdr: &str) -> Result<RestoreStatus> {
    let tokens: Vec<&str> = restore_hdr.splitn(2, ",").collect();
    let progress_tokens: Vec<&str> = tokens[0].splitn(2, "=").collect();
    if progress_tokens.len() != 2 {
        return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
    }
    if progress_tokens[0].trim() != "ongoing-request" {
        return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
    }

    match progress_tokens[1] {
        "true" | "\"true\"" if tokens.len() == 1 => {
            return Ok(RestoreStatus {
                is_restore_in_progress: Some(true),
                ..Default::default()
            });
        }
        "false" | "\"false\"" => {
            if tokens.len() != 2 {
                return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
            }
            let expiry_tokens: Vec<&str> = tokens[1].splitn(2, "=").collect();
            if expiry_tokens.len() != 2 {
                return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
            }
            if expiry_tokens[0].trim() != "expiry-date" {
                return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
            }
            let expiry = OffsetDateTime::parse(expiry_tokens[1].trim_matches('"'), &Rfc3339)
                .map_err(|_| Error::other(ERR_RESTORE_HDR_MALFORMED))?;
            return Ok(RestoreStatus {
                is_restore_in_progress: Some(false),
                restore_expiry_date: Some(Timestamp::from(expiry)),
            });
        }
        _ => (),
    }
    Err(Error::other(ERR_RESTORE_HDR_MALFORMED))
}

pub fn is_restored_object_on_disk(meta: &HashMap<String, String>) -> bool {
    if let Some(restore_hdr) = meta.get(X_AMZ_RESTORE.as_str())
        && let Ok(restore_status) = parse_restore_obj_status(restore_hdr)
    {
        return restore_status.on_disk();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::{hash_map, vec};
    use proptest::prelude::*;

    // backlog#949: distribution range/permutation validation.
    #[test]
    fn is_valid_distribution_accepts_permutation() {
        assert!(is_valid_distribution(&[1, 2, 3, 4], 4));
        assert!(is_valid_distribution(&[4, 2, 1, 3], 4));
        assert!(is_valid_distribution(&[1], 1));
    }

    #[test]
    fn is_valid_distribution_rejects_zero_value() {
        // A `0` would underflow `block_idx - 1` in the shuffle helpers.
        assert!(!is_valid_distribution(&[0, 2, 3, 4], 4));
    }

    #[test]
    fn is_valid_distribution_rejects_out_of_range_value() {
        // A value greater than N would index out of bounds in the shuffle helpers.
        assert!(!is_valid_distribution(&[1, 2, 3, 5], 4));
        assert!(!is_valid_distribution(&[usize::MAX, 2, 3, 4], 4));
    }

    #[test]
    fn is_valid_distribution_rejects_duplicates() {
        // In-range but not a permutation.
        assert!(!is_valid_distribution(&[1, 1, 3, 4], 4));
    }

    #[test]
    fn is_valid_distribution_rejects_wrong_length_and_empty() {
        assert!(!is_valid_distribution(&[1, 2, 3], 4));
        assert!(!is_valid_distribution(&[1, 2, 3, 4, 4], 4));
        assert!(!is_valid_distribution(&[], 0));
        assert!(!is_valid_distribution(&[], 4));
    }

    fn distribution_test_fileinfo() -> FileInfo {
        // data=2, parity=2 => N=4, distribution is a valid permutation of 1..=4.
        let mut fi = FileInfo::new("bucket/object", 2, 2);
        fi.erasure.index = 1;
        fi
    }

    #[test]
    fn is_valid_accepts_well_formed_distribution() {
        assert!(distribution_test_fileinfo().is_valid());
    }

    #[test]
    fn is_valid_rejects_corrupt_distribution_values() {
        // Zero value.
        let mut fi = distribution_test_fileinfo();
        fi.erasure.distribution = vec![0, 2, 3, 4];
        assert!(!fi.is_valid());

        // Out-of-range value.
        let mut fi = distribution_test_fileinfo();
        fi.erasure.distribution = vec![1, 2, 3, 9];
        assert!(!fi.is_valid());

        // Duplicate value (not a permutation).
        let mut fi = distribution_test_fileinfo();
        fi.erasure.distribution = vec![1, 1, 3, 4];
        assert!(!fi.is_valid());

        // Wrong length.
        let mut fi = distribution_test_fileinfo();
        fi.erasure.distribution = vec![1, 2, 3];
        assert!(!fi.is_valid());
    }

    fn small_string_strategy() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[A-Za-z0-9._/-]{0,16}").expect("small string regex should compile")
    }

    fn small_required_string_strategy() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[A-Za-z0-9._/-]{1,16}").expect("required string regex should compile")
    }

    fn bytes_strategy(max_len: usize) -> impl Strategy<Value = Bytes> {
        vec(any::<u8>(), 0..=max_len).prop_map(Bytes::from)
    }

    fn uuid_strategy() -> impl Strategy<Value = Uuid> {
        any::<[u8; 16]>().prop_map(Uuid::from_bytes)
    }

    fn optional_uuid_strategy() -> impl Strategy<Value = Option<Uuid>> {
        proptest::option::of(uuid_strategy())
    }

    fn timestamp_strategy() -> impl Strategy<Value = OffsetDateTime> {
        (0i64..=4_102_444_800i64)
            .prop_map(|seconds| OffsetDateTime::from_unix_timestamp(seconds).expect("bounded timestamp should be valid"))
    }

    fn optional_timestamp_strategy() -> impl Strategy<Value = Option<OffsetDateTime>> {
        proptest::option::of(timestamp_strategy())
    }

    fn hash_algorithm_strategy() -> impl Strategy<Value = HashAlgorithm> {
        prop_oneof![
            Just(HashAlgorithm::SHA256),
            Just(HashAlgorithm::HighwayHash256),
            Just(HashAlgorithm::HighwayHash256S),
            Just(HashAlgorithm::HighwayHash256SLegacy),
            Just(HashAlgorithm::BLAKE2b512),
            Just(HashAlgorithm::Md5),
            Just(HashAlgorithm::None),
        ]
    }

    fn checksum_info_strategy() -> impl Strategy<Value = ChecksumInfo> {
        (0usize..=4, hash_algorithm_strategy(), bytes_strategy(32)).prop_map(|(part_number, algorithm, hash)| ChecksumInfo {
            part_number,
            algorithm,
            hash,
        })
    }

    fn erasure_info_strategy() -> impl Strategy<Value = ErasureInfo> {
        (1usize..=4, 0usize..=3, 2usize..=4096)
            .prop_flat_map(|(data_blocks, parity_blocks, block_size)| {
                let total_blocks = data_blocks + parity_blocks;
                (
                    Just(data_blocks),
                    Just(parity_blocks),
                    Just(block_size),
                    0usize..=total_blocks,
                    vec(1usize..=(total_blocks.max(1)), total_blocks),
                    vec(checksum_info_strategy(), 0..=3),
                    small_required_string_strategy(),
                )
            })
            .prop_map(
                |(data_blocks, parity_blocks, block_size, index, distribution, checksums, algorithm)| ErasureInfo {
                    algorithm,
                    data_blocks,
                    parity_blocks,
                    block_size,
                    index,
                    distribution,
                    checksums,
                },
            )
    }

    fn object_part_info_strategy() -> impl Strategy<Value = ObjectPartInfo> {
        (
            small_string_strategy(),
            0usize..=8,
            0usize..=4096,
            -1_000_000i64..=1_000_000i64,
            optional_timestamp_strategy(),
            proptest::option::of(bytes_strategy(16)),
            proptest::option::of(hash_map(small_string_strategy(), small_string_strategy(), 0..=3)),
            proptest::option::of(small_string_strategy()),
        )
            .prop_map(|(etag, number, size, actual_size, mod_time, index, checksums, error)| ObjectPartInfo {
                etag,
                number,
                size,
                actual_size,
                mod_time,
                index,
                checksums,
                error,
            })
    }

    fn file_info_strategy() -> impl Strategy<Value = FileInfo> {
        (
            (
                small_string_strategy(),
                small_string_strategy(),
                optional_uuid_strategy(),
                any::<bool>(),
                any::<bool>(),
                small_string_strategy(),
                small_string_strategy(),
                small_string_strategy(),
                optional_uuid_strategy(),
                any::<bool>(),
            ),
            (
                optional_uuid_strategy(),
                optional_timestamp_strategy(),
                -1_000_000i64..=1_000_000i64,
                proptest::option::of(any::<u32>()),
                proptest::option::of(any::<u64>()),
                hash_map(small_string_strategy(), small_string_strategy(), 0..=4),
                vec(object_part_info_strategy(), 0..=3),
                erasure_info_strategy(),
                any::<bool>(),
                proptest::option::of(bytes_strategy(32)),
            ),
            (
                0usize..=4,
                optional_timestamp_strategy(),
                any::<bool>(),
                0usize..=4,
                proptest::option::of(bytes_strategy(32)),
                any::<bool>(),
                any::<bool>(),
            ),
        )
            .prop_map(|(head, middle, tail)| {
                let (
                    volume,
                    name,
                    version_id,
                    is_latest,
                    deleted,
                    transition_status,
                    transitioned_objname,
                    transition_tier,
                    transition_version_id,
                    expire_restored,
                ) = head;
                let (data_dir, mod_time, size, mode, written_by_version, metadata, parts, erasure, mark_deleted, data) = middle;
                let (num_versions, successor_mod_time, fresh, idx, checksum, versioned, uses_legacy_checksum) = tail;

                FileInfo {
                    volume,
                    name,
                    version_id,
                    is_latest,
                    deleted,
                    transition_status,
                    transitioned_objname,
                    transition_tier,
                    transition_version_id,
                    expire_restored,
                    data_dir,
                    mod_time,
                    size,
                    mode,
                    written_by_version,
                    metadata,
                    parts,
                    erasure,
                    mark_deleted,
                    replication_state_internal: None,
                    data,
                    num_versions,
                    successor_mod_time,
                    fresh,
                    idx,
                    checksum,
                    versioned,
                    uses_legacy_checksum,
                }
            })
    }

    proptest! {
        #[test]
        fn fileinfo_msgpack_round_trips(value in file_info_strategy()) {
            let encoded = value
                .marshal_msg()
                .expect("FileInfo should serialize in the property roundtrip test");
            let decoded = FileInfo::unmarshal(&encoded)
                .expect("FileInfo should deserialize in the property roundtrip test");

            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn fileinfo_unmarshal_never_panics(input in vec(any::<u8>(), 0..=1024)) {
            let result = std::panic::catch_unwind(|| FileInfo::unmarshal(&input));
            prop_assert!(result.is_ok(), "FileInfo::unmarshal panicked for arbitrary input");
        }
    }

    #[test]
    fn replication_info_equals_compares_mark_deleted_and_replication_state() {
        let base = FileInfo::default();

        // Identical (both default) infos are equal.
        assert!(base.replication_info_equals(&FileInfo::default()));

        // Differing mark_deleted breaks equality.
        let marked = FileInfo {
            mark_deleted: true,
            ..Default::default()
        };
        assert!(!base.replication_info_equals(&marked));

        // Differing replication_state_internal breaks equality (regression guard:
        // this field used to be ignored by replication_info_equals).
        let with_state = FileInfo {
            replication_state_internal: Some(ReplicationState {
                replicate_decision_str: "arn:aws:s3:::dest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!base.replication_info_equals(&with_state));

        // Equal replication states remain equal.
        let with_state_clone = FileInfo {
            replication_state_internal: with_state.replication_state_internal.clone(),
            ..Default::default()
        };
        assert!(with_state.replication_info_equals(&with_state_clone));
    }
}
