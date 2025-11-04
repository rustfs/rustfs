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
use rustfs_utils::http::headers::{RESERVED_METADATA_PREFIX_LOWER, RUSTFS_HEALING};
use s3s::dto::{RestoreStatus, Timestamp};
use s3s::header::X_AMZ_RESTORE;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
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
        self.algorithm == other.algorithm
            && self.data_blocks == other.data_blocks
            && self.parity_blocks == other.parity_blocks
            && self.block_size == other.block_size
            && self.index == other.index
            && self.distribution == other.distribution
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
}

impl FileInfo {
    pub fn new(object: &str, data_blocks: usize, parity_blocks: usize) -> Self {
        let indices = {
            let cardinality = data_blocks + parity_blocks;
            let mut nums = vec![0; cardinality];
            let key_crc = crc32fast::hash(object.as_bytes());

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
                && self.erasure.distribution.len() == (data_blocks + parity_blocks))
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

        self.parts.sort_by(|a, b| a.number.cmp(&b.number));
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
        self.metadata.insert(RUSTFS_HEALING.to_string(), "true".to_string());
    }

    pub fn set_tier_free_version_id(&mut self, version_id: &str) {
        self.metadata
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_ID}"), version_id.to_string());
    }

    pub fn tier_free_version_id(&self) -> String {
        self.metadata[&format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_ID}")].clone()
    }

    pub fn set_tier_free_version(&mut self) {
        self.metadata
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_MARKER}"), "".to_string());
    }

    pub fn set_skip_tier_free_version(&mut self) {
        self.metadata
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_SKIP_FV_ID}"), "".to_string());
    }

    pub fn skip_tier_free_version(&self) -> bool {
        self.metadata
            .contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_SKIP_FV_ID}"))
    }

    pub fn tier_free_version(&self) -> bool {
        self.metadata
            .contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_MARKER}"))
    }

    pub fn set_inline_data(&mut self) {
        self.metadata
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").to_owned(), "true".to_owned());
    }

    pub fn set_data_moved(&mut self) {
        self.metadata
            .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}data-moved").to_owned(), "true".to_owned());
    }

    pub fn inline_data(&self) -> bool {
        self.metadata
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").as_str())
            && !self.is_remote()
    }

    /// Check if the object is compressed
    pub fn is_compressed(&self) -> bool {
        self.metadata
            .contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"))
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
        self.data_dir.map_or("".to_string(), |dir| dir.to_string())
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
            return false;
        }

        // Check transition info
        if !self.transition_info_equals(other) {
            return false;
        }

        // Check mod time
        if self.mod_time != other.mod_time {
            return false;
        }

        // Check erasure info
        self.erasure.equals(&other.erasure)
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
        self.mark_deleted == other.mark_deleted
        // TODO: Add replication_state comparison when implemented
        // && self.replication_state == other.replication_state
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
}

fn parse_restore_obj_status(restore_hdr: &str) -> Result<RestoreStatus> {
    let tokens: Vec<&str> = restore_hdr.splitn(2, ",").collect();
    let progress_tokens: Vec<&str> = tokens[0].splitn(2, "=").collect();
    if progress_tokens.len() != 2 {
        return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
    }
    if progress_tokens[0].trim() != "ongoing-request" {
        return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
    }

    match progress_tokens[1] {
        "true" | "\"true\"" => {
            if tokens.len() == 1 {
                return Ok(RestoreStatus {
                    is_restore_in_progress: Some(true),
                    ..Default::default()
                });
            }
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
            let expiry = OffsetDateTime::parse(expiry_tokens[1].trim_matches('"'), &Rfc3339).unwrap();
            /*if err != nil {
                return Err(Error::other(ERR_RESTORE_HDR_MALFORMED));
            }*/
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
    if let Some(restore_hdr) = meta.get(X_AMZ_RESTORE.as_str()) {
        if let Ok(restore_status) = parse_restore_obj_status(restore_hdr) {
            return restore_status.on_disk();
        }
    }
    false
}
