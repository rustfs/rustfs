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
    AMZ_OBJECT_TAGGING, SUFFIX_COMPRESSION, SUFFIX_DATA_MOVED, SUFFIX_DATA_MOVED_TAGS, SUFFIX_FREE_VERSION, SUFFIX_HEALING,
    SUFFIX_INLINE_DATA, SUFFIX_OBJECT_TRANSACTION_EPOCH, SUFFIX_TIER_FV_ID, SUFFIX_TIER_FV_MARKER, SUFFIX_TIER_SKIP_FV_ID,
    contains_key_str, get_consistent_str, get_str, has_internal_suffix, insert_str, is_encryption_metadata_key,
    starts_with_ignore_ascii_case,
};
use s3s::dto::{RestoreStatus, Timestamp};
use s3s::header::X_AMZ_RESTORE;
use serde::de::{self, MapAccess, SeqAccess, Visitor, value::MapAccessDeserializer};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use time::{format_description::FormatItem, macros::format_description};
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1024 * 1024; // 1M

const MAX_ERASURE_SHARDS: usize = 16;
const MAX_FILEINFO_PARTS: usize = 10_000;
const MAX_FILEINFO_CHECKSUMS: usize = 10_000;
const FILEINFO_PART_BITMAP_WORD_BITS: usize = std::mem::size_of::<u64>() * 8;
const FILEINFO_PART_BITMAP_WORDS: usize = MAX_FILEINFO_PARTS.div_ceil(FILEINFO_PART_BITMAP_WORD_BITS);

// Additional constants from Go version
// Intentionally duplicated (S3 wire literal): rustfs-replication and
// rustfs-object-data-cache carry their own independent "null" constants so
// they stay free of a rustfs-filemeta dependency. Keep all three in sync.
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

fn checked_calc_shard_size(block_size: usize, data_shards: usize) -> Option<usize> {
    if data_shards == 0 {
        return None;
    }

    block_size.div_ceil(data_shards).checked_add(1).map(|size| size & !1)
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
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TransitionVersionState {
    #[default]
    Unknown,
    KnownDisabled,
    SuspendedNull,
    Exact,
}

impl TransitionVersionState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::KnownDisabled => "known-disabled",
            Self::SuspendedNull => "suspended-null",
            Self::Exact => "exact",
        }
    }
}

#[derive(PartialEq, Clone, Default)]
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
    pub transition_version: Option<String>,
    pub transition_version_state: TransitionVersionState,
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

/// Metadata keys whose values carry sealed encryption material (KEK-wrapped DEK,
/// IV) under either the `x-rustfs-internal-` or `x-minio-internal-` prefix.
/// Values of these keys must never reach logs at any level.
fn is_sensitive_metadata_key(key: &str) -> bool {
    // `is_encryption_metadata_key` covers the x-minio-internal- SSE prefix but not
    // its reserved x-rustfs-internal- twin, which has no writer today but must
    // stay redacted in case one appears.
    is_encryption_metadata_key(key)
        || starts_with_ignore_ascii_case(key, rustfs_utils::http::RUSTFS_INTERNAL_ENCRYPTION_PREFIX)
        || rustfs_utils::http::REPLICATION_SSE_TRANSPORT_PREFIXES
            .iter()
            .any(|prefix| starts_with_ignore_ascii_case(key, prefix))
}

struct RedactedMetadata<'a>(&'a HashMap<String, String>);

impl std::fmt::Debug for RedactedMetadata<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.0 {
            if is_sensitive_metadata_key(key) {
                map.entry(key, &format_args!("<redacted {} bytes>", value.len()));
            } else {
                map.entry(key, value);
            }
        }
        map.finish()
    }
}

struct ElidedBytes<'a>(&'a Option<Bytes>);

impl std::fmt::Debug for ElidedBytes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(bytes) => write!(f, "Some(<{} bytes elided>)", bytes.len()),
            None => f.write_str("None"),
        }
    }
}

// Manual Debug: `data` holds full inline object bytes (plaintext user content for
// non-SSE objects) and `metadata` holds sealed key material — both must stay out
// of Debug output so whole-struct log dumps cannot leak them. `checksum` is elided
// too: for non-SSE objects it fingerprints plaintext content, and its raw bytes
// carry no diagnostic value. The exhaustive destructuring (no `..`) forces every
// future field through an explicit show/redact decision here.
impl std::fmt::Debug for FileInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            volume,
            name,
            version_id,
            is_latest,
            deleted,
            transition_status,
            transitioned_objname,
            transition_tier,
            transition_version_id,
            transition_version,
            transition_version_state,
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
            replication_state_internal,
            data,
            num_versions,
            successor_mod_time,
            fresh,
            idx,
            checksum,
            versioned,
            uses_legacy_checksum,
        } = self;
        f.debug_struct("FileInfo")
            .field("volume", volume)
            .field("name", name)
            .field("version_id", version_id)
            .field("is_latest", is_latest)
            .field("deleted", deleted)
            .field("transition_status", transition_status)
            .field("transitioned_objname", transitioned_objname)
            .field("transition_tier", transition_tier)
            .field("transition_version_id", transition_version_id)
            .field("transition_version", transition_version)
            .field("transition_version_state", transition_version_state)
            .field("expire_restored", expire_restored)
            .field("data_dir", data_dir)
            .field("mod_time", mod_time)
            .field("size", size)
            .field("mode", mode)
            .field("written_by_version", written_by_version)
            .field("metadata", &RedactedMetadata(metadata))
            .field("parts", parts)
            .field("erasure", erasure)
            .field("mark_deleted", mark_deleted)
            .field("replication_state_internal", replication_state_internal)
            .field("data", &ElidedBytes(data))
            .field("num_versions", num_versions)
            .field("successor_mod_time", successor_mod_time)
            .field("fresh", fresh)
            .field("idx", idx)
            .field("checksum", &ElidedBytes(checksum))
            .field("versioned", versioned)
            .field("uses_legacy_checksum", uses_legacy_checksum)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(remote = "FileInfo")]
struct FileInfoMapDef {
    volume: String,
    name: String,
    version_id: Option<Uuid>,
    is_latest: bool,
    deleted: bool,
    transition_status: String,
    transitioned_objname: String,
    transition_tier: String,
    transition_version_id: Option<Uuid>,
    #[serde(default)]
    transition_version: Option<String>,
    #[serde(default)]
    transition_version_state: TransitionVersionState,
    expire_restored: bool,
    data_dir: Option<Uuid>,
    mod_time: Option<OffsetDateTime>,
    size: i64,
    mode: Option<u32>,
    written_by_version: Option<u64>,
    metadata: HashMap<String, String>,
    parts: Vec<ObjectPartInfo>,
    erasure: ErasureInfo,
    mark_deleted: bool,
    replication_state_internal: Option<ReplicationState>,
    data: Option<Bytes>,
    num_versions: usize,
    successor_mod_time: Option<OffsetDateTime>,
    fresh: bool,
    idx: usize,
    checksum: Option<Bytes>,
    versioned: bool,
    uses_legacy_checksum: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TransitionVersionOrExpireRestored {
    TransitionVersion(Option<String>),
    ExpireRestored(bool),
}

const FILE_INFO_FIELDS: &[&str] = &[
    "volume",
    "name",
    "version_id",
    "is_latest",
    "deleted",
    "transition_status",
    "transitioned_objname",
    "transition_tier",
    "transition_version_id",
    "transition_version",
    "transition_version_state",
    "expire_restored",
    "data_dir",
    "mod_time",
    "size",
    "mode",
    "written_by_version",
    "metadata",
    "parts",
    "erasure",
    "mark_deleted",
    "replication_state_internal",
    "data",
    "num_versions",
    "successor_mod_time",
    "fresh",
    "idx",
    "checksum",
    "versioned",
    "uses_legacy_checksum",
];

impl Serialize for FileInfo {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(FILE_INFO_FIELDS.len()))?;
        map.serialize_entry("volume", &self.volume)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("version_id", &self.version_id)?;
        map.serialize_entry("is_latest", &self.is_latest)?;
        map.serialize_entry("deleted", &self.deleted)?;
        map.serialize_entry("transition_status", &self.transition_status)?;
        map.serialize_entry("transitioned_objname", &self.transitioned_objname)?;
        map.serialize_entry("transition_tier", &self.transition_tier)?;
        map.serialize_entry("transition_version_id", &self.transition_version_id)?;
        map.serialize_entry("transition_version", &self.transition_version)?;
        map.serialize_entry("transition_version_state", &self.transition_version_state)?;
        map.serialize_entry("expire_restored", &self.expire_restored)?;
        map.serialize_entry("data_dir", &self.data_dir)?;
        map.serialize_entry("mod_time", &self.mod_time)?;
        map.serialize_entry("size", &self.size)?;
        map.serialize_entry("mode", &self.mode)?;
        map.serialize_entry("written_by_version", &self.written_by_version)?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.serialize_entry("parts", &self.parts)?;
        map.serialize_entry("erasure", &self.erasure)?;
        map.serialize_entry("mark_deleted", &self.mark_deleted)?;
        map.serialize_entry("replication_state_internal", &self.replication_state_internal)?;
        map.serialize_entry("data", &self.data)?;
        map.serialize_entry("num_versions", &self.num_versions)?;
        map.serialize_entry("successor_mod_time", &self.successor_mod_time)?;
        map.serialize_entry("fresh", &self.fresh)?;
        map.serialize_entry("idx", &self.idx)?;
        map.serialize_entry("checksum", &self.checksum)?;
        map.serialize_entry("versioned", &self.versioned)?;
        map.serialize_entry("uses_legacy_checksum", &self.uses_legacy_checksum)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for FileInfo {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FileInfoVisitor;

        impl<'de> Visitor<'de> for FileInfoVisitor {
            type Value = FileInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a FileInfo map or supported positional array")
            }

            fn visit_map<A>(self, map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                FileInfoMapDef::deserialize(MapAccessDeserializer::new(map))
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                // RUSTFS_COMPAT_TODO(rustfs-5509): beta.11 and beta.12 wrote incompatible positional arrays. Remove after every supported direct-upgrade release writes named maps and retained RPC payloads cannot contain either array.
                let declared_len = seq.size_hint();
                if let Some(len) = declared_len
                    && len != 28
                    && len != 30
                {
                    return Err(de::Error::invalid_length(len, &self));
                }

                macro_rules! next_field {
                    ($field:literal) => {
                        seq.next_element()?
                            .ok_or_else(|| de::Error::missing_field($field))?
                    };
                }

                let volume = next_field!("volume");
                let name = next_field!("name");
                let version_id = next_field!("version_id");
                let is_latest = next_field!("is_latest");
                let deleted = next_field!("deleted");
                let transition_status = next_field!("transition_status");
                let transitioned_objname = next_field!("transitioned_objname");
                let transition_tier = next_field!("transition_tier");
                let transition_version_id = next_field!("transition_version_id");
                let transition_or_expire = next_field!("transition_version or expire_restored");
                let (transition_version, transition_version_state, expire_restored) = match transition_or_expire {
                    TransitionVersionOrExpireRestored::TransitionVersion(transition_version) => (
                        transition_version,
                        next_field!("transition_version_state"),
                        next_field!("expire_restored"),
                    ),
                    TransitionVersionOrExpireRestored::ExpireRestored(expire_restored) => {
                        (None, TransitionVersionState::Unknown, expire_restored)
                    }
                };
                let data_dir = next_field!("data_dir");
                let mod_time = next_field!("mod_time");
                let size = next_field!("size");
                let mode = next_field!("mode");
                let written_by_version = next_field!("written_by_version");
                let metadata = next_field!("metadata");
                let parts = next_field!("parts");
                let erasure = next_field!("erasure");
                let mark_deleted = next_field!("mark_deleted");
                let replication_state_internal = next_field!("replication_state_internal");
                let data = next_field!("data");
                let num_versions = next_field!("num_versions");
                let successor_mod_time = next_field!("successor_mod_time");
                let fresh = next_field!("fresh");
                let idx = next_field!("idx");
                let checksum = next_field!("checksum");
                let versioned = next_field!("versioned");
                let uses_legacy_checksum = next_field!("uses_legacy_checksum");

                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(declared_len.unwrap_or(29), &self));
                }

                Ok(FileInfo {
                    volume,
                    name,
                    version_id,
                    is_latest,
                    deleted,
                    transition_status,
                    transitioned_objname,
                    transition_tier,
                    transition_version_id,
                    transition_version,
                    transition_version_state,
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
                    replication_state_internal,
                    data,
                    num_versions,
                    successor_mod_time,
                    fresh,
                    idx,
                    checksum,
                    versioned,
                    uses_legacy_checksum,
                })
            }
        }

        deserializer.deserialize_struct("FileInfo", FILE_INFO_FIELDS, FileInfoVisitor)
    }
}

/// Selects the validation policy for a trusted operation boundary.
///
/// This mode is deliberately caller-selected and is never inferred from
/// serialized [`FileInfo`] state such as `deleted` or `transition_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    /// Validate the complete storage erasure layout before payload access.
    RequireErasure,
    /// Validate delete metadata without requiring a payload erasure layout.
    DeleteOnly,
}

/// Erasure geometry that passed the storage-layout validation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedErasureLayout {
    data_blocks: usize,
    block_size: usize,
    shard_size: usize,
}

impl ValidatedErasureLayout {
    /// Calculates a shard file size without overflowing the metadata's integer format.
    pub fn shard_file_size(&self, total_length: usize) -> Option<i64> {
        let full_blocks = total_length / self.block_size;
        let last_block_size = total_length % self.block_size;
        let last_shard_size = checked_calc_shard_size(last_block_size, self.data_blocks)?;
        let shard_file_size = full_blocks.checked_mul(self.shard_size)?.checked_add(last_shard_size)?;
        i64::try_from(shard_file_size).ok()
    }
}

fn mark_fileinfo_part(bitmap: &mut [u64; FILEINFO_PART_BITMAP_WORDS], part_number: usize) -> bool {
    let Some(index) = part_number.checked_sub(1).filter(|&index| index < MAX_FILEINFO_PARTS) else {
        return false;
    };
    let word = index / FILEINFO_PART_BITMAP_WORD_BITS;
    let mask = 1u64 << (index % FILEINFO_PART_BITMAP_WORD_BITS);
    let was_absent = bitmap[word] & mask == 0;
    bitmap[word] |= mask;
    was_absent
}

fn has_fileinfo_part(bitmap: &[u64; FILEINFO_PART_BITMAP_WORDS], part_number: usize) -> bool {
    let Some(index) = part_number.checked_sub(1).filter(|&index| index < MAX_FILEINFO_PARTS) else {
        return false;
    };
    bitmap[index / FILEINFO_PART_BITMAP_WORD_BITS] & (1u64 << (index % FILEINFO_PART_BITMAP_WORD_BITS)) != 0
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
    if n == 0 || n > MAX_ERASURE_SHARDS || distribution.len() != n {
        return false;
    }

    let mut seen = [false; MAX_ERASURE_SHARDS];
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

    fn validate_erasure_geometry_with_index(&self, index: usize) -> Result<ValidatedErasureLayout> {
        let erasure = &self.erasure;
        if erasure.data_blocks == 0 || erasure.block_size == 0 || erasure.data_blocks < erasure.parity_blocks {
            return Err(Error::FileCorrupt);
        }

        let total_blocks = erasure
            .data_blocks
            .checked_add(erasure.parity_blocks)
            .ok_or(Error::FileCorrupt)?;
        if total_blocks > MAX_ERASURE_SHARDS
            || index == 0
            || index > total_blocks
            || !is_valid_distribution(&erasure.distribution, total_blocks)
        {
            return Err(Error::FileCorrupt);
        }

        let shard_size = checked_calc_shard_size(erasure.block_size, erasure.data_blocks)
            .filter(|&size| i64::try_from(size).is_ok())
            .ok_or(Error::FileCorrupt)?;
        i64::try_from(erasure.block_size).map_err(|_| Error::FileCorrupt)?;

        let layout = ValidatedErasureLayout {
            data_blocks: erasure.data_blocks,
            block_size: erasure.block_size,
            shard_size,
        };
        let object_size = usize::try_from(self.size).map_err(|_| Error::FileCorrupt)?;
        layout.shard_file_size(object_size).ok_or(Error::FileCorrupt)?;
        Ok(layout)
    }

    fn validate_erasure_geometry(&self) -> Result<ValidatedErasureLayout> {
        self.validate_erasure_geometry_with_index(self.erasure.index)
    }

    fn validate_collection_bounds(&self) -> Result<()> {
        if self.parts.len() > MAX_FILEINFO_PARTS
            || self.erasure.checksums.len() > MAX_FILEINFO_CHECKSUMS
            || self.erasure.distribution.len() > MAX_ERASURE_SHARDS
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    fn validate_collection_contents(&self, layout: Option<&ValidatedErasureLayout>) -> Result<()> {
        if layout.is_some() && self.size > 0 && self.parts.is_empty() {
            return Err(Error::FileCorrupt);
        }

        let mut part_numbers = [0u64; FILEINFO_PART_BITMAP_WORDS];
        for part in &self.parts {
            if let Some(layout) = layout {
                i64::try_from(part.size).map_err(|_| Error::FileCorrupt)?;
                layout.shard_file_size(part.size).ok_or(Error::FileCorrupt)?;

                // A negative `actual_size` is the documented "unknown size" sentinel for
                // compressed streaming objects (see `ObjectPartInfo::actual_size` /
                // `ObjectInfo::get_actual_size`), written to xl.meta by both RustFS and
                // MinIO. Only shard-validate a real, non-negative size; rejecting the
                // sentinel would make legitimate compressed objects unreadable.
                if let Ok(actual_size) = usize::try_from(part.actual_size) {
                    layout.shard_file_size(actual_size).ok_or(Error::FileCorrupt)?;
                }
            }
            if !mark_fileinfo_part(&mut part_numbers, part.number) {
                return Err(Error::FileCorrupt);
            }
        }

        let mut checksum_parts = [0u64; FILEINFO_PART_BITMAP_WORDS];
        for checksum in &self.erasure.checksums {
            if !has_fileinfo_part(&part_numbers, checksum.part_number)
                || !mark_fileinfo_part(&mut checksum_parts, checksum.part_number)
            {
                return Err(Error::FileCorrupt);
            }
        }
        Ok(())
    }

    fn validate_delete_marker_shape(&self, expected_index: usize, allow_nil_version_id: bool) -> Result<()> {
        if !self.deleted {
            return Err(Error::FileCorrupt);
        }
        let erasure = &self.erasure;
        let stored_free_version = contains_key_str(&self.metadata, SUFFIX_FREE_VERSION);
        if self.mod_time.is_none_or(|mod_time| mod_time <= OffsetDateTime::UNIX_EPOCH)
            || (!allow_nil_version_id && self.version_id.is_some_and(|version_id| version_id.is_nil()))
            || self.transition_version_id.is_some_and(|version_id| version_id.is_nil())
            || self
                .transition_version
                .as_ref()
                .is_some_and(|version_id| version_id.is_empty())
            || self.size != 0
            || self.data_dir.is_some()
            || self.mode.is_some()
            || self.written_by_version.is_some()
            || self.data.is_some()
            || self.checksum.is_some()
            || !self.parts.is_empty()
            || !erasure.algorithm.is_empty()
            || erasure.data_blocks != 0
            || erasure.parity_blocks != 0
            || erasure.block_size != 0
            || erasure.index != expected_index
            || !erasure.distribution.is_empty()
            || !erasure.checksums.is_empty()
            || stored_free_version != self.tier_free_version()
            || (stored_free_version && (self.transition_tier.is_empty() || self.transitioned_objname.is_empty()))
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    fn validate_tier_free_version_delete_shape(&self) -> Result<()> {
        let erasure = &self.erasure;
        if !self.deleted
            || !self.tier_free_version()
            || self.version_id.is_none_or(|version_id| version_id.is_nil())
            || self.mod_time.is_some()
            || self.is_latest
            || !self.transition_status.is_empty()
            || !self.transitioned_objname.is_empty()
            || !self.transition_tier.is_empty()
            || self.transition_version_id.is_some()
            || self.transition_version.is_some()
            || self.expire_restored
            || self.size != 0
            || self.data_dir.is_some()
            || self.mode.is_some()
            || self.written_by_version.is_some()
            || self.mark_deleted
            || self.replication_state_internal.is_some()
            || self.data.is_some()
            || self.num_versions != 0
            || self.successor_mod_time.is_some()
            || self.fresh
            || self.idx != 0
            || self.checksum.is_some()
            || self.versioned
            || self.uses_legacy_checksum
            || !self.parts.is_empty()
            || self.metadata.is_empty()
            || self.metadata.len() > 2
            || self
                .metadata
                .iter()
                .any(|(key, value)| !has_internal_suffix(key, SUFFIX_TIER_FV_MARKER) || !value.is_empty())
            || !erasure.algorithm.is_empty()
            || erasure.data_blocks != 0
            || erasure.parity_blocks != 0
            || erasure.block_size != 0
            || erasure.index != 0
            || !erasure.distribution.is_empty()
            || !erasure.checksums.is_empty()
        {
            return Err(Error::FileCorrupt);
        }
        Ok(())
    }

    /// Validates this metadata for the caller-selected operation boundary and
    /// returns the payload erasure layout when one is established.
    ///
    /// All modes enforce collection bounds and checksum-to-part associations.
    /// Only [`ValidationMode::RequireErasure`] establishes a payload erasure
    /// layout; the other modes cannot be upgraded based on serialized flags and
    /// return `None`.
    pub fn validate(&self, mode: ValidationMode) -> Result<Option<ValidatedErasureLayout>> {
        self.validate_collection_bounds()?;
        if let (Some(version), Some(version_id)) = (&self.transition_version, self.transition_version_id)
            && Uuid::parse_str(version).ok() != Some(version_id)
        {
            return Err(Error::FileCorrupt);
        }
        let transition_state_valid = match self.transition_version_state {
            TransitionVersionState::Unknown => true,
            TransitionVersionState::KnownDisabled => self.transition_version.is_none() && self.transition_version_id.is_none(),
            TransitionVersionState::SuspendedNull => {
                self.transition_version.as_deref() == Some("null") && self.transition_version_id.is_none()
            }
            TransitionVersionState::Exact => self
                .transition_version
                .as_deref()
                .is_some_and(|version| version != "null" && !version.is_empty()),
        };
        if !transition_state_valid {
            return Err(Error::FileCorrupt);
        }

        let erasure_layout = match mode {
            ValidationMode::RequireErasure => Some(self.validate_erasure_geometry()?),
            ValidationMode::DeleteOnly => {
                self.validate_delete_marker_shape(0, false)?;
                None
            }
        };
        self.validate_collection_contents(erasure_layout.as_ref())?;

        Ok(erasure_layout)
    }

    /// Validate metadata returned by a disk or peer as a strict tagged union.
    /// Payload entries require complete erasure geometry, including
    /// purge-pending object versions whose replication state sets `deleted`.
    /// Only entries that are not valid payloads may fall back to the canonical
    /// delete-marker shape, so `deleted` cannot bypass payload validation.
    pub fn validate_for_metadata_read(&self) -> Result<()> {
        if self.validate(ValidationMode::RequireErasure).is_ok() {
            return Ok(());
        }

        self.validate(ValidationMode::DeleteOnly).map(|_| ())
    }

    /// Cheap shape check for metadata that already passed
    /// [`Self::validate_for_metadata_read`] at its decode boundary.
    pub fn has_valid_metadata_shape(&self) -> bool {
        self.has_valid_erasure_geometry() || self.is_canonical_delete_marker()
    }

    /// Returns whether this is a canonical delete marker rather than an
    /// erasure-backed object version in a purge-pending state.
    pub fn is_canonical_delete_marker(&self) -> bool {
        self.validate_delete_marker_shape(0, false).is_ok()
    }

    /// Storage-only marker classification after an absent version ID has been
    /// normalized to the internal nil UUID. Network and disk boundaries must
    /// continue to use [`Self::is_canonical_delete_marker`].
    pub(crate) fn is_storage_delete_marker(&self) -> bool {
        self.validate_delete_marker_shape(0, true).is_ok()
    }

    /// Whether this is a canonical delete marker carrying only the per-disk
    /// index assigned by an older rename coordinator.
    pub fn is_legacy_indexed_delete_marker(&self) -> bool {
        self.erasure.index > 0
            && self.erasure.index <= MAX_ERASURE_SHARDS
            && self.validate_delete_marker_shape(self.erasure.index, false).is_ok()
    }

    /// Validates complete metadata before a write fanout assigns its per-disk
    /// shard index. An index of zero is treated only as the pending assignment;
    /// every other geometry and collection invariant remains mandatory.
    pub fn validate_for_erasure_write(&self) -> Result<()> {
        self.validate_collection_bounds()?;
        let index = if self.erasure.index == 0 { 1 } else { self.erasure.index };
        let layout = self.validate_erasure_geometry_with_index(index)?;
        self.validate_collection_contents(Some(&layout))
    }

    /// Validates metadata used to mutate a version list during a delete.
    /// Delete-marker creation requires the canonical marker shape; other
    /// delete operations do not access payload shards but still validate all
    /// collection bounds and checksum-to-part associations before mutation.
    pub fn validate_for_delete_operation(&self) -> Result<()> {
        self.validate_collection_bounds()?;

        if self.deleted && self.tier_free_version() {
            return self.validate_tier_free_version_delete_shape();
        }

        if self.deleted {
            return self.validate(ValidationMode::DeleteOnly).map(|_| ());
        }

        self.validate_collection_contents(None)
    }

    /// Performs the cheap layout check used by quorum selection after a decode
    /// boundary has already called [`Self::validate`].
    pub fn has_valid_erasure_geometry(&self) -> bool {
        self.validate_erasure_geometry().is_ok()
    }

    /// Validates the complete payload metadata shape.
    ///
    /// Repeated quorum passes over metadata that was already fully validated
    /// should use [`Self::has_valid_erasure_geometry`] instead.
    pub fn is_valid(&self) -> bool {
        self.validate(ValidationMode::RequireErasure).is_ok()
    }

    pub fn get_etag(&self) -> Option<String> {
        self.metadata.get("etag").cloned()
    }

    pub fn write_quorum(&self, quorum: usize) -> usize {
        if self.deleted && !self.has_valid_erasure_geometry() {
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
        if self.size > 0 && self.parts.is_empty() {
            return Err(Error::FileCorrupt);
        }

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

    /// Reader for the marker [`Self::set_healing`] writes: true when this
    /// FileInfo is being committed by the heal path.
    pub fn is_healing(&self) -> bool {
        contains_key_str(&self.metadata, SUFFIX_HEALING)
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
        let tags_proof = format!("v1:{}", self.metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str).unwrap_or_default());
        insert_str(&mut self.metadata, SUFFIX_DATA_MOVED_TAGS, tags_proof);
        insert_str(&mut self.metadata, SUFFIX_DATA_MOVED, "true".to_string());
    }

    pub fn acknowledge_data_movement(&mut self) {
        // Keep both empty aliases so mixed-version disks retain one metadata identity.
        insert_str(&mut self.metadata, SUFFIX_DATA_MOVED, String::new());
    }

    pub fn set_object_transaction_epoch(&mut self, epoch: Uuid) {
        insert_str(&mut self.metadata, SUFFIX_OBJECT_TRANSACTION_EPOCH, epoch.to_string());
    }

    pub fn object_transaction_epoch(&self) -> Result<Option<Uuid>> {
        if !contains_key_str(&self.metadata, SUFFIX_OBJECT_TRANSACTION_EPOCH) {
            return Ok(None);
        }
        let value = get_consistent_str(&self.metadata, SUFFIX_OBJECT_TRANSACTION_EPOCH).ok_or(Error::FileCorrupt)?;
        let epoch = Uuid::parse_str(value).map_err(|_| Error::FileCorrupt)?;
        if epoch.is_nil() {
            return Err(Error::FileCorrupt);
        }
        Ok(Some(epoch))
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
            && self.transition_version == other.transition_version
            && self.transition_version_state == other.transition_version_state
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

    // backlog#959 / ECA-18: the interleaved per-block bitrot subsystem in
    // rustfs-ecstore (BitrotWriter / bitrot_verify / bitrot_shard_file_size) is
    // only self-consistent for the streaming Highway variants. That safety rests
    // on production always resolving a streaming checksum algorithm, so pin the
    // default here: a part with no explicit ChecksumInfo must resolve to
    // HighwayHash256S. If this default ever changes, the ecstore bitrot layout
    // assumptions must be revisited in lockstep.
    #[test]
    fn get_checksum_info_defaults_to_highwayhash256s() {
        let ei = ErasureInfo::default();
        assert!(ei.checksums.is_empty(), "default ErasureInfo carries no checksums");
        let info = ei.get_checksum_info(1);
        assert_eq!(
            info.algorithm,
            HashAlgorithm::HighwayHash256S,
            "missing ChecksumInfo must default to the streaming HighwayHash256S algorithm"
        );

        // A present entry is returned as-is; the default only applies on miss.
        let ei = ErasureInfo {
            checksums: vec![ChecksumInfo {
                part_number: 2,
                algorithm: HashAlgorithm::SHA256,
                hash: Bytes::new(),
            }],
            ..Default::default()
        };
        assert_eq!(ei.get_checksum_info(2).algorithm, HashAlgorithm::SHA256);
        // A part_number with no entry still falls back to the streaming default.
        assert_eq!(ei.get_checksum_info(99).algorithm, HashAlgorithm::HighwayHash256S);
    }

    #[test]
    fn object_transaction_epoch_uses_consistent_dual_internal_metadata() {
        let mut fi = validation_test_fileinfo();
        assert_eq!(fi.object_transaction_epoch().expect("absent epoch should decode"), None);

        let epoch = Uuid::new_v4();
        let epoch_text = epoch.to_string();
        fi.set_object_transaction_epoch(epoch);
        assert_eq!(fi.object_transaction_epoch().expect("written epoch should decode"), Some(epoch));
        assert_eq!(fi.metadata.get("x-rustfs-internal-object-transaction-epoch"), Some(&epoch_text));
        assert_eq!(fi.metadata.get("x-minio-internal-object-transaction-epoch"), Some(&epoch_text));

        let mut rustfs_only = validation_test_fileinfo();
        rustfs_only
            .metadata
            .insert("x-rustfs-internal-object-transaction-epoch".to_string(), epoch_text);
        assert_eq!(
            rustfs_only
                .object_transaction_epoch()
                .expect("single compatibility key should decode"),
            Some(epoch)
        );

        let mut conflicting = fi.clone();
        conflicting
            .metadata
            .insert("x-minio-internal-object-transaction-epoch".to_string(), Uuid::new_v4().to_string());
        assert_eq!(conflicting.object_transaction_epoch(), Err(Error::FileCorrupt));

        let mut malformed = validation_test_fileinfo();
        malformed
            .metadata
            .insert("x-rustfs-internal-object-transaction-epoch".to_string(), "not-a-uuid".to_string());
        assert_eq!(malformed.object_transaction_epoch(), Err(Error::FileCorrupt));

        let mut nil = validation_test_fileinfo();
        nil.set_object_transaction_epoch(Uuid::nil());
        assert_eq!(nil.object_transaction_epoch(), Err(Error::FileCorrupt));
    }

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

    fn validation_test_fileinfo() -> FileInfo {
        let mut fi = FileInfo::new("bucket/object", 4, 2);
        fi.erasure.index = 1;
        fi.parts = vec![
            ObjectPartInfo {
                number: 1,
                ..Default::default()
            },
            ObjectPartInfo {
                number: 2,
                ..Default::default()
            },
        ];
        fi.erasure.checksums = vec![
            ChecksumInfo {
                part_number: 1,
                algorithm: HashAlgorithm::HighwayHash256S,
                hash: Bytes::from_static(b"checksum-one"),
            },
            ChecksumInfo {
                part_number: 2,
                algorithm: HashAlgorithm::SHA256,
                hash: Bytes::from_static(b"checksum-two"),
            },
        ];
        fi
    }

    fn assert_file_corrupt(fi: &FileInfo, mode: ValidationMode) {
        assert_eq!(fi.validate(mode).expect_err("invalid FileInfo must be rejected"), Error::FileCorrupt);
    }

    #[test]
    fn validate_require_erasure_returns_checked_layout() {
        let fi = validation_test_fileinfo();
        let layout = fi
            .validate(ValidationMode::RequireErasure)
            .expect("well-formed erasure metadata must validate")
            .expect("strict validation must return an erasure layout");
        assert_eq!(layout.data_blocks, 4);
        assert_eq!(layout.block_size, BLOCK_SIZE_V2);
        assert_eq!(layout.shard_size, calc_shard_size(BLOCK_SIZE_V2, 4));
        let shard_size = i64::try_from(layout.shard_size).expect("validated shard size must fit i64");
        assert_eq!(layout.shard_file_size(0), Some(0));
        assert_eq!(layout.shard_file_size(BLOCK_SIZE_V2 - 1), Some(shard_size));
        assert_eq!(layout.shard_file_size(BLOCK_SIZE_V2), Some(shard_size));
        assert_eq!(layout.shard_file_size(BLOCK_SIZE_V2 + 1), Some(shard_size + 2));
    }

    #[test]
    fn validate_require_erasure_accepts_zero_parity() {
        let mut fi = FileInfo::new("bucket/object", 16, 0);
        fi.erasure.index = 1;

        let layout = fi
            .validate(ValidationMode::RequireErasure)
            .expect("zero parity is a valid storage layout");
        assert_eq!(layout.expect("strict layout must be present").data_blocks, 16);
    }

    #[test]
    fn validate_require_erasure_rejects_positive_size_without_parts() {
        let mut fi = FileInfo::new("bucket/object", 4, 2);
        fi.erasure.index = 1;
        fi.size = 1;

        assert_eq!(fi.validate_for_metadata_read(), Err(Error::FileCorrupt));
        assert_eq!(fi.to_part_offset(0), Err(Error::FileCorrupt));

        fi.size = 0;
        fi.validate_for_metadata_read()
            .expect("zero-byte object metadata may omit parts");
        assert_eq!(fi.to_part_offset(0), Ok((0, 0)));
    }

    #[test]
    fn validate_for_erasure_write_only_relaxes_pending_shard_index() {
        let mut fi = validation_test_fileinfo();
        fi.erasure.index = 0;

        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        fi.validate_for_erasure_write()
            .expect("write preparation must accept an index that fanout has not assigned yet");

        fi.erasure.index = fi.erasure.data_blocks + fi.erasure.parity_blocks + 1;
        assert_eq!(fi.validate_for_erasure_write(), Err(Error::FileCorrupt));

        fi.erasure.index = 0;
        fi.parts.push(fi.parts[0].clone());
        assert_eq!(fi.validate_for_erasure_write(), Err(Error::FileCorrupt));
    }

    #[test]
    fn validate_require_erasure_rejects_invalid_geometry() {
        let mut fi = validation_test_fileinfo();
        fi.erasure.data_blocks = 0;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.block_size = 0;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.data_blocks = usize::MAX;
        fi.erasure.parity_blocks = 1;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.data_blocks = 9;
        fi.erasure.parity_blocks = 8;
        fi.erasure.index = 1;
        fi.erasure.distribution = (1..=17).collect();
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.data_blocks = 1;
        fi.erasure.parity_blocks = 2;
        fi.erasure.index = 1;
        fi.erasure.distribution = vec![1, 2, 3];
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.index = 0;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.index = 7;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = validation_test_fileinfo();
        fi.erasure.distribution = vec![1, 2, 3, 4, 5, 5];
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
    }

    fn one_shard_validation_fileinfo(block_size: usize) -> FileInfo {
        let mut fi = FileInfo::new("bucket/object", 1, 0);
        fi.erasure.index = 1;
        fi.erasure.block_size = block_size;
        fi
    }

    #[test]
    fn validate_require_erasure_rejects_unrepresentable_shard_sizes() {
        let fi = one_shard_validation_fileinfo(usize::MAX);
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        if let Some((max_i64, above_i64_max)) = usize::try_from(i64::MAX)
            .ok()
            .and_then(|max_i64| max_i64.checked_add(1).map(|above_i64_max| (max_i64, above_i64_max)))
        {
            let mut fi = FileInfo::new("bucket/object", 2, 0);
            fi.erasure.index = 1;
            fi.erasure.block_size = above_i64_max;
            assert_file_corrupt(&fi, ValidationMode::RequireErasure);

            let mut fi = FileInfo::new("bucket/object", 2, 0);
            fi.erasure.index = 1;
            fi.erasure.block_size = max_i64;
            fi.parts = vec![ObjectPartInfo {
                number: 1,
                size: above_i64_max,
                ..Default::default()
            }];
            assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        }

        let mut fi = one_shard_validation_fileinfo(2);
        fi.size = -1;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = one_shard_validation_fileinfo(2);
        fi.size = i64::MAX;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        let mut fi = one_shard_validation_fileinfo(2);
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            size: usize::MAX,
            ..Default::default()
        }];
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);

        // A negative `actual_size` is the compressed "unknown size" sentinel, not an
        // unrepresentable size: it must stay readable so existing compressed objects
        // remain accessible. Only real (non-negative) sizes are shard-validated.
        let mut fi = one_shard_validation_fileinfo(2);
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            actual_size: -1,
            ..Default::default()
        }];
        fi.validate(ValidationMode::RequireErasure)
            .expect("negative actual_size sentinel (compressed objects) must remain readable");

        let mut fi = one_shard_validation_fileinfo(2);
        fi.parts = vec![ObjectPartInfo {
            number: 1,
            actual_size: i64::MAX,
            ..Default::default()
        }];
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
    }

    #[test]
    fn metadata_read_validation_rejects_flags_that_try_to_relax_payload_validation() {
        let mut fi = FileInfo::new("bucket/legacy", 0, 2);
        fi.erasure.index = 1;
        fi.deleted = true;
        fi.transition_status = TRANSITION_COMPLETE.to_owned();
        fi.transitioned_objname = "remote/object".to_owned();
        fi.transition_tier = "WARM".to_owned();

        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert!(!fi.is_valid(), "wire-controlled state flags must not relax is_valid");
        assert!(matches!(fi.validate_for_metadata_read(), Err(Error::FileCorrupt)));
        assert_file_corrupt(&fi, ValidationMode::DeleteOnly);
    }

    #[test]
    fn metadata_read_validation_rejects_conflicting_transition_versions() {
        let mut fi = one_shard_validation_fileinfo(1);
        fi.transition_version_id = Some(Uuid::new_v4());
        fi.transition_version = Some(Uuid::new_v4().to_string());

        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
    }

    #[test]
    fn metadata_read_validation_requires_canonical_delete_marker_shape() {
        let marker = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        marker
            .validate_for_metadata_read()
            .expect("canonical delete marker should validate");

        let mut missing_time = marker.clone();
        missing_time.mod_time = None;
        assert!(matches!(missing_time.validate_for_metadata_read(), Err(Error::FileCorrupt)));

        let mut epoch_time = marker.clone();
        epoch_time.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
        assert!(matches!(epoch_time.validate_for_metadata_read(), Err(Error::FileCorrupt)));

        let mut disguised_payload = marker.clone();
        disguised_payload.erasure.data_blocks = 1;
        assert!(matches!(disguised_payload.validate_for_metadata_read(), Err(Error::FileCorrupt)));

        let mut marker_with_part = marker;
        marker_with_part.parts.push(ObjectPartInfo {
            number: 1,
            ..Default::default()
        });
        assert!(matches!(marker_with_part.validate_for_metadata_read(), Err(Error::FileCorrupt)));

        let mut purge_pending = validation_test_fileinfo();
        purge_pending.deleted = true;
        purge_pending
            .validate_for_metadata_read()
            .expect("erasure-backed purge-pending objects remain valid payload metadata");
        assert!(!purge_pending.is_canonical_delete_marker());
    }

    #[test]
    fn rename_validation_accepts_only_legacy_assigned_delete_marker_index() {
        let marker = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let mut legacy_marker = marker.clone();
        legacy_marker.erasure.index = 2;
        assert_eq!(legacy_marker.validate_for_metadata_read(), Err(Error::FileCorrupt));
        assert!(legacy_marker.is_legacy_indexed_delete_marker());
        legacy_marker.erasure.index = 0;
        legacy_marker
            .validate_for_metadata_read()
            .expect("normalized legacy marker should pass strict metadata validation");

        let mut malformed_marker = legacy_marker;
        malformed_marker.erasure.index = 2;
        malformed_marker.erasure.data_blocks = 1;
        assert!(!malformed_marker.is_legacy_indexed_delete_marker());
        assert_eq!(malformed_marker.validate_for_metadata_read(), Err(Error::FileCorrupt));

        let mut forged_free_marker = marker.clone();
        insert_str(&mut forged_free_marker.metadata, SUFFIX_FREE_VERSION, String::new());
        assert_eq!(forged_free_marker.validate_for_metadata_read(), Err(Error::FileCorrupt));

        let mut free_marker = marker.clone();
        insert_str(&mut free_marker.metadata, SUFFIX_FREE_VERSION, String::new());
        free_marker.set_tier_free_version();
        free_marker.transition_tier = "WARM".to_string();
        free_marker.transitioned_objname = "remote/object".to_string();
        free_marker
            .validate_for_metadata_read()
            .expect("complete tier free-version marker should remain valid");

        let mut out_of_range_marker = marker;
        out_of_range_marker.erasure.index = MAX_ERASURE_SHARDS + 1;
        assert!(!out_of_range_marker.is_legacy_indexed_delete_marker());
        assert_eq!(out_of_range_marker.validate_for_metadata_read(), Err(Error::FileCorrupt));
    }

    #[test]
    fn delete_operation_accepts_only_the_worker_tier_free_version_request_shape() {
        let mut worker_request = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            ..Default::default()
        };
        worker_request.set_tier_free_version();
        worker_request
            .validate_for_delete_operation()
            .expect("the lifecycle worker request shape must remain valid");

        let mut timestamped = worker_request.clone();
        timestamped.mod_time = Some(OffsetDateTime::now_utc());
        assert_eq!(timestamped.validate_for_delete_operation(), Err(Error::FileCorrupt));

        let mut with_payload = worker_request.clone();
        with_payload.size = 1;
        assert_eq!(with_payload.validate_for_delete_operation(), Err(Error::FileCorrupt));

        let mut with_unrelated_metadata = worker_request;
        with_unrelated_metadata
            .metadata
            .insert("unexpected".to_string(), "value".to_string());
        assert_eq!(with_unrelated_metadata.validate_for_delete_operation(), Err(Error::FileCorrupt));
    }

    #[test]
    fn erasure_and_delete_operation_validation_enforce_basic_collection_bounds() {
        let mut fi = validation_test_fileinfo();
        fi.parts = (1..=10_001)
            .map(|number| ObjectPartInfo {
                number,
                ..Default::default()
            })
            .collect();
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));

        let mut fi = validation_test_fileinfo();
        fi.erasure.checksums = (1..=10_001)
            .map(|part_number| ChecksumInfo {
                part_number,
                ..Default::default()
            })
            .collect();
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));

        let mut fi = validation_test_fileinfo();
        fi.erasure.distribution = vec![1; 17];
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));
    }

    #[test]
    fn validate_accepts_exact_collection_limits() {
        let mut fi = validation_test_fileinfo();
        fi.parts = (1..=10_000)
            .map(|number| ObjectPartInfo {
                number,
                ..Default::default()
            })
            .collect();
        fi.erasure.checksums = (1..=10_000)
            .map(|part_number| ChecksumInfo {
                part_number,
                ..Default::default()
            })
            .collect();

        fi.validate(ValidationMode::RequireErasure)
            .expect("exact collection limits must remain valid");
        fi.validate_for_delete_operation()
            .expect("non-marker delete requests must accept exact collection limits");
    }

    #[test]
    fn erasure_and_delete_operation_validation_reject_checksum_association_errors() {
        let mut fi = validation_test_fileinfo();
        fi.erasure.checksums[0].part_number = 99;
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));

        let mut fi = validation_test_fileinfo();
        let duplicate = fi.erasure.checksums[0].clone();
        fi.erasure.checksums.push(duplicate);
        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));
    }

    #[test]
    fn erasure_and_delete_operation_validation_reject_duplicate_parts() {
        let mut fi = validation_test_fileinfo();
        fi.parts[1].number = fi.parts[0].number;
        fi.erasure.checksums.clear();

        assert_file_corrupt(&fi, ValidationMode::RequireErasure);
        assert_eq!(fi.validate_for_delete_operation(), Err(Error::FileCorrupt));
    }

    #[test]
    fn write_quorum_distinguishes_purge_pending_payload_from_delete_marker() {
        let mut purge_pending = FileInfo::new("bucket/object", 5, 1);
        purge_pending.erasure.index = 1;
        purge_pending.deleted = true;
        assert_eq!(purge_pending.write_quorum(4), 5);

        let marker = FileInfo {
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        assert_eq!(marker.write_quorum(4), 4);

        let malformed_marker = FileInfo {
            deleted: true,
            ..Default::default()
        };
        assert_eq!(malformed_marker.write_quorum(4), 4, "invalid marker metadata must never reduce quorum");
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
                    transition_version: transition_version_id.map(|version_id| version_id.to_string()),
                    transition_version_state: if transition_version_id.is_some() {
                        TransitionVersionState::Exact
                    } else {
                        TransitionVersionState::Unknown
                    },
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

    fn positional_compat_file_info() -> FileInfo {
        FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            transition_version_id: Some(Uuid::from_u128(1)),
            transition_version: Some(Uuid::from_u128(1).to_string()),
            transition_version_state: TransitionVersionState::Exact,
            expire_restored: true,
            size: -1,
            fresh: true,
            idx: 7,
            versioned: true,
            uses_legacy_checksum: true,
            ..Default::default()
        }
    }

    #[derive(Clone, Copy)]
    enum HistoricalFileInfoLayout {
        Beta11,
        Beta12,
    }

    fn encode_historical_file_info(value: &FileInfo, layout: HistoricalFileInfoLayout) -> Vec<u8> {
        let mut encoded = Vec::new();
        let field_count = match layout {
            HistoricalFileInfoLayout::Beta11 => 28,
            HistoricalFileInfoLayout::Beta12 => 30,
        };
        rmp::encode::write_array_len(&mut encoded, field_count).expect("historical FileInfo array header should encode");

        macro_rules! encode_fields {
            ($($field:expr),+ $(,)?) => {
                $($field
                    .serialize(&mut Serializer::new(&mut encoded))
                    .expect("historical FileInfo field should encode");)+
            };
        }

        encode_fields!(
            &value.volume,
            &value.name,
            &value.version_id,
            &value.is_latest,
            &value.deleted,
            &value.transition_status,
            &value.transitioned_objname,
            &value.transition_tier,
            &value.transition_version_id,
        );
        if matches!(layout, HistoricalFileInfoLayout::Beta12) {
            encode_fields!(&value.transition_version, &value.transition_version_state);
        }
        encode_fields!(
            &value.expire_restored,
            &value.data_dir,
            &value.mod_time,
            &value.size,
            &value.mode,
            &value.written_by_version,
            &value.metadata,
            &value.parts,
            &value.erasure,
            &value.mark_deleted,
            &value.replication_state_internal,
            &value.data,
            &value.num_versions,
            &value.successor_mod_time,
            &value.fresh,
            &value.idx,
            &value.checksum,
            &value.versioned,
            &value.uses_legacy_checksum,
        );
        encoded
    }

    fn field_order_file_info() -> FileInfo {
        let transition_version_id = Uuid::from_u128(12);
        FileInfo {
            volume: "volume-sentinel".to_string(),
            name: "name-sentinel".to_string(),
            version_id: Some(Uuid::from_u128(11)),
            is_latest: true,
            deleted: false,
            transition_status: "transition-status".to_string(),
            transitioned_objname: "transitioned-object".to_string(),
            transition_tier: "transition-tier".to_string(),
            transition_version_id: Some(transition_version_id),
            transition_version: Some(transition_version_id.to_string()),
            transition_version_state: TransitionVersionState::Exact,
            expire_restored: true,
            data_dir: Some(Uuid::from_u128(13)),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(14)),
            size: 15,
            mode: Some(16),
            written_by_version: Some(17),
            metadata: [("metadata-key".to_string(), "metadata-value".to_string())]
                .into_iter()
                .collect(),
            parts: vec![ObjectPartInfo {
                etag: "part-etag".to_string(),
                number: 18,
                size: 19,
                actual_size: 20,
                mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(21)),
                index: Some(Bytes::from_static(b"part-index")),
                checksums: Some(
                    [("part-checksum".to_string(), "checksum-value".to_string())]
                        .into_iter()
                        .collect(),
                ),
                error: Some("part-error".to_string()),
            }],
            erasure: ErasureInfo {
                algorithm: "erasure-algorithm".to_string(),
                data_blocks: 2,
                parity_blocks: 1,
                block_size: 1024,
                index: 3,
                distribution: vec![2, 1, 3],
                checksums: vec![ChecksumInfo {
                    part_number: 22,
                    algorithm: HashAlgorithm::SHA256,
                    hash: Bytes::from_static(b"erasure-hash"),
                }],
            },
            mark_deleted: false,
            replication_state_internal: Some(ReplicationState {
                replicate_decision_str: "replication-decision".to_string(),
                delete_marker: true,
                ..Default::default()
            }),
            data: Some(Bytes::from_static(b"inline-data")),
            num_versions: 23,
            successor_mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(24)),
            fresh: true,
            idx: 25,
            checksum: Some(Bytes::from_static(b"combined-checksum")),
            versioned: false,
            uses_legacy_checksum: true,
        }
    }

    const BETA11_FILEINFO_FIXTURE: &[u8] = &[
        220, 0, 28, 166, 98, 117, 99, 107, 101, 116, 166, 111, 98, 106, 101, 99, 116, 192, 194, 194, 160, 160, 160, 196, 16, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 195, 192, 192, 255, 192, 192, 128, 144, 151, 160, 0, 0, 0, 0, 144, 144, 194,
        192, 192, 0, 192, 195, 7, 192, 195, 195,
    ];

    const BETA12_FILEINFO_FIXTURE: &[u8] = &[
        220, 0, 30, 166, 98, 117, 99, 107, 101, 116, 166, 111, 98, 106, 101, 99, 116, 192, 194, 194, 160, 160, 160, 196, 16, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 217, 36, 48, 48, 48, 48, 48, 48, 48, 48, 45, 48, 48, 48, 48, 45, 48, 48, 48,
        48, 45, 48, 48, 48, 48, 45, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 49, 165, 101, 120, 97, 99, 116, 195, 192, 192,
        255, 192, 192, 128, 144, 151, 160, 0, 0, 0, 0, 144, 144, 194, 192, 192, 0, 192, 195, 7, 192, 195, 195,
    ];

    #[test]
    fn fileinfo_decodes_beta11_positional_layout() {
        let expected = positional_compat_file_info();
        let decoded = FileInfo::unmarshal(BETA11_FILEINFO_FIXTURE).expect("beta.11 positional FileInfo should decode");

        assert_eq!(decoded.volume, expected.volume);
        assert_eq!(decoded.expire_restored, expected.expire_restored);
        assert_eq!(decoded.idx, expected.idx);
        assert_eq!(decoded.transition_version, None);
        assert_eq!(decoded.transition_version_state, TransitionVersionState::Unknown);
    }

    #[test]
    fn fileinfo_decodes_beta12_positional_layout() {
        let expected = positional_compat_file_info();
        let decoded = FileInfo::unmarshal(BETA12_FILEINFO_FIXTURE).expect("beta.12 positional FileInfo should decode");

        assert_eq!(decoded, expected);
    }

    #[test]
    fn fileinfo_historical_layouts_preserve_all_field_positions() {
        let expected = field_order_file_info();
        let beta12 = encode_historical_file_info(&expected, HistoricalFileInfoLayout::Beta12);
        assert_eq!(FileInfo::unmarshal(&beta12).expect("beta.12 full-field FileInfo should decode"), expected);

        let beta11 = encode_historical_file_info(&expected, HistoricalFileInfoLayout::Beta11);
        let mut beta11_expected = expected;
        beta11_expected.transition_version = None;
        beta11_expected.transition_version_state = TransitionVersionState::Unknown;
        assert_eq!(
            FileInfo::unmarshal(&beta11).expect("beta.11 full-field FileInfo should decode"),
            beta11_expected
        );
    }

    #[test]
    fn fileinfo_rejects_unsupported_positional_lengths() {
        let mut fields_29 = BETA11_FILEINFO_FIXTURE.to_vec();
        fields_29[2] = 29;
        fields_29.push(0xc0);
        let error = FileInfo::unmarshal(&fields_29).expect_err("29-field FileInfo must fail closed");
        assert!(matches!(error, Error::RmpSerdeDecode(message) if message.contains("invalid length 29")));

        let mut fields_31 = BETA12_FILEINFO_FIXTURE.to_vec();
        fields_31[2] = 31;
        fields_31.push(0xc0);
        let error = FileInfo::unmarshal(&fields_31).expect_err("31-field FileInfo must fail closed");
        assert!(matches!(error, Error::RmpSerdeDecode(message) if message.contains("invalid length 31")));
    }

    #[test]
    fn fileinfo_rejects_truncated_historical_layouts() {
        let beta11 = &BETA11_FILEINFO_FIXTURE[..BETA11_FILEINFO_FIXTURE.len() - 1];
        assert!(matches!(FileInfo::unmarshal(beta11), Err(Error::RmpSerdeDecode(_))));

        let state_offset = BETA12_FILEINFO_FIXTURE
            .windows(6)
            .position(|window| window == [0xa5, b'e', b'x', b'a', b'c', b't'])
            .expect("beta.12 fixture should contain the exact transition state");
        assert!(matches!(
            FileInfo::unmarshal(&BETA12_FILEINFO_FIXTURE[..state_offset]),
            Err(Error::RmpSerdeDecode(_))
        ));
    }

    fn wrap_historical_file_info(fixture: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        rmp::encode::write_array_len(&mut encoded, 5).expect("FileInfoVersions array header should encode");
        "bucket"
            .serialize(&mut Serializer::new(&mut encoded))
            .expect("FileInfoVersions volume should encode");
        "object"
            .serialize(&mut Serializer::new(&mut encoded))
            .expect("FileInfoVersions name should encode");
        Option::<OffsetDateTime>::None
            .serialize(&mut Serializer::new(&mut encoded))
            .expect("FileInfoVersions mod time should encode");
        rmp::encode::write_array_len(&mut encoded, 1).expect("FileInfoVersions versions header should encode");
        encoded.extend_from_slice(fixture);
        rmp::encode::write_array_len(&mut encoded, 0).expect("FileInfoVersions free versions header should encode");
        encoded
    }

    #[test]
    fn fileinfo_versions_decodes_nested_historical_layouts() {
        for fixture in [BETA11_FILEINFO_FIXTURE, BETA12_FILEINFO_FIXTURE] {
            let encoded = wrap_historical_file_info(fixture);
            let decoded: FileInfoVersions =
                rmp_serde::from_slice(&encoded).expect("nested historical FileInfo should decode through FileInfoVersions");
            assert_eq!(decoded.versions.len(), 1);
            assert_eq!(decoded.versions[0].name, "object");
            assert!(decoded.free_versions.is_empty());
        }
    }

    #[derive(Deserialize)]
    struct Beta11MapProbe {
        volume: String,
        expire_restored: bool,
        idx: usize,
    }

    #[derive(Deserialize)]
    struct Beta12MapProbe {
        transition_version: Option<String>,
        transition_version_state: TransitionVersionState,
        expire_restored: bool,
    }

    #[derive(Deserialize)]
    struct Beta11NestedMapProbe {
        file_info: Beta11MapProbe,
    }

    #[derive(Serialize)]
    struct NestedFileInfo<'a> {
        file_info: &'a FileInfo,
    }

    #[test]
    fn fileinfo_serializes_as_map_readable_by_beta11_and_beta12_shapes() {
        let expected = positional_compat_file_info();
        let encoded = expected.marshal_msg().expect("current FileInfo map should encode");
        let mut cursor = encoded.as_slice();
        let field_count = usize::try_from(rmp::decode::read_map_len(&mut cursor).expect("FileInfo should start with a map"))
            .expect("FileInfo map field count should fit usize");
        assert_eq!(field_count, FILE_INFO_FIELDS.len());

        let beta11: Beta11MapProbe = rmp_serde::from_slice(&encoded).expect("beta.11 field shape should read current map");
        assert_eq!(beta11.volume, expected.volume);
        assert_eq!(beta11.expire_restored, expected.expire_restored);
        assert_eq!(beta11.idx, expected.idx);

        let beta12: Beta12MapProbe = rmp_serde::from_slice(&encoded).expect("beta.12 field shape should read current map");
        assert_eq!(beta12.transition_version, expected.transition_version);
        assert_eq!(beta12.transition_version_state, expected.transition_version_state);
        assert_eq!(beta12.expire_restored, expected.expire_restored);

        let nested = NestedFileInfo { file_info: &expected };
        let nested_encoded = rmp_serde::to_vec(&nested).expect("nested FileInfo should encode");
        let nested_beta11: Beta11NestedMapProbe =
            rmp_serde::from_slice(&nested_encoded).expect("beta.11 field shape should read nested current map");
        assert_eq!(nested_beta11.file_info.volume, expected.volume);
        assert_eq!(nested_beta11.file_info.expire_restored, expected.expire_restored);
    }

    #[test]
    fn fileinfo_deserializes_beta11_map_without_transition_fields() {
        let expected = positional_compat_file_info();
        let mut value = serde_json::to_value(&expected).expect("current FileInfo should serialize to JSON");
        let object = value.as_object_mut().expect("FileInfo JSON should be an object");
        object.remove("transition_version");
        object.remove("transition_version_state");

        let decoded: FileInfo = serde_json::from_value(value).expect("beta.11 FileInfo JSON should decode");
        assert_eq!(decoded.volume, expected.volume);
        assert_eq!(decoded.expire_restored, expected.expire_restored);
        assert_eq!(decoded.transition_version, None);
        assert_eq!(decoded.transition_version_state, TransitionVersionState::Unknown);
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

    #[test]
    fn debug_redacts_sealed_encryption_metadata_values() {
        let sealed_key = "IAAfANqt7wIJfVSgFAG3f5S6HuC2eyM5DdJlx7RSJKw2ZakSb3d5";
        let sealed_iv = "0Vr8QLGvQThk8gIWFCUnBOTUwZgs7TTBteRnAK9avD0=";
        let mut fi = FileInfo::default();
        for key in [
            "X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key",
            "X-Minio-Internal-Server-Side-Encryption-Sealed-Key",
        ] {
            fi.metadata.insert(key.to_string(), sealed_key.to_string());
        }
        for key in [
            "X-Rustfs-Internal-Server-Side-Encryption-Iv",
            "X-Minio-Internal-Server-Side-Encryption-Iv",
            "x-rustfs-encryption-iv",
        ] {
            fi.metadata.insert(key.to_string(), sealed_iv.to_string());
        }
        fi.metadata.insert("content-type".to_string(), "text/plain".to_string());

        let dump = format!("{fi:?}");
        assert!(!dump.contains(sealed_key), "sealed key leaked into Debug output: {dump}");
        assert!(!dump.contains(sealed_iv), "sealed IV leaked into Debug output: {dump}");
        // Keys stay visible so operators can still see which metadata is present.
        assert!(dump.contains("X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key"));
        assert!(dump.contains(&format!("<redacted {} bytes>", sealed_key.len())));
        // Non-sensitive metadata values keep their diagnostic value.
        assert!(dump.contains("text/plain"));
    }

    #[test]
    fn debug_redacts_replication_sse_transport_metadata_values() {
        let sealed_key = "IAAfANqt7wIJfVSgFAG3f5S6HuC2eyM5DdJlx7RSJKw2ZakSb3d5";
        let mut fi = FileInfo::default();
        for key in [
            "X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key",
            "X-Rustfs-Replication-Server-Side-Encryption-Iv",
            "X-Rustfs-Replication-Encryption-Iv",
            "X-Rustfs-Replication-Ssec-Key-Md5",
        ] {
            fi.metadata.insert(key.to_string(), sealed_key.to_string());
        }
        fi.metadata.insert("content-type".to_string(), "text/plain".to_string());

        let dump = format!("{fi:?}");
        assert!(
            !dump.contains(sealed_key),
            "replication SSE transport value leaked into Debug output: {dump}"
        );
        assert!(dump.contains("X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key"));
        assert!(dump.contains(&format!("<redacted {} bytes>", sealed_key.len())));
        assert!(dump.contains("text/plain"));
    }

    #[test]
    fn debug_elides_inline_data_bytes() {
        let fi = FileInfo {
            data: Some(Bytes::from_static(b"plaintext user object content")),
            checksum: Some(Bytes::from_static(b"\x01\x02checksumblob")),
            ..Default::default()
        };

        let dump = format!("{fi:?}");
        assert!(
            !dump.contains("plaintext user object content"),
            "inline data leaked into Debug output: {dump}"
        );
        assert!(!dump.contains("checksumblob"), "checksum bytes leaked into Debug output: {dump}");
        assert!(dump.contains("data: Some(<29 bytes elided>)"), "missing data length summary: {dump}");

        let empty = FileInfo::default();
        assert!(format!("{empty:?}").contains("data: None"));
    }
}
