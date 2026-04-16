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

//! Version meta parsing with legacy compatibility.
//!
//! **Rule**: To parse version meta bytes (from `FileMetaShallowVersion.meta` or raw `&[u8]`),
//! always use one of:
//! - `FileMetaShallowVersion::parse_version_meta()` or `into_fileinfo()`
//! - `FileMetaVersion::try_from(buf)`
//!
//! Do NOT use `FileMetaVersion::default()` + `unmarshal_msg()` directly, as that fails on
//! legacy (rmp_serde) format. `try_from` falls back to rmp_serde when hand-written decode fails.

use super::msgp_decode::{PrependByteReader, read_nil_or_array_len, read_nil_or_map_len, skip_msgp_value};
use super::*;
use crate::ChecksumInfo;
use rustfs_utils::HashAlgorithm;
use rustfs_utils::http::{
    SUFFIX_CRC, SUFFIX_FREE_VERSION, SUFFIX_INLINE_DATA, SUFFIX_PURGESTATUS, SUFFIX_TIER_FV_ID, SUFFIX_TIER_FV_MARKER,
    SUFFIX_TRANSITION_STATUS, SUFFIX_TRANSITION_TIER, SUFFIX_TRANSITIONED_OBJECTNAME, SUFFIX_TRANSITIONED_VERSION_ID,
    contains_key_bytes, get_bytes, has_internal_suffix, insert_bytes, is_internal_key, remove_bytes, strip_internal_prefix,
};

const MSGPACK_EXT8: u8 = 0xc7;
const MSGPACK_EXT16: u8 = 0xc8;
const MSGPACK_EXT32: u8 = 0xc9;
const MSGPACK_FIXEXT4: u8 = 0xd6;
const MSGPACK_FIXEXT8: u8 = 0xd7;
const MSGPACK_TIME_EXT_LEGACY: i8 = 5;
const MSGPACK_TIME_EXT_OFFICIAL: i8 = -1;

fn read_msgp_string<R: std::io::Read>(rd: &mut R) -> Result<String> {
    let len = rmp::decode::read_str_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn read_msgp_bin<R: std::io::Read>(rd: &mut R) -> Result<Vec<u8>> {
    let len = rmp::decode::read_bin_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(buf)
}

fn deserialize_legacy_uuid_bytes<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct LegacyUuidBytesVisitor;

    impl<'de> serde::de::Visitor<'de> for LegacyUuidBytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("nil or binary UUID bytes")
        }

        fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_vec())
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut value = Vec::new();
            while let Some(byte) = seq.next_element()? {
                value.push(byte);
            }
            Ok(value)
        }
    }

    deserializer.deserialize_any(LegacyUuidBytesVisitor)
}

fn decode_msgp_time_payload(ext_type: i8, payload: &[u8]) -> Result<OffsetDateTime> {
    let (secs, nanos) = match (ext_type, payload.len()) {
        (MSGPACK_TIME_EXT_LEGACY, 12) => {
            let secs = i64::from_be_bytes(payload[..8].try_into().unwrap());
            let nanos = u32::from_be_bytes(payload[8..12].try_into().unwrap());
            (secs, nanos)
        }
        (MSGPACK_TIME_EXT_OFFICIAL, 4) => (u32::from_be_bytes(payload.try_into().unwrap()) as i64, 0),
        (MSGPACK_TIME_EXT_OFFICIAL, 8) => {
            let v = u64::from_be_bytes(payload.try_into().unwrap());
            let nanos = (v >> 34) as u32;
            let secs = (v & ((1 << 34) - 1)) as i64;
            (secs, nanos)
        }
        (MSGPACK_TIME_EXT_OFFICIAL, 12) => {
            let nanos = u32::from_be_bytes(payload[..4].try_into().unwrap());
            let secs = i64::from_be_bytes(payload[4..12].try_into().unwrap());
            (secs, nanos)
        }
        _ => {
            return Err(Error::other(format!(
                "unsupported msgpack time ext type {ext_type} len {}",
                payload.len()
            )));
        }
    };

    if nanos > 999_999_999 {
        return Err(Error::other(format!("invalid msgpack time nanos: {nanos}")));
    }

    OffsetDateTime::from_unix_timestamp_nanos(secs as i128 * 1_000_000_000 + nanos as i128).map_err(Error::from)
}

fn read_msgp_time<R: std::io::Read>(rd: &mut R) -> Result<OffsetDateTime> {
    let mut tag = [0u8; 1];
    rd.read_exact(&mut tag)?;

    let (len, ext_type) = match tag[0] {
        MSGPACK_FIXEXT4 => {
            let mut typ = [0u8; 1];
            rd.read_exact(&mut typ)?;
            (4usize, typ[0] as i8)
        }
        MSGPACK_FIXEXT8 => {
            let mut typ = [0u8; 1];
            rd.read_exact(&mut typ)?;
            (8usize, typ[0] as i8)
        }
        MSGPACK_EXT8 => {
            let mut len = [0u8; 1];
            let mut typ = [0u8; 1];
            rd.read_exact(&mut len)?;
            rd.read_exact(&mut typ)?;
            (len[0] as usize, typ[0] as i8)
        }
        MSGPACK_EXT16 => {
            let mut len = [0u8; 2];
            let mut typ = [0u8; 1];
            rd.read_exact(&mut len)?;
            rd.read_exact(&mut typ)?;
            (u16::from_be_bytes(len) as usize, typ[0] as i8)
        }
        MSGPACK_EXT32 => {
            let mut len = [0u8; 4];
            let mut typ = [0u8; 1];
            rd.read_exact(&mut len)?;
            rd.read_exact(&mut typ)?;
            (u32::from_be_bytes(len) as usize, typ[0] as i8)
        }
        other => return Err(Error::other(format!("unsupported msgpack time marker: 0x{other:02x}"))),
    };

    let mut payload = vec![0u8; len];
    rd.read_exact(&mut payload)?;
    decode_msgp_time_payload(ext_type, &payload)
}

fn parse_legacy_uuid_bytes(bytes: &[u8], field: &str) -> Result<Option<Uuid>> {
    if bytes.is_empty() {
        return Ok(None);
    }

    if bytes.len() != 16 {
        return Err(Error::other(format!("legacy {field} must be 16 bytes, got {}", bytes.len())));
    }

    let id = Uuid::from_slice(bytes).map_err(Error::from)?;
    Ok((!id.is_nil()).then_some(id))
}

fn parse_legacy_erasure_algo(value: &str) -> ErasureAlgo {
    match value {
        "ReedSolomon" => ErasureAlgo::ReedSolomon,
        _ => ErasureAlgo::Invalid,
    }
}

fn parse_legacy_checksum_algo(value: &str) -> ChecksumAlgo {
    match value {
        "HighwayHash" => ChecksumAlgo::HighwayHash,
        _ => ChecksumAlgo::Invalid,
    }
}

#[derive(Debug, Deserialize)]
enum LegacyMetaV2VersionType {
    #[serde(rename = "Object")]
    Object,
    #[serde(rename = "Delete")]
    Delete,
    #[serde(rename = "DeleteMarker")]
    DeleteMarker,
}

#[derive(Debug, Deserialize)]
struct LegacyMetaV2Version {
    version_type: LegacyMetaV2VersionType,
    object: Option<LegacyMetaV2Object>,
    delete_marker: Option<LegacyMetaV2DeleteMarker>,
    write_version: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyMetaV2Object {
    #[serde(default, deserialize_with = "deserialize_legacy_uuid_bytes")]
    version_id: Vec<u8>,
    #[serde(default, deserialize_with = "deserialize_legacy_uuid_bytes")]
    data_dir: Vec<u8>,
    erasure_algorithm: String,
    erasure_m: usize,
    erasure_n: usize,
    erasure_block_size: usize,
    erasure_index: usize,
    erasure_dist: Vec<u8>,
    bitrot_checksum_algo: String,
    part_numbers: Vec<usize>,
    part_etags: Vec<String>,
    part_sizes: Vec<usize>,
    part_actual_sizes: Vec<i64>,
    part_indices: Vec<Vec<u8>>,
    size: i64,
    mod_time: Option<OffsetDateTime>,
    meta_sys: HashMap<String, Vec<u8>>,
    meta_user: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct LegacyMetaV2DeleteMarker {
    #[serde(default, deserialize_with = "deserialize_legacy_uuid_bytes")]
    version_id: Vec<u8>,
    mod_time: Option<OffsetDateTime>,
    meta_sys: HashMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone, Eq, PartialOrd, Ord)]
pub struct FileMetaShallowVersion {
    pub header: FileMetaVersionHeader,
    pub meta: Vec<u8>, // FileMetaVersion.marshal_msg
}

impl FileMetaShallowVersion {
    /// Parse version meta with legacy format compatibility.
    /// Use this instead of `FileMetaVersion::default()` + `unmarshal_msg()` to handle old-version xl.meta.
    pub fn parse_version_meta(&self) -> Result<FileMetaVersion> {
        FileMetaVersion::try_from(self.meta.as_slice())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfo> {
        let file_version = self.parse_version_meta()?;
        Ok(file_version.into_fileinfo(volume, path, all_parts))
    }
}

impl TryFrom<FileMetaVersion> for FileMetaShallowVersion {
    type Error = Error;

    fn try_from(value: FileMetaVersion) -> std::result::Result<Self, Self::Error> {
        let header = value.header();
        let meta = value.marshal_msg()?;
        Ok(Self { meta, header })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct FileMetaVersion {
    #[serde(rename = "Type")]
    pub version_type: VersionType,
    #[serde(rename = "V1Obj")]
    pub legacy_object: Option<MetaObjectV1>,
    #[serde(rename = "V2Obj")]
    pub object: Option<MetaObject>,
    #[serde(rename = "DelObj")]
    pub delete_marker: Option<MetaDeleteMarker>,
    #[serde(rename = "v")]
    pub write_version: u64, // rustfs version
    /// True when parsed via rmp_serde fallback (legacy format). Used for checksum algorithm selection.
    #[serde(skip)]
    pub uses_legacy_checksum: bool,
}

impl FileMetaVersion {
    pub fn valid(&self) -> bool {
        if !self.version_type.valid() {
            return false;
        }

        match self.version_type {
            VersionType::Object => self
                .object
                .as_ref()
                .map(|v| v.erasure_algorithm.valid() && v.bitrot_checksum_algo.valid() && v.mod_time.is_some())
                .unwrap_or_default(),
            VersionType::Legacy => self.legacy_object.as_ref().map(MetaObjectV1::valid).unwrap_or_default(),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .map(|v| v.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH) > OffsetDateTime::UNIX_EPOCH)
                .unwrap_or_default(),
            _ => false,
        }
    }

    pub fn get_data_dir(&self) -> Option<Uuid> {
        if self.valid() {
            {
                if self.version_type == VersionType::Object {
                    self.object.as_ref().map(|v| v.data_dir).unwrap_or_default()
                } else {
                    None
                }
            }
        } else {
            Default::default()
        }
    }

    pub fn get_version_id(&self) -> Option<Uuid> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.version_id).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.version_id).unwrap_or_default(),
            VersionType::Legacy => self.legacy_object.as_ref().and_then(MetaObjectV1::version_id),
            VersionType::Invalid => None,
        }
    }

    pub fn get_mod_time(&self) -> Option<OffsetDateTime> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            VersionType::Legacy => self.legacy_object.as_ref().and_then(|v| v.stat.mod_time),
            VersionType::Invalid => None,
        }
    }

    // decode_data_dir_from_meta reads data_dir from meta TODO: directly parse only data_dir from meta buf, msg.skip
    pub fn decode_data_dir_from_meta(buf: &[u8]) -> Result<Option<Uuid>> {
        Ok(Self::try_from(buf)?.get_data_dir())
    }

    pub fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = FileMetaVersion::default();

        while fields > 0 {
            fields -= 1;

            let key_len = rmp::decode::read_str_len(rd)?;
            let mut key_buf = vec![0u8; key_len as usize];
            rd.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf)?;

            match key.as_str() {
                "Type" => {
                    let v: i64 = rmp::decode::read_int(rd)?;
                    self.version_type = VersionType::from_u8(v as u8);
                }
                "V1Obj" => {
                    let mut buf = [0u8; 1];
                    rd.read_exact(&mut buf).map_err(Error::from)?;
                    if buf[0] == 0xc0 {
                        self.legacy_object = None;
                    } else {
                        let mut prepend = PrependByteReader {
                            byte: Some(buf[0]),
                            inner: rd,
                        };
                        let mut obj = MetaObjectV1::default();
                        obj.decode_from(&mut prepend)?;
                        self.legacy_object = Some(obj);
                    }
                }
                "V2Obj" => {
                    let mut buf = [0u8; 1];
                    rd.read_exact(&mut buf).map_err(Error::from)?;
                    if buf[0] == 0xc0 {
                        self.object = None;
                    } else {
                        let mut prepend = PrependByteReader {
                            byte: Some(buf[0]),
                            inner: rd,
                        };
                        let mut obj = MetaObject::default();
                        obj.decode_from(&mut prepend)?;
                        self.object = Some(obj);
                    }
                }
                "DelObj" => {
                    let mut buf = [0u8; 1];
                    rd.read_exact(&mut buf).map_err(Error::from)?;
                    if buf[0] == 0xc0 {
                        self.delete_marker = None;
                    } else {
                        let mut prepend = PrependByteReader {
                            byte: Some(buf[0]),
                            inner: rd,
                        };
                        let mut dm = MetaDeleteMarker::default();
                        dm.decode_from(&mut prepend)?;
                        self.delete_marker = Some(dm);
                    }
                }
                "v" => {
                    let v: i64 = rmp::decode::read_int(rd)?;
                    if v < 0 {
                        return Err(Error::other("negative write_version not supported"));
                    }
                    self.write_version = v as u64;
                }
                other => {
                    tracing::debug!(field = %other, "decode_from: skipping unknown field");
                    skip_msgp_value(rd)?;
                }
            }
        }

        Ok(())
    }

    pub fn encode_to<W: std::io::Write>(&self, wr: &mut W) -> Result<()> {
        // Variable map size: omit V2Obj/DelObj when None
        let mut map_len: u32 = 2; // "Type" + "v"
        if self.object.is_some() {
            map_len += 1;
        }
        if self.delete_marker.is_some() {
            map_len += 1;
        }

        rmp::encode::write_map_len(wr, map_len)?;

        // Type
        rmp::encode::write_str(wr, "Type")?;
        rmp::encode::write_uint(wr, self.version_type.to_u8() as u64)?;

        // V2Obj
        if let Some(ref obj) = self.object {
            rmp::encode::write_str(wr, "V2Obj")?;
            obj.encode_to(wr)?;
        }

        // DelObj
        if let Some(ref dm) = self.delete_marker {
            rmp::encode::write_str(wr, "DelObj")?;
            dm.encode_to(wr)?;
        }

        // v
        rmp::encode::write_str(wr, "v")?;
        rmp::encode::write_uint(wr, self.write_version)?;

        Ok(())
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = std::io::Cursor::new(buf);
        self.decode_from(&mut cur)?;
        Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        self.encode_to(&mut wr)?;
        Ok(wr)
    }

    pub fn free_version(&self) -> bool {
        self.version_type == VersionType::Delete && self.delete_marker.as_ref().map(|m| m.free_version()).unwrap_or_default()
    }

    pub fn header(&self) -> FileMetaVersionHeader {
        FileMetaVersionHeader::from(self.clone())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        let mut fi = match self.version_type {
            VersionType::Invalid | VersionType::Legacy => {
                if let Some(ref legacy) = self.legacy_object {
                    legacy.to_fileinfo(volume, path)
                } else {
                    FileInfo {
                        name: path.to_string(),
                        volume: volume.to_string(),
                        ..Default::default()
                    }
                }
            }
            VersionType::Object => {
                let default_object = MetaObject::default();
                self.object
                    .as_ref()
                    .unwrap_or(&default_object)
                    .into_fileinfo(volume, path, all_parts)
            }
            VersionType::Delete => {
                let default_marker = MetaDeleteMarker::default();
                self.delete_marker
                    .as_ref()
                    .unwrap_or(&default_marker)
                    .into_fileinfo(volume, path, all_parts)
            }
        };
        fi.uses_legacy_checksum = self.uses_legacy_checksum;
        fi
    }

    /// Support for Legacy version type
    pub fn is_legacy(&self) -> bool {
        self.version_type == VersionType::Legacy
    }

    /// Get signature for version
    pub fn get_signature(&self) -> [u8; 4] {
        match self.version_type {
            VersionType::Object => {
                if let Some(ref obj) = self.object {
                    // Calculate signature based on object metadata
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(obj.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = obj.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            VersionType::Delete => {
                if let Some(ref dm) = self.delete_marker {
                    // Calculate signature for delete marker
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(dm.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = dm.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            VersionType::Legacy => self.legacy_object.as_ref().map(MetaObjectV1::get_signature).unwrap_or([0; 4]),
            _ => [0; 4],
        }
    }

    /// Check if this version uses data directory
    pub fn uses_data_dir(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.uses_data_dir()).unwrap_or(false),
            VersionType::Legacy => false,
            _ => false,
        }
    }

    /// Check if this version uses inline data
    pub fn uses_inline_data(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.inlinedata()).unwrap_or(false),
            VersionType::Legacy => false,
            _ => false,
        }
    }
}

impl TryFrom<&[u8]> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut ver = FileMetaVersion::default();
        if ver.unmarshal_msg(value).is_ok() && ver.valid() {
            ver.uses_legacy_checksum = false;
            return Ok(ver);
        }

        if let Ok(legacy_ver) = rmp_serde::from_slice::<LegacyMetaV2Version>(value) {
            let mut ver = FileMetaVersion::try_from(legacy_ver)?;
            ver.uses_legacy_checksum = true;
            return Ok(ver);
        }

        // Fallback for legacy ver_meta: rmp_serde format
        let mut ver: Self = rmp_serde::from_slice(value).map_err(Error::other)?;
        ver.uses_legacy_checksum = true;
        Ok(ver)
    }
}

impl TryFrom<LegacyMetaV2Version> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: LegacyMetaV2Version) -> std::result::Result<Self, Self::Error> {
        let (version_type, object, delete_marker) = match value.version_type {
            LegacyMetaV2VersionType::Object => (VersionType::Object, value.object.map(TryInto::try_into).transpose()?, None),
            LegacyMetaV2VersionType::Delete | LegacyMetaV2VersionType::DeleteMarker => {
                (VersionType::Delete, None, value.delete_marker.map(TryInto::try_into).transpose()?)
            }
        };

        Ok(Self {
            version_type,
            legacy_object: None,
            object,
            delete_marker,
            write_version: value.write_version,
            uses_legacy_checksum: true,
        })
    }
}

impl From<FileInfo> for FileMetaVersion {
    fn from(value: FileInfo) -> Self {
        if value.deleted {
            FileMetaVersion {
                version_type: VersionType::Delete,
                legacy_object: None,
                delete_marker: Some(MetaDeleteMarker::from(value)),
                object: None,
                write_version: 0,
                uses_legacy_checksum: false,
            }
        } else {
            FileMetaVersion {
                version_type: VersionType::Object,
                legacy_object: None,
                delete_marker: None,
                object: Some(MetaObject::from(value)),
                write_version: 0,
                uses_legacy_checksum: false,
            }
        }
    }
}

impl TryFrom<FileMetaShallowVersion> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: FileMetaShallowVersion) -> std::result::Result<Self, Self::Error> {
        FileMetaVersion::try_from(value.meta.as_slice())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
pub struct FileMetaVersionHeader {
    pub version_id: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub signature: [u8; 4],
    pub version_type: VersionType,
    pub flags: u8,
    pub ec_n: u8,
    pub ec_m: u8,
}

impl FileMetaVersionHeader {
    fn reset_for_unmarshal(&mut self) {
        self.version_id = None;
        self.mod_time = None;
        self.signature = [0; 4];
        self.version_type = VersionType::Invalid;
        self.flags = 0;
        self.ec_n = 0;
        self.ec_m = 0;
    }

    pub fn has_ec(&self) -> bool {
        self.ec_m > 0 && self.ec_n > 0
    }

    pub fn matches_not_strict(&self, o: &FileMetaVersionHeader) -> bool {
        let mut ok = self.version_id == o.version_id && self.version_type == o.version_type && self.matches_ec(o);
        if self.version_id.is_none() {
            ok = ok && self.mod_time == o.mod_time;
        }

        ok
    }

    pub fn matches_ec(&self, o: &FileMetaVersionHeader) -> bool {
        if self.has_ec() && o.has_ec() {
            return self.ec_n == o.ec_n && self.ec_m == o.ec_m;
        }

        true
    }

    pub fn free_version(&self) -> bool {
        self.flags & XL_FLAG_FREE_VERSION != 0
    }

    pub fn sorts_before(&self, o: &FileMetaVersionHeader) -> bool {
        if self == o {
            return false;
        }

        // Prefer newest modtime.
        if self.mod_time != o.mod_time {
            return self.mod_time > o.mod_time;
        }

        match self.mod_time.cmp(&o.mod_time) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        // The following doesn't make too much sense, but we want sort to be consistent nonetheless.
        // Prefer lower types
        if self.version_type != o.version_type {
            return self.version_type < o.version_type;
        }
        // Consistent sort on signature
        match self.version_id.cmp(&o.version_id) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        if self.flags != o.flags {
            return self.flags > o.flags;
        }

        false
    }

    pub fn uses_data_dir(&self) -> bool {
        self.flags & Flags::UsesDataDir as u8 != 0
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();

        // array len 7
        rmp::encode::write_array_len(&mut wr, 7)?;

        // version_id
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or_default().as_bytes())?;
        // mod_time
        rmp::encode::write_i64(&mut wr, self.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp_nanos() as i64)?;
        // signature
        rmp::encode::write_bin(&mut wr, self.signature.as_slice())?;
        // version_type
        rmp::encode::write_uint8(&mut wr, self.version_type.to_u8())?;
        // flags
        rmp::encode::write_uint8(&mut wr, self.flags)?;
        // ec_n
        rmp::encode::write_uint8(&mut wr, self.ec_n)?;
        // ec_m
        rmp::encode::write_uint8(&mut wr, self.ec_m)?;

        Ok(wr)
    }

    pub fn unmarshal_v(&mut self, version: u8, buf: &[u8]) -> Result<u64> {
        match version {
            1 => self.unmarshal_v1(buf),
            2 => self.unmarshal_v2(buf),
            3 => self.unmarshal_msg(buf),
            _ => Err(Error::other(format!("unknown xl header version: {version}"))),
        }
    }

    pub fn unmarshal_v1(&mut self, buf: &[u8]) -> Result<u64> {
        self.reset_for_unmarshal();

        let mut cur = Cursor::new(buf);
        let alen = rmp::decode::read_array_len(&mut cur)?;
        if alen != 4 {
            return Err(Error::other(format!("version header array len err need 4 got {alen}")));
        }

        rmp::decode::read_bin_len(&mut cur)?;
        let mut version_id = [0u8; 16];
        cur.read_exact(&mut version_id)?;
        self.version_id = Some(Uuid::from_bytes(version_id));

        let unix: i128 = rmp::decode::read_int(&mut cur)?;
        let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
        if time != OffsetDateTime::UNIX_EPOCH {
            self.mod_time = Some(time);
        }

        let typ: u8 = rmp::decode::read_int(&mut cur)?;
        self.version_type = VersionType::from_u8(typ);
        self.flags = rmp::decode::read_int(&mut cur)?;

        Ok(cur.position())
    }

    pub fn unmarshal_v2(&mut self, buf: &[u8]) -> Result<u64> {
        self.reset_for_unmarshal();

        let mut cur = Cursor::new(buf);
        let alen = rmp::decode::read_array_len(&mut cur)?;
        if alen != 5 {
            return Err(Error::other(format!("version header array len err need 5 got {alen}")));
        }

        rmp::decode::read_bin_len(&mut cur)?;
        let mut version_id = [0u8; 16];
        cur.read_exact(&mut version_id)?;
        self.version_id = Some(Uuid::from_bytes(version_id));

        let unix: i128 = rmp::decode::read_int(&mut cur)?;
        let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
        if time != OffsetDateTime::UNIX_EPOCH {
            self.mod_time = Some(time);
        }

        rmp::decode::read_bin_len(&mut cur)?;
        cur.read_exact(&mut self.signature)?;

        let typ: u8 = rmp::decode::read_int(&mut cur)?;
        self.version_type = VersionType::from_u8(typ);
        self.flags = rmp::decode::read_int(&mut cur)?;

        Ok(cur.position())
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        self.reset_for_unmarshal();

        let mut cur = Cursor::new(buf);
        let alen = rmp::decode::read_array_len(&mut cur)?;
        if alen != 7 {
            return Err(Error::other(format!("version header array len err need 7 got {alen}")));
        }

        // version_id
        rmp::decode::read_bin_len(&mut cur)?;
        let mut buf = [0u8; 16];
        cur.read_exact(&mut buf)?;
        self.version_id = {
            let id = Uuid::from_bytes(buf);
            // if id.is_nil() { None } else { Some(id) }
            Some(id)
        };

        // mod_time
        let unix: i128 = rmp::decode::read_int(&mut cur)?;

        let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
        if time == OffsetDateTime::UNIX_EPOCH {
            self.mod_time = None;
        } else {
            self.mod_time = Some(time);
        }

        // signature
        rmp::decode::read_bin_len(&mut cur)?;
        cur.read_exact(&mut self.signature)?;

        // version_type
        let typ: u8 = rmp::decode::read_int(&mut cur)?;
        self.version_type = VersionType::from_u8(typ);

        // flags
        self.flags = rmp::decode::read_int(&mut cur)?;
        // ec_n
        self.ec_n = rmp::decode::read_int(&mut cur)?;
        // ec_m
        self.ec_m = rmp::decode::read_int(&mut cur)?;

        Ok(cur.position())
    }

    /// Get signature for header
    pub fn get_signature(&self) -> [u8; 4] {
        self.signature
    }

    /// Check if this header represents inline data
    pub fn inline_data(&self) -> bool {
        self.flags & Flags::InlineData as u8 != 0
    }

    /// Update signature based on version content
    pub fn update_signature(&mut self, version: &FileMetaVersion) {
        self.signature = version.get_signature();
    }
}

impl PartialOrd for FileMetaVersionHeader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileMetaVersionHeader {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.mod_time.cmp(&other.mod_time) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        match self.version_type.cmp(&other.version_type) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.signature.cmp(&other.signature) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.version_id.cmp(&other.version_id) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.flags.cmp(&other.flags)
    }
}

impl From<FileMetaVersion> for FileMetaVersionHeader {
    fn from(value: FileMetaVersion) -> Self {
        let flags = {
            let mut f: u8 = 0;
            if value.free_version() {
                f |= Flags::FreeVersion as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.uses_data_dir()).unwrap_or_default() {
                f |= Flags::UsesDataDir as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.inlinedata()).unwrap_or_default() {
                f |= Flags::InlineData as u8;
            }

            f
        };

        let (ec_n, ec_m) = match (value.version_type == VersionType::Object, value.object.as_ref()) {
            (true, Some(obj)) => (obj.erasure_n as u8, obj.erasure_m as u8),
            _ => (0, 0),
        };

        Self {
            version_id: value.get_version_id(),
            mod_time: value.get_mod_time(),
            signature: [0, 0, 0, 0],
            version_type: value.version_type,
            flags,
            ec_n,
            ec_m,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
// Because of custom message_pack, field order must be guaranteed
pub struct MetaObject {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID
    #[serde(rename = "DDir")]
    pub data_dir: Option<Uuid>, // Data dir ID
    #[serde(rename = "EcAlgo")]
    pub erasure_algorithm: ErasureAlgo, // Erasure coding algorithm
    #[serde(rename = "EcM")]
    pub erasure_m: usize, // Erasure data blocks
    #[serde(rename = "EcN")]
    pub erasure_n: usize, // Erasure parity blocks
    #[serde(rename = "EcBSize")]
    pub erasure_block_size: usize, // Erasure block size
    #[serde(rename = "EcIndex")]
    pub erasure_index: usize, // Erasure disk index
    #[serde(rename = "EcDist")]
    pub erasure_dist: Vec<u8>, // Erasure distribution
    #[serde(rename = "CSumAlgo")]
    pub bitrot_checksum_algo: ChecksumAlgo, // Bitrot checksum algo
    #[serde(rename = "PartNums")]
    pub part_numbers: Vec<usize>, // Part Numbers
    #[serde(rename = "PartETags")]
    pub part_etags: Vec<String>, // Part ETags
    #[serde(rename = "PartSizes")]
    pub part_sizes: Vec<usize>, // Part Sizes
    #[serde(rename = "PartASizes")]
    pub part_actual_sizes: Vec<i64>, // Part ActualSizes (compression)
    #[serde(rename = "PartIdx")]
    pub part_indices: Vec<Bytes>, // Part Indexes (compression)
    #[serde(rename = "Size")]
    pub size: i64, // Object version size
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object version modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Object version internal metadata
    #[serde(rename = "MetaUsr")]
    pub meta_user: HashMap<String, String>, // Object version metadata set by user
}

impl TryFrom<LegacyMetaV2Object> for MetaObject {
    type Error = Error;

    fn try_from(value: LegacyMetaV2Object) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            version_id: parse_legacy_uuid_bytes(&value.version_id, "version_id")?,
            data_dir: parse_legacy_uuid_bytes(&value.data_dir, "data_dir")?,
            erasure_algorithm: parse_legacy_erasure_algo(&value.erasure_algorithm),
            erasure_m: value.erasure_m,
            erasure_n: value.erasure_n,
            erasure_block_size: value.erasure_block_size,
            erasure_index: value.erasure_index,
            erasure_dist: value.erasure_dist,
            bitrot_checksum_algo: parse_legacy_checksum_algo(&value.bitrot_checksum_algo),
            part_numbers: value.part_numbers,
            part_etags: value.part_etags,
            part_sizes: value.part_sizes,
            part_actual_sizes: value.part_actual_sizes,
            part_indices: value.part_indices.into_iter().map(Bytes::from).collect(),
            size: value.size,
            mod_time: value.mod_time,
            meta_sys: value.meta_sys,
            meta_user: value.meta_user,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct MetaObjectV1 {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Format")]
    pub format: String,
    #[serde(rename = "Stat")]
    pub stat: MetaObjectV1Stat,
    #[serde(rename = "Erasure")]
    pub erasure: MetaObjectV1Erasure,
    #[serde(rename = "Meta")]
    pub meta: HashMap<String, String>,
    #[serde(rename = "Parts")]
    pub parts: Vec<MetaObjectV1Part>,
    #[serde(rename = "VersionID")]
    pub version_id: String,
    #[serde(rename = "DataDir")]
    pub data_dir: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct MetaObjectV1Stat {
    #[serde(rename = "Size")]
    pub size: i64,
    #[serde(rename = "ModTime")]
    pub mod_time: Option<OffsetDateTime>,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Dir")]
    pub dir: bool,
    #[serde(rename = "Mode")]
    pub mode: u32,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct MetaObjectV1ChecksumInfo {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "Algorithm")]
    pub algorithm: String,
    #[serde(rename = "Hash")]
    pub hash: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct MetaObjectV1Erasure {
    #[serde(rename = "Algorithm")]
    pub algorithm: String,
    #[serde(rename = "DataBlocks")]
    pub data_blocks: usize,
    #[serde(rename = "ParityBlocks")]
    pub parity_blocks: usize,
    #[serde(rename = "BlockSize")]
    pub block_size: usize,
    #[serde(rename = "Index")]
    pub index: usize,
    #[serde(rename = "Distribution")]
    pub distribution: Vec<usize>,
    #[serde(rename = "Checksums")]
    pub checksums: Vec<MetaObjectV1ChecksumInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct MetaObjectV1Part {
    #[serde(rename = "e")]
    pub etag: String,
    #[serde(rename = "n")]
    pub number: usize,
    #[serde(rename = "s")]
    pub size: usize,
    #[serde(rename = "as")]
    pub actual_size: i64,
    #[serde(rename = "mt")]
    pub mod_time: Option<OffsetDateTime>,
    #[serde(rename = "i")]
    pub index: Option<Bytes>,
    #[serde(rename = "crc")]
    pub checksums: Option<HashMap<String, String>>,
    #[serde(rename = "err")]
    pub error: Option<String>,
}

impl MetaObjectV1 {
    fn version_id(&self) -> Option<Uuid> {
        if self.version_id.is_empty() {
            None
        } else {
            Uuid::parse_str(&self.version_id).ok().filter(|id| !id.is_nil())
        }
    }

    fn valid(&self) -> bool {
        if self.format != "xl" || self.stat.mod_time.is_none() {
            return false;
        }

        let data_blocks = self.erasure.data_blocks;
        let parity_blocks = self.erasure.parity_blocks;
        data_blocks > 0
            && data_blocks >= parity_blocks
            && self.erasure.index > 0
            && self.erasure.distribution.len() == data_blocks + parity_blocks
    }

    fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_string(rd)?;
            match key.as_str() {
                "Version" => self.version = read_msgp_string(rd)?,
                "Format" => self.format = read_msgp_string(rd)?,
                "Stat" => self.stat.decode_from(rd)?,
                "Erasure" => self.erasure.decode_from(rd)?,
                "Meta" => {
                    let len = rmp::decode::read_map_len(rd)? as usize;
                    self.meta.clear();
                    for _ in 0..len {
                        self.meta.insert(read_msgp_string(rd)?, read_msgp_string(rd)?);
                    }
                }
                "Parts" => {
                    let len = rmp::decode::read_array_len(rd)? as usize;
                    self.parts.clear();
                    self.parts.reserve(len);
                    for _ in 0..len {
                        let mut part = MetaObjectV1Part::default();
                        part.decode_from(rd)?;
                        self.parts.push(part);
                    }
                }
                "VersionID" => self.version_id = read_msgp_string(rd)?,
                "DataDir" => self.data_dir = read_msgp_string(rd)?,
                _ => skip_msgp_value(rd)?,
            }
        }

        Ok(())
    }

    fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version.as_bytes());
        hasher.update(self.format.as_bytes());
        hasher.update(&self.stat.size.to_le_bytes());
        hasher.update(&self.stat.mode.to_le_bytes());
        if let Some(mod_time) = self.stat.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        hasher.update(self.erasure.algorithm.as_bytes());
        hasher.update(&(self.erasure.data_blocks as u64).to_le_bytes());
        hasher.update(&(self.erasure.parity_blocks as u64).to_le_bytes());
        hasher.update(&(self.erasure.block_size as u64).to_le_bytes());
        for v in &self.erasure.distribution {
            hasher.update(&(*v as u64).to_le_bytes());
        }
        for checksum in &self.erasure.checksums {
            hasher.update(&(checksum.part_number as u64).to_le_bytes());
            hasher.update(checksum.algorithm.as_bytes());
            hasher.update(&checksum.hash);
        }
        let mut meta_keys: Vec<_> = self.meta.iter().collect();
        meta_keys.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in meta_keys {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        }
        for part in &self.parts {
            hasher.update(&(part.number as u64).to_le_bytes());
            hasher.update(&(part.size as u64).to_le_bytes());
            hasher.update(&part.actual_size.to_le_bytes());
            hasher.update(part.etag.as_bytes());
            if let Some(mod_time) = part.mod_time {
                hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
            }
            if let Some(index) = &part.index {
                hasher.update(index);
            }
        }
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    fn to_fileinfo(&self, volume: &str, path: &str) -> FileInfo {
        FileInfo {
            volume: volume.to_string(),
            name: path.to_string(),
            version_id: self.version_id(),
            mod_time: self.stat.mod_time,
            size: self.stat.size,
            mode: Some(self.stat.mode),
            metadata: self.meta.clone(),
            parts: self.parts.iter().cloned().map(Into::into).collect(),
            erasure: self.erasure.clone().into(),
            num_versions: 1,
            data_dir: Uuid::parse_str(&self.data_dir).ok().filter(|id| !id.is_nil()),
            ..Default::default()
        }
    }
}

impl MetaObjectV1Stat {
    fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_string(rd)?;
            match key.as_str() {
                "Size" => self.size = rmp::decode::read_int(rd)?,
                "ModTime" => self.mod_time = Some(read_msgp_time(rd)?),
                "Name" => self.name = read_msgp_string(rd)?,
                "Dir" => self.dir = rmp::decode::read_bool(rd)?,
                "Mode" => self.mode = rmp::decode::read_u32(rd)?,
                _ => skip_msgp_value(rd)?,
            }
        }

        Ok(())
    }
}

impl MetaObjectV1Erasure {
    fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_string(rd)?;
            match key.as_str() {
                "Algorithm" => self.algorithm = read_msgp_string(rd)?,
                "DataBlocks" => self.data_blocks = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "ParityBlocks" => self.parity_blocks = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "BlockSize" => self.block_size = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "Index" => self.index = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "Distribution" => {
                    let len = rmp::decode::read_array_len(rd)? as usize;
                    self.distribution.clear();
                    self.distribution.reserve(len);
                    for _ in 0..len {
                        self.distribution.push(rmp::decode::read_int::<i64, _>(rd)? as usize);
                    }
                }
                "Checksums" => {
                    let len = rmp::decode::read_array_len(rd)? as usize;
                    self.checksums.clear();
                    self.checksums.reserve(len);
                    for _ in 0..len {
                        let mut checksum = MetaObjectV1ChecksumInfo::default();
                        checksum.decode_from(rd)?;
                        self.checksums.push(checksum);
                    }
                }
                _ => skip_msgp_value(rd)?,
            }
        }

        Ok(())
    }
}

impl MetaObjectV1ChecksumInfo {
    fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_string(rd)?;
            match key.as_str() {
                "PartNumber" => self.part_number = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "Algorithm" => self.algorithm = read_msgp_string(rd)?,
                "Hash" => self.hash = read_msgp_bin(rd)?,
                _ => skip_msgp_value(rd)?,
            }
        }

        Ok(())
    }
}

impl MetaObjectV1Part {
    fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = Self::default();

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_string(rd)?;
            match key.as_str() {
                "e" => self.etag = read_msgp_string(rd)?,
                "n" => self.number = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "s" => self.size = rmp::decode::read_int::<i64, _>(rd)? as usize,
                "as" => self.actual_size = rmp::decode::read_int(rd)?,
                "mt" => self.mod_time = Some(read_msgp_time(rd)?),
                "i" => self.index = Some(Bytes::from(read_msgp_bin(rd)?)),
                "crc" => {
                    let len = rmp::decode::read_map_len(rd)? as usize;
                    let mut checksums = HashMap::with_capacity(len);
                    for _ in 0..len {
                        checksums.insert(read_msgp_string(rd)?, read_msgp_string(rd)?);
                    }
                    self.checksums = Some(checksums);
                }
                "err" => self.error = Some(read_msgp_string(rd)?),
                _ => skip_msgp_value(rd)?,
            }
        }

        Ok(())
    }
}

impl From<MetaObjectV1Erasure> for ErasureInfo {
    fn from(value: MetaObjectV1Erasure) -> Self {
        ErasureInfo {
            algorithm: value.algorithm,
            data_blocks: value.data_blocks,
            parity_blocks: value.parity_blocks,
            block_size: value.block_size,
            index: value.index,
            distribution: value.distribution,
            checksums: value.checksums.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MetaObjectV1ChecksumInfo> for ChecksumInfo {
    fn from(value: MetaObjectV1ChecksumInfo) -> Self {
        ChecksumInfo {
            part_number: value.part_number,
            algorithm: match value.algorithm.as_str() {
                "sha256" => HashAlgorithm::SHA256,
                "highwayhash256" => HashAlgorithm::HighwayHash256,
                "highwayhash256S" => HashAlgorithm::HighwayHash256S,
                "blake2b" | "blake2b512" => HashAlgorithm::BLAKE2b512,
                _ => HashAlgorithm::HighwayHash256S,
            },
            hash: Bytes::from(value.hash),
        }
    }
}

impl From<MetaObjectV1Part> for ObjectPartInfo {
    fn from(value: MetaObjectV1Part) -> Self {
        ObjectPartInfo {
            etag: value.etag,
            number: value.number,
            size: value.size,
            actual_size: value.actual_size,
            mod_time: value.mod_time,
            index: value.index,
            checksums: value.checksums,
            error: value.error,
        }
    }
}

impl MetaObject {
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = std::io::Cursor::new(buf);
        self.decode_from(&mut cur)?;
        Ok(cur.position())
    }

    // marshal_msg custom messagepack naming consistent with go
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        self.encode_to(&mut wr)?;
        Ok(wr)
    }

    pub fn encode_to<W: std::io::Write>(&self, wr: &mut W) -> Result<()> {
        // Variable map size: omit PartIdx when empty
        let mut map_len = 18u32;
        if self.part_indices.is_empty() {
            map_len -= 1;
        }
        rmp::encode::write_map_len(wr, map_len)?;

        // ID
        rmp::encode::write_str(wr, "ID")?;
        rmp::encode::write_bin(wr, self.version_id.unwrap_or_default().as_bytes())?;

        // DDir
        rmp::encode::write_str(wr, "DDir")?;
        rmp::encode::write_bin(wr, self.data_dir.unwrap_or_default().as_bytes())?;

        // EcAlgo
        rmp::encode::write_str(wr, "EcAlgo")?;
        rmp::encode::write_uint(wr, self.erasure_algorithm.to_u8() as u64)?;

        // EcM
        rmp::encode::write_str(wr, "EcM")?;
        rmp::encode::write_sint(wr, self.erasure_m as i64)?;

        // EcN
        rmp::encode::write_str(wr, "EcN")?;
        rmp::encode::write_sint(wr, self.erasure_n as i64)?;

        // EcBSize
        rmp::encode::write_str(wr, "EcBSize")?;
        rmp::encode::write_sint(wr, self.erasure_block_size as i64)?;

        // EcIndex
        rmp::encode::write_str(wr, "EcIndex")?;
        rmp::encode::write_sint(wr, self.erasure_index as i64)?;

        // EcDist
        rmp::encode::write_str(wr, "EcDist")?;
        rmp::encode::write_array_len(wr, self.erasure_dist.len() as u32)?;
        for v in &self.erasure_dist {
            rmp::encode::write_uint(wr, *v as u64)?;
        }

        // CSumAlgo
        rmp::encode::write_str(wr, "CSumAlgo")?;
        rmp::encode::write_uint(wr, self.bitrot_checksum_algo.to_u8() as u64)?;

        // PartNums
        rmp::encode::write_str(wr, "PartNums")?;
        rmp::encode::write_array_len(wr, self.part_numbers.len() as u32)?;
        for n in &self.part_numbers {
            rmp::encode::write_sint(wr, *n as i64)?;
        }

        // PartETags (write nil when empty)
        rmp::encode::write_str(wr, "PartETags")?;
        if self.part_etags.is_empty() {
            rmp::encode::write_nil(wr)?;
        } else {
            rmp::encode::write_array_len(wr, self.part_etags.len() as u32)?;
            for et in &self.part_etags {
                rmp::encode::write_str(wr, et)?;
            }
        }

        // PartSizes
        rmp::encode::write_str(wr, "PartSizes")?;
        rmp::encode::write_array_len(wr, self.part_sizes.len() as u32)?;
        for s in &self.part_sizes {
            rmp::encode::write_sint(wr, *s as i64)?;
        }

        // PartASizes (write nil when empty)
        rmp::encode::write_str(wr, "PartASizes")?;
        if self.part_actual_sizes.is_empty() {
            rmp::encode::write_nil(wr)?;
        } else {
            rmp::encode::write_array_len(wr, self.part_actual_sizes.len() as u32)?;
            for s in &self.part_actual_sizes {
                rmp::encode::write_sint(wr, *s)?;
            }
        }

        // PartIdx (omit when empty)
        if !self.part_indices.is_empty() {
            rmp::encode::write_str(wr, "PartIdx")?;
            rmp::encode::write_array_len(wr, self.part_indices.len() as u32)?;
            for idx in &self.part_indices {
                rmp::encode::write_bin(wr, idx)?;
            }
        }

        // Size
        rmp::encode::write_str(wr, "Size")?;
        rmp::encode::write_sint(wr, self.size)?;

        // MTime Unix timestamp nanos
        rmp::encode::write_str(wr, "MTime")?;
        let nanos = self.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp_nanos();
        rmp::encode::write_sint(wr, nanos as i64)?;

        // MetaSys (write nil when empty)
        rmp::encode::write_str(wr, "MetaSys")?;
        if self.meta_sys.is_empty() {
            rmp::encode::write_nil(wr)?;
        } else {
            rmp::encode::write_map_len(wr, self.meta_sys.len() as u32)?;
            for (k, v) in &self.meta_sys {
                rmp::encode::write_str(wr, k)?;
                rmp::encode::write_bin(wr, v)?;
            }
        }

        // MetaUsr (write nil when empty)
        rmp::encode::write_str(wr, "MetaUsr")?;
        if self.meta_user.is_empty() {
            rmp::encode::write_nil(wr)?;
        } else {
            rmp::encode::write_map_len(wr, self.meta_user.len() as u32)?;
            for (k, v) in &self.meta_user {
                rmp::encode::write_str(wr, k)?;
                rmp::encode::write_str(wr, v)?;
            }
        }

        Ok(())
    }

    pub fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd).map_err(|e| {
            tracing::error!(error = %e, "decode_from: read_map_len failed");
            e
        })?;
        *self = MetaObject::default();

        while fields > 0 {
            fields -= 1;

            let key_len = rmp::decode::read_str_len(rd).map_err(|e| {
                tracing::error!(error = %e, "decode_from: read_str_len key failed");
                e
            })?;
            let mut key_buf = vec![0u8; key_len as usize];
            rd.read_exact(&mut key_buf).map_err(|e| {
                tracing::error!(error = %e, "decode_from: read_exact key_buf failed");
                e
            })?;
            let key = String::from_utf8(key_buf).map_err(|e| {
                tracing::error!(error = %e, "decode_from: from_utf8 key failed");
                e
            })?;

            match key.as_str() {
                "ID" => {
                    let _ = rmp::decode::read_bin_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_bin_len ID failed");
                        e
                    })?;
                    let mut buf = [0u8; 16];
                    rd.read_exact(&mut buf).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_exact ID buf failed");
                        e
                    })?;
                    let id = Uuid::from_bytes(buf);
                    self.version_id = if id.is_nil() { None } else { Some(id) };
                }
                "DDir" => {
                    let _ = rmp::decode::read_bin_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_bin_len DDir failed");
                        e
                    })?;
                    let mut buf = [0u8; 16];
                    rd.read_exact(&mut buf).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_exact DDir buf failed");
                        e
                    })?;
                    let id = Uuid::from_bytes(buf);
                    self.data_dir = if id.is_nil() { None } else { Some(id) };
                }
                "EcAlgo" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int EcAlgo failed");
                        e
                    })?;
                    self.erasure_algorithm = ErasureAlgo::from_u8(v as u8);
                }
                "EcM" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int EcM failed");
                        e
                    })?;
                    self.erasure_m = v as usize;
                }
                "EcN" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int EcN failed");
                        e
                    })?;
                    self.erasure_n = v as usize;
                }
                "EcBSize" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int EcBSize failed");
                        e
                    })?;
                    self.erasure_block_size = v as usize;
                }
                "EcIndex" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int EcIndex failed");
                        e
                    })?;
                    self.erasure_index = v as usize;
                }
                "EcDist" => {
                    let len = rmp::decode::read_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_array_len EcDist failed");
                        e
                    })? as usize;
                    self.erasure_dist.clear();
                    self.erasure_dist.reserve(len);
                    for _ in 0..len {
                        let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_int EcDist item failed");
                            e
                        })?;
                        self.erasure_dist.push(v as u8);
                    }
                }
                "CSumAlgo" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int CSumAlgo failed");
                        e
                    })?;
                    self.bitrot_checksum_algo = ChecksumAlgo::from_u8(v as u8);
                }
                "PartNums" => {
                    let len = rmp::decode::read_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_array_len PartNums failed");
                        e
                    })? as usize;
                    self.part_numbers.clear();
                    self.part_numbers.reserve(len);
                    for _ in 0..len {
                        let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_int PartNums item failed");
                            e
                        })?;
                        self.part_numbers.push(v as usize);
                    }
                }
                "PartETags" => {
                    let len = match read_nil_or_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read PartETags failed");
                        e
                    })? {
                        None => {
                            self.part_etags.clear();
                            continue;
                        }
                        Some(n) => n,
                    };
                    self.part_etags.clear();
                    self.part_etags.reserve(len);
                    for _ in 0..len {
                        let s_len = rmp::decode::read_str_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_str_len PartETags item failed");
                            e
                        })?;
                        let mut sbuf = vec![0u8; s_len as usize];
                        rd.read_exact(&mut sbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact PartETags sbuf failed");
                            e
                        })?;
                        let s = String::from_utf8(sbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: from_utf8 PartETags item failed");
                            e
                        })?;
                        self.part_etags.push(s);
                    }
                }
                "PartSizes" => {
                    let len = rmp::decode::read_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_array_len PartSizes failed");
                        e
                    })? as usize;
                    self.part_sizes.clear();
                    self.part_sizes.reserve(len);
                    for _ in 0..len {
                        let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_int PartSizes item failed");
                            e
                        })?;
                        self.part_sizes.push(v as usize);
                    }
                }
                "PartASizes" => {
                    let len = match read_nil_or_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read PartASizes failed");
                        e
                    })? {
                        None => {
                            self.part_actual_sizes.clear();
                            continue;
                        }
                        Some(n) => n,
                    };
                    self.part_actual_sizes.clear();
                    self.part_actual_sizes.reserve(len);
                    for _ in 0..len {
                        let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_int PartASizes item failed");
                            e
                        })?;
                        self.part_actual_sizes.push(v);
                    }
                }
                "PartIdx" => {
                    let len = rmp::decode::read_array_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_array_len PartIdx failed");
                        e
                    })? as usize;
                    self.part_indices.clear();
                    self.part_indices.reserve(len);
                    for _ in 0..len {
                        let blen = rmp::decode::read_bin_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_bin_len PartIdx item failed");
                            e
                        })? as usize;
                        let mut buf = vec![0u8; blen];
                        rd.read_exact(&mut buf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact PartIdx buf failed");
                            e
                        })?;
                        self.part_indices.push(Bytes::from(buf));
                    }
                }
                "Size" => {
                    let v: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int Size failed");
                        e
                    })?;
                    self.size = v;
                }
                "MTime" => {
                    let nanos: i64 = rmp::decode::read_int(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read_int MTime failed");
                        e
                    })?;
                    let time = OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).inspect_err(|&e| {
                        tracing::error!(error = %e, "decode_from: from_unix_timestamp_nanos MTime failed");
                    })?;
                    self.mod_time = if time == OffsetDateTime::UNIX_EPOCH {
                        None
                    } else {
                        Some(time)
                    };
                }
                "MetaSys" => {
                    let len = match read_nil_or_map_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read MetaSys failed");
                        e
                    })? {
                        None => {
                            self.meta_sys.clear();
                            continue;
                        }
                        Some(n) => n,
                    };
                    self.meta_sys.clear();
                    for _ in 0..len {
                        let k_len = rmp::decode::read_str_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_str_len MetaSys key failed");
                            e
                        })?;
                        let mut kbuf = vec![0u8; k_len as usize];
                        rd.read_exact(&mut kbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact MetaSys kbuf failed");
                            e
                        })?;
                        let k = String::from_utf8(kbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: from_utf8 MetaSys key failed");
                            e
                        })?;

                        let blen = rmp::decode::read_bin_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_bin_len MetaSys value failed");
                            e
                        })? as usize;
                        let mut v = vec![0u8; blen];
                        rd.read_exact(&mut v).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact MetaSys value failed");
                            e
                        })?;

                        self.meta_sys.insert(k, v);
                    }
                }
                "MetaUsr" => {
                    let len = match read_nil_or_map_len(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: read MetaUsr failed");
                        e
                    })? {
                        None => {
                            self.meta_user.clear();
                            continue;
                        }
                        Some(n) => n,
                    };
                    self.meta_user.clear();
                    for _ in 0..len {
                        let k_len = rmp::decode::read_str_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_str_len MetaUsr key failed");
                            e
                        })?;
                        let mut kbuf = vec![0u8; k_len as usize];
                        rd.read_exact(&mut kbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact MetaUsr kbuf failed");
                            e
                        })?;
                        let k = String::from_utf8(kbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: from_utf8 MetaUsr key failed");
                            e
                        })?;

                        let v_len = rmp::decode::read_str_len(rd).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_str_len MetaUsr value failed");
                            e
                        })?;
                        let mut vbuf = vec![0u8; v_len as usize];
                        rd.read_exact(&mut vbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: read_exact MetaUsr vbuf failed");
                            e
                        })?;
                        let v = String::from_utf8(vbuf).map_err(|e| {
                            tracing::error!(error = %e, "decode_from: from_utf8 MetaUsr value failed");
                            e
                        })?;

                        self.meta_user.insert(k, v);
                    }
                }
                other => {
                    // Skip unknown fields for forward compatibility with Go (dc.Skip())
                    tracing::debug!(field = %other, "decode_from: skipping unknown field");
                    skip_msgp_value(rd).map_err(|e| {
                        tracing::error!(error = %e, "decode_from: skip unknown field failed");
                        e
                    })?;
                }
            }
        }

        Ok(())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        let version_id = self.version_id.filter(|&vid| !vid.is_nil());

        let parts = if all_parts {
            let mut parts = vec![ObjectPartInfo::default(); self.part_numbers.len()];

            for (i, part) in parts.iter_mut().enumerate() {
                part.number = self.part_numbers[i];
                part.size = self.part_sizes[i];
                part.actual_size = self.part_actual_sizes[i];

                if self.part_etags.len() == self.part_numbers.len() {
                    part.etag = self.part_etags[i].clone();
                }

                if self.part_indices.len() == self.part_numbers.len() {
                    part.index = if self.part_indices[i].is_empty() {
                        None
                    } else {
                        Some(self.part_indices[i].clone())
                    };
                }
            }
            parts
        } else {
            Vec::new()
        };

        let mut metadata = HashMap::with_capacity(self.meta_user.len() + self.meta_sys.len());
        for (k, v) in &self.meta_user {
            if k == AMZ_META_UNENCRYPTED_CONTENT_LENGTH || k == AMZ_META_UNENCRYPTED_CONTENT_MD5 {
                continue;
            }

            if k == AMZ_STORAGE_CLASS && v == "STANDARD" {
                continue;
            }

            metadata.insert(k.to_owned(), v.to_owned());
        }

        for (k, v) in &self.meta_sys {
            let lower_k = k.to_lowercase();

            if has_internal_suffix(&lower_k, SUFFIX_TIER_FV_ID) || has_internal_suffix(&lower_k, SUFFIX_TIER_FV_MARKER) {
                continue;
            }

            if lower_k == AMZ_STORAGE_CLASS.to_lowercase() && v == b"STANDARD" {
                continue;
            }

            if is_internal_key(k) {
                metadata.insert(k.to_owned(), String::from_utf8(v.to_owned()).unwrap_or_default());
            }
        }

        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut deleted = false;

        if let Some(v) = replication_state_internal.as_ref() {
            if !v.composite_version_purge_status().is_empty() {
                deleted = true;
            }

            let st = v.composite_replication_status();
            if !st.is_empty() {
                metadata.insert(AMZ_BUCKET_REPLICATION_STATUS.to_string(), st.to_string());
            }
        }

        let checksum = get_bytes(&self.meta_sys, SUFFIX_CRC).map(Bytes::from);

        let erasure = ErasureInfo {
            algorithm: self.erasure_algorithm.to_string(),
            data_blocks: self.erasure_m,
            parity_blocks: self.erasure_n,
            block_size: self.erasure_block_size,
            index: self.erasure_index,
            distribution: self.erasure_dist.iter().map(|&v| v as usize).collect(),
            ..Default::default()
        };

        let transition_status = get_bytes(&self.meta_sys, SUFFIX_TRANSITION_STATUS)
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();
        let transitioned_objname = get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_OBJECTNAME)
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();
        let transition_version_id =
            get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID).map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        let transition_tier = get_bytes(&self.meta_sys, SUFFIX_TRANSITION_TIER)
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();

        FileInfo {
            version_id,
            erasure,
            data_dir: self.data_dir,
            mod_time: self.mod_time,
            size: self.size,
            name: path.to_string(),
            volume: volume.to_string(),
            parts,
            metadata,
            replication_state_internal,
            deleted,
            checksum,
            transition_status,
            transitioned_objname,
            transition_version_id,
            transition_tier,
            ..Default::default()
        }
    }

    pub fn set_transition(&mut self, fi: &FileInfo) {
        insert_bytes(&mut self.meta_sys, SUFFIX_TRANSITION_STATUS, fi.transition_status.as_bytes().to_vec());
        insert_bytes(
            &mut self.meta_sys,
            SUFFIX_TRANSITIONED_OBJECTNAME,
            fi.transitioned_objname.as_bytes().to_vec(),
        );
        if let Some(transition_version_id) = fi.transition_version_id.as_ref() {
            insert_bytes(
                &mut self.meta_sys,
                SUFFIX_TRANSITIONED_VERSION_ID,
                transition_version_id.as_bytes().to_vec(),
            );
        }
        insert_bytes(&mut self.meta_sys, SUFFIX_TRANSITION_TIER, fi.transition_tier.as_bytes().to_vec());
    }

    pub fn remove_restore_hdrs(&mut self) {
        self.meta_user.remove(X_AMZ_RESTORE.as_str());
        self.meta_user.remove(AMZ_RESTORE_EXPIRY_DAYS);
        self.meta_user.remove(AMZ_RESTORE_REQUEST_DATE);
    }

    pub fn uses_data_dir(&self) -> bool {
        if let Some(status) = get_bytes(&self.meta_sys, SUFFIX_TRANSITION_STATUS)
            && status == TRANSITION_COMPLETE.as_bytes().to_vec()
        {
            return false;
        }

        is_restored_object_on_disk(&self.meta_user)
    }

    pub fn inlinedata(&self) -> bool {
        contains_key_bytes(&self.meta_sys, SUFFIX_INLINE_DATA)
    }

    pub fn reset_inline_data(&mut self) {
        remove_bytes(&mut self.meta_sys, SUFFIX_INLINE_DATA);
    }

    /// Remove restore headers
    pub fn remove_restore_headers(&mut self) {
        // Remove any restore-related metadata
        self.meta_sys.retain(|k, _| !k.starts_with("X-Amz-Restore"));
    }

    /// Get object signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        hasher.update(&self.size.to_le_bytes());
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    pub fn init_free_version(&self, fi: &FileInfo) -> (FileMetaVersion, bool) {
        if fi.skip_tier_free_version() {
            return (FileMetaVersion::default(), false);
        }
        if let Some(status) = get_bytes(&self.meta_sys, SUFFIX_TRANSITION_STATUS)
            && status == TRANSITION_COMPLETE.as_bytes().to_vec()
        {
            let vid = Uuid::parse_str(&fi.tier_free_version_id());
            if let Err(err) = vid {
                panic!("Invalid Tier Object delete marker versionId {} {}", fi.tier_free_version_id(), err);
            }
            let vid = vid.unwrap();
            let mut free_entry = FileMetaVersion {
                version_type: VersionType::Delete,
                write_version: 0,
                ..Default::default()
            };
            free_entry.delete_marker = Some(MetaDeleteMarker {
                version_id: Some(vid),
                mod_time: self.mod_time,
                meta_sys: HashMap::<String, Vec<u8>>::new(),
            });

            let delete_marker = free_entry.delete_marker.as_mut().unwrap();

            insert_bytes(&mut delete_marker.meta_sys, SUFFIX_FREE_VERSION, vec![]);

            for suffix in [
                SUFFIX_TRANSITION_TIER,
                SUFFIX_TRANSITIONED_OBJECTNAME,
                SUFFIX_TRANSITIONED_VERSION_ID,
            ] {
                if let Some(v) = get_bytes(&self.meta_sys, suffix) {
                    insert_bytes(&mut delete_marker.meta_sys, suffix, v);
                }
            }
            return (free_entry, true);
        }
        (FileMetaVersion::default(), false)
    }
}

impl From<FileInfo> for MetaObject {
    fn from(value: FileInfo) -> Self {
        let part_etags = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.etag.clone()).collect()
        } else {
            vec![]
        };

        let part_indices = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.index.clone().unwrap_or_default()).collect()
        } else {
            vec![]
        };

        let mut meta_sys = HashMap::new();
        let mut meta_user = HashMap::new();
        for (k, v) in value.metadata.iter() {
            if is_internal_key(k) {
                if is_skip_meta_key(k) {
                    continue;
                }

                meta_sys.insert(k.to_owned(), v.as_bytes().to_vec());
            } else {
                meta_user.insert(k.to_owned(), v.to_owned());
            }
        }

        if !value.transition_status.is_empty() {
            insert_bytes(&mut meta_sys, SUFFIX_TRANSITION_STATUS, value.transition_status.as_bytes().to_vec());
        }

        if !value.transitioned_objname.is_empty() {
            insert_bytes(
                &mut meta_sys,
                SUFFIX_TRANSITIONED_OBJECTNAME,
                value.transitioned_objname.as_bytes().to_vec(),
            );
        }

        if let Some(vid) = &value.transition_version_id {
            insert_bytes(&mut meta_sys, SUFFIX_TRANSITIONED_VERSION_ID, vid.as_bytes().to_vec());
        }

        if !value.transition_tier.is_empty() {
            insert_bytes(&mut meta_sys, SUFFIX_TRANSITION_TIER, value.transition_tier.as_bytes().to_vec());
        }

        if let Some(content_hash) = value.checksum {
            insert_bytes(&mut meta_sys, SUFFIX_CRC, content_hash.to_vec());
        }

        Self {
            version_id: value.version_id,
            data_dir: value.data_dir,
            size: value.size,
            mod_time: value.mod_time,
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            erasure_m: value.erasure.data_blocks,
            erasure_n: value.erasure.parity_blocks,
            erasure_block_size: value.erasure.block_size,
            erasure_index: value.erasure.index,
            erasure_dist: value.erasure.distribution.iter().map(|x| *x as u8).collect(),
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: value.parts.iter().map(|v| v.number).collect(),
            part_etags,
            part_sizes: value.parts.iter().map(|v| v.size).collect(),
            part_actual_sizes: value.parts.iter().map(|v| v.actual_size).collect(),
            part_indices,
            meta_sys,
            meta_user,
        }
    }
}

fn get_internal_replication_state(metadata: &HashMap<String, String>) -> Option<ReplicationState> {
    let mut rs = ReplicationState::default();
    let mut has = false;

    for (k, v) in metadata.iter() {
        if has_internal_suffix(k, SUFFIX_PURGESTATUS) {
            rs.version_purge_status_internal = Some(v.clone());
            rs.purge_targets = version_purge_statuses_map(v.as_str());
            has = true;
            continue;
        }

        let sub_key_opt = strip_internal_prefix(k);
        if let Some(ref sub_key) = sub_key_opt {
            match sub_key.as_str() {
                "replica-timestamp" => {
                    has = true;
                    rs.replica_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
                }
                "replica-status" => {
                    has = true;
                    rs.replica_status = ReplicationStatusType::from(v.as_str());
                }
                "replication-timestamp" => {
                    has = true;
                    rs.replication_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH))
                }
                "replication-status" => {
                    has = true;
                    rs.replication_status_internal = Some(v.clone());
                    rs.targets = replication_statuses_map(v.as_str());
                }
                _ => {
                    if let Some(arn) = sub_key.strip_prefix("replication-reset-") {
                        has = true;
                        rs.reset_statuses_map.insert(arn.to_string(), v.clone());
                    }
                }
            }
        }
    }

    if has { Some(rs) } else { None }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct MetaDeleteMarker {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID for delete marker
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object delete marker modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Delete marker internal metadata
}

impl TryFrom<LegacyMetaV2DeleteMarker> for MetaDeleteMarker {
    type Error = Error;

    fn try_from(value: LegacyMetaV2DeleteMarker) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            version_id: parse_legacy_uuid_bytes(&value.version_id, "version_id")?,
            mod_time: value.mod_time,
            meta_sys: value.meta_sys,
        })
    }
}

impl MetaDeleteMarker {
    pub fn free_version(&self) -> bool {
        contains_key_bytes(&self.meta_sys, SUFFIX_FREE_VERSION)
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, _all_parts: bool) -> FileInfo {
        let metadata = self
            .meta_sys
            .clone()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
            .collect();
        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut fi = FileInfo {
            version_id: self.version_id.filter(|&vid| !vid.is_nil()),
            name: path.to_string(),
            volume: volume.to_string(),
            deleted: true,
            mod_time: self.mod_time,
            metadata,
            replication_state_internal,
            ..Default::default()
        };

        if self.free_version() {
            fi.set_tier_free_version();
            fi.transition_tier = get_bytes(&self.meta_sys, SUFFIX_TRANSITION_TIER)
                .map(|v| String::from_utf8_lossy(&v).to_string())
                .unwrap_or_default();

            fi.transitioned_objname = get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_OBJECTNAME)
                .map(|v| String::from_utf8_lossy(&v).to_string())
                .unwrap_or_default();

            fi.transition_version_id = get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID)
                .map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        }

        fi
    }

    pub fn encode_to<W: std::io::Write>(&self, wr: &mut W) -> Result<()> {
        rmp::encode::write_map_len(wr, 3)?;

        // ID
        rmp::encode::write_str(wr, "ID")?;
        rmp::encode::write_bin(wr, self.version_id.unwrap_or_default().as_bytes())?;

        // MTime Unix timestamp nanos
        rmp::encode::write_str(wr, "MTime")?;
        let nanos = self.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp_nanos();
        rmp::encode::write_sint(wr, nanos as i64)?;

        // MetaSys
        rmp::encode::write_str(wr, "MetaSys")?;
        rmp::encode::write_map_len(wr, self.meta_sys.len() as u32)?;
        for (k, v) in &self.meta_sys {
            rmp::encode::write_str(wr, k)?;
            rmp::encode::write_bin(wr, v)?;
        }

        Ok(())
    }

    pub fn decode_from<R: std::io::Read>(&mut self, rd: &mut R) -> Result<()> {
        let mut fields = rmp::decode::read_map_len(rd)?;
        *self = MetaDeleteMarker::default();

        while fields > 0 {
            fields -= 1;

            let key_len = rmp::decode::read_str_len(rd)?;
            let mut key_buf = vec![0u8; key_len as usize];
            rd.read_exact(&mut key_buf)?;
            let key = String::from_utf8(key_buf)?;

            match key.as_str() {
                "ID" => {
                    let _ = rmp::decode::read_bin_len(rd)?;
                    let mut buf = [0u8; 16];
                    rd.read_exact(&mut buf)?;
                    let id = Uuid::from_bytes(buf);
                    self.version_id = if id.is_nil() { None } else { Some(id) };
                }
                "MTime" => {
                    let nanos: i64 = rmp::decode::read_int(rd)?;
                    let time = OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)?;
                    self.mod_time = if time == OffsetDateTime::UNIX_EPOCH {
                        None
                    } else {
                        Some(time)
                    };
                }
                "MetaSys" => {
                    let len = rmp::decode::read_map_len(rd)? as usize;
                    self.meta_sys.clear();
                    for _ in 0..len {
                        let k_len = rmp::decode::read_str_len(rd)?;
                        let mut kbuf = vec![0u8; k_len as usize];
                        rd.read_exact(&mut kbuf)?;
                        let k = String::from_utf8(kbuf)?;

                        let blen = rmp::decode::read_bin_len(rd)? as usize;
                        let mut v = vec![0u8; blen];
                        rd.read_exact(&mut v)?;

                        self.meta_sys.insert(k, v);
                    }
                }
                other => {
                    return Err(Error::other(format!("unsupported field in MetaDeleteMarker: {other}")));
                }
            }
        }

        Ok(())
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = std::io::Cursor::new(buf);
        self.decode_from(&mut cur)?;
        Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        self.encode_to(&mut wr)?;
        Ok(wr)
    }

    /// Get delete marker signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }
}

impl From<FileInfo> for MetaDeleteMarker {
    fn from(value: FileInfo) -> Self {
        Self {
            version_id: value.version_id,
            mod_time: value.mod_time,
            meta_sys: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone, PartialOrd, Ord, Hash)]
pub enum VersionType {
    #[default]
    Invalid = 0,
    Object = 1,
    Delete = 2,
    Legacy = 3,
}

impl VersionType {
    pub fn valid(&self) -> bool {
        matches!(*self, VersionType::Object | VersionType::Delete | VersionType::Legacy)
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            VersionType::Invalid => 0,
            VersionType::Object => 1,
            VersionType::Delete => 2,
            VersionType::Legacy => 3,
        }
    }

    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => VersionType::Object,
            2 => VersionType::Delete,
            3 => VersionType::Legacy,
            _ => VersionType::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum ChecksumAlgo {
    #[default]
    Invalid = 0,
    HighwayHash = 1,
}

impl ChecksumAlgo {
    pub fn valid(&self) -> bool {
        *self > ChecksumAlgo::Invalid
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ChecksumAlgo::Invalid => 0,
            ChecksumAlgo::HighwayHash => 1,
        }
    }
    pub fn from_u8(u: u8) -> Self {
        match u {
            1 => ChecksumAlgo::HighwayHash,
            _ => ChecksumAlgo::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum Flags {
    #[default]
    FreeVersion = 1 << 0,
    UsesDataDir = 1 << 1,
    InlineData = 1 << 2,
}

// mergeXLV2Versions
pub fn merge_file_meta_versions(
    mut quorum: usize,
    mut strict: bool,
    requested_versions: usize,
    versions: &[Vec<FileMetaShallowVersion>],
) -> Vec<FileMetaShallowVersion> {
    if quorum == 0 {
        quorum = 1;
    }

    if versions.len() < quorum || versions.is_empty() {
        return Vec::new();
    }

    if versions.len() == 1 {
        return versions[0].clone();
    }

    if quorum == 1 {
        strict = true;
    }

    let mut versions = versions.to_owned();

    let mut n_versions = 0;

    let mut merged = Vec::new();
    loop {
        let mut tops = Vec::new();
        let mut top_sig = FileMetaVersionHeader::default();
        let mut consistent = true;
        for vers in versions.iter() {
            if vers.is_empty() {
                consistent = false;
                continue;
            }
            if tops.is_empty() {
                consistent = true;
                top_sig = vers[0].header.clone();
            } else {
                consistent = consistent && vers[0].header == top_sig;
            }
            tops.push(vers[0].clone());
        }

        // check if done...
        if tops.len() < quorum {
            break;
        }

        let mut latest = FileMetaShallowVersion::default();
        if consistent {
            merged.push(tops[0].clone());
            if !tops[0].header.free_version() {
                n_versions += 1;
            }
        } else {
            let mut latest_count = 0;
            for (i, ver) in tops.iter().enumerate() {
                if ver.header == latest.header {
                    latest_count += 1;
                    continue;
                }

                if i == 0 || ver.header.sorts_before(&latest.header) {
                    if i == 0 || latest_count == 0 {
                        latest_count = 1;
                    } else if !strict && ver.header.matches_not_strict(&latest.header) {
                        latest_count += 1;
                    } else {
                        latest_count = 1;
                    }
                    latest = ver.clone();
                    continue;
                }

                // Mismatch, but older.
                if latest_count > 0 && !strict && ver.header.matches_not_strict(&latest.header) {
                    latest_count += 1;
                    continue;
                }

                if latest_count > 0 && ver.header.version_id == latest.header.version_id {
                    let mut x: HashMap<FileMetaVersionHeader, usize> = HashMap::new();
                    for a in tops.iter() {
                        if a.header.version_id != ver.header.version_id {
                            continue;
                        }
                        let mut a_clone = a.clone();
                        if !strict {
                            a_clone.header.signature = [0; 4];
                        }
                        *x.entry(a_clone.header).or_insert(0) += 1;
                    }
                    latest_count = 0;
                    for (k, v) in x.iter() {
                        if *v < latest_count {
                            continue;
                        }
                        if *v == latest_count && latest.header.sorts_before(k) {
                            continue;
                        }
                        tops.iter().for_each(|a| {
                            let mut hdr = a.header.clone();
                            if !strict {
                                hdr.signature = [0; 4];
                            }
                            if hdr == *k {
                                latest = a.clone();
                            }
                        });

                        latest_count = *v;
                    }
                    break;
                }
            }
            if latest_count >= quorum {
                if !latest.header.free_version() {
                    n_versions += 1;
                }
                merged.push(latest.clone());
            }
        }

        // Remove from all streams up until latest modtime or if selected.
        versions.iter_mut().for_each(|vers| {
            // // Keep top entry (and remaining)...
            let mut bre = false;
            vers.retain(|ver| {
                if bre {
                    return true;
                }
                if let Ordering::Greater = ver.header.mod_time.cmp(&latest.header.mod_time) {
                    bre = true;
                    return false;
                }
                if ver.header == latest.header {
                    bre = true;
                    return false;
                }
                if let Ordering::Equal = latest.header.version_id.cmp(&ver.header.version_id) {
                    bre = true;
                    return false;
                }
                for merged_v in merged.iter() {
                    if let Ordering::Equal = ver.header.version_id.cmp(&merged_v.header.version_id) {
                        bre = true;
                        return false;
                    }
                }
                true
            });
        });
        if requested_versions > 0 && requested_versions == n_versions {
            merged.append(&mut versions[0]);
            break;
        }
    }

    // Sanity check. Enable if duplicates show up.
    // todo
    merged
}

pub fn file_info_from_raw(
    ri: RawFileInfo,
    bucket: &str,
    object: &str,
    read_data: bool,
    include_free_versions: bool,
) -> Result<FileInfo> {
    get_file_info(
        &ri.buf,
        bucket,
        object,
        "",
        FileInfoOpts {
            data: read_data,
            include_free_versions,
        },
    )
}

pub struct FileInfoOpts {
    pub data: bool,
    pub include_free_versions: bool,
}

pub fn get_file_info(buf: &[u8], volume: &str, path: &str, version_id: &str, opts: FileInfoOpts) -> Result<FileInfo> {
    let vid = {
        if version_id.is_empty() {
            None
        } else {
            Some(Uuid::parse_str(version_id)?)
        }
    };

    let meta = FileMeta::load(buf)?;
    if meta.versions.is_empty() {
        return Ok(FileInfo {
            volume: volume.to_owned(),
            name: path.to_owned(),
            version_id: vid,
            is_latest: true,
            deleted: true,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1)?),
            ..Default::default()
        });
    }

    let fi = meta.into_fileinfo(volume, path, version_id, opts.data, opts.include_free_versions, true)?;
    Ok(fi)
}

async fn read_more<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    total_size: usize,
    read_size: usize,
    has_full: bool,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let has = buf.len();

    if has >= read_size {
        return Ok(());
    }

    if has_full || read_size > total_size {
        return Err(Error::other(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF")));
    }

    let extra = read_size - has;
    if buf.capacity() >= read_size {
        // Extend the buffer if we have enough space.
        buf.resize(read_size, 0);
    } else {
        buf.extend(vec![0u8; extra]);
    }

    reader.read_exact(&mut buf[has..]).await?;
    Ok(())
}

pub async fn read_xl_meta_no_data<R: AsyncRead + Unpin>(reader: &mut R, size: usize) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut initial = size;
    let mut has_full = true;

    if initial > META_DATA_READ_DEFAULT {
        initial = META_DATA_READ_DEFAULT;
        has_full = false;
    }

    let mut buf = vec![0u8; initial];
    reader.read_exact(&mut buf).await?;

    let (tmp_buf, major, minor) = FileMeta::check_xl2_v1(&buf)?;

    match major {
        1 => match minor {
            0 => {
                read_more(reader, &mut buf, size, size, has_full).await?;
                Ok(buf)
            }
            1..=3 => {
                let (sz, tmp_buf) = FileMeta::read_bytes_header(tmp_buf)?;
                let mut want = sz as usize + (buf.len() - tmp_buf.len());

                if minor < 2 {
                    read_more(reader, &mut buf, size, want, has_full).await?;
                    buf.truncate(want);
                    return Ok(buf);
                }

                let want_max = usize::min(want + MSGP_UINT32_SIZE, size);
                read_more(reader, &mut buf, size, want_max, has_full).await?;

                if buf.len() < want {
                    return Err(Error::FileCorrupt);
                }

                let tmp = &buf[want..];
                let crc_size = 5;
                let other_size = tmp.len() - crc_size;

                want += tmp.len() - other_size;

                buf.truncate(want);
                Ok(buf)
            }
            _ => Err(Error::other(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown minor metadata version",
            ))),
        },
        _ => Err(Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unknown major metadata version",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    enum LegacyDeleteVersionTypeFixture {
        #[serde(rename = "DeleteMarker")]
        DeleteMarker,
    }

    #[derive(Serialize)]
    struct LegacyDeleteMarkerFixture {
        version_id: Vec<u8>,
        mod_time: Option<OffsetDateTime>,
        meta_sys: HashMap<String, Vec<u8>>,
    }

    #[derive(Serialize)]
    struct LegacyDeleteVersionFixture {
        version_type: LegacyDeleteVersionTypeFixture,
        object: Option<()>,
        delete_marker: Option<LegacyDeleteMarkerFixture>,
        write_version: u64,
    }

    #[derive(Serialize)]
    struct LegacyDeleteMarkerNilFixture {
        version_id: Option<Vec<u8>>,
        mod_time: Option<OffsetDateTime>,
        meta_sys: HashMap<String, Vec<u8>>,
    }

    #[derive(Serialize)]
    struct LegacyDeleteVersionNilFixture {
        version_type: LegacyDeleteVersionTypeFixture,
        object: Option<()>,
        delete_marker: Option<LegacyDeleteMarkerNilFixture>,
        write_version: u64,
    }

    #[derive(Serialize)]
    enum LegacyObjectVersionTypeFixture {
        #[serde(rename = "Object")]
        Object,
    }

    #[derive(Serialize)]
    struct LegacyObjectFixture {
        version_id: Option<Vec<u8>>,
        data_dir: Option<Vec<u8>>,
        erasure_algorithm: String,
        erasure_m: usize,
        erasure_n: usize,
        erasure_block_size: usize,
        erasure_index: usize,
        erasure_dist: Vec<u8>,
        bitrot_checksum_algo: String,
        part_numbers: Vec<usize>,
        part_etags: Vec<String>,
        part_sizes: Vec<usize>,
        part_actual_sizes: Vec<i64>,
        part_indices: Vec<Vec<u8>>,
        size: i64,
        mod_time: Option<OffsetDateTime>,
        meta_sys: HashMap<String, Vec<u8>>,
        meta_user: HashMap<String, String>,
    }

    #[derive(Serialize)]
    struct LegacyObjectVersionFixture {
        version_type: LegacyObjectVersionTypeFixture,
        object: Option<LegacyObjectFixture>,
        delete_marker: Option<()>,
        write_version: u64,
    }

    fn sample_version_id() -> Uuid {
        Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap()
    }

    fn sample_mod_time() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(1_705_312_200_123_456_789).unwrap()
    }

    fn sample_header() -> FileMetaVersionHeader {
        FileMetaVersionHeader {
            version_id: Some(sample_version_id()),
            mod_time: Some(sample_mod_time()),
            signature: [0x96, 0x33, 0x4c, 0x78],
            version_type: VersionType::Object,
            flags: 0x06,
            ec_n: 4,
            ec_m: 2,
        }
    }

    fn encode_v1_header(header: &FileMetaVersionHeader) -> Vec<u8> {
        let mut wr = Vec::new();
        rmp::encode::write_array_len(&mut wr, 4).unwrap();
        rmp::encode::write_bin(&mut wr, header.version_id.unwrap().as_bytes()).unwrap();
        rmp::encode::write_i64(&mut wr, header.mod_time.unwrap().unix_timestamp_nanos() as i64).unwrap();
        rmp::encode::write_uint8(&mut wr, header.version_type.to_u8()).unwrap();
        rmp::encode::write_uint8(&mut wr, header.flags).unwrap();
        wr
    }

    fn encode_v2_header(header: &FileMetaVersionHeader) -> Vec<u8> {
        let mut wr = Vec::new();
        rmp::encode::write_array_len(&mut wr, 5).unwrap();
        rmp::encode::write_bin(&mut wr, header.version_id.unwrap().as_bytes()).unwrap();
        rmp::encode::write_i64(&mut wr, header.mod_time.unwrap().unix_timestamp_nanos() as i64).unwrap();
        rmp::encode::write_bin(&mut wr, header.signature.as_slice()).unwrap();
        rmp::encode::write_uint8(&mut wr, header.version_type.to_u8()).unwrap();
        rmp::encode::write_uint8(&mut wr, header.flags).unwrap();
        wr
    }

    fn write_legacy_time(wr: &mut Vec<u8>, ts: OffsetDateTime) {
        wr.push(MSGPACK_EXT8);
        wr.push(12);
        wr.push(MSGPACK_TIME_EXT_LEGACY as u8);
        wr.extend_from_slice(&ts.unix_timestamp().to_be_bytes());
        wr.extend_from_slice(&ts.nanosecond().to_be_bytes());
    }

    fn encode_legacy_v1_body() -> Vec<u8> {
        let mut wr = Vec::new();
        let mod_time = sample_mod_time();

        rmp::encode::write_map_len(&mut wr, 3).unwrap();

        rmp::encode::write_str(&mut wr, "Type").unwrap();
        rmp::encode::write_uint8(&mut wr, VersionType::Legacy.to_u8()).unwrap();

        rmp::encode::write_str(&mut wr, "V1Obj").unwrap();
        rmp::encode::write_map_len(&mut wr, 8).unwrap();

        rmp::encode::write_str(&mut wr, "Version").unwrap();
        rmp::encode::write_str(&mut wr, "1.0.1").unwrap();
        rmp::encode::write_str(&mut wr, "Format").unwrap();
        rmp::encode::write_str(&mut wr, "xl").unwrap();

        rmp::encode::write_str(&mut wr, "Stat").unwrap();
        rmp::encode::write_map_len(&mut wr, 5).unwrap();
        rmp::encode::write_str(&mut wr, "Size").unwrap();
        rmp::encode::write_sint(&mut wr, 11).unwrap();
        rmp::encode::write_str(&mut wr, "ModTime").unwrap();
        write_legacy_time(&mut wr, mod_time);
        rmp::encode::write_str(&mut wr, "Name").unwrap();
        rmp::encode::write_str(&mut wr, "hello.txt").unwrap();
        rmp::encode::write_str(&mut wr, "Dir").unwrap();
        rmp::encode::write_bool(&mut wr, false).unwrap();
        rmp::encode::write_str(&mut wr, "Mode").unwrap();
        rmp::encode::write_u32(&mut wr, 0o644).unwrap();

        rmp::encode::write_str(&mut wr, "Erasure").unwrap();
        rmp::encode::write_map_len(&mut wr, 7).unwrap();
        rmp::encode::write_str(&mut wr, "Algorithm").unwrap();
        rmp::encode::write_str(&mut wr, "ReedSolomon").unwrap();
        rmp::encode::write_str(&mut wr, "DataBlocks").unwrap();
        rmp::encode::write_sint(&mut wr, 4).unwrap();
        rmp::encode::write_str(&mut wr, "ParityBlocks").unwrap();
        rmp::encode::write_sint(&mut wr, 2).unwrap();
        rmp::encode::write_str(&mut wr, "BlockSize").unwrap();
        rmp::encode::write_sint(&mut wr, 1_048_576).unwrap();
        rmp::encode::write_str(&mut wr, "Index").unwrap();
        rmp::encode::write_sint(&mut wr, 1).unwrap();
        rmp::encode::write_str(&mut wr, "Distribution").unwrap();
        rmp::encode::write_array_len(&mut wr, 6).unwrap();
        for value in 1..=6 {
            rmp::encode::write_sint(&mut wr, value).unwrap();
        }
        rmp::encode::write_str(&mut wr, "Checksums").unwrap();
        rmp::encode::write_array_len(&mut wr, 0).unwrap();

        rmp::encode::write_str(&mut wr, "Meta").unwrap();
        rmp::encode::write_map_len(&mut wr, 1).unwrap();
        rmp::encode::write_str(&mut wr, "content-type").unwrap();
        rmp::encode::write_str(&mut wr, "text/plain").unwrap();

        rmp::encode::write_str(&mut wr, "Parts").unwrap();
        rmp::encode::write_array_len(&mut wr, 1).unwrap();
        rmp::encode::write_map_len(&mut wr, 5).unwrap();
        rmp::encode::write_str(&mut wr, "e").unwrap();
        rmp::encode::write_str(&mut wr, "etag-1").unwrap();
        rmp::encode::write_str(&mut wr, "n").unwrap();
        rmp::encode::write_sint(&mut wr, 1).unwrap();
        rmp::encode::write_str(&mut wr, "s").unwrap();
        rmp::encode::write_sint(&mut wr, 11).unwrap();
        rmp::encode::write_str(&mut wr, "as").unwrap();
        rmp::encode::write_sint(&mut wr, 11).unwrap();
        rmp::encode::write_str(&mut wr, "mt").unwrap();
        write_legacy_time(&mut wr, mod_time);

        rmp::encode::write_str(&mut wr, "VersionID").unwrap();
        rmp::encode::write_str(&mut wr, "").unwrap();
        rmp::encode::write_str(&mut wr, "DataDir").unwrap();
        rmp::encode::write_str(&mut wr, "legacy").unwrap();

        rmp::encode::write_str(&mut wr, "v").unwrap();
        rmp::encode::write_uint(&mut wr, 1).unwrap();

        wr
    }

    #[test]
    fn version_header_unmarshal_v1_uses_legacy_layout_defaults() {
        let expected = sample_header();
        let encoded = encode_v1_header(&expected);

        let mut decoded = FileMetaVersionHeader::default();
        decoded.unmarshal_v(1, &encoded).unwrap();

        assert_eq!(decoded.version_id, expected.version_id);
        assert_eq!(decoded.mod_time, expected.mod_time);
        assert_eq!(decoded.version_type, expected.version_type);
        assert_eq!(decoded.flags, expected.flags);
        assert_eq!(decoded.signature, [0; 4]);
        assert_eq!(decoded.ec_n, 0);
        assert_eq!(decoded.ec_m, 0);
    }

    #[test]
    fn version_header_unmarshal_v2_keeps_signature_and_zeroes_ec() {
        let expected = sample_header();
        let encoded = encode_v2_header(&expected);

        let mut decoded = FileMetaVersionHeader::default();
        decoded.unmarshal_v(2, &encoded).unwrap();

        assert_eq!(decoded.version_id, expected.version_id);
        assert_eq!(decoded.mod_time, expected.mod_time);
        assert_eq!(decoded.signature, expected.signature);
        assert_eq!(decoded.version_type, expected.version_type);
        assert_eq!(decoded.flags, expected.flags);
        assert_eq!(decoded.ec_n, 0);
        assert_eq!(decoded.ec_m, 0);
    }

    #[test]
    fn version_header_unmarshal_v3_round_trips_current_layout() {
        let expected = sample_header();
        let encoded = expected.marshal_msg().unwrap();

        let mut decoded = FileMetaVersionHeader::default();
        decoded.unmarshal_v(3, &encoded).unwrap();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn legacy_v1_object_body_decodes_into_fileinfo() {
        let encoded = encode_legacy_v1_body();
        let decoded = FileMetaVersion::try_from(encoded.as_slice()).unwrap();

        assert_eq!(decoded.version_type, VersionType::Legacy);
        assert!(decoded.valid());
        assert!(decoded.legacy_object.is_some());

        let fi = decoded.into_fileinfo("bucket", "hello.txt", true);
        assert_eq!(fi.volume, "bucket");
        assert_eq!(fi.name, "hello.txt");
        assert_eq!(fi.size, 11);
        assert_eq!(fi.mod_time, Some(sample_mod_time()));
        assert_eq!(fi.mode, Some(0o644));
        assert_eq!(fi.parts.len(), 1);
        assert_eq!(fi.parts[0].etag, "etag-1");
        assert_eq!(fi.parts[0].size, 11);
        assert_eq!(fi.erasure.data_blocks, 4);
        assert_eq!(fi.erasure.parity_blocks, 2);
        assert_eq!(fi.metadata.get("content-type").map(String::as_str), Some("text/plain"));
    }

    #[test]
    fn legacy_meta_v2_delete_marker_decodes_into_delete_fileinfo() {
        let payload = LegacyDeleteVersionFixture {
            version_type: LegacyDeleteVersionTypeFixture::DeleteMarker,
            object: None,
            delete_marker: Some(LegacyDeleteMarkerFixture {
                version_id: sample_version_id().as_bytes().to_vec(),
                mod_time: Some(sample_mod_time()),
                meta_sys: HashMap::from([("x-rustfs-test".to_string(), b"gone".to_vec())]),
            }),
            write_version: 9,
        };
        let encoded = rmp_serde::to_vec_named(&payload).unwrap();

        let decoded = FileMetaVersion::try_from(encoded.as_slice()).unwrap();

        assert_eq!(decoded.version_type, VersionType::Delete);
        assert!(decoded.object.is_none());
        assert!(decoded.delete_marker.is_some());
        assert!(decoded.uses_legacy_checksum);

        let fi = decoded.into_fileinfo("bucket", "gone.txt", true);
        assert!(fi.deleted);
        assert_eq!(fi.volume, "bucket");
        assert_eq!(fi.name, "gone.txt");
        assert_eq!(fi.version_id, Some(sample_version_id()));
        assert_eq!(fi.mod_time, Some(sample_mod_time()));
        assert_eq!(fi.metadata.get("x-rustfs-test").map(String::as_str), Some("gone"));
        assert!(fi.uses_legacy_checksum);
    }

    #[test]
    fn legacy_meta_v2_delete_marker_decodes_into_delete_fileinfo_via_struct() {
        let version_id = sample_version_id();
        let mod_time = sample_mod_time();
        let version = LegacyMetaV2Version {
            version_type: LegacyMetaV2VersionType::DeleteMarker,
            object: None,
            delete_marker: Some(LegacyMetaV2DeleteMarker {
                version_id: version_id.as_bytes().to_vec(),
                mod_time: Some(mod_time),
                meta_sys: HashMap::from([("x-minio-internal".to_string(), b"present".to_vec())]),
            }),
            write_version: 7,
        };

        let decoded = FileMetaVersion::try_from(version).unwrap();

        assert_eq!(decoded.version_type, VersionType::Delete);
        assert!(decoded.uses_legacy_checksum);
        assert!(decoded.object.is_none());

        let delete_marker = decoded.delete_marker.as_ref().expect("delete marker should be decoded");
        assert_eq!(delete_marker.version_id, Some(version_id));
        assert_eq!(delete_marker.mod_time, Some(mod_time));

        let fi = decoded.into_fileinfo("bucket", "deleted.txt", true);
        assert!(fi.deleted);
        assert_eq!(fi.version_id, Some(version_id));
        assert_eq!(fi.mod_time, Some(mod_time));
        assert_eq!(fi.metadata.get("x-minio-internal").map(String::as_str), Some("present"));
    }

    #[test]
    fn legacy_meta_v2_delete_marker_rejects_invalid_uuid_bytes() {
        let payload = LegacyDeleteVersionFixture {
            version_type: LegacyDeleteVersionTypeFixture::DeleteMarker,
            object: None,
            delete_marker: Some(LegacyDeleteMarkerFixture {
                version_id: vec![7; 15],
                mod_time: Some(sample_mod_time()),
                meta_sys: HashMap::new(),
            }),
            write_version: 10,
        };
        let encoded = rmp_serde::to_vec_named(&payload).unwrap();

        let err = FileMetaVersion::try_from(encoded.as_slice()).expect_err("invalid legacy delete marker UUID must fail");
        assert!(err.to_string().contains("legacy version_id must be 16 bytes"));
    }

    #[test]
    fn legacy_meta_v2_delete_marker_rejects_invalid_uuid_bytes_via_struct() {
        let err = MetaDeleteMarker::try_from(LegacyMetaV2DeleteMarker {
            version_id: vec![1, 2, 3],
            mod_time: Some(sample_mod_time()),
            meta_sys: HashMap::new(),
        })
        .expect_err("invalid legacy delete-marker version ids should be rejected");

        assert!(err.to_string().contains("legacy version_id must be 16 bytes"));
    }

    #[test]
    fn legacy_meta_v2_object_accepts_nil_uuid_fields() {
        let payload = LegacyObjectVersionFixture {
            version_type: LegacyObjectVersionTypeFixture::Object,
            object: Some(LegacyObjectFixture {
                version_id: None,
                data_dir: None,
                erasure_algorithm: "ReedSolomon".to_string(),
                erasure_m: 2,
                erasure_n: 4,
                erasure_block_size: 1_048_576,
                erasure_index: 1,
                erasure_dist: vec![1, 2, 3, 4, 5, 6],
                bitrot_checksum_algo: "HighwayHash".to_string(),
                part_numbers: vec![1],
                part_etags: vec!["etag-1".to_string()],
                part_sizes: vec![11],
                part_actual_sizes: vec![11],
                part_indices: vec![Vec::new()],
                size: 11,
                mod_time: Some(sample_mod_time()),
                meta_sys: HashMap::new(),
                meta_user: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            }),
            delete_marker: None,
            write_version: 3,
        };
        let encoded = rmp_serde::to_vec_named(&payload).unwrap();

        let decoded = FileMetaVersion::try_from(encoded.as_slice()).unwrap();
        let object = decoded.object.as_ref().expect("object should be decoded");

        assert_eq!(decoded.version_type, VersionType::Object);
        assert!(decoded.uses_legacy_checksum);
        assert_eq!(object.version_id, None);
        assert_eq!(object.data_dir, None);

        let fi = decoded.into_fileinfo("bucket", "legacy-nil.txt", true);
        assert_eq!(fi.version_id, None);
        assert_eq!(fi.data_dir, None);
        assert_eq!(fi.metadata.get("content-type").map(String::as_str), Some("text/plain"));
    }

    #[test]
    fn legacy_meta_v2_delete_marker_accepts_nil_version_id() {
        let payload = LegacyDeleteVersionNilFixture {
            version_type: LegacyDeleteVersionTypeFixture::DeleteMarker,
            object: None,
            delete_marker: Some(LegacyDeleteMarkerNilFixture {
                version_id: None,
                mod_time: Some(sample_mod_time()),
                meta_sys: HashMap::from([("x-rustfs-test".to_string(), b"gone".to_vec())]),
            }),
            write_version: 11,
        };
        let encoded = rmp_serde::to_vec_named(&payload).unwrap();

        let decoded = FileMetaVersion::try_from(encoded.as_slice()).unwrap();
        let delete_marker = decoded.delete_marker.as_ref().expect("delete marker should be decoded");

        assert_eq!(decoded.version_type, VersionType::Delete);
        assert!(decoded.uses_legacy_checksum);
        assert_eq!(delete_marker.version_id, None);
        assert_eq!(delete_marker.mod_time, Some(sample_mod_time()));

        let fi = decoded.into_fileinfo("bucket", "deleted.txt", true);
        assert!(fi.deleted);
        assert_eq!(fi.version_id, None);
        assert_eq!(fi.mod_time, Some(sample_mod_time()));
        assert_eq!(fi.metadata.get("x-rustfs-test").map(String::as_str), Some("gone"));
    }
}
