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

use byteorder::{ByteOrder, LittleEndian};

use crate::{Error, Result};

pub use crate::filemeta::{MrfOpKind, MrfReplicateEntry};

pub const MRF_META_FORMAT: u16 = 1;
pub const MRF_META_VERSION: u16 = 1;

pub const MRF_V2_NAMESPACE: &str = "config/replication-v2";
pub const MRF_V2_FILE: &str = "config/replication-v2/mrf.bin";
pub const MRF_V2_FORMAT: u16 = 2;
pub const MRF_V2_VERSION: u16 = 2;

const MRF_V2_MAGIC: [u8; 4] = *b"MRF2";
const MRF_V2_HEADER_LEN: usize = 24;
const MRF_V2_KNOWN_CAPABILITIES: u64 = 0b1111;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MrfV2Error {
    InvalidMagic,
    Truncated,
    UnsupportedNamespace { namespace: String },
    UnsupportedFormat { format: u16 },
    UnsupportedVersion { version: u16 },
    InvalidVersionRange { version: u16, min_reader_version: u16 },
    RollbackFenced { min_reader_version: u16, reader_version: u16 },
    ReservedHeaderBits { bits: u16 },
    UnknownCapabilities { bits: u64 },
    MissingCapabilities { required: u64, available: u64 },
    PayloadLengthMismatch { declared: u32, actual: usize },
    WriterEnabled,
}

impl std::fmt::Display for MrfV2Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid MRF v2 magic"),
            Self::Truncated => write!(f, "truncated MRF v2 envelope"),
            Self::UnsupportedNamespace { namespace } => write!(f, "unsupported MRF v2 namespace {namespace}"),
            Self::UnsupportedFormat { format } => write!(f, "unsupported MRF v2 format {format}"),
            Self::UnsupportedVersion { version } => write!(f, "unsupported MRF v2 version {version}"),
            Self::InvalidVersionRange {
                version,
                min_reader_version,
            } => {
                write!(f, "invalid MRF v2 version range: version {version}, minimum reader {min_reader_version}")
            }
            Self::RollbackFenced {
                min_reader_version,
                reader_version,
            } => write!(
                f,
                "MRF v2 rollback fenced: reader version {reader_version} is below required {min_reader_version}"
            ),
            Self::ReservedHeaderBits { bits } => write!(f, "reserved MRF v2 header bits are set: 0x{bits:04x}"),
            Self::UnknownCapabilities { bits } => write!(f, "unknown MRF v2 capability bits 0x{bits:016x}"),
            Self::MissingCapabilities { required, available } => {
                write!(f, "MRF v2 capabilities 0x{available:016x} do not satisfy required 0x{required:016x}")
            }
            Self::PayloadLengthMismatch { declared, actual } => {
                write!(f, "MRF v2 payload is {actual} bytes, expected {declared}")
            }
            Self::WriterEnabled => write!(f, "MRF v2 writer must remain dormant"),
        }
    }
}

impl std::error::Error for MrfV2Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MrfV2Capabilities(u64);

impl MrfV2Capabilities {
    pub const fn current() -> Self {
        Self(MRF_V2_KNOWN_CAPABILITIES)
    }

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub fn from_bits(bits: u64) -> std::result::Result<Self, MrfV2Error> {
        let unknown = bits & !MRF_V2_KNOWN_CAPABILITIES;
        if unknown != 0 {
            return Err(MrfV2Error::UnknownCapabilities { bits: unknown });
        }
        Ok(Self(bits))
    }

    pub const fn supports(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MrfV2Readiness {
    reader_version: u16,
    capabilities: MrfV2Capabilities,
    writer_enabled: bool,
}

impl MrfV2Readiness {
    pub const fn dormant() -> Self {
        Self {
            reader_version: MRF_V2_VERSION,
            capabilities: MrfV2Capabilities::current(),
            writer_enabled: false,
        }
    }

    pub const fn new(reader_version: u16, capabilities: MrfV2Capabilities, writer_enabled: bool) -> Self {
        Self {
            reader_version,
            capabilities,
            writer_enabled,
        }
    }

    pub const fn writer_enabled(self) -> bool {
        self.writer_enabled
    }

    pub fn reader(self) -> std::result::Result<MrfV2Reader, MrfV2Error> {
        if self.writer_enabled {
            return Err(MrfV2Error::WriterEnabled);
        }
        if self.reader_version != MRF_V2_VERSION {
            return Err(MrfV2Error::UnsupportedVersion {
                version: self.reader_version,
            });
        }
        MrfV2Capabilities::from_bits(self.capabilities.bits())?;
        Ok(MrfV2Reader { readiness: self })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MrfV2Reader {
    readiness: MrfV2Readiness,
}

impl MrfV2Reader {
    pub fn read(self, namespace: &str, data: &[u8]) -> std::result::Result<MrfV2Envelope, MrfV2Error> {
        if namespace != MRF_V2_NAMESPACE {
            return Err(MrfV2Error::UnsupportedNamespace {
                namespace: namespace.to_string(),
            });
        }
        if data.len() < MRF_V2_HEADER_LEN {
            return Err(MrfV2Error::Truncated);
        }
        if data[..4] != MRF_V2_MAGIC {
            return Err(MrfV2Error::InvalidMagic);
        }
        let format = LittleEndian::read_u16(&data[4..6]);
        if format != MRF_V2_FORMAT {
            return Err(MrfV2Error::UnsupportedFormat { format });
        }
        let version = LittleEndian::read_u16(&data[6..8]);
        if version != MRF_V2_VERSION || version != self.readiness.reader_version {
            return Err(MrfV2Error::UnsupportedVersion { version });
        }
        let min_reader_version = LittleEndian::read_u16(&data[8..10]);
        if min_reader_version > self.readiness.reader_version {
            return Err(MrfV2Error::RollbackFenced {
                min_reader_version,
                reader_version: self.readiness.reader_version,
            });
        }
        if min_reader_version > version {
            return Err(MrfV2Error::InvalidVersionRange {
                version,
                min_reader_version,
            });
        }
        let reserved = LittleEndian::read_u16(&data[10..12]);
        if reserved != 0 {
            return Err(MrfV2Error::ReservedHeaderBits { bits: reserved });
        }
        let capabilities = MrfV2Capabilities::from_bits(LittleEndian::read_u64(&data[12..20]))?;
        if !self.readiness.capabilities.supports(capabilities) {
            return Err(MrfV2Error::MissingCapabilities {
                required: capabilities.bits(),
                available: self.readiness.capabilities.bits(),
            });
        }
        let payload_len = LittleEndian::read_u32(&data[20..24]);
        let actual = data.len() - MRF_V2_HEADER_LEN;
        if usize::try_from(payload_len).map_err(|_| MrfV2Error::PayloadLengthMismatch {
            declared: payload_len,
            actual,
        })? != actual
        {
            return Err(MrfV2Error::PayloadLengthMismatch {
                declared: payload_len,
                actual,
            });
        }
        Ok(MrfV2Envelope {
            version,
            min_reader_version,
            capabilities,
            payload: data[MRF_V2_HEADER_LEN..].to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MrfV2Envelope {
    version: u16,
    min_reader_version: u16,
    capabilities: MrfV2Capabilities,
    payload: Vec<u8>,
}

impl MrfV2Envelope {
    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn min_reader_version(&self) -> u16 {
        self.min_reader_version
    }

    pub const fn capabilities(&self) -> MrfV2Capabilities {
        self.capabilities
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub fn encode_mrf_file(entries: &[MrfReplicateEntry]) -> Result<Vec<u8>> {
    let payload = rmp_serde::to_vec_named(entries).map_err(|e| Error::Other(e.to_string()))?;
    let mut data = Vec::with_capacity(4 + payload.len());
    let mut fmt = [0u8; 2];
    LittleEndian::write_u16(&mut fmt, MRF_META_FORMAT);
    data.extend_from_slice(&fmt);
    let mut ver = [0u8; 2];
    LittleEndian::write_u16(&mut ver, MRF_META_VERSION);
    data.extend_from_slice(&ver);
    data.extend_from_slice(&payload);
    Ok(data)
}

pub fn decode_mrf_file(data: &[u8]) -> Result<Vec<MrfReplicateEntry>> {
    if data.len() <= 4 {
        return Err(Error::CorruptedFormat);
    }
    let mut fmt = [0u8; 2];
    fmt.copy_from_slice(&data[0..2]);
    if LittleEndian::read_u16(&fmt) != MRF_META_FORMAT {
        return Err(Error::CorruptedFormat);
    }
    let mut ver = [0u8; 2];
    ver.copy_from_slice(&data[2..4]);
    if LittleEndian::read_u16(&ver) != MRF_META_VERSION {
        return Err(Error::CorruptedFormat);
    }
    rmp_serde::from_slice(&data[4..]).map_err(|e| Error::Other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn mrf_file_round_trips_object_metadata_and_delete_entries() {
        let obj_vid = Uuid::new_v4();
        let del_vid = Uuid::new_v4();
        let entries = vec![
            MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "metadata-a".to_string(),
                version_id: Some(obj_vid),
                retry_count: 1,
                size: 1024,
                op: MrfOpKind::Metadata,
                force_delete_id: None,
                force_delete_generation: None,
                force_delete_local_commit: false,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: vec!["arn:target-a".to_string()],
            },
            MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "object-a".to_string(),
                version_id: Some(obj_vid),
                retry_count: 2,
                size: 1024,
                op: MrfOpKind::Object,
                force_delete_id: None,
                force_delete_generation: None,
                force_delete_local_commit: false,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: vec!["arn:target-a".to_string(), "arn:target-b".to_string()],
            },
            MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "delete-a".to_string(),
                version_id: None,
                retry_count: 0,
                size: 0,
                op: MrfOpKind::Delete,
                force_delete_id: None,
                force_delete_generation: None,
                force_delete_local_commit: false,
                force_delete: true,
                delete_marker_version_id: Some(del_vid),
                delete_marker: true,
                delete_marker_mtime: Some(1_705_312_200_123_456_789),
                target_arns: vec!["arn:target-a".to_string()],
            },
        ];

        let encoded = encode_mrf_file(&entries).expect("mrf file should encode");
        let decoded = decode_mrf_file(&encoded).expect("mrf file should decode");

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].version_id, Some(obj_vid));
        assert_eq!(decoded[0].op, MrfOpKind::Metadata);
        assert_eq!(decoded[0].target_arns, vec!["arn:target-a".to_string()]);
        assert_eq!(decoded[0].delete_marker_mtime, None);
        assert!(!decoded[0].force_delete);
        assert_eq!(decoded[1].op, MrfOpKind::Object);
        assert!(!decoded[1].force_delete);
        assert_eq!(decoded[1].target_arns, vec!["arn:target-a".to_string(), "arn:target-b".to_string()]);
        assert_eq!(decoded[1].delete_marker_mtime, None);
        assert_eq!(decoded[2].delete_marker_version_id, Some(del_vid));
        assert_eq!(decoded[2].op, MrfOpKind::Delete);
        assert!(decoded[2].force_delete);
        assert_eq!(decoded[2].target_arns, vec!["arn:target-a".to_string()]);
        assert!(decoded[2].delete_marker);
        assert_eq!(
            decoded[2].delete_marker_mtime,
            Some(1_705_312_200_123_456_789),
            "delete-marker mtime must survive the MRF disk round-trip"
        );
    }

    #[test]
    fn mrf_legacy_file_without_op_decodes_as_object() {
        let mut payload = Vec::new();
        rmp::encode::write_array_len(&mut payload, 1).expect("array len should encode");
        rmp::encode::write_map_len(&mut payload, 4).expect("map len should encode");
        rmp::encode::write_str(&mut payload, "bucket").expect("bucket key should encode");
        rmp::encode::write_str(&mut payload, "old-bucket").expect("bucket value should encode");
        rmp::encode::write_str(&mut payload, "object").expect("object key should encode");
        rmp::encode::write_str(&mut payload, "old-key").expect("object value should encode");
        rmp::encode::write_str(&mut payload, "retryCount").expect("retry key should encode");
        rmp::encode::write_i32(&mut payload, 2).expect("retry value should encode");
        rmp::encode::write_str(&mut payload, "size").expect("size key should encode");
        rmp::encode::write_i64(&mut payload, 100).expect("size value should encode");

        let mut data = Vec::with_capacity(4 + payload.len());
        data.extend_from_slice(&MRF_META_FORMAT.to_le_bytes());
        data.extend_from_slice(&MRF_META_VERSION.to_le_bytes());
        data.extend_from_slice(&payload);

        let decoded = decode_mrf_file(&data).expect("legacy mrf file should decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].bucket, "old-bucket");
        assert_eq!(decoded[0].object, "old-key");
        assert_eq!(decoded[0].retry_count, 2);
        assert_eq!(decoded[0].size, 100);
        assert_eq!(decoded[0].op, MrfOpKind::Object);
        assert!(decoded[0].target_arns.is_empty());
        assert!(!decoded[0].force_delete);
        // Old files lack the deleteMarkerMtime key; it must default to None so replay keeps the
        // pre-#867 fallback to the current time.
        assert_eq!(decoded[0].delete_marker_mtime, None);
    }

    #[test]
    fn mrf_file_defaults_missing_force_delete_to_false() {
        let mut payload = Vec::new();
        rmp::encode::write_array_len(&mut payload, 1).expect("array len should encode");
        rmp::encode::write_map_len(&mut payload, 5).expect("map len should encode");
        rmp::encode::write_str(&mut payload, "bucket").expect("bucket key should encode");
        rmp::encode::write_str(&mut payload, "old-bucket").expect("bucket value should encode");
        rmp::encode::write_str(&mut payload, "object").expect("object key should encode");
        rmp::encode::write_str(&mut payload, "old-key").expect("object value should encode");
        rmp::encode::write_str(&mut payload, "retryCount").expect("retry key should encode");
        rmp::encode::write_i32(&mut payload, 1).expect("retry value should encode");
        rmp::encode::write_str(&mut payload, "size").expect("size key should encode");
        rmp::encode::write_i64(&mut payload, 0).expect("size value should encode");
        rmp::encode::write_str(&mut payload, "op").expect("op key should encode");
        rmp::encode::write_str(&mut payload, "delete").expect("op value should encode");

        let mut data = Vec::with_capacity(4 + payload.len());
        data.extend_from_slice(&MRF_META_FORMAT.to_le_bytes());
        data.extend_from_slice(&MRF_META_VERSION.to_le_bytes());
        data.extend_from_slice(&payload);

        let decoded = decode_mrf_file(&data).expect("MRF payload should decode");

        assert_eq!(decoded[0].op, MrfOpKind::Delete);
        assert!(!decoded[0].force_delete);
    }

    #[test]
    fn mrf_file_rejects_invalid_header() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u16.to_le_bytes());
        data.extend_from_slice(&MRF_META_VERSION.to_le_bytes());
        data.push(0x90);

        assert!(matches!(decode_mrf_file(&data), Err(Error::CorruptedFormat)));
    }

    #[test]
    fn v1_file_contract_and_v2_namespace_remain_separate() {
        assert_eq!(encode_mrf_file(&[]).expect("v1 empty file should encode"), vec![1, 0, 1, 0, 0x90]);
        assert_eq!(MRF_V2_FILE, "config/replication-v2/mrf.bin");
        assert_ne!(MRF_V2_FILE, "config/replication/mrf.bin");
    }

    #[test]
    fn dormant_v2_reader_accepts_stable_fixture() {
        let fixture = [
            b'M', b'R', b'F', b'2', 2, 0, 2, 0, 2, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3,
        ];
        let readiness = MrfV2Readiness::dormant();
        assert!(!readiness.writer_enabled());
        let envelope = readiness
            .reader()
            .expect("dormant readiness should expose the reader")
            .read(MRF_V2_NAMESPACE, &fixture)
            .expect("v2 fixture should decode");
        assert_eq!(envelope.version(), MRF_V2_VERSION);
        assert_eq!(envelope.min_reader_version(), MRF_V2_VERSION);
        assert_eq!(envelope.capabilities(), MrfV2Capabilities::current());
        assert_eq!(envelope.payload(), &[1, 2, 3]);
    }

    #[test]
    fn v2_reader_rejects_wrong_namespace_version_capability_and_rollback() {
        let fixture = [
            b'M', b'R', b'F', b'2', 2, 0, 2, 0, 2, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3,
        ];
        let reader = MrfV2Readiness::dormant().reader().expect("dormant reader should initialize");
        assert!(matches!(
            reader.read("config/replication/mrf.bin", &fixture),
            Err(MrfV2Error::UnsupportedNamespace { .. })
        ));

        let mut version = fixture;
        version[6..8].copy_from_slice(&1u16.to_le_bytes());
        assert_eq!(
            reader.read(MRF_V2_NAMESPACE, &version),
            Err(MrfV2Error::UnsupportedVersion { version: 1 })
        );

        let mut capabilities = fixture;
        capabilities[12..20].copy_from_slice(&(1u64 << 63).to_le_bytes());
        assert_eq!(
            reader.read(MRF_V2_NAMESPACE, &capabilities),
            Err(MrfV2Error::UnknownCapabilities { bits: 1u64 << 63 })
        );

        let mut rollback = fixture;
        rollback[8..10].copy_from_slice(&3u16.to_le_bytes());
        assert_eq!(
            reader.read(MRF_V2_NAMESPACE, &rollback),
            Err(MrfV2Error::RollbackFenced {
                min_reader_version: 3,
                reader_version: 2,
            })
        );
    }

    #[test]
    fn v2_readiness_rejects_writer_enablement_and_missing_capabilities() {
        assert_eq!(
            MrfV2Readiness::new(2, MrfV2Capabilities::current(), true).reader(),
            Err(MrfV2Error::WriterEnabled)
        );

        let fixture = [
            b'M', b'R', b'F', b'2', 2, 0, 2, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3,
        ];
        let reader = MrfV2Readiness::new(2, MrfV2Capabilities::empty(), false)
            .reader()
            .expect("readiness with valid empty capabilities should initialize");
        assert_eq!(
            reader.read(MRF_V2_NAMESPACE, &fixture),
            Err(MrfV2Error::MissingCapabilities {
                required: 1,
                available: 0,
            })
        );
    }
}
