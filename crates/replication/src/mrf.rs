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
use std::fmt;

use crate::{Error, Result};

pub use crate::filemeta::{MrfOpKind, MrfReplicateEntry};

pub const MRF_META_FORMAT: u16 = 1;
pub const MRF_META_VERSION: u16 = 1;

const MRF_ENVELOPE_MAGIC: [u8; 4] = *b"MRFE";
const MRF_ENVELOPE_HEADER_LEN: usize = 24;
pub const MRF_ENVELOPE_FORMAT: u16 = 1;
pub const MRF_ENVELOPE_VERSION: u16 = 1;

const CAPABILITY_OPERATION_KIND: u64 = 1 << 0;
const CAPABILITY_TARGET_ARNS: u64 = 1 << 1;
const CAPABILITY_FORCE_DELETE: u64 = 1 << 2;
const CAPABILITY_DELETE_MARKER_MTIME: u64 = 1 << 3;
const MRF_KNOWN_CAPABILITIES: u64 =
    CAPABILITY_OPERATION_KIND | CAPABILITY_TARGET_ARNS | CAPABILITY_FORCE_DELETE | CAPABILITY_DELETE_MARKER_MTIME;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MrfCapability {
    OperationKind,
    TargetArns,
    ForceDelete,
    DeleteMarkerMtime,
}

impl MrfCapability {
    const fn bit(self) -> u64 {
        match self {
            Self::OperationKind => CAPABILITY_OPERATION_KIND,
            Self::TargetArns => CAPABILITY_TARGET_ARNS,
            Self::ForceDelete => CAPABILITY_FORCE_DELETE,
            Self::DeleteMarkerMtime => CAPABILITY_DELETE_MARKER_MTIME,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MrfCapabilities(u64);

impl MrfCapabilities {
    pub const fn current() -> Self {
        Self(MRF_KNOWN_CAPABILITIES)
    }

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn with(capability: MrfCapability) -> Self {
        Self(capability.bit())
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub const fn contains(self, capability: MrfCapability) -> bool {
        self.0 & capability.bit() != 0
    }

    pub const fn supports(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    pub fn from_bits(bits: u64) -> std::result::Result<Self, MrfEnvelopeError> {
        let unknown = bits & !MRF_KNOWN_CAPABILITIES;
        if unknown != 0 {
            return Err(MrfEnvelopeError::UnknownCapabilities { bits: unknown });
        }
        Ok(Self(bits))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MrfProtocolCapabilities {
    version: u16,
    min_reader_version: u16,
    capabilities: MrfCapabilities,
}

impl MrfProtocolCapabilities {
    pub const fn new(version: u16, min_reader_version: u16, capabilities: MrfCapabilities) -> Self {
        Self {
            version,
            min_reader_version,
            capabilities,
        }
    }

    pub const fn current() -> Self {
        Self {
            version: MRF_ENVELOPE_VERSION,
            min_reader_version: MRF_ENVELOPE_VERSION,
            capabilities: MrfCapabilities::current(),
        }
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn min_reader_version(self) -> u16 {
        self.min_reader_version
    }

    pub const fn capabilities(self) -> MrfCapabilities {
        self.capabilities
    }

    pub fn negotiate(self, peer: Self) -> std::result::Result<Self, MrfEnvelopeError> {
        MrfCapabilities::from_bits(self.capabilities.bits())?;
        MrfCapabilities::from_bits(peer.capabilities.bits())?;
        if self.min_reader_version > self.version {
            return Err(MrfEnvelopeError::InvalidVersionRange {
                version: self.version,
                min_reader_version: self.min_reader_version,
            });
        }
        if peer.min_reader_version > peer.version {
            return Err(MrfEnvelopeError::InvalidVersionRange {
                version: peer.version,
                min_reader_version: peer.min_reader_version,
            });
        }
        if self.version < MRF_ENVELOPE_VERSION {
            return Err(MrfEnvelopeError::UnsupportedVersion { version: self.version });
        }
        if peer.version < MRF_ENVELOPE_VERSION {
            return Err(MrfEnvelopeError::UnsupportedVersion { version: peer.version });
        }
        if peer.min_reader_version > self.version {
            return Err(MrfEnvelopeError::RollbackFenced {
                min_reader_version: peer.min_reader_version,
                supported_version: self.version,
            });
        }
        if self.min_reader_version > peer.version {
            return Err(MrfEnvelopeError::RollbackFenced {
                min_reader_version: self.min_reader_version,
                supported_version: peer.version,
            });
        }
        Ok(Self {
            version: self.version.min(peer.version),
            min_reader_version: self.min_reader_version.max(peer.min_reader_version),
            capabilities: MrfCapabilities(self.capabilities.bits() & peer.capabilities.bits()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MrfEnvelope {
    protocol: MrfProtocolCapabilities,
    payload: Vec<u8>,
}

impl MrfEnvelope {
    pub fn new(protocol: MrfProtocolCapabilities, payload: Vec<u8>) -> std::result::Result<Self, MrfEnvelopeError> {
        if protocol.version != MRF_ENVELOPE_VERSION {
            return Err(MrfEnvelopeError::UnsupportedVersion {
                version: protocol.version,
            });
        }
        if protocol.min_reader_version > protocol.version {
            return Err(MrfEnvelopeError::InvalidVersionRange {
                version: protocol.version,
                min_reader_version: protocol.min_reader_version,
            });
        }
        MrfCapabilities::from_bits(protocol.capabilities.bits())?;
        Ok(Self { protocol, payload })
    }

    pub const fn protocol(&self) -> MrfProtocolCapabilities {
        self.protocol
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, MrfEnvelopeError> {
        let payload_len: u32 = self.payload.len().try_into().map_err(|_| MrfEnvelopeError::PayloadTooLarge)?;
        let mut data = Vec::with_capacity(MRF_ENVELOPE_HEADER_LEN + self.payload.len());
        data.extend_from_slice(&MRF_ENVELOPE_MAGIC);
        data.extend_from_slice(&MRF_ENVELOPE_FORMAT.to_le_bytes());
        data.extend_from_slice(&self.protocol.version.to_le_bytes());
        data.extend_from_slice(&self.protocol.min_reader_version.to_le_bytes());
        data.extend_from_slice(&0u16.to_le_bytes());
        data.extend_from_slice(&self.protocol.capabilities.bits().to_le_bytes());
        data.extend_from_slice(&payload_len.to_le_bytes());
        data.extend_from_slice(&self.payload);
        Ok(data)
    }

    pub fn decode(data: &[u8], supported: MrfProtocolCapabilities) -> std::result::Result<Self, MrfEnvelopeError> {
        if data.len() < MRF_ENVELOPE_HEADER_LEN {
            return Err(MrfEnvelopeError::Truncated);
        }
        if data[..4] != MRF_ENVELOPE_MAGIC {
            return Err(MrfEnvelopeError::InvalidMagic);
        }
        let format = LittleEndian::read_u16(&data[4..6]);
        if format != MRF_ENVELOPE_FORMAT {
            return Err(MrfEnvelopeError::UnsupportedFormat { format });
        }
        let version = LittleEndian::read_u16(&data[6..8]);
        if version < MRF_ENVELOPE_VERSION {
            return Err(MrfEnvelopeError::UnsupportedVersion { version });
        }
        let min_reader_version = LittleEndian::read_u16(&data[8..10]);
        if min_reader_version > supported.version {
            return Err(MrfEnvelopeError::RollbackFenced {
                min_reader_version,
                supported_version: supported.version,
            });
        }
        if min_reader_version > version {
            return Err(MrfEnvelopeError::InvalidVersionRange {
                version,
                min_reader_version,
            });
        }
        let reserved = LittleEndian::read_u16(&data[10..12]);
        if reserved != 0 {
            return Err(MrfEnvelopeError::ReservedHeaderBits { bits: reserved });
        }
        let capabilities = MrfCapabilities::from_bits(LittleEndian::read_u64(&data[12..20]))?;
        if !supported.capabilities.supports(capabilities) {
            return Err(MrfEnvelopeError::MissingCapabilities {
                required: capabilities.bits(),
                available: supported.capabilities.bits(),
            });
        }
        let payload_len = LittleEndian::read_u32(&data[20..24]);
        let actual_len = data.len() - MRF_ENVELOPE_HEADER_LEN;
        if usize::try_from(payload_len).map_err(|_| MrfEnvelopeError::PayloadTooLarge)? != actual_len {
            return Err(MrfEnvelopeError::PayloadLengthMismatch {
                declared: payload_len,
                actual: actual_len,
            });
        }
        Ok(Self {
            protocol: MrfProtocolCapabilities {
                version,
                min_reader_version,
                capabilities,
            },
            payload: data[MRF_ENVELOPE_HEADER_LEN..].to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MrfEnvelopeError {
    Truncated,
    InvalidMagic,
    UnsupportedFormat {
        format: u16,
    },
    UnsupportedVersion {
        version: u16,
    },
    InvalidVersionRange {
        version: u16,
        min_reader_version: u16,
    },
    RollbackFenced {
        min_reader_version: u16,
        supported_version: u16,
    },
    ReservedHeaderBits {
        bits: u16,
    },
    UnknownCapabilities {
        bits: u64,
    },
    MissingCapabilities {
        required: u64,
        available: u64,
    },
    PayloadLengthMismatch {
        declared: u32,
        actual: usize,
    },
    PayloadTooLarge,
}

impl fmt::Display for MrfEnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated MRF envelope"),
            Self::InvalidMagic => write!(f, "invalid MRF envelope magic"),
            Self::UnsupportedFormat { format } => write!(f, "unsupported MRF envelope format {format}"),
            Self::UnsupportedVersion { version } => write!(f, "unsupported MRF envelope version {version}"),
            Self::InvalidVersionRange {
                version,
                min_reader_version,
            } => {
                write!(f, "invalid MRF version range: version {version}, minimum reader {min_reader_version}")
            }
            Self::RollbackFenced {
                min_reader_version,
                supported_version,
            } => write!(
                f,
                "MRF rollback fenced: reader version {supported_version} is below required {min_reader_version}"
            ),
            Self::ReservedHeaderBits { bits } => write!(f, "reserved MRF envelope header bits are set: 0x{bits:04x}"),
            Self::UnknownCapabilities { bits } => write!(f, "unknown MRF capability bits 0x{bits:016x}"),
            Self::MissingCapabilities { required, available } => {
                write!(f, "MRF capabilities 0x{available:016x} do not satisfy required 0x{required:016x}")
            }
            Self::PayloadLengthMismatch { declared, actual } => {
                write!(f, "MRF payload length is {actual}, expected {declared}")
            }
            Self::PayloadTooLarge => write!(f, "MRF payload exceeds the envelope length limit"),
        }
    }
}

impl std::error::Error for MrfEnvelopeError {}

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

    const ENVELOPE_FIXTURE: &[u8] = &[
        b'M', b'R', b'F', b'E', 1, 0, 1, 0, 1, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3,
    ];

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
                target_delete_marker_version_id: None,
                source_mod_time: None,
                enqueued_order: None,
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
                target_delete_marker_version_id: None,
                source_mod_time: None,
                enqueued_order: None,
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
                target_delete_marker_version_id: Some("opaque-target-version".to_string()),
                source_mod_time: None,
                enqueued_order: None,
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
        assert_eq!(decoded[2].target_delete_marker_version_id.as_deref(), Some("opaque-target-version"));
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
        assert_eq!(decoded[0].target_delete_marker_version_id, None);
    }

    #[test]
    fn older_mrf_reader_ignores_target_delete_marker_version_id() {
        #[derive(serde::Deserialize)]
        struct LegacyMrfEntry {
            bucket: String,
            object: String,
            #[serde(rename = "retryCount")]
            retry_count: i32,
        }

        let entry = MrfReplicateEntry {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 1,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: vec!["arn:target".to_string()],
            target_delete_marker_version_id: Some("opaque-target-version".to_string()),
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let encoded = encode_mrf_file(&[entry]).expect("new MRF should encode");
        let legacy: Vec<LegacyMrfEntry> = rmp_serde::from_slice(&encoded[4..]).expect("old reader should ignore new map keys");

        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].bucket, "bucket");
        assert_eq!(legacy[0].object, "object");
        assert_eq!(legacy[0].retry_count, 1);
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
    fn envelope_fixture_is_stable_and_round_trips() {
        let envelope =
            MrfEnvelope::new(MrfProtocolCapabilities::current(), vec![1, 2, 3]).expect("current MRF envelope should be valid");
        assert_eq!(envelope.encode().expect("envelope should encode"), ENVELOPE_FIXTURE);
        let decoded = MrfEnvelope::decode(ENVELOPE_FIXTURE, MrfProtocolCapabilities::current()).expect("fixture should decode");
        assert_eq!(decoded.protocol(), MrfProtocolCapabilities::current());
        assert_eq!(decoded.payload(), &[1, 2, 3]);
    }

    #[test]
    fn envelope_accepts_a_forward_compatible_writer_version() {
        let mut version = ENVELOPE_FIXTURE.to_vec();
        version[6..8].copy_from_slice(&2u16.to_le_bytes());
        let decoded =
            MrfEnvelope::decode(&version, MrfProtocolCapabilities::current()).expect("compatible v2 envelope should decode");
        assert_eq!(decoded.protocol().version(), 2);
        assert_eq!(decoded.protocol().min_reader_version(), 1);

        let mut fenced = version;
        fenced[8..10].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            MrfEnvelope::decode(&fenced, MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::RollbackFenced {
                min_reader_version: 2,
                supported_version: 1,
            })
        );
    }

    #[test]
    fn envelope_rejects_unknown_capability_bits() {
        let mut capabilities = ENVELOPE_FIXTURE.to_vec();
        capabilities[12..20].copy_from_slice(&(1u64 << 63).to_le_bytes());

        assert_eq!(
            MrfEnvelope::decode(&capabilities, MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::UnknownCapabilities { bits: 1u64 << 63 })
        );

        let mut reserved = ENVELOPE_FIXTURE.to_vec();
        reserved[10..12].copy_from_slice(&1u16.to_le_bytes());
        assert_eq!(
            MrfEnvelope::decode(&reserved, MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::ReservedHeaderBits { bits: 1 })
        );

        let mut legacy = ENVELOPE_FIXTURE.to_vec();
        legacy[6..8].copy_from_slice(&0u16.to_le_bytes());
        assert_eq!(
            MrfEnvelope::decode(&legacy, MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::UnsupportedVersion { version: 0 })
        );
    }

    #[test]
    fn envelope_rejects_rollback_and_missing_capabilities() {
        let mut rollback = ENVELOPE_FIXTURE.to_vec();
        rollback[8..10].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            MrfEnvelope::decode(&rollback, MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::RollbackFenced {
                min_reader_version: 2,
                supported_version: 1,
            })
        );

        let required = MrfCapabilities::with(MrfCapability::TargetArns);
        let envelope =
            MrfEnvelope::new(MrfProtocolCapabilities::new(1, 1, required), Vec::new()).expect("known capability should be valid");
        let encoded = envelope.encode().expect("envelope should encode");
        assert_eq!(
            MrfEnvelope::decode(&encoded, MrfProtocolCapabilities::new(1, 1, MrfCapabilities::empty())),
            Err(MrfEnvelopeError::MissingCapabilities {
                required: required.bits(),
                available: 0,
            })
        );
    }

    #[test]
    fn protocol_negotiation_fences_rollback() {
        let current = MrfProtocolCapabilities::current();
        let rollback = MrfProtocolCapabilities::new(1, 2, MrfCapabilities::current());
        assert_eq!(
            current.negotiate(rollback),
            Err(MrfEnvelopeError::InvalidVersionRange {
                version: 1,
                min_reader_version: 2,
            })
        );
    }

    #[test]
    fn protocol_negotiation_rejects_invalid_local_version_range() {
        let invalid = MrfProtocolCapabilities::new(1, 2, MrfCapabilities::current());
        assert_eq!(
            invalid.negotiate(MrfProtocolCapabilities::current()),
            Err(MrfEnvelopeError::InvalidVersionRange {
                version: 1,
                min_reader_version: 2,
            })
        );
    }

    #[test]
    fn protocol_negotiation_intersects_capabilities() {
        let local = MrfProtocolCapabilities::new(1, 1, MrfCapabilities::with(MrfCapability::TargetArns));
        let peer = MrfProtocolCapabilities::new(1, 1, MrfCapabilities::with(MrfCapability::ForceDelete));
        let negotiated = local.negotiate(peer).expect("same-version peers should negotiate");
        assert_eq!(negotiated.capabilities(), MrfCapabilities::empty());
    }

    #[test]
    fn protocol_negotiation_accepts_a_forward_compatible_peer() {
        let reader = MrfProtocolCapabilities::current();
        let writer = MrfProtocolCapabilities::new(2, 1, MrfCapabilities::current());
        let negotiated = reader
            .negotiate(writer)
            .expect("v1 reader should negotiate with a compatible v2 writer");
        assert_eq!(negotiated.version(), 1);
        assert_eq!(negotiated.min_reader_version(), 1);
    }
}
