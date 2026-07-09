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
    fn mrf_file_round_trips_object_and_delete_entries() {
        let obj_vid = Uuid::new_v4();
        let del_vid = Uuid::new_v4();
        let entries = vec![
            MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "object-a".to_string(),
                version_id: Some(obj_vid),
                retry_count: 2,
                size: 1024,
                op: MrfOpKind::Object,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
            },
            MrfReplicateEntry {
                bucket: "bucket-a".to_string(),
                object: "delete-a".to_string(),
                version_id: None,
                retry_count: 0,
                size: 0,
                op: MrfOpKind::Delete,
                delete_marker_version_id: Some(del_vid),
                delete_marker: true,
                delete_marker_mtime: Some(1_705_312_200_123_456_789),
            },
        ];

        let encoded = encode_mrf_file(&entries).expect("mrf file should encode");
        let decoded = decode_mrf_file(&encoded).expect("mrf file should decode");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].version_id, Some(obj_vid));
        assert_eq!(decoded[0].op, MrfOpKind::Object);
        assert_eq!(decoded[0].delete_marker_mtime, None);
        assert_eq!(decoded[1].delete_marker_version_id, Some(del_vid));
        assert_eq!(decoded[1].op, MrfOpKind::Delete);
        assert!(decoded[1].delete_marker);
        assert_eq!(
            decoded[1].delete_marker_mtime,
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
        // Old files lack the deleteMarkerMtime key; it must default to None so replay keeps the
        // pre-#867 fallback to the current time.
        assert_eq!(decoded[0].delete_marker_mtime, None);
    }

    #[test]
    fn mrf_file_rejects_invalid_header() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u16.to_le_bytes());
        data.extend_from_slice(&MRF_META_VERSION.to_le_bytes());
        data.push(0x90);

        assert!(matches!(decode_mrf_file(&data), Err(Error::CorruptedFormat)));
    }
}
