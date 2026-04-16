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

use crate::{ChecksumAlgo, FileMeta, FileMetaShallowVersion, FileMetaVersion, MetaDeleteMarker, MetaObject, Result, VersionType};
use std::collections::HashMap;
use time::OffsetDateTime;
use uuid::Uuid;
use xxhash_rust::xxh64;

const MSGPACK_EXT8: u8 = 0xc7;
const MSGPACK_TIME_EXT_LEGACY: i8 = 5;

/// Create real xl.meta file data for testing
pub fn create_real_xlmeta() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // Create a real object version
    let version_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef")?;
    let data_dir = Uuid::parse_str("fedcba98-7654-3210-fedc-ba9876543210")?;

    let mut metadata = HashMap::new();
    metadata.insert("Content-Type".to_string(), "text/plain".to_string());
    metadata.insert("X-Amz-Meta-Author".to_string(), "test-user".to_string());
    metadata.insert("X-Amz-Meta-Created".to_string(), "2024-01-15T10:30:00Z".to_string());

    let object_version = MetaObject {
        version_id: Some(version_id),
        data_dir: Some(data_dir),
        erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
        erasure_m: 4,
        erasure_n: 2,
        erasure_block_size: 1024 * 1024, // 1MB
        erasure_index: 1,
        erasure_dist: vec![0, 1, 2, 3, 4, 5],
        bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
        part_numbers: vec![1],
        part_etags: vec!["d41d8cd98f00b204e9800998ecf8427e".to_string()],
        part_sizes: vec![1024],
        part_actual_sizes: vec![1024],
        part_indices: Vec::new(),
        size: 1024,
        mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200)?), // 2024-01-15 10:30:00 UTC
        meta_sys: HashMap::new(),
        meta_user: metadata,
    };

    let file_version = FileMetaVersion {
        version_type: VersionType::Object,
        legacy_object: None,
        object: Some(object_version),
        delete_marker: None,
        write_version: 1,
        uses_legacy_checksum: false,
    };

    let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
    fm.versions.push(shallow_version);

    // Add a delete marker version
    let delete_version_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555")?;
    let delete_marker = MetaDeleteMarker {
        version_id: Some(delete_version_id),
        mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312260)?), // 1 minute later
        meta_sys: HashMap::new(),
    };

    let delete_file_version = FileMetaVersion {
        version_type: VersionType::Delete,
        legacy_object: None,
        object: None,
        delete_marker: Some(delete_marker),
        write_version: 2,
        uses_legacy_checksum: false,
    };

    let delete_shallow_version = FileMetaShallowVersion::try_from(delete_file_version)?;
    fm.versions.push(delete_shallow_version);

    // Add a Legacy version for testing
    let legacy_version_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")?;
    let legacy_version = FileMetaVersion {
        version_type: VersionType::Legacy,
        legacy_object: None,
        object: None,
        delete_marker: None,
        write_version: 3,
        uses_legacy_checksum: false,
    };

    let mut legacy_shallow = FileMetaShallowVersion::try_from(legacy_version)?;
    legacy_shallow.header.version_id = Some(legacy_version_id);
    legacy_shallow.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(1705312140)?); // earlier time
    fm.versions.push(legacy_shallow);

    // Sort by modification time (newest first)
    fm.versions
        .sort_by_key(|v| (v.header.mod_time.is_none(), std::cmp::Reverse(v.header.mod_time)));

    fm.marshal_msg()
}

fn decode_hex_fixture(input: &str) -> Result<Vec<u8>> {
    let input = input.trim();
    if !input.len().is_multiple_of(2) {
        return Err(crate::Error::other("hex fixture must have even length"));
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for idx in (0..bytes.len()).step_by(2) {
        let hi = (bytes[idx] as char)
            .to_digit(16)
            .ok_or_else(|| crate::Error::other(format!("invalid hex at index {idx}")))?;
        let lo = (bytes[idx + 1] as char)
            .to_digit(16)
            .ok_or_else(|| crate::Error::other(format!("invalid hex at index {}", idx + 1)))?;
        out.push(((hi << 4) | lo) as u8);
    }

    Ok(out)
}

/// Real legacy xl.meta captured in issue #2288. Header/meta versions are 2/1.
pub fn create_issue_2288_legacy_xlmeta() -> Result<Vec<u8>> {
    decode_hex_fixture(include_str!("../tests/fixtures/issue_2288_legacy_xlmeta.hex"))
}

/// Legacy xl.meta captured in issue #2265. Header/meta versions are 3/2.
pub fn create_issue_2265_legacy_meta_v2_object_xlmeta() -> Result<Vec<u8>> {
    decode_hex_fixture(include_str!("../tests/fixtures/issue_2265_legacy_meta_v2_object.hex"))
}

/// Legacy config xl.meta captured in issue #2265. Header/meta versions are 3/2.
pub fn create_issue_2265_legacy_meta_v2_config_xlmeta() -> Result<Vec<u8>> {
    decode_hex_fixture(include_str!("../tests/fixtures/issue_2265_legacy_meta_v2_config.hex"))
}

/// Legacy pool xl.meta captured in issue #2434. Header/meta versions are 3/2.
pub fn create_issue_2434_legacy_meta_v2_pool_xlmeta() -> Result<Vec<u8>> {
    decode_hex_fixture(include_str!("../tests/fixtures/issue_2434_legacy_meta_v2_pool.hex"))
}

fn write_legacy_time(wr: &mut Vec<u8>, ts: OffsetDateTime) {
    wr.push(MSGPACK_EXT8);
    wr.push(12);
    wr.push(MSGPACK_TIME_EXT_LEGACY as u8);
    wr.extend_from_slice(&ts.unix_timestamp().to_be_bytes());
    wr.extend_from_slice(&ts.nanosecond().to_be_bytes());
}

fn encode_legacy_v1_header(version_id: Uuid, mod_time: OffsetDateTime) -> Vec<u8> {
    let mut wr = Vec::new();
    rmp::encode::write_array_len(&mut wr, 4).unwrap();
    rmp::encode::write_bin(&mut wr, version_id.as_bytes()).unwrap();
    rmp::encode::write_i64(&mut wr, mod_time.unix_timestamp_nanos() as i64).unwrap();
    rmp::encode::write_uint8(&mut wr, VersionType::Legacy.to_u8()).unwrap();
    rmp::encode::write_uint8(&mut wr, 0).unwrap();
    wr
}

fn encode_legacy_v1_body(version_id: Uuid, data_dir: Uuid, mod_time: OffsetDateTime) -> Vec<u8> {
    let mut wr = Vec::new();

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
    rmp::encode::write_str(&mut wr, &version_id.to_string()).unwrap();
    rmp::encode::write_str(&mut wr, "DataDir").unwrap();
    rmp::encode::write_str(&mut wr, &data_dir.to_string()).unwrap();

    rmp::encode::write_str(&mut wr, "v").unwrap();
    rmp::encode::write_uint(&mut wr, 1).unwrap();

    wr
}

/// Legacy xl.meta with a V1Obj body and v1 header layout.
pub fn create_legacy_v1_object_xlmeta() -> Result<Vec<u8>> {
    let version_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef")?;
    let data_dir = Uuid::parse_str("fedcba98-7654-3210-fedc-ba9876543210")?;
    let mod_time = OffsetDateTime::from_unix_timestamp_nanos(1_705_312_200_123_456_789)?;

    let header = encode_legacy_v1_header(version_id, mod_time);
    let body = encode_legacy_v1_body(version_id, data_dir, mod_time);

    let mut wr = Vec::new();
    wr.extend_from_slice(b"XL2 ");
    wr.extend_from_slice(&1u16.to_le_bytes());
    wr.extend_from_slice(&3u16.to_le_bytes());
    wr.extend_from_slice(&[0xc6, 0, 0, 0, 0]);

    let offset = wr.len();
    rmp::encode::write_uint(&mut wr, 1)?;
    rmp::encode::write_uint(&mut wr, 1)?;
    rmp::encode::write_sint(&mut wr, 1)?;
    rmp::encode::write_bin(&mut wr, &header)?;
    rmp::encode::write_bin(&mut wr, &body)?;

    let data_len = (wr.len() - offset) as u32;
    wr[offset - 4..offset].copy_from_slice(&data_len.to_be_bytes());

    let crc = xxh64::xxh64(&wr[offset..], 0) as u32;
    wr.push(0xce);
    wr.extend_from_slice(&crc.to_be_bytes());

    Ok(wr)
}

/// Create a complex xl.meta file with multiple versions
pub fn create_complex_xlmeta() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // Create 10 object versions
    for i in 0i64..10i64 {
        let version_id = Uuid::new_v4();
        let data_dir = if i % 3 == 0 { Some(Uuid::new_v4()) } else { None };

        let mut metadata = HashMap::new();
        metadata.insert("Content-Type".to_string(), "application/octet-stream".to_string());
        metadata.insert("X-Amz-Meta-Version".to_string(), i.to_string());
        metadata.insert("X-Amz-Meta-Test".to_string(), format!("test-value-{i}"));

        let object_version = MetaObject {
            version_id: Some(version_id),
            data_dir,
            erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
            erasure_m: 4,
            erasure_n: 2,
            erasure_block_size: 1024 * 1024,
            erasure_index: (i % 6) as usize,
            erasure_dist: vec![0, 1, 2, 3, 4, 5],
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: vec![1],
            part_etags: vec![format!("etag-{:08x}", i)],
            part_sizes: vec![1024 * (i + 1) as usize],
            part_actual_sizes: vec![1024 * (i + 1)],
            part_indices: Vec::new(),
            size: 1024 * (i + 1),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200 + i * 60)?),
            meta_sys: HashMap::new(),
            meta_user: metadata,
        };

        let file_version = FileMetaVersion {
            version_type: VersionType::Object,
            legacy_object: None,
            object: Some(object_version),
            delete_marker: None,
            write_version: (i + 1) as u64,
            uses_legacy_checksum: false,
        };

        let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
        fm.versions.push(shallow_version);

        // Add a delete marker every 3 versions
        if i % 3 == 2 {
            let delete_version_id = Uuid::new_v4();
            let delete_marker = MetaDeleteMarker {
                version_id: Some(delete_version_id),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1705312200 + i * 60 + 30)?),
                meta_sys: HashMap::new(),
            };

            let delete_file_version = FileMetaVersion {
                version_type: VersionType::Delete,
                legacy_object: None,
                object: None,
                delete_marker: Some(delete_marker),
                write_version: (i + 100) as u64,
                uses_legacy_checksum: false,
            };

            let delete_shallow_version = FileMetaShallowVersion::try_from(delete_file_version)?;
            fm.versions.push(delete_shallow_version);
        }
    }

    // Sort by modification time (newest first)
    fm.versions
        .sort_by_key(|v| (v.header.mod_time.is_none(), std::cmp::Reverse(v.header.mod_time)));

    fm.marshal_msg()
}

/// Create a corrupted xl.meta file for error handling tests
pub fn create_corrupted_xlmeta() -> Vec<u8> {
    let mut data = vec![
        // Correct file header
        b'X', b'L', b'2', b' ', // version
        1, 0, 3, 0, // version
        0xc6, 0x00, 0x00, 0x00, 0x10, // correct bin32 length marker, but data length mismatch
    ];

    // Add insufficient data (less than declared length)
    data.extend_from_slice(&[0x42; 8]); // only 8 bytes, but declared 16 bytes

    data
}

/// Create an empty xl.meta file
pub fn create_empty_xlmeta() -> Result<Vec<u8>> {
    let fm = FileMeta::new();
    fm.marshal_msg()
}

/// Helper function to verify parsing results
pub fn verify_parsed_metadata(fm: &FileMeta, expected_versions: usize) -> Result<()> {
    assert_eq!(fm.versions.len(), expected_versions, "Version count mismatch");
    assert_eq!(fm.meta_ver, crate::filemeta::XL_META_VERSION, "Metadata version mismatch");

    // Verify versions are sorted by modification time
    for i in 1..fm.versions.len() {
        let prev_time = fm.versions[i - 1].header.mod_time;
        let curr_time = fm.versions[i].header.mod_time;

        if let (Some(prev), Some(curr)) = (prev_time, curr_time) {
            assert!(prev >= curr, "Versions not sorted correctly by modification time");
        }
    }

    Ok(())
}

/// Create an xl.meta file with inline data
pub fn create_xlmeta_with_inline_data() -> Result<Vec<u8>> {
    let mut fm = FileMeta::new();

    // Add inline data
    let inline_data = b"This is inline data for testing purposes";
    let version_id = Uuid::new_v4();
    fm.data.replace(&version_id.to_string(), inline_data.to_vec())?;

    let object_version = MetaObject {
        version_id: Some(version_id),
        data_dir: None,
        erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
        erasure_m: 1,
        erasure_n: 1,
        erasure_block_size: 64 * 1024,
        erasure_index: 0,
        erasure_dist: vec![0, 1],
        bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
        part_numbers: vec![1],
        part_etags: Vec::new(),
        part_sizes: vec![inline_data.len()],
        part_actual_sizes: Vec::new(),
        part_indices: Vec::new(),
        size: inline_data.len() as i64,
        mod_time: Some(OffsetDateTime::now_utc()),
        meta_sys: HashMap::new(),
        meta_user: HashMap::new(),
    };

    let file_version = FileMetaVersion {
        version_type: VersionType::Object,
        legacy_object: None,
        object: Some(object_version),
        delete_marker: None,
        write_version: 1,
        uses_legacy_checksum: false,
    };

    let shallow_version = FileMetaShallowVersion::try_from(file_version)?;
    fm.versions.push(shallow_version);

    fm.marshal_msg()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileMeta;

    #[test]
    fn test_create_real_xlmeta() {
        let data = create_real_xlmeta().expect("Failed to create test data");
        assert!(!data.is_empty(), "Generated data should not be empty");

        // Verify file header
        assert_eq!(&data[0..4], b"XL2 ", "Incorrect file header");

        // Try to parse
        let fm = FileMeta::load(&data).expect("Failed to parse");
        verify_parsed_metadata(&fm, 3).expect("Verification failed");
    }

    #[test]
    fn test_create_complex_xlmeta() {
        let data = create_complex_xlmeta().expect("Failed to create complex test data");
        assert!(!data.is_empty(), "Generated data should not be empty");

        let fm = FileMeta::load(&data).expect("Failed to parse");
        assert!(fm.versions.len() >= 10, "Should have at least 10 versions");
    }

    #[test]
    fn test_create_xlmeta_with_inline_data() {
        let data = create_xlmeta_with_inline_data().expect("Failed to create inline data test");
        assert!(!data.is_empty(), "Generated data should not be empty");

        let fm = FileMeta::load(&data).expect("Failed to parse");
        assert_eq!(fm.versions.len(), 1, "Should have 1 version");
        assert!(!fm.data.as_slice().is_empty(), "Should contain inline data");
    }

    #[test]
    fn test_corrupted_xlmeta_handling() {
        let data = create_corrupted_xlmeta();
        let result = FileMeta::load(&data);
        assert!(result.is_err(), "Corrupted data should fail to parse");
    }

    #[test]
    fn test_empty_xlmeta() {
        let data = create_empty_xlmeta().expect("Failed to create empty test data");
        let fm = FileMeta::load(&data).expect("Failed to parse empty data");
        assert_eq!(fm.versions.len(), 0, "Empty file should have no versions");
    }
}
