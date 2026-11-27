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

//! Tests of what only an `m-compatible` build compiles.

use super::*;
use crate::object_api::readers::layout::{DARE_PACKAGE_SIZE, DARE_PAYLOAD_SIZE, get_encrypted_compressed_offsets};

#[tokio::test]
async fn test_ranged_decompress_reader_with_rio_v2_s2_stream() {
    let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    let mut compressed = Vec::new();
    crate::io_support::rio::CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
        .read_to_end(&mut compressed)
        .await
        .expect("compress plaintext into rio_v2 stream");

    let decompress_reader =
        crate::io_support::rio::DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
    let mut ranged_reader = RangedDecompressReader::new(decompress_reader, 5, 7, plaintext.len()).expect("create ranged reader");

    let mut actual = Vec::new();
    ranged_reader
        .read_to_end(&mut actual)
        .await
        .expect("read ranged decompressed plaintext");

    assert_eq!(actual, b"fghijkl");
}

#[tokio::test]
async fn test_read_plan_accepts_minio_s2_compression_scheme() {
    let object_info = ObjectInfo {
        size: 3_000_000,
        user_defined: Arc::new(HashMap::from([
            ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
            ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
        ])),
        ..Default::default()
    };

    let plan = ReadPlan::build(None, &object_info, &ObjectOptions::default(), &HeaderMap::new())
        .await
        .expect("MinIO S2 compression scheme should be accepted");

    assert!(matches!(
        plan.transform,
        ReadTransform::Compressed {
            decompressed_offset: 0,
            decompressed_length: 4_194_304,
            ..
        }
    ));
    assert_eq!(plan.object_size, 4_194_304);
}

#[tokio::test]
async fn test_read_plan_accepts_minio_headerless_compression_index() {
    let mut index = Index::new();
    index.add(0, 0).unwrap();
    index.add(1_048_576, 2_097_152).unwrap();
    let headerless_index = crate::io_support::rio::compression_index_storage_bytes(&index);
    assert!(
        !headerless_index.starts_with(&[0x50, 0x2A, 0x4D, 0x18]),
        "rio_v2 should store MinIO-style headerless compression indexes"
    );

    let object_info = ObjectInfo {
        size: 3_000_000,
        parts: Arc::new(vec![ObjectPartInfo {
            etag: String::new(),
            number: 1,
            size: 3_000_000,
            actual_size: 4_194_304,
            index: Some(headerless_index),
            ..Default::default()
        }]),
        user_defined: Arc::new(HashMap::from([
            ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
            ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
        ])),
        ..Default::default()
    };

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 2_097_152,
        end: 2_097_161,
    };

    let decoded = crate::io_support::rio::decode_compression_index_bytes(object_info.parts[0].index.as_ref().unwrap())
        .expect("headerless MinIO-style compression index should decode");
    let (compressed_offset, uncompressed_offset) = decoded.find(range.start).expect("seek into decoded index");
    assert!(compressed_offset > 0);
    assert_eq!(uncompressed_offset, range.start);

    let (physical_offset, _, decompressed_skip) =
        compressed_storage_window(&object_info, range.start as usize, 10).expect("indexed range");
    assert!(physical_offset > 0);
    assert_eq!(decompressed_skip, 0);

    let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
        .await
        .expect("MinIO headerless compression index should be decoded");

    assert_eq!(plan.storage_offset, physical_offset);
    assert_eq!(plan.storage_length, object_info.size - plan.storage_offset as i64);
    assert_eq!(plan.object_size, 10);
}

#[test]
fn test_get_encrypted_compressed_offsets_aligns_encrypted_ranges_to_dare_packages() {
    let mut index = Index::new();
    index.add(0, 0).unwrap();
    index.add(200_000, 2_097_152).unwrap();
    let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
    let expected_comp_off = crate::io_support::rio::decode_compression_index_bytes(&stored_index)
        .expect("decode stored index")
        .find(2_097_152)
        .expect("find offset in stored index")
        .0;

    let object_info = ObjectInfo {
        size: 400_000,
        parts: Arc::new(vec![ObjectPartInfo {
            etag: String::new(),
            number: 1,
            size: 400_000,
            actual_size: 4_194_304,
            index: Some(stored_index),
            ..Default::default()
        }]),
        user_defined: Arc::new(HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-original-size".to_string(), "4194304".to_string()),
            (
                "x-minio-internal-compression".to_string(),
                crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
            ),
            ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
        ])),
        ..Default::default()
    };

    let (physical_offset, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
        get_encrypted_compressed_offsets(&object_info, 2_097_152);

    assert_eq!(first_part_idx, 0);
    assert_eq!(physical_offset, (expected_comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE);
    assert_eq!(decompressed_skip, 0);
    assert_eq!(decrypt_skip, expected_comp_off % DARE_PAYLOAD_SIZE);
    assert_eq!(seq_num, (expected_comp_off / DARE_PAYLOAD_SIZE) as u64);
}

#[tokio::test]
async fn test_get_object_reader_decrypts_ssec_sealed_object_key_full_object() {
    let plaintext = b"ecstore-rio-v2-ssec-sealed-object-key".repeat(4096);
    let customer_key = [0x31; 32];
    let object_key = [0x67; 32];
    let bucket = "bucket";
    let object = "sealed-object";
    let mut encrypted = Vec::new();
    crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
        .read_to_end(&mut encrypted)
        .await
        .expect("encrypt object with rio-v2 object key");

    let object_info = ObjectInfo {
        bucket: bucket.to_string(),
        name: object.to_string(),
        size: encrypted.len() as i64,
        user_defined: Arc::new(HashMap::from([
            (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(object_key)),
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            (
                "x-amz-server-side-encryption-customer-key-md5".to_string(),
                BASE64_STANDARD.encode_to_string(md5_bytes(customer_key)),
            ),
            (
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                plaintext.len().to_string(),
            ),
        ])),
        ..Default::default()
    };

    let (mut reader, offset, length) = GetObjectReader::new(
        Box::new(Cursor::new(encrypted.clone())),
        None,
        &object_info,
        &ObjectOptions::default(),
        &ssec_headers_from_key(customer_key),
    )
    .await
    .expect("rio-v2 ssec sealed-object-key read should be supported");

    let mut actual = Vec::new();
    reader
        .read_to_end(&mut actual)
        .await
        .expect("read decrypted rio-v2 ssec object");

    assert_eq!(offset, 0);
    assert_eq!(length, encrypted.len() as i64);
    assert_eq!(reader.object_info.size, plaintext.len() as i64);
    assert_eq!(actual, plaintext);
}

/// A sealed single-part ETag as MinIO stores it: one DARE package, 96 hex
/// characters, never the 32 of a bare MD5.
const MINIO_SEALED_ETAG: &str =
    "20010f00cbc498738108113178f20c5507f319f350e962a89c610c16699bca30b4f4914b6552c532550cca4ec1eb39b3";

/// The metadata MinIO leaves on a single-part SSE object: the seal
/// algorithm of its sealed object key and nothing that records a size.
fn minio_single_part_sse_object(encrypted_len: usize, plaintext_len: usize, object_key: [u8; 32]) -> ObjectInfo {
    ObjectInfo {
        bucket: "bucket".to_string(),
        name: "minio-single-part".to_string(),
        size: encrypted_len as i64,
        etag: Some(MINIO_SEALED_ETAG.to_string()),
        parts: Arc::new(vec![ObjectPartInfo {
            number: 1,
            size: encrypted_len,
            actual_size: plaintext_len as i64,
            ..Default::default()
        }]),
        user_defined: Arc::new(HashMap::from([
            (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(object_key)),
            (
                rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
            ),
            (
                rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                "sealed-object-key".to_string(),
            ),
        ])),
        ..Default::default()
    }
}

/// MinIO seals the ETag of a single-part SSE object, so its length says
/// nothing about how the object was uploaded: the whole object is one DARE
/// stream under the object key, not a stream per part under part keys.
#[tokio::test]
async fn minio_single_part_sse_object_reads_as_one_stream_despite_its_sealed_etag() {
    let object_key = [0x67; 32];
    // Every interesting shape: one short package, an exact package, and
    // several packages with a short tail.
    for plaintext_len in [1usize, 4096, 64 * 1024, 300 * 1024, 1024 * 1024 + 1] {
        let plaintext: Vec<u8> = (0..plaintext_len).map(|index| (index % 251) as u8).collect();
        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt as one DARE stream");
        let object_info = minio_single_part_sse_object(encrypted.len(), plaintext_len, object_key);

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await
        .expect("plan a full read");
        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .unwrap_or_else(|error| panic!("full read of {plaintext_len} bytes: {error}"));

        assert_eq!((offset, length), (0, encrypted.len() as i64));
        assert_eq!(
            reader.object_info.size, plaintext_len as i64,
            "the response length is the plaintext length"
        );
        assert_eq!(actual, plaintext, "{plaintext_len} bytes");
        assert_eq!(
            reader
                .object_info
                .get_actual_size()
                .expect("size of the object behind the read"),
            plaintext_len as i64,
            "the object's size is still derivable from what the read hands back"
        );

        // The last byte lies beyond the plaintext size a reader that takes
        // the stored size for the plaintext size would compute offsets from.
        let last = plaintext_len as i64 - 1;
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: last,
            end: last,
        };
        let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("plan a read of the last byte");
        let storage_offset = plan.storage_offset;
        let (mut reader, _, _) = plan
            .into_reader(Box::new(Cursor::new(encrypted[storage_offset..].to_vec())), &object_info)
            .expect("build the ranged reader");
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.expect("read the last byte");
        assert_eq!(tail, plaintext[plaintext_len - 1..], "last byte of {plaintext_len}");
        assert_eq!(reader.object_info.size, 1, "a ranged read hands back the length it delivers");
        assert_eq!(
            reader
                .object_info
                .get_actual_size()
                .expect("size of the object behind the ranged read"),
            plaintext_len as i64,
            "a ranged response still needs the whole object's size for Content-Range"
        );
    }
}

/// MinIO's own marker, not the part count, says an object is one cipher
/// stream per part: a one-part multipart upload is still keyed per part.
#[tokio::test]
async fn minio_one_part_multipart_sse_object_reads_under_its_part_key() {
    let object_key = [0x67; 32];
    let plaintext: Vec<u8> = (0..70_000usize).map(|index| (index % 251) as u8).collect();
    let mut encrypted = Vec::new();
    crate::io_support::rio::EncryptReader::new_multipart_with_object_key(Cursor::new(plaintext.clone()), object_key, 1)
        .read_to_end(&mut encrypted)
        .await
        .expect("encrypt as part 1 of a multipart upload");

    let mut object_info = minio_single_part_sse_object(encrypted.len(), plaintext.len(), object_key);
    object_info.etag = Some("b228928307afa1ffeb9aa36de4526de8-1".to_string());
    Arc::make_mut(&mut object_info.user_defined)
        .insert(rustfs_utils::http::MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER.to_string(), String::new());

    let (mut reader, _, _) = GetObjectReader::new(
        Box::new(Cursor::new(encrypted)),
        None,
        &object_info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("plan a full read");
    let mut actual = Vec::new();
    reader
        .read_to_end(&mut actual)
        .await
        .expect("read the one-part multipart object");

    assert_eq!(actual, plaintext);
}

#[tokio::test]
async fn test_get_object_reader_uses_dare_package_offset_for_large_ssec_ranges() {
    const DARE_PACKAGE_SIZE: usize = 64 * 1024 + 32;

    let plaintext = vec![0x7Bu8; 2 * 64 * 1024 + 97];
    let customer_key = [0x61; 32];
    let object_key = [0x68; 32];
    let bucket = "bucket";
    let object = "large-range-object";
    let mut encrypted = Vec::new();
    crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
        .read_to_end(&mut encrypted)
        .await
        .expect("encrypt large ranged object");

    let object_info = ObjectInfo {
        bucket: bucket.to_string(),
        name: object.to_string(),
        size: encrypted.len() as i64,
        user_defined: Arc::new(HashMap::from([
            (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(object_key)),
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            (
                "x-amz-server-side-encryption-customer-key-md5".to_string(),
                BASE64_STANDARD.encode_to_string(md5_bytes(customer_key)),
            ),
            (
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                plaintext.len().to_string(),
            ),
        ])),
        ..Default::default()
    };
    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 70_000,
        end: 70_063,
    };

    let (mut reader, offset, _length) = GetObjectReader::new(
        Box::new(Cursor::new(encrypted[DARE_PACKAGE_SIZE..].to_vec())),
        Some(range),
        &object_info,
        &ObjectOptions::default(),
        &ssec_headers_from_key(customer_key),
    )
    .await
    .expect("large ssec range read should be supported");

    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.expect("read ranged decrypted object");

    assert_eq!(
        offset, DARE_PACKAGE_SIZE,
        "rio_v2 encrypted ranges should start from the second DARE package"
    );
    assert_eq!(reader.object_info.size, 64);
    assert_eq!(actual, plaintext[70_000..70_064]);
}

#[tokio::test]
async fn test_get_object_reader_uses_compression_index_for_encrypted_ranges() {
    use crate::io_support::rio::TryGetIndex;

    let plaintext: Vec<u8> = (0..(10 * 1024 * 1024 + 123_456))
        .map(|i| (((i as u64).wrapping_mul(1_103_515_245).wrapping_add(12_345) >> 16) & 0xFF) as u8)
        .collect();
    let customer_key = [0x73; 32];
    let object_key = [0x74; 32];
    let bucket = "bucket";
    let object = "compressed-large-object";
    let mut compressor = crate::io_support::rio::CompressReader::with_encrypted_padding(
        Cursor::new(plaintext.clone()),
        CompressionAlgorithm::default(),
    );
    let mut compressed = Vec::new();
    compressor
        .read_to_end(&mut compressed)
        .await
        .expect("compress large plaintext");

    let index = compressor
        .try_get_index()
        .cloned()
        .expect("large rio_v2 encrypted+compressed object should expose a compression index");
    let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
    let decoded_index =
        crate::io_support::rio::decode_compression_index_bytes(&stored_index).expect("decode stored encrypted compression index");

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 5 * 1024 * 1024,
        end: 5 * 1024 * 1024 + 63,
    };
    let original_offsets = index
        .find(range.start)
        .expect("find large-range compression block in original index");
    let (comp_off, uncomp_off) = decoded_index
        .find(range.start)
        .expect("find large-range compression block in stored index");
    assert_eq!(original_offsets, (comp_off, uncomp_off));
    assert!(comp_off > DARE_PAYLOAD_SIZE);
    assert!(uncomp_off <= range.start);

    let expected_storage_offset = ((comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE) as usize;
    let expected_decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
    assert!(expected_storage_offset > 0);
    assert!(expected_decrypt_skip >= 0);

    let mut encrypted = Vec::new();
    crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(compressed.clone()), object_key)
        .read_to_end(&mut encrypted)
        .await
        .expect("encrypt compressed plaintext");

    let expected_sequence_number = u32::try_from(comp_off / DARE_PAYLOAD_SIZE).expect("sequence number fits in u32");
    let chunk_offset = comp_off as usize;
    let chunk_type = compressed[chunk_offset];
    let chunk_len = (compressed[chunk_offset + 1] as usize)
        | ((compressed[chunk_offset + 2] as usize) << 8)
        | ((compressed[chunk_offset + 3] as usize) << 16);
    assert!(matches!(chunk_type, 0x00 | 0x01 | 0xff | 0xfe));
    assert!(chunk_offset + 4 + chunk_len <= compressed.len());

    let mut decrypted_tail = Vec::new();
    crate::io_support::rio::DecryptReader::new_with_object_key_and_sequence(
        Cursor::new(encrypted[expected_storage_offset..].to_vec()),
        object_key,
        expected_sequence_number,
    )
    .read_to_end(&mut decrypted_tail)
    .await
    .expect("decrypt package-aligned ciphertext tail");
    assert_eq!(
        &decrypted_tail[expected_decrypt_skip as usize..],
        &compressed[comp_off as usize..],
        "package-aligned decryption plus decrypt_skip must land on the indexed S2 chunk boundary"
    );

    let mut direct_reader = crate::io_support::rio::DecompressReader::new(
        Cursor::new(decrypted_tail[expected_decrypt_skip as usize..].to_vec()),
        CompressionAlgorithm::default(),
    );
    let mut direct_plaintext = Vec::new();
    direct_reader
        .read_to_end(&mut direct_plaintext)
        .await
        .expect("decompress tail starting at indexed S2 chunk boundary");
    assert_eq!(direct_plaintext, plaintext[uncomp_off as usize..]);

    let object_info = ObjectInfo {
        bucket: bucket.to_string(),
        name: object.to_string(),
        size: encrypted.len() as i64,
        parts: Arc::new(vec![ObjectPartInfo {
            etag: String::new(),
            number: 1,
            size: encrypted.len(),
            actual_size: plaintext.len() as i64,
            index: Some(stored_index),
            ..Default::default()
        }]),
        user_defined: Arc::new(HashMap::from([
            (TEST_OBJECT_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(object_key)),
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            (
                "x-amz-server-side-encryption-customer-key-md5".to_string(),
                BASE64_STANDARD.encode_to_string(md5_bytes(customer_key)),
            ),
            (
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                plaintext.len().to_string(),
            ),
            (
                "x-minio-internal-compression".to_string(),
                crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
            ),
            ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
        ])),
        ..Default::default()
    };

    let plan = ReadPlan::build(
        Some(range.clone()),
        &object_info,
        &ObjectOptions::default(),
        &ssec_headers_from_key(customer_key),
    )
    .await
    .expect("build encrypted+compressed read plan");
    assert_eq!(plan.storage_offset, expected_storage_offset);
    assert_eq!(plan.storage_length, encrypted.len() as i64 - expected_storage_offset as i64);
    assert_eq!(plan.object_size, 64);
    match plan.transform {
        ReadTransform::Encrypted {
            sequence_number,
            decrypt_skip,
            plaintext_offset,
            plaintext_length,
            ..
        } => {
            assert_eq!(sequence_number, expected_sequence_number);
            assert_eq!(decrypt_skip, expected_decrypt_skip as usize);
            assert_eq!(plaintext_offset as i64, range.start - uncomp_off);
            assert_eq!(plaintext_length, 64);
        }
        _ => panic!("expected encrypted read plan"),
    }

    let (mut reader, offset, length) = GetObjectReader::new(
        Box::new(Cursor::new(encrypted[expected_storage_offset..].to_vec())),
        Some(range.clone()),
        &object_info,
        &ObjectOptions::default(),
        &ssec_headers_from_key(customer_key),
    )
    .await
    .expect("encrypted+compressed indexed range read should be supported");

    let mut actual = Vec::new();
    reader
        .read_to_end(&mut actual)
        .await
        .expect("read indexed encrypted+compressed range");

    assert_eq!(offset, expected_storage_offset);
    assert_eq!(length, encrypted.len() as i64 - expected_storage_offset as i64);
    assert_eq!(reader.object_info.size, 64);
    assert_eq!(actual, plaintext[range.start as usize..range.start as usize + 64]);
}
