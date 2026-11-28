use super::*;
use rustfs_utils::http::metadata_compat::{SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, insert_str};
use sha2::{Digest, Sha256};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

const STREAM: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/compression-index/legacy-lz4-stream.bin"
));
const HISTORICAL_INDEX: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/compression-index/pre-zigzag-index.bin"
));
const CURRENT_INDEX: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/compression-index/zigzag-index.bin"));
const PLAINTEXT_LEN: usize = 3 * 1024 * 1024 + 123;
const PLAINTEXT_SHA256: &str = "68f9edf6f9600395d6d2b5517d97570fecd2a0c6dbe6130617ef415a3b48d89c";

fn fixture_info(index: &[u8]) -> ObjectInfo {
    let mut metadata = HashMap::new();
    insert_str(&mut metadata, SUFFIX_COMPRESSION, "lz4".to_owned());
    insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, PLAINTEXT_LEN.to_string());
    ObjectInfo {
        size: i64::try_from(STREAM.len()).expect("fixed fixture size fits i64"),
        parts: Arc::new(vec![ObjectPartInfo {
            number: 1,
            size: STREAM.len(),
            actual_size: i64::try_from(PLAINTEXT_LEN).expect("fixed plaintext length fits i64"),
            index: Some(Bytes::copy_from_slice(index)),
            ..Default::default()
        }]),
        user_defined: Arc::new(metadata),
        ..Default::default()
    }
}

fn fixture_plaintext() -> Vec<u8> {
    (0..PLAINTEXT_LEN)
        .map(|offset| {
            let chunk = offset / 1024;
            let lane = offset % 1024;
            if lane < 8 {
                u64::try_from(chunk).expect("fixed chunk number fits u64").to_le_bytes()[lane]
            } else {
                b"abcdefghijklmnopqrstuvwxyz"[(offset + chunk) % 26]
            }
        })
        .collect()
}

#[tokio::test]
async fn historical_index_preserves_the_complete_compressed_body() {
    let info = fixture_info(HISTORICAL_INDEX);
    let (mut reader, storage_offset, _) = GetObjectReader::new(
        Box::new(Cursor::new(STREAM.to_vec())),
        None,
        &info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("construct reader for fixed historical index");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.expect("read complete historical body");
    assert_eq!(storage_offset, 0);
    assert_eq!(actual.len(), PLAINTEXT_LEN);
    assert_eq!(
        hex_simd::encode_to_string(Sha256::digest(&actual), hex_simd::AsciiCase::Lower),
        PLAINTEXT_SHA256
    );
}

#[tokio::test]
async fn historical_index_preserves_ranges_across_compression_blocks() {
    let info = fixture_info(HISTORICAL_INDEX);
    let expected = fixture_plaintext();
    for start in [7, 1024 * 1024 - 17, 2 * 1024 * 1024 + 31, PLAINTEXT_LEN - 97] {
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: i64::try_from(start).expect("fixed range start fits i64"),
            end: i64::try_from(start + 63).expect("fixed range end fits i64"),
        };
        let plan = ReadPlan::build(Some(range.clone()), &info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("plan historical compressed range");
        let physical = plan.storage_offset;
        let input = STREAM.get(physical..).expect("planned offset stays within the fixed stream");
        let (mut reader, _, _) = GetObjectReader::new(
            Box::new(Cursor::new(input.to_vec())),
            Some(range),
            &info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await
        .expect("construct historical range reader");
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read historical range");
        assert_eq!(actual, expected[start..start + 64], "range starting at {start}");
    }
}

#[cfg(not(feature = "m-compatible"))]
#[tokio::test]
async fn current_index_selects_a_fixed_compressed_block_for_a_range() {
    let info = fixture_info(CURRENT_INDEX);
    let start = 2 * 1024 * 1024 + 31;
    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: i64::try_from(start).expect("fixed range start fits i64"),
        end: i64::try_from(start + 63).expect("fixed range end fits i64"),
    };
    let plan = ReadPlan::build(Some(range.clone()), &info, &ObjectOptions::default(), &HeaderMap::new())
        .await
        .expect("plan range with fixed current index");
    assert_eq!(plan.storage_offset, 34_663, "use the recorded second block boundary");
    let physical = plan.storage_offset;
    let (mut reader, _, _) = GetObjectReader::new(
        Box::new(Cursor::new(STREAM[physical..].to_vec())),
        Some(range),
        &info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("construct indexed range reader");
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await.expect("read indexed range");
    assert_eq!(actual, fixture_plaintext()[start..start + 64]);
}

#[tokio::test]
async fn truncated_index_falls_back_without_truncating_the_range() {
    let info = fixture_info(&HISTORICAL_INDEX[..HISTORICAL_INDEX.len() - 3]);
    let start = 2 * 1024 * 1024 + 31;
    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: i64::try_from(start).expect("fixed range start fits i64"),
        end: i64::try_from(start + 96).expect("fixed range end fits i64"),
    };
    let plan = ReadPlan::build(Some(range.clone()), &info, &ObjectOptions::default(), &HeaderMap::new())
        .await
        .expect("invalid index still permits a sequential range plan");
    assert_eq!(plan.storage_offset, 0, "truncated index must not select a storage offset");
    let (mut reader, _, _) = GetObjectReader::new(
        Box::new(Cursor::new(STREAM.to_vec())),
        Some(range),
        &info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("construct sequential fallback reader");
    let mut actual = Vec::new();
    reader
        .read_to_end(&mut actual)
        .await
        .expect("read range without the truncated index");
    assert_eq!(actual, fixture_plaintext()[start..start + 97]);
}

#[test]
fn compression_metadata_requires_one_complete_index() {
    use crate::io_support::rio::decode_compression_index_bytes;

    let index = Bytes::from_static(CURRENT_INDEX);
    assert!(decode_compression_index_bytes(&index).is_some());
    let trailing = Bytes::from([CURRENT_INDEX, b"trailing bytes"].concat());
    assert!(decode_compression_index_bytes(&trailing).is_none());
}

#[tokio::test]
async fn historical_index_fallback_does_not_hide_a_body_checksum_failure() {
    let info = fixture_info(HISTORICAL_INDEX);
    let mut corrupted = STREAM.to_vec();
    corrupted[4] ^= 1; // Corrupt the first block's stored CRC, leaving its payload intact.
    let (mut reader, _, _) = GetObjectReader::new(
        Box::new(Cursor::new(corrupted)),
        None,
        &info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("construct reader before body checksum verification");
    let mut actual = Vec::new();
    let error = reader
        .read_to_end(&mut actual)
        .await
        .expect_err("bad body CRC cannot become successful EOF");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(actual.len() < PLAINTEXT_LEN);
}

#[tokio::test]
async fn compressed_ranges_bound_storage_for_large_index_and_unsampled_tail() {
    use crate::io_support::rio::compression_index_storage_bytes;
    use rustfs_rio::{CompressReader, TryGetIndex};

    const BLOCK: usize = 1024 * 1024;
    const BLOCKS: usize = 203;
    const TAIL: usize = 32 * 1024;
    const LENGTH: usize = 16 * 1024;
    let len = BLOCK * BLOCKS + TAIL;
    let mut compressor = CompressReader::new(
        tokio::io::repeat(b'a')
            .take((len - TAIL) as u64)
            .chain(tokio::io::repeat(b'z').take(TAIL as u64)),
        CompressionAlgorithm::Lz4,
    );
    let mut stored = Vec::new();
    compressor.read_to_end(&mut stored).await.expect("compress fixture");
    let index = compressor.try_get_index().expect("legacy writer has an index");
    assert!(index.len() > 200);
    assert_eq!(index.total_uncompressed, (len - TAIL) as i64, "short final block is not indexed");
    let index = compression_index_storage_bytes(index);
    let mut info = fixture_info(&index);
    info.size = stored.len() as i64;
    info.actual_size = len as i64;
    let part = &mut Arc::make_mut(&mut info.parts)[0];
    part.size = stored.len();
    part.actual_size = len as i64;

    for start in [1000, 9999, BLOCK - 7, len - LENGTH] {
        let range = if start == len - LENGTH {
            HTTPRangeSpec {
                is_suffix_length: true,
                start: -(LENGTH as i64),
                end: -1,
            }
        } else {
            HTTPRangeSpec {
                is_suffix_length: false,
                start: start as i64,
                end: (start + LENGTH - 1) as i64,
            }
        };
        let plan = ReadPlan::build(Some(range), &info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("plan valid range");
        let physical_start = plan.storage_offset;
        let physical_end = physical_start + usize::try_from(plan.storage_length).expect("positive storage window");
        let mut output = Vec::new();
        let (mut reader, _, _) = plan
            .into_reader(Box::new(Cursor::new(stored[physical_start..physical_end].to_vec())), &info)
            .expect("valid plaintext range has a reader");
        reader.read_to_end(&mut output).await.expect("decode from planned window");
        let expected_byte = if start == len - LENGTH { b'z' } else { b'a' };
        assert_eq!(output, vec![expected_byte; LENGTH], "range starting at {start}");
        assert!(
            physical_end - physical_start < stored.len() / 20,
            "range at {start} reads too much compressed data"
        );
        if start == len - LENGTH {
            assert!(physical_start > stored.len() * 9 / 10, "suffix must seek within the final part");
        }
    }
}

#[tokio::test]
async fn compressed_multipart_window_stops_at_the_last_covering_part() {
    let mut info = fixture_info(CURRENT_INDEX);
    let parts = Arc::make_mut(&mut info.parts);
    parts.push(ObjectPartInfo {
        number: 2,
        ..parts[0].clone()
    });
    parts.push(ObjectPartInfo {
        number: 3,
        ..parts[0].clone()
    });
    info.size *= 3;
    info.actual_size = (PLAINTEXT_LEN * 3) as i64;

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: PLAINTEXT_LEN as i64 - 16,
        end: PLAINTEXT_LEN as i64 + 15,
    };
    let plan = ReadPlan::build(Some(range), &info, &ObjectOptions::default(), &HeaderMap::new())
        .await
        .expect("valid range across parts");
    let physical_start = plan.storage_offset;
    let physical_end = physical_start + plan.storage_length as usize;
    assert!(physical_end > STREAM.len(), "range crosses the stored part boundary");
    assert!(physical_end < STREAM.len() * 2, "range should stop at the first checkpoint in part two");
    let stored = STREAM.repeat(3);
    let (mut reader, _, _) = plan
        .into_reader(Box::new(Cursor::new(stored[physical_start..physical_end].to_vec())), &info)
        .expect("create multipart range reader");
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await.expect("decode part boundary");
    let plaintext = fixture_plaintext();
    assert_eq!(output, [&plaintext[PLAINTEXT_LEN - 16..], &plaintext[..16]].concat());
}
