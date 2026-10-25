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
#[cfg(not(feature = "rio-v2"))]
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

#[cfg(not(feature = "rio-v2"))]
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
