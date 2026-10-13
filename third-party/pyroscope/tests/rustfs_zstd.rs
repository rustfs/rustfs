//! Regression coverage for the temporary RustFS dependency patch.
#![cfg(feature = "backend-pprof-rs")]

use object::read::{CompressedData, CompressionFormat};

fn rle_frame(size: u32) -> Vec<u8> {
    // A non-single-segment frame with a 1 KiB window and one final RLE block.
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0, 0];
    let block_header = (size << 3) | 3;
    frame.extend_from_slice(&block_header.to_le_bytes()[..3]);
    frame.push(b'a');
    frame
}

#[test]
fn decodes_zstd_block_at_window_limit() {
    let frame = rle_frame(1024);
    let data = CompressedData {
        format: CompressionFormat::Zstandard,
        data: &frame,
        uncompressed_size: 1024,
    }
    .decompress()
    .expect("a block exactly at the frame window limit remains valid");
    assert_eq!(data.as_ref(), vec![b'a'; 1024]);
}

#[test]
fn rejects_zstd_block_exceeding_window_limit() {
    let frame = rle_frame(2048);
    assert!(
        CompressedData {
            format: CompressionFormat::Zstandard,
            data: &frame,
            uncompressed_size: 2048,
        }
        .decompress()
        .is_err(),
        "a compressed section must reject a block exceeding the frame window limit"
    );
}
