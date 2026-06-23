#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_filemeta::FileMeta;
use rustfs_utils::compress::{CompressionAlgorithm, decompress_block};

fn pick_algorithm(tag: u8) -> CompressionAlgorithm {
    match tag % 6 {
        0 => CompressionAlgorithm::Gzip,
        1 => CompressionAlgorithm::Deflate,
        2 => CompressionAlgorithm::Zstd,
        3 => CompressionAlgorithm::Lz4,
        4 => CompressionAlgorithm::Brotli,
        _ => CompressionAlgorithm::Snappy,
    }
}

/// Full exercise: all parsers + decompression on the complete payload.
fn exercise_full(payload: &[u8], algorithm: CompressionAlgorithm) {
    let _ = FileMeta::load(payload);
    let _ = FileMeta::load_or_convert(payload);
    let _ = FileMeta::read_format_versions(payload);
    let _ = decompress_block(payload, algorithm);
}

/// Lightweight exercise: only metadata parsers on a prefix (no decompression).
/// Prefix tests catch partial-input / truncated-header panics cheaply.
fn exercise_prefix(payload: &[u8]) {
    let _ = FileMeta::load(payload);
    let _ = FileMeta::read_format_versions(payload);
}

fuzz_target!(|data: &[u8]| {
    let algorithm = pick_algorithm(data.first().copied().unwrap_or_default());

    // Full exercise on the complete input.
    exercise_full(data, algorithm);

    // Prefix probing aligned with xl.meta binary layout:
    //   4  = magic header ("XL2 ")
    //   5  = magic + version byte
    //   8  = magic + version + start of bin32 length field
    //   12 = magic + version + bin32 length (4 bytes) + CRC start
    //   len-1 = truncated-at-end
    //
    // Only test prefixes that are strictly smaller than the full input
    // (the full input is already tested above).
    for prefix_len in [4usize, 5, 8, 12] {
        if prefix_len < data.len() {
            exercise_prefix(&data[..prefix_len]);
        }
    }
    if data.len() > 1 {
        exercise_prefix(&data[..data.len() - 1]);
    }
});
