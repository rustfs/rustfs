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

    // Prefix probing — only the fast parsers, critical boundary lengths:
    //   0   = empty input
    //   1-5 = partial magic header ("XL2\0" is 4 bytes + version byte)
    //   8   = covers the header + start of bin32 length field
    if data.len() > 0 {
        exercise_prefix(&data[..0]);
    }
    if data.len() > 1 {
        exercise_prefix(&data[..1]);
    }
    if data.len() > 5 {
        exercise_prefix(&data[..5]);
    }
    if data.len() > 8 {
        exercise_prefix(&data[..8]);
    }
    if data.len() > 1 {
        exercise_prefix(&data[..data.len() - 1]);
    }
});
