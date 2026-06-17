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

fuzz_target!(|data: &[u8]| {
    let algorithm = pick_algorithm(data.first().copied().unwrap_or_default());

    let _ = FileMeta::load(data);
    let _ = FileMeta::read_format_versions(data);
    let _ = decompress_block(data, algorithm);
});
