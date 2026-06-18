#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_filemeta::FileMeta;
use rustfs_utils::compress::{CompressionAlgorithm, decompress_block};
use std::collections::BTreeSet;

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

fn exercise_payload(payload: &[u8], algorithm: CompressionAlgorithm) {
    let _ = FileMeta::load(payload);
    let _ = FileMeta::load_or_convert(payload);
    let _ = FileMeta::read_format_versions(payload);
    let _ = decompress_block(payload, algorithm);
}

fn interesting_prefix_lengths(len: usize) -> BTreeSet<usize> {
    let mut lengths = BTreeSet::from([0usize, 1, 2, 4, 5, 8, 16, 32]);
    lengths.insert(len);
    lengths.insert(len.saturating_sub(1));
    lengths.into_iter().filter(|candidate| *candidate <= len).collect()
}

fuzz_target!(|data: &[u8]| {
    let algorithm = pick_algorithm(data.first().copied().unwrap_or_default());

    exercise_payload(data, algorithm);

    for prefix_len in interesting_prefix_lengths(data.len()) {
        exercise_payload(&data[..prefix_len], algorithm);
    }
});
