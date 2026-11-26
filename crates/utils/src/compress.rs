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

use std::io::{self, Read, Write};
use std::{fmt, str};

/// Cap the Zstd history window when decoding a bounded block so a crafted frame
/// cannot reserve a 128 MiB window before the output limit observes any bytes.
const ZSTD_BLOCK_MAX_WINDOW_LOG: u32 = 24;
const ZSTD_BOUNDED_BLOCK_LIMIT: usize = 1 << ZSTD_BLOCK_MAX_WINDOW_LOG;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Deflate,
    Zstd,
    #[default]
    Lz4,
    Brotli,
    Snappy,
}

impl CompressionAlgorithm {
    pub fn as_str(&self) -> &str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Deflate => "deflate",
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::Brotli => "brotli",
            CompressionAlgorithm::Snappy => "snappy",
        }
    }
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
impl str::FromStr for CompressionAlgorithm {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(CompressionAlgorithm::Gzip),
            "deflate" => Ok(CompressionAlgorithm::Deflate),
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            "lz4" => Ok(CompressionAlgorithm::Lz4),
            "brotli" => Ok(CompressionAlgorithm::Brotli),
            "snappy" => Ok(CompressionAlgorithm::Snappy),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(std::io::Error::other(format!("Unsupported compression algorithm: {s}"))),
        }
    }
}

/// Compress a block of data using the specified compression algorithm.
/// Returns the compressed data as a Vec<u8>.
///
/// # Arguments
/// * `input` - The input data to be compressed.
/// * `algorithm` - The compression algorithm to use.
///
/// # Returns
/// * A Vec<u8> containing the compressed data.
///
pub fn compress_block(input: &[u8], algorithm: CompressionAlgorithm) -> Vec<u8> {
    match algorithm {
        CompressionAlgorithm::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            let _ = encoder.write_all(input);
            let _ = encoder.flush();
            encoder.finish().unwrap_or_default()
        }
        CompressionAlgorithm::Deflate => {
            let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            let _ = encoder.write_all(input);
            let _ = encoder.flush();
            encoder.finish().unwrap_or_default()
        }
        CompressionAlgorithm::Zstd => {
            let mut encoder = zstd::Encoder::new(Vec::new(), 0).expect("zstd encoder");
            let _ = encoder.write_all(input);
            encoder.finish().unwrap_or_default()
        }
        CompressionAlgorithm::Lz4 => {
            let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).expect("lz4 encoder");
            let _ = encoder.write_all(input);
            let (out, result) = encoder.finish();
            result.expect("lz4 finish");
            out
        }
        CompressionAlgorithm::Brotli => {
            let mut out = Vec::new();
            brotli::CompressorWriter::new(&mut out, 4096, 5, 22)
                .write_all(input)
                .expect("brotli compress");
            out
        }
        CompressionAlgorithm::Snappy => {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            let _ = encoder.write_all(input);
            encoder.into_inner().unwrap_or_default()
        }
        CompressionAlgorithm::None => input.to_vec(),
    }
}

/// Decompress a block of data using the specified compression algorithm.
/// Returns the decompressed data as a Vec<u8>.
///
/// # Arguments
/// * `compressed` - The compressed data to be decompressed.
/// * `algorithm` - The compression algorithm used for compression.
///
/// # Returns
/// * A Result containing a Vec<u8> with the decompressed data, or an io::Error.
///
pub fn decompress_block(compressed: &[u8], algorithm: CompressionAlgorithm) -> io::Result<Vec<u8>> {
    decompress_block_with_limit(compressed, algorithm, usize::MAX)
}

/// Decompress a block of data while capping the decoded output at `max_len` bytes.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidData`] if the decompressed output exceeds `max_len`,
/// or propagates any underlying codec initialization or decoding error.
pub fn decompress_block_with_limit(compressed: &[u8], algorithm: CompressionAlgorithm, max_len: usize) -> io::Result<Vec<u8>> {
    let input = std::io::Cursor::new(compressed);
    match algorithm {
        CompressionAlgorithm::Gzip => read_to_end_with_limit(flate2::read::GzDecoder::new(input), max_len),
        CompressionAlgorithm::Deflate => read_to_end_with_limit(flate2::read::DeflateDecoder::new(input), max_len),
        CompressionAlgorithm::Zstd => {
            let mut decoder = zstd::Decoder::new(input)?;
            if max_len <= ZSTD_BOUNDED_BLOCK_LIMIT {
                decoder.window_log_max(ZSTD_BLOCK_MAX_WINDOW_LOG)?;
            }
            read_to_end_with_limit(decoder, max_len)
        }
        CompressionAlgorithm::Lz4 => read_to_end_with_limit(lz4::Decoder::new(input)?, max_len),
        CompressionAlgorithm::Brotli => read_to_end_with_limit(brotli::Decompressor::new(input, 4096), max_len),
        CompressionAlgorithm::Snappy => read_to_end_with_limit(snap::read::FrameDecoder::new(input), max_len),
        CompressionAlgorithm::None => Ok(Vec::new()),
    }
}

fn read_to_end_with_limit(decoder: impl Read, max_len: usize) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    // Read at most one byte past `max_len`: enough to distinguish an exact-length
    // frame (and verify its trailer) from an over-limit expansion without unbounded allocation.
    let probe_len = u64::try_from(max_len).unwrap_or(u64::MAX).saturating_add(1);
    decoder.take(probe_len).read_to_end(&mut out)?;
    if out.len() > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed block exceeds its declared length",
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::time::Instant;

    #[test]
    fn test_compress_decompress_gzip() {
        let data = b"hello gzip compress";
        let compressed = compress_block(data, CompressionAlgorithm::Gzip);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Gzip).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_deflate() {
        let data = b"hello deflate compress";
        let compressed = compress_block(data, CompressionAlgorithm::Deflate);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Deflate).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"hello zstd compress";
        let compressed = compress_block(data, CompressionAlgorithm::Zstd);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Zstd).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"hello lz4 compress";
        let compressed = compress_block(data, CompressionAlgorithm::Lz4);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Lz4).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_brotli() {
        let data = b"hello brotli compress";
        let compressed = compress_block(data, CompressionAlgorithm::Brotli);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Brotli).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let data = b"hello snappy compress";
        let compressed = compress_block(data, CompressionAlgorithm::Snappy);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Snappy).expect("operation should succeed");
        assert_eq!(decompressed, data);
    }

    fn build_zstd_rle_frame(block_count: usize, block_len: u32) -> Vec<u8> {
        let mut frame = Vec::with_capacity(6 + block_count * 4);
        frame.extend_from_slice(&[0x28, 0xB5, 0x2F, 0xFD]); // Zstd magic
        frame.push(0x00); // Frame_Header_Descriptor
        frame.push(0x38); // Window_Descriptor: exponent=7 -> 128 KiB window
        for idx in 0..block_count {
            let last_block = u32::from(idx + 1 == block_count);
            let block_header = last_block | (1 << 1) | (block_len << 3);
            frame.extend_from_slice(&block_header.to_le_bytes()[..3]);
            frame.push(0x00); // repeated byte
        }
        frame
    }

    #[test]
    fn test_decompress_block_with_limit_stops_at_limit() {
        let data = vec![0u8; 1024 * 1024];
        for algo in [
            CompressionAlgorithm::Gzip,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Snappy,
        ] {
            let compressed = compress_block(&data, algo);
            let exact = decompress_block_with_limit(&compressed, algo, data.len()).expect("exact limit should succeed");
            assert_eq!(exact, data, "{algo}");

            let err = decompress_block_with_limit(&compressed, algo, data.len() - 1).expect_err("limit must be enforced");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{algo}");
            assert_eq!(err.to_string(), "decompressed block exceeds its declared length", "{algo}");

            let zero_err = decompress_block_with_limit(&compressed, algo, 0).expect_err("zero limit must reject non-empty block");
            assert_eq!(zero_err.kind(), io::ErrorKind::InvalidData, "{algo}");
        }
        assert!(
            decompress_block_with_limit(b"ignored", CompressionAlgorithm::None, 0)
                .expect("CompressionAlgorithm::None returns empty")
                .is_empty()
        );
    }

    #[test]
    fn test_decompress_block_with_limit_rejects_high_ratio_zstd_rle_frame() {
        // 512 x 128 KiB = 64 MiB of zeros in ~2 KiB of compressed data.
        let frame = build_zstd_rle_frame(512, 128 * 1024);
        assert!(frame.len() < 4096, "fixture must be compact");

        let err = decompress_block_with_limit(&frame, CompressionAlgorithm::Zstd, 4096)
            .expect_err("high-ratio expansion past limit must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "decompressed block exceeds its declared length");
    }

    #[test]
    fn test_decompress_block_with_limit_caps_zstd_window_log() {
        // Window_Descriptor exponent=17 -> windowLog = 27 (128 MiB > 16 MiB cap).
        let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x00, 17 << 3];
        let last_raw_1byte: u32 = 1 | (1 << 3);
        frame.extend_from_slice(&last_raw_1byte.to_le_bytes()[..3]);
        frame.push(b'a');

        let err = decompress_block_with_limit(&frame, CompressionAlgorithm::Zstd, 16)
            .expect_err("bounded decode must reject zstd frames requesting > 16 MiB window");
        assert_eq!(err.kind(), io::ErrorKind::Other);

        let unbounded = decompress_block(&frame, CompressionAlgorithm::Zstd).expect("unbounded decode keeps compatibility");
        assert_eq!(unbounded, b"a");
    }

    #[test]
    fn test_decompress_block_lz4_malformed_frame_returns_error_without_panic() {
        for truncated in [b"".as_slice(), b"\x04\x22\x4D\x18".as_slice()] {
            if let Ok(out) = decompress_block_with_limit(truncated, CompressionAlgorithm::Lz4, 1024) {
                assert!(out.is_empty(), "truncated lz4 frame must not yield data");
            }
        }
        let cases: &[&[u8]] = &[
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x04\x22\x4D\x18\xFF\xFF\xFF\xFF", // valid magic with invalid flag/reserved bits
            b"not-an-lz4-frame-payload",
        ];
        for malformed in cases {
            assert!(
                decompress_block(malformed, CompressionAlgorithm::Lz4).is_err(),
                "unbounded lz4 decode must return Err for malformed input {malformed:?}"
            );
            assert!(
                decompress_block_with_limit(malformed, CompressionAlgorithm::Lz4, 1024).is_err(),
                "bounded lz4 decode must return Err for malformed input {malformed:?}"
            );
        }
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            CompressionAlgorithm::from_str("gzip").expect("operation should succeed"),
            CompressionAlgorithm::Gzip
        );
        assert_eq!(
            CompressionAlgorithm::from_str("deflate").expect("operation should succeed"),
            CompressionAlgorithm::Deflate
        );
        assert_eq!(
            CompressionAlgorithm::from_str("zstd").expect("operation should succeed"),
            CompressionAlgorithm::Zstd
        );
        assert_eq!(
            CompressionAlgorithm::from_str("lz4").expect("operation should succeed"),
            CompressionAlgorithm::Lz4
        );
        assert_eq!(
            CompressionAlgorithm::from_str("brotli").expect("operation should succeed"),
            CompressionAlgorithm::Brotli
        );
        assert_eq!(
            CompressionAlgorithm::from_str("snappy").expect("operation should succeed"),
            CompressionAlgorithm::Snappy
        );
        assert!(CompressionAlgorithm::from_str("unknown").is_err());
    }

    #[test]
    fn test_compare_compression_algorithms() {
        use std::time::Instant;
        let data = vec![42u8; 1024 * 100]; // 100KB of repetitive data

        let start = Instant::now();

        let mut times = Vec::new();
        times.push(("original", start.elapsed(), data.len()));

        let start = Instant::now();
        let gzip = compress_block(&data, CompressionAlgorithm::Gzip);
        let gzip_time = start.elapsed();
        times.push(("gzip", gzip_time, gzip.len()));

        let start = Instant::now();
        let deflate = compress_block(&data, CompressionAlgorithm::Deflate);
        let deflate_time = start.elapsed();
        times.push(("deflate", deflate_time, deflate.len()));

        let start = Instant::now();
        let zstd = compress_block(&data, CompressionAlgorithm::Zstd);
        let zstd_time = start.elapsed();
        times.push(("zstd", zstd_time, zstd.len()));

        let start = Instant::now();
        let lz4 = compress_block(&data, CompressionAlgorithm::Lz4);
        let lz4_time = start.elapsed();
        times.push(("lz4", lz4_time, lz4.len()));

        let start = Instant::now();
        let brotli = compress_block(&data, CompressionAlgorithm::Brotli);
        let brotli_time = start.elapsed();
        times.push(("brotli", brotli_time, brotli.len()));

        let start = Instant::now();
        let snappy = compress_block(&data, CompressionAlgorithm::Snappy);
        let snappy_time = start.elapsed();
        times.push(("snappy", snappy_time, snappy.len()));

        println!("Compression results:");
        for (name, dur, size) in &times {
            println!("{name}: {size} bytes, {dur:?}");
        }
        // All should decompress to the original
        assert_eq!(
            decompress_block(&gzip, CompressionAlgorithm::Gzip).expect("operation should succeed"),
            data
        );
        assert_eq!(
            decompress_block(&deflate, CompressionAlgorithm::Deflate).expect("operation should succeed"),
            data
        );
        assert_eq!(
            decompress_block(&zstd, CompressionAlgorithm::Zstd).expect("operation should succeed"),
            data
        );
        assert_eq!(decompress_block(&lz4, CompressionAlgorithm::Lz4).expect("operation should succeed"), data);
        assert_eq!(
            decompress_block(&brotli, CompressionAlgorithm::Brotli).expect("operation should succeed"),
            data
        );
        assert_eq!(
            decompress_block(&snappy, CompressionAlgorithm::Snappy).expect("operation should succeed"),
            data
        );
        // All compressed results should not be empty
        assert!(
            !gzip.is_empty()
                && !deflate.is_empty()
                && !zstd.is_empty()
                && !lz4.is_empty()
                && !brotli.is_empty()
                && !snappy.is_empty()
        );
    }

    #[test]
    fn test_compression_benchmark() {
        let sizes = [128 * 1024, 512 * 1024, 1024 * 1024];
        let algorithms = [
            CompressionAlgorithm::Gzip,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Snappy,
        ];

        println!("\nCompression algorithm benchmark results:");
        println!(
            "{:<10} {:<10} {:<15} {:<15} {:<15}",
            "Data Size", "Algorithm", "Compress Time(ms)", "Compressed Size", "Compression Ratio"
        );

        for size in sizes {
            // Generate compressible data (repeated text pattern)
            let pattern = b"Hello, this is a test pattern that will be repeated multiple times to create compressible data. ";
            let data: Vec<u8> = pattern.iter().cycle().take(size).copied().collect();

            for algo in algorithms {
                // Compression test
                let start = Instant::now();
                let compressed = compress_block(&data, algo);
                let compression_time = start.elapsed();

                // Decompression test
                let start = Instant::now();
                let _decompressed = decompress_block(&compressed, algo).expect("operation should succeed");
                let _decompression_time = start.elapsed();

                // Calculate compression ratio
                let compression_ratio = (size as f64 / compressed.len() as f64) as f32;

                println!(
                    "{:<10} {:<10} {:<15.2} {:<15} {:<15.2}x",
                    format!("{}KB", size / 1024),
                    algo.as_str(),
                    compression_time.as_secs_f64() * 1000.0,
                    compressed.len(),
                    compression_ratio
                );

                // Verify decompression result
                assert_eq!(_decompressed, data);
            }
            println!(); // Add blank line to separate results of different sizes
        }
    }
}
