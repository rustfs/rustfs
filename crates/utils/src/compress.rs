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

use std::io::Write;
use std::{fmt, str};
use tokio::io;

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
    match algorithm {
        CompressionAlgorithm::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(compressed));
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::Deflate => {
            let mut decoder = flate2::read::DeflateDecoder::new(std::io::Cursor::new(compressed));
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::Zstd => {
            let mut decoder = zstd::Decoder::new(std::io::Cursor::new(compressed))?;
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::Lz4 => {
            let mut decoder = lz4::Decoder::new(std::io::Cursor::new(compressed)).expect("lz4 decoder");
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::Brotli => {
            let mut out = Vec::new();
            let mut decoder = brotli::Decompressor::new(std::io::Cursor::new(compressed), 4096);
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::Snappy => {
            let mut decoder = snap::read::FrameDecoder::new(std::io::Cursor::new(compressed));
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
        CompressionAlgorithm::None => Ok(Vec::new()),
    }
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
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Gzip).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_deflate() {
        let data = b"hello deflate compress";
        let compressed = compress_block(data, CompressionAlgorithm::Deflate);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Deflate).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"hello zstd compress";
        let compressed = compress_block(data, CompressionAlgorithm::Zstd);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Zstd).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"hello lz4 compress";
        let compressed = compress_block(data, CompressionAlgorithm::Lz4);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Lz4).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_brotli() {
        let data = b"hello brotli compress";
        let compressed = compress_block(data, CompressionAlgorithm::Brotli);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Brotli).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let data = b"hello snappy compress";
        let compressed = compress_block(data, CompressionAlgorithm::Snappy);
        let decompressed = decompress_block(&compressed, CompressionAlgorithm::Snappy).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_from_str() {
        assert_eq!(CompressionAlgorithm::from_str("gzip").unwrap(), CompressionAlgorithm::Gzip);
        assert_eq!(CompressionAlgorithm::from_str("deflate").unwrap(), CompressionAlgorithm::Deflate);
        assert_eq!(CompressionAlgorithm::from_str("zstd").unwrap(), CompressionAlgorithm::Zstd);
        assert_eq!(CompressionAlgorithm::from_str("lz4").unwrap(), CompressionAlgorithm::Lz4);
        assert_eq!(CompressionAlgorithm::from_str("brotli").unwrap(), CompressionAlgorithm::Brotli);
        assert_eq!(CompressionAlgorithm::from_str("snappy").unwrap(), CompressionAlgorithm::Snappy);
        assert!(CompressionAlgorithm::from_str("unknown").is_err());
    }

    #[test]
    fn test_compare_compression_algorithms() {
        use std::time::Instant;
        let data = vec![42u8; 1024 * 100]; // 100KB of repetitive data

        // let mut data = vec![0u8; 1024 * 1024];
        // rand::thread_rng().fill(&mut data[..]);

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
        assert_eq!(decompress_block(&gzip, CompressionAlgorithm::Gzip).unwrap(), data);
        assert_eq!(decompress_block(&deflate, CompressionAlgorithm::Deflate).unwrap(), data);
        assert_eq!(decompress_block(&zstd, CompressionAlgorithm::Zstd).unwrap(), data);
        assert_eq!(decompress_block(&lz4, CompressionAlgorithm::Lz4).unwrap(), data);
        assert_eq!(decompress_block(&brotli, CompressionAlgorithm::Brotli).unwrap(), data);
        assert_eq!(decompress_block(&snappy, CompressionAlgorithm::Snappy).unwrap(), data);
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
                let _decompressed = decompress_block(&compressed, algo).unwrap();
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
