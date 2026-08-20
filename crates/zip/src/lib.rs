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

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder, ZstdDecoder};
use thiserror::Error;
use tokio::io::{AsyncRead, BufReader};

pub type Result<T> = std::result::Result<T, ZipError>;

#[derive(Debug, Error)]
pub enum ZipError {
    #[error("unsupported {operation} for format {format:?}")]
    UnsupportedFormat {
        format: CompressionFormat,
        operation: &'static str,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompressionFormat {
    Gzip,
    Bzip2,
    Zip,
    Xz,
    Zlib,
    Zstd,
    Tar,
    Unknown,
}

/// Archive guardrails. The values are carried here so every archive caller
/// shares one default policy; enforcement belongs to the caller, which maps a
/// breach onto its own protocol error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_entries: usize,
    pub max_entry_size: u64,
    pub max_total_unpacked_size: u64,
    pub max_path_length: usize,
    pub validate_entry_paths: bool,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_entry_size: 1_073_741_824,
            max_total_unpacked_size: 10_737_418_240,
            max_path_length: 1024,
            validate_entry_paths: true,
        }
    }
}

impl CompressionFormat {
    /// Map an archive extension onto the stream codec needed to read it.
    /// Tar-family suffixes (`tgz`, `tbz2`, `txz`, `tzst`, ...) resolve to their
    /// codec because the tar container itself is read from the decoded stream.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "gz" | "gzip" | "tgz" => CompressionFormat::Gzip,
            "bz2" | "bzip2" | "tbz" | "tbz2" => CompressionFormat::Bzip2,
            "xz" | "txz" => CompressionFormat::Xz,
            "zlib" | "zz" => CompressionFormat::Zlib,
            "zst" | "zstd" | "tzst" => CompressionFormat::Zstd,
            "tar" => CompressionFormat::Tar,
            "zip" => CompressionFormat::Zip,
            _ => CompressionFormat::Unknown,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            CompressionFormat::Gzip => "gz",
            CompressionFormat::Bzip2 => "bz2",
            CompressionFormat::Zip => "zip",
            CompressionFormat::Xz => "xz",
            CompressionFormat::Zlib => "zlib",
            CompressionFormat::Zstd => "zst",
            CompressionFormat::Tar => "tar",
            CompressionFormat::Unknown => "",
        }
    }

    pub fn get_decoder<R>(&self, input: R) -> Result<Box<dyn AsyncRead + Send + Unpin>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let reader = BufReader::new(input);

        let decoder: Box<dyn AsyncRead + Send + Unpin + 'static> = match self {
            CompressionFormat::Gzip => Box::new(GzipDecoder::new(reader)),
            CompressionFormat::Bzip2 => Box::new(BzDecoder::new(reader)),
            CompressionFormat::Zlib => Box::new(ZlibDecoder::new(reader)),
            CompressionFormat::Xz => Box::new(XzDecoder::new(reader)),
            CompressionFormat::Zstd => Box::new(ZstdDecoder::new(reader)),
            CompressionFormat::Tar => Box::new(reader),
            CompressionFormat::Zip => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "stream decoding",
                });
            }
            CompressionFormat::Unknown => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "decoding",
                });
            }
        };

        Ok(decoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::write::GzipEncoder;
    use std::mem::size_of;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_compression_format_from_extension() {
        assert_eq!(CompressionFormat::from_extension("gz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_extension("ZIP"), CompressionFormat::Zip);
        assert_eq!(CompressionFormat::from_extension("tzst"), CompressionFormat::Zstd);
        assert_eq!(CompressionFormat::from_extension("txt"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_compression_format_size_is_small() {
        assert!(size_of::<CompressionFormat>() <= 8);
        assert!(size_of::<Option<CompressionFormat>>() <= 16);
    }

    #[tokio::test]
    async fn test_get_decoder_round_trips_gzip_stream() {
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(b"payload").await.expect("gzip encode should succeed");
        encoder.shutdown().await.expect("gzip encoder shutdown should succeed");

        let mut decoder = CompressionFormat::Gzip
            .get_decoder(std::io::Cursor::new(encoder.into_inner()))
            .expect("gzip decoder should be created");
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.expect("gzip decode should succeed");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_get_decoder_rejects_zip_and_unknown_formats() {
        let zip_err = CompressionFormat::Zip
            .get_decoder(std::io::Cursor::new(Vec::<u8>::new()))
            .err()
            .expect("zip stream decoding should be rejected");
        assert!(matches!(
            zip_err,
            ZipError::UnsupportedFormat {
                format: CompressionFormat::Zip,
                operation: "stream decoding",
            }
        ));

        let unknown_err = CompressionFormat::Unknown
            .get_decoder(std::io::Cursor::new(Vec::<u8>::new()))
            .err()
            .expect("unknown format decoding should be rejected");
        assert!(matches!(
            unknown_err,
            ZipError::UnsupportedFormat {
                format: CompressionFormat::Unknown,
                operation: "decoding",
            }
        ));
    }
}
