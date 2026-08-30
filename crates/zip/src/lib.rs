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

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, Lz4Decoder, XzDecoder, ZlibDecoder, ZstdDecoder};
use rustfs_rio::S2Decoder;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf};

const MAGIC_SNIFF_LEN: usize = 6;

pub type Result<T> = std::result::Result<T, ZipError>;

#[derive(Debug, Error)]
pub enum ZipError {
    #[error("unsupported {operation} for format {format:?}")]
    UnsupportedFormat {
        format: CompressionFormat,
        operation: &'static str,
    },
    #[error("failed to inspect archive compression magic")]
    InspectStream(#[source] io::Error),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompressionFormat {
    Gzip,
    Bzip2,
    Zip,
    Xz,
    Zlib,
    Zstd,
    Lz4,
    S2,
    Tar,
    Unknown,
}

/// Reader returned by [`CompressionFormat::sniff`]. It replays the bounded
/// prefix consumed for detection before continuing with the original stream.
#[derive(Debug)]
pub struct SniffedReader<R> {
    inner: R,
    prefix: [u8; MAGIC_SNIFF_LEN],
    prefix_len: usize,
    prefix_pos: usize,
}

impl<R> AsyncRead for SniffedReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.prefix_pos < self.prefix_len && buf.remaining() > 0 {
            let to_copy = buf.remaining().min(self.prefix_len - self.prefix_pos);
            buf.put_slice(&self.prefix[self.prefix_pos..self.prefix_pos + to_copy]);
            self.prefix_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

/// Archive guardrails. The values are carried here so every archive caller
/// shares one default policy; enforcement belongs to the caller, which maps a
/// breach onto its own protocol error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_entries: usize,
    pub max_entry_size: u64,
    pub max_total_unpacked_size: u64,
    pub max_decoded_size: u64,
    pub max_path_length: usize,
    pub max_pax_metadata_size: u64,
    pub max_total_pax_metadata_size: u64,
    pub max_pax_metadata_records: usize,
    pub max_total_pax_metadata_records: usize,
    pub validate_entry_paths: bool,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_entry_size: 1_073_741_824,
            max_total_unpacked_size: 10_737_418_240,
            max_decoded_size: 11_811_160_064,
            max_path_length: 1024,
            max_pax_metadata_size: 1_048_576,
            max_total_pax_metadata_size: 67_108_864,
            max_pax_metadata_records: 4_096,
            max_total_pax_metadata_records: 100_000,
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
            "lz4" | "tlz4" => CompressionFormat::Lz4,
            "s2" | "snappy" => CompressionFormat::S2,
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
            CompressionFormat::Lz4 => "lz4",
            CompressionFormat::S2 => "s2",
            CompressionFormat::Tar => "tar",
            CompressionFormat::Unknown => "",
        }
    }

    /// Detect a stream codec from a bounded prefix. Unknown bytes are a raw
    /// TAR stream, matching MinIO Snowball behavior; object names and suffixes
    /// do not participate in detection.
    pub fn from_magic(prefix: &[u8]) -> Self {
        if prefix.starts_with(&[0x1f, 0x8b, 0x08]) {
            return Self::Gzip;
        }
        if prefix.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
            || (prefix.len() >= 4 && (0x50..=0x5f).contains(&prefix[0]) && prefix[1..4] == [0x2a, 0x4d, 0x18])
        {
            return Self::Zstd;
        }
        if prefix.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
            return Self::Lz4;
        }
        if prefix.starts_with(&[0xff, 0x06, 0x00, 0x00]) {
            return Self::S2;
        }
        if prefix.starts_with(b"BZh") {
            return Self::Bzip2;
        }
        if prefix.starts_with(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]) {
            return Self::Xz;
        }
        if prefix.starts_with(b"PK\x03\x04") || prefix.starts_with(b"PK\x05\x06") || prefix.starts_with(b"PK\x07\x08") {
            return Self::Zip;
        }
        if prefix.len() >= 2 {
            let cmf = prefix[0];
            let flg = prefix[1];
            let header = u16::from(cmf) << 8 | u16::from(flg);
            if cmf & 0x0f == 8 && cmf >> 4 <= 7 && header % 31 == 0 {
                return Self::Zlib;
            }
        }

        Self::Tar
    }

    /// Read at most [`MAGIC_SNIFF_LEN`] bytes to identify the codec and return
    /// a reader that losslessly replays the inspected prefix.
    pub async fn sniff<R>(mut input: R) -> Result<(Self, SniffedReader<R>)>
    where
        R: AsyncRead + Unpin,
    {
        let mut prefix = [0u8; MAGIC_SNIFF_LEN];
        let mut prefix_len = 0usize;
        while prefix_len < prefix.len() {
            let read = input.read(&mut prefix[prefix_len..]).await.map_err(ZipError::InspectStream)?;
            if read == 0 {
                break;
            }
            prefix_len += read;
        }

        let format = Self::from_magic(&prefix[..prefix_len]);
        Ok((
            format,
            SniffedReader {
                inner: input,
                prefix,
                prefix_len,
                prefix_pos: 0,
            },
        ))
    }

    pub fn get_decoder<R>(&self, input: R) -> Result<Box<dyn AsyncRead + Send + Unpin>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let reader = BufReader::new(input);

        let decoder: Box<dyn AsyncRead + Send + Unpin + 'static> = match self {
            CompressionFormat::Gzip => {
                let mut decoder = GzipDecoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Bzip2 => {
                let mut decoder = BzDecoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Zlib => {
                let mut decoder = ZlibDecoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Xz => {
                let mut decoder = XzDecoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Zstd => {
                let mut decoder = ZstdDecoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Lz4 => {
                let mut decoder = Lz4Decoder::new(reader);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::S2 => Box::new(S2Decoder::new(reader)),
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
    use async_compression::tokio::write::{GzipEncoder, Lz4Encoder};
    use std::mem::size_of;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_compression_format_from_extension() {
        assert_eq!(CompressionFormat::from_extension("gz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_extension("ZIP"), CompressionFormat::Zip);
        assert_eq!(CompressionFormat::from_extension("tzst"), CompressionFormat::Zstd);
        assert_eq!(CompressionFormat::from_extension("s2"), CompressionFormat::S2);
        assert_eq!(CompressionFormat::from_extension("txt"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_compression_format_from_magic() {
        let cases: &[(&[u8], CompressionFormat)] = &[
            (&[0x1f, 0x8b, 0x08, 0x00], CompressionFormat::Gzip),
            (b"BZh9", CompressionFormat::Bzip2),
            (&[0x28, 0xb5, 0x2f, 0xfd], CompressionFormat::Zstd),
            (&[0x50, 0x2a, 0x4d, 0x18], CompressionFormat::Zstd),
            (&[0x04, 0x22, 0x4d, 0x18], CompressionFormat::Lz4),
            (&[0xff, 0x06, 0x00, 0x00], CompressionFormat::S2),
            (&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00], CompressionFormat::Xz),
            (&[0x78, 0x9c], CompressionFormat::Zlib),
            (b"PK\x03\x04", CompressionFormat::Zip),
            (b"plain tar bytes", CompressionFormat::Tar),
        ];

        for (magic, expected) in cases {
            assert_eq!(CompressionFormat::from_magic(magic), *expected, "magic={magic:02x?}");
        }
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

        let (format, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(encoder.into_inner()))
            .await
            .expect("gzip magic should be inspected");
        assert_eq!(format, CompressionFormat::Gzip);
        let mut decoder = format.get_decoder(sniffed).expect("gzip decoder should be created");
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.expect("gzip decode should succeed");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_get_decoder_consumes_concatenated_gzip_members() {
        async fn gzip_member(payload: &[u8]) -> Vec<u8> {
            let mut encoder = GzipEncoder::new(Vec::new());
            encoder.write_all(payload).await.expect("gzip encode should succeed");
            encoder.shutdown().await.expect("gzip encoder shutdown should succeed");
            encoder.into_inner()
        }

        let mut encoded = gzip_member(b"first-").await;
        encoded.extend(gzip_member(b"second").await);
        let mut decoder = CompressionFormat::Gzip
            .get_decoder(std::io::Cursor::new(encoded))
            .expect("gzip decoder should be created");
        let mut decoded = Vec::new();

        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("concatenated gzip members should decode");

        assert_eq!(decoded, b"first-second");
    }

    #[tokio::test]
    async fn test_get_decoder_round_trips_lz4_stream() {
        let mut encoder = Lz4Encoder::new(Vec::new());
        encoder.write_all(b"payload").await.expect("LZ4 encode should succeed");
        encoder.shutdown().await.expect("LZ4 encoder shutdown should succeed");

        let (format, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(encoder.into_inner()))
            .await
            .expect("LZ4 magic should be inspected");
        assert_eq!(format, CompressionFormat::Lz4);
        let mut decoder = format.get_decoder(sniffed).expect("LZ4 decoder should be created");
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.expect("LZ4 decode should succeed");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_sniff_replays_raw_tar_prefix() {
        let input = b"ustar-prefix-and-payload".to_vec();
        let (format, mut sniffed) = CompressionFormat::sniff(std::io::Cursor::new(input.clone()))
            .await
            .expect("raw TAR prefix should be inspected");
        let mut replayed = Vec::new();
        sniffed
            .read_to_end(&mut replayed)
            .await
            .expect("sniffed prefix should replay");

        assert_eq!(format, CompressionFormat::Tar);
        assert_eq!(replayed, input);
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
