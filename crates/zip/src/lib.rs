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

use async_compression::{
    tokio::bufread::{BzDecoder, GzipDecoder, Lz4Decoder, XzDecoder, ZlibDecoder, ZstdDecoder},
    zstd::DParameter,
};
use rustfs_rio::S2Decoder;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf};

const MAGIC_SNIFF_LEN: usize = 6;
const TAR_HEADER_LEN: usize = 512;
const TAR_CHECKSUM_START: usize = 148;
const TAR_CHECKSUM_END: usize = 156;
const SHARED_SKIPPABLE_FRAME_HEADER_LEN: usize = 8;
const SNIFF_DISCARD_BUFFER_LEN: usize = 8 * 1024;
const SNIFF_YIELD_AFTER_BYTES: usize = 64 * 1024;
const SNIFF_YIELD_AFTER_FRAMES: usize = 64;
const SNIFF_YIELD_AFTER_READY_READS: usize = 64;

// XZ declares its LZMA2 dictionary size before producing decoded bytes, so the
// decoded-size guard cannot prevent that allocation. This accepts every
// standard xz preset (the largest needs about 65 MiB to decode) while keeping
// one Snowball decoder well below the multi-gigabyte archive budget.
const XZ_DECODER_MEMORY_LIMIT_BYTES: u64 = 128_u64 * 1024 * 1024;
// Match MinIO's Snowball decoder boundary so a frame cannot reserve an
// oversized history window before the decoded-size guard observes any bytes.
const ZSTD_DECODER_MAX_WINDOW_LOG: u32 = 24;

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
/// prefix that identified the codec before continuing with the original
/// stream. Codec-neutral leading skippable frames are consumed by `sniff` and
/// intentionally omitted from the replayed stream.
#[derive(Debug)]
pub struct SniffedReader<R> {
    inner: R,
    prefix: Vec<u8>,
    prefix_pos: usize,
}

impl<R> AsyncRead for SniffedReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.prefix_pos < self.prefix.len() && buf.remaining() > 0 {
            let to_copy = buf.remaining().min(self.prefix.len() - self.prefix_pos);
            buf.put_slice(&self.prefix[self.prefix_pos..self.prefix_pos + to_copy]);
            self.prefix_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

fn is_shared_skippable_magic(prefix: &[u8]) -> bool {
    prefix.len() >= 4 && (0x50..=0x5f).contains(&prefix[0]) && prefix[1..4] == [0x2a, 0x4d, 0x18]
}

#[derive(Debug, Default)]
struct SniffYieldBudget {
    ready_reads: usize,
    input_bytes: usize,
    frames: usize,
}

impl SniffYieldBudget {
    async fn record_read(&mut self, read: usize) {
        self.ready_reads = self.ready_reads.saturating_add(1);
        self.input_bytes = self.input_bytes.saturating_add(read);
        if self.ready_reads >= SNIFF_YIELD_AFTER_READY_READS || self.input_bytes >= SNIFF_YIELD_AFTER_BYTES {
            self.yield_now().await;
        }
    }

    async fn record_frame(&mut self) {
        self.frames = self.frames.saturating_add(1);
        if self.frames >= SNIFF_YIELD_AFTER_FRAMES {
            self.yield_now().await;
        }
    }

    async fn yield_now(&mut self) {
        tokio::task::yield_now().await;
        self.ready_reads = 0;
        self.input_bytes = 0;
        self.frames = 0;
    }
}

async fn inspect_fill_to<R>(input: &mut R, output: &mut Vec<u8>, target_len: usize, budget: &mut SniffYieldBudget) -> Result<bool>
where
    R: AsyncRead + Unpin,
{
    let mut scratch = [0u8; TAR_HEADER_LEN];
    while output.len() < target_len {
        let remaining = target_len - output.len();
        let to_read = remaining.min(scratch.len());
        let read = input.read(&mut scratch[..to_read]).await.map_err(ZipError::InspectStream)?;
        if read == 0 {
            return Ok(false);
        }
        output.extend_from_slice(&scratch[..read]);
        budget.record_read(read).await;
    }
    Ok(true)
}

async fn inspect_discard_fully<R>(
    input: &mut R,
    mut remaining: u64,
    budget: &mut SniffYieldBudget,
    truncated_message: &'static str,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut discard = [0u8; SNIFF_DISCARD_BUFFER_LEN];
    while remaining > 0 {
        let to_read = usize::try_from(remaining).map_or(discard.len(), |remaining| remaining.min(discard.len()));
        let read = input.read(&mut discard[..to_read]).await.map_err(ZipError::InspectStream)?;
        if read == 0 {
            return Err(ZipError::InspectStream(io::Error::new(io::ErrorKind::UnexpectedEof, truncated_message)));
        }
        let read_u64 =
            u64::try_from(read).map_err(|_| ZipError::InspectStream(io::Error::other("skippable frame read size overflowed")))?;
        remaining -= read_u64;
        budget.record_read(read).await;
    }
    Ok(())
}

fn tar_octal_field(field: &[u8]) -> Option<u64> {
    let mut value = 0u64;
    let mut saw_digit = false;
    let mut terminated = false;

    for byte in field {
        match *byte {
            b'0'..=b'7' if !terminated => {
                saw_digit = true;
                value = value.checked_mul(8)?.checked_add(u64::from(*byte - b'0'))?;
            }
            b' ' if !saw_digit && !terminated => {}
            b' ' | 0 if saw_digit || terminated => terminated = true,
            0 => terminated = true,
            _ => return None,
        }
    }

    saw_digit.then_some(value)
}

fn tar_numeric_field_has_shape(field: &[u8]) -> bool {
    if field.first().is_some_and(|byte| byte & 0x80 != 0) {
        return true;
    }
    tar_octal_field(field).is_some()
}

fn is_tar_header(prefix: &[u8]) -> bool {
    let Some(header) = prefix.get(..TAR_HEADER_LEN) else {
        return false;
    };
    if header.iter().all(|byte| *byte == 0) {
        return true;
    }

    let has_path = header[..100].iter().any(|byte| *byte != 0);
    if !has_path
        || ![(100, 108), (108, 116), (116, 124), (124, 136), (136, 148)]
            .iter()
            .all(|(start, end)| tar_numeric_field_has_shape(&header[*start..*end]))
    {
        return false;
    }

    let Some(expected) = tar_octal_field(&header[TAR_CHECKSUM_START..TAR_CHECKSUM_END]) else {
        return false;
    };
    let actual = header[..TAR_CHECKSUM_START]
        .iter()
        .chain(std::iter::repeat_n(&b' ', TAR_CHECKSUM_END - TAR_CHECKSUM_START))
        .chain(&header[TAR_CHECKSUM_END..])
        .fold(0u64, |sum, byte| sum + u64::from(*byte));
    actual == expected
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

    /// Detect a stream codec from an unambiguous bounded prefix. Ordinary
    /// unknown bytes are a raw TAR stream, matching MinIO Snowball behavior;
    /// the skippable-frame magic shared by Zstd and LZ4 remains
    /// [`CompressionFormat::Unknown`] until [`Self::sniff`] sees a later frame.
    /// Object names and suffixes do not participate in detection.
    ///
    /// Zlib deliberately has no magic match here: its two-byte header can also
    /// be the start of a valid TAR member name. Callers preserving historical
    /// zlib-by-extension behavior must apply that compatibility fallback only
    /// after this method returns [`CompressionFormat::Tar`].
    /// ZIP signatures are also left as TAR because stream ZIP decoding is not
    /// supported and those bytes are valid at the start of a TAR member name.
    pub fn from_magic(prefix: &[u8]) -> Self {
        if prefix.starts_with(&[0x1f, 0x8b, 0x08]) {
            return Self::Gzip;
        }
        if prefix.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            return Self::Zstd;
        }
        if is_shared_skippable_magic(prefix) {
            return Self::Unknown;
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
        Self::Tar
    }

    /// Identify the codec and return a reader that replays the deciding prefix.
    /// A complete, checksum-valid TAR header wins over a codec-like member-name
    /// prefix. Leading skippable frames shared by the Zstd and LZ4 frame formats
    /// are discarded with bounded memory until a non-skippable frame identifies
    /// the decoder. The underlying reader still observes every discarded byte,
    /// so callers can keep transport length and checksum accounting below this
    /// boundary.
    pub async fn sniff<R>(mut input: R) -> Result<(Self, SniffedReader<R>)>
    where
        R: AsyncRead + Unpin,
    {
        let mut prefix = Vec::with_capacity(TAR_HEADER_LEN);
        let mut budget = SniffYieldBudget::default();
        let _ = inspect_fill_to(&mut input, &mut prefix, MAGIC_SNIFF_LEN, &mut budget).await?;
        let deciding_prefix_len = prefix.len().min(MAGIC_SNIFF_LEN);
        let initial_format = Self::from_magic(&prefix[..deciding_prefix_len]);
        let needs_tar_disambiguation =
            matches!(initial_format, Self::Bzip2 | Self::Lz4) || is_shared_skippable_magic(&prefix[..deciding_prefix_len]);
        if !needs_tar_disambiguation {
            return Ok((
                initial_format,
                SniffedReader {
                    inner: input,
                    prefix,
                    prefix_pos: 0,
                },
            ));
        }

        let _ = inspect_fill_to(&mut input, &mut prefix, TAR_HEADER_LEN, &mut budget).await?;
        if is_tar_header(&prefix) {
            return Ok((
                Self::Tar,
                SniffedReader {
                    inner: input,
                    prefix,
                    prefix_pos: 0,
                },
            ));
        }

        let mut skipped_shared_frame = false;

        loop {
            if prefix.len() < MAGIC_SNIFF_LEN {
                let _ = inspect_fill_to(&mut input, &mut prefix, MAGIC_SNIFF_LEN, &mut budget).await?;
            }
            let deciding_prefix_len = prefix.len().min(MAGIC_SNIFF_LEN);
            if !is_shared_skippable_magic(&prefix[..deciding_prefix_len]) {
                let mut format = Self::from_magic(&prefix[..deciding_prefix_len]);
                if skipped_shared_frame && !matches!(format, Self::Zstd | Self::Lz4) {
                    format = Self::Unknown;
                }
                return Ok((
                    format,
                    SniffedReader {
                        inner: input,
                        prefix,
                        prefix_pos: 0,
                    },
                ));
            }

            skipped_shared_frame = true;
            let complete_header =
                inspect_fill_to(&mut input, &mut prefix, SHARED_SKIPPABLE_FRAME_HEADER_LEN, &mut budget).await?;
            if !complete_header {
                return Err(ZipError::InspectStream(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated shared Zstd/LZ4 skippable frame header",
                )));
            }
            let payload_len =
                usize::try_from(u32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]])).map_err(|_| {
                    ZipError::InspectStream(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "shared Zstd/LZ4 skippable frame length does not fit usize",
                    ))
                })?;
            let buffered_payload_len = (prefix.len() - SHARED_SKIPPABLE_FRAME_HEADER_LEN).min(payload_len);
            let buffered_frame_len = SHARED_SKIPPABLE_FRAME_HEADER_LEN
                .checked_add(buffered_payload_len)
                .ok_or_else(|| ZipError::InspectStream(io::Error::other("skippable frame buffered length overflowed")))?;
            prefix.drain(..buffered_frame_len);
            let remaining_payload_len = payload_len - buffered_payload_len;
            inspect_discard_fully(
                &mut input,
                u64::try_from(remaining_payload_len)
                    .map_err(|_| ZipError::InspectStream(io::Error::other("skippable frame remaining length exceeds u64")))?,
                &mut budget,
                "truncated shared Zstd/LZ4 skippable frame payload",
            )
            .await?;
            budget.record_frame().await;
        }
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
                let mut decoder = XzDecoder::with_mem_limit(reader, XZ_DECODER_MEMORY_LIMIT_BYTES);
                decoder.multiple_members(true);
                Box::new(decoder)
            }
            CompressionFormat::Zstd => {
                let mut decoder = ZstdDecoder::with_params(reader, &[DParameter::window_log_max(ZSTD_DECODER_MAX_WINDOW_LOG)]);
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
    use async_compression::{
        Level,
        tokio::write::{BzEncoder, GzipEncoder, Lz4Encoder, XzEncoder, ZstdEncoder},
        zstd::CParameter,
    };
    use std::future::Future;
    use std::mem::size_of;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Wake, Waker};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn encode_xz(payload: &[u8]) -> Vec<u8> {
        let mut encoder = XzEncoder::with_quality(Vec::new(), Level::Fastest);
        encoder.write_all(payload).await.expect("XZ encode should succeed");
        encoder.shutdown().await.expect("XZ encoder shutdown should succeed");
        encoder.into_inner()
    }

    async fn encode_lz4(payload: &[u8]) -> Vec<u8> {
        let mut encoder = Lz4Encoder::new(Vec::new());
        encoder.write_all(payload).await.expect("LZ4 encode should succeed");
        encoder.shutdown().await.expect("LZ4 encoder shutdown should succeed");
        encoder.into_inner()
    }

    async fn encode_zstd_with_window(payload: &[u8], window_log: u32) -> Vec<u8> {
        let mut encoder = ZstdEncoder::with_quality_and_params(
            Vec::new(),
            Level::Default,
            &[CParameter::window_log(window_log), CParameter::content_size_flag(false)],
        );
        encoder.write_all(payload).await.expect("Zstd encode should succeed");
        encoder.shutdown().await.expect("Zstd encoder shutdown should succeed");
        encoder.into_inner()
    }

    fn shared_skippable_frame(variant: u8, payload: &[u8]) -> Vec<u8> {
        assert!(variant <= 0x0f);
        let payload_len = u32::try_from(payload.len()).expect("test skippable payload should fit u32");
        let mut frame = Vec::with_capacity(SHARED_SKIPPABLE_FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(&[0x50 + variant, 0x2a, 0x4d, 0x18]);
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn raw_tar_with_name(name: &[u8]) -> Vec<u8> {
        assert!(!name.is_empty() && name.len() <= 100, "test TAR name must fit the legacy name field");

        let mut header = [0u8; TAR_HEADER_LEN];
        header[..name.len()].copy_from_slice(name);
        for (start, end) in [(100, 108), (108, 116), (116, 124), (124, 136), (136, 148)] {
            header[start..end].fill(b'0');
            header[end - 1] = 0;
        }
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[TAR_CHECKSUM_START..TAR_CHECKSUM_END].fill(b' ');
        let checksum = header.iter().fold(0u64, |sum, byte| sum + u64::from(*byte));
        let checksum_field = format!("{checksum:06o}\0 ");
        assert_eq!(checksum_field.len(), TAR_CHECKSUM_END - TAR_CHECKSUM_START);
        header[TAR_CHECKSUM_START..TAR_CHECKSUM_END].copy_from_slice(checksum_field.as_bytes());

        let mut archive = header.to_vec();
        archive.resize(TAR_HEADER_LEN * 3, 0);
        archive
    }

    struct FragmentedReader {
        bytes: Vec<u8>,
        position: usize,
        return_pending: bool,
    }

    impl FragmentedReader {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                position: 0,
                return_pending: true,
            }
        }
    }

    impl AsyncRead for FragmentedReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.return_pending {
                self.return_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.position >= self.bytes.len() || output.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let byte = self.bytes[self.position];
            self.position += 1;
            self.return_pending = true;
            output.put_slice(&[byte]);
            Poll::Ready(Ok(()))
        }
    }

    struct AlwaysReadyOneByteReader {
        bytes: Vec<u8>,
        position: usize,
        ready_reads: Arc<AtomicUsize>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl AsyncRead for AlwaysReadyOneByteReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            self.ready_reads.fetch_add(1, Ordering::Relaxed);
            if self.position >= self.bytes.len() || output.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let byte = self.bytes[self.position];
            self.position += 1;
            self.bytes_read.fetch_add(1, Ordering::Relaxed);
            output.put_slice(&[byte]);
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct ErrorAfterBytes {
        bytes: Vec<u8>,
        position: usize,
    }

    impl AsyncRead for ErrorAfterBytes {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.position >= self.bytes.len() {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "sentinel inspect failure")));
            }
            let available = self.bytes.len() - self.position;
            let to_copy = available.min(output.remaining());
            output.put_slice(&self.bytes[self.position..self.position + to_copy]);
            self.position += to_copy;
            Poll::Ready(Ok(()))
        }
    }

    async fn assert_shared_skippable_round_trip(format: CompressionFormat, encoded: Vec<u8>, frames: &[Vec<u8>]) {
        let mut stream = Vec::new();
        for frame in frames {
            stream.extend_from_slice(frame);
        }
        stream.extend_from_slice(&encoded);

        let (detected, mut sniffed) = CompressionFormat::sniff(std::io::Cursor::new(stream.clone()))
            .await
            .expect("shared skippable prefix should be inspected");
        assert_eq!(detected, format);
        let mut replayed_prefix = [0u8; MAGIC_SNIFF_LEN];
        sniffed
            .read_exact(&mut replayed_prefix)
            .await
            .expect("deciding codec prefix should replay completely");
        assert_eq!(replayed_prefix, encoded[..MAGIC_SNIFF_LEN]);

        let (detected, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(stream))
            .await
            .expect("shared skippable prefix should be inspected for decoding");
        let mut decoder = detected.get_decoder(sniffed).expect("detected decoder should be created");
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.expect("framed stream should decode");
        assert_eq!(decoded, b"payload");
    }

    async fn assert_ready_sniff_yields(input: Vec<u8>, encoded_prefix: &[u8]) {
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut sniff = Box::pin(CompressionFormat::sniff(std::io::Cursor::new(input)));
        {
            let mut cx = Context::from_waker(&waker);
            assert!(sniff.as_mut().poll(&mut cx).is_pending(), "bounded inspection must yield");
        }
        tokio::task::yield_now().await;
        assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1, "yield must arrange another poll");

        let (format, mut sniffed) = sniff.await.expect("inspection should resume after yielding");
        assert_eq!(format, CompressionFormat::Lz4);
        let mut replayed_prefix = [0u8; MAGIC_SNIFF_LEN];
        sniffed
            .read_exact(&mut replayed_prefix)
            .await
            .expect("deciding codec prefix should replay after yielding");
        assert_eq!(replayed_prefix, encoded_prefix);
    }

    fn xz_crc32(bytes: &[u8]) -> u32 {
        let mut crc = u32::MAX;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0_u32.wrapping_sub(crc & 1);
                crc = (crc >> 1) ^ (0xedb8_8320 & mask);
            }
        }
        !crc
    }

    async fn xz_with_dictionary_property(payload: &[u8], dictionary_property: u8) -> Vec<u8> {
        const XZ_STREAM_HEADER_LEN: usize = 12;

        let mut encoded = encode_xz(payload).await;
        assert_eq!(&encoded[..MAGIC_SNIFF_LEN], b"\xfd7zXZ\0");

        let block_header_start = XZ_STREAM_HEADER_LEN;
        let block_header_len = (usize::from(encoded[block_header_start]) + 1) * 4;
        let block_header_crc_start = block_header_start + block_header_len - 4;
        assert_eq!(block_header_len, 12, "default XZ fixture should use one compact LZMA2 filter header");
        assert_eq!(
            &encoded[block_header_start + 1..block_header_start + 4],
            &[0x00, 0x21, 0x01],
            "default XZ fixture should contain one LZMA2 filter with one property byte"
        );

        encoded[block_header_start + 4] = dictionary_property;
        let block_header_crc = xz_crc32(&encoded[block_header_start..block_header_crc_start]).to_le_bytes();
        encoded[block_header_crc_start..block_header_crc_start + 4].copy_from_slice(&block_header_crc);
        encoded
    }

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
            (&[0x50, 0x2a, 0x4d, 0x18], CompressionFormat::Unknown),
            (&[0x04, 0x22, 0x4d, 0x18], CompressionFormat::Lz4),
            (&[0xff, 0x06, 0x00, 0x00], CompressionFormat::S2),
            (&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00], CompressionFormat::Xz),
            (&[0x78, 0x9c], CompressionFormat::Tar),
            (&[0x78, 0x5e, b'o', b'b'], CompressionFormat::Tar),
            (b"PK\x03\x04", CompressionFormat::Tar),
            (b"plain tar bytes", CompressionFormat::Tar),
        ];

        for (magic, expected) in cases {
            assert_eq!(CompressionFormat::from_magic(magic), *expected, "magic={magic:02x?}");
        }
    }

    #[tokio::test]
    async fn test_sniff_prefers_checksum_valid_tar_over_codec_like_member_names() {
        let cases: &[(&[u8], CompressionFormat)] = &[
            (b"BZh9-report.txt", CompressionFormat::Bzip2),
            (b"\x04\x22\x4d\x18-report.txt", CompressionFormat::Lz4),
        ];

        for (name, prefix_format) in cases {
            assert_eq!(CompressionFormat::from_magic(name), *prefix_format);
            let archive = raw_tar_with_name(name);
            let (format, mut sniffed) = CompressionFormat::sniff(std::io::Cursor::new(archive.clone()))
                .await
                .expect("checksum-valid TAR header should be inspectable");
            assert_eq!(format, CompressionFormat::Tar, "member name prefix={name:02x?}");

            let mut replayed = Vec::new();
            sniffed
                .read_to_end(&mut replayed)
                .await
                .expect("TAR lookahead should replay without loss");
            assert_eq!(replayed, archive);
        }

        let mut bad_checksum = raw_tar_with_name(b"BZh9-bad-checksum.txt");
        bad_checksum[99] = b'x';
        let (format, _) = CompressionFormat::sniff(std::io::Cursor::new(bad_checksum))
            .await
            .expect("malformed TAR lookahead should still permit codec detection");
        assert_eq!(format, CompressionFormat::Bzip2, "TAR priority must require a valid checksum");
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
    async fn test_sniff_still_recognizes_a_real_bzip2_stream() {
        let mut encoder = BzEncoder::new(Vec::new());
        encoder.write_all(b"payload").await.expect("Bzip2 encode should succeed");
        encoder.shutdown().await.expect("Bzip2 encoder shutdown should succeed");

        let (format, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(encoder.into_inner()))
            .await
            .expect("Bzip2 magic should be inspected after TAR lookahead");
        assert_eq!(format, CompressionFormat::Bzip2);
        let mut decoder = format.get_decoder(sniffed).expect("Bzip2 decoder should be created");
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.expect("Bzip2 decode should succeed");

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
    async fn test_sniff_skips_shared_frames_before_lz4_and_zstd() {
        let lz4 = encode_lz4(b"payload").await;
        assert_shared_skippable_round_trip(
            CompressionFormat::Lz4,
            lz4,
            &[shared_skippable_frame(0, b""), shared_skippable_frame(15, b"lz4 metadata")],
        )
        .await;

        let zstd = encode_zstd_with_window(b"payload", ZSTD_DECODER_MAX_WINDOW_LOG).await;
        let large_metadata = vec![0x5a; SNIFF_YIELD_AFTER_BYTES + 1];
        assert_shared_skippable_round_trip(
            CompressionFormat::Zstd,
            zstd,
            &[shared_skippable_frame(3, &large_metadata), shared_skippable_frame(4, b"")],
        )
        .await;
    }

    #[tokio::test]
    async fn test_sniff_rejects_truncated_shared_skippable_frames() {
        let truncated_header = [0x50, 0x2a, 0x4d, 0x18, 0x04, 0x00];
        let err = CompressionFormat::sniff(std::io::Cursor::new(truncated_header))
            .await
            .expect_err("truncated shared frame header must fail");
        assert!(matches!(
            err,
            ZipError::InspectStream(ref source) if source.kind() == io::ErrorKind::UnexpectedEof
        ));

        let mut truncated_payload = shared_skippable_frame(0, b"abcd");
        truncated_payload.truncate(truncated_payload.len() - 2);
        let err = CompressionFormat::sniff(std::io::Cursor::new(truncated_payload))
            .await
            .expect_err("truncated shared frame payload must fail");
        assert!(matches!(
            err,
            ZipError::InspectStream(ref source) if source.kind() == io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn test_sniff_shared_frames_handles_fragmented_pending_reads() {
        let encoded = encode_lz4(b"payload").await;
        let mut input = Vec::new();
        for variant in 0..=SNIFF_YIELD_AFTER_FRAMES {
            input.extend_from_slice(&shared_skippable_frame(u8::try_from(variant % 16).expect("variant should fit u8"), b""));
        }
        input.extend_from_slice(&encoded);

        let (format, sniffed) = CompressionFormat::sniff(FragmentedReader::new(input))
            .await
            .expect("fragmented shared frames should be inspected");
        assert_eq!(format, CompressionFormat::Lz4);
        let mut decoder = format.get_decoder(sniffed).expect("LZ4 decoder should be created");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("fragmented stream should decode");
        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_sniff_shared_frames_yields_at_frame_budget_with_ready_reader() {
        let encoded = encode_lz4(b"payload").await;
        let mut input = Vec::new();
        for variant in 0..SNIFF_YIELD_AFTER_FRAMES {
            let variant = u8::try_from(variant % 16).expect("variant should fit u8");
            input.extend_from_slice(&shared_skippable_frame(variant, b""));
        }
        input.extend_from_slice(&encoded);

        assert_ready_sniff_yields(input, &encoded[..MAGIC_SNIFF_LEN]).await;
    }

    #[tokio::test]
    async fn test_sniff_shared_frame_yields_at_byte_budget_with_ready_reader() {
        let encoded = encode_lz4(b"payload").await;
        let metadata = vec![0x5a; SNIFF_YIELD_AFTER_BYTES];
        let mut input = shared_skippable_frame(0, &metadata);
        input.extend_from_slice(&encoded);

        assert_ready_sniff_yields(input, &encoded[..MAGIC_SNIFF_LEN]).await;
    }

    #[tokio::test]
    async fn test_sniff_shared_frame_bounds_always_ready_one_byte_reads_per_poll() {
        let encoded = encode_lz4(b"payload").await;
        let metadata = vec![0x5a; TAR_HEADER_LEN * 4];
        let mut input = shared_skippable_frame(0, &metadata);
        input.extend_from_slice(&encoded);

        let ready_reads = Arc::new(AtomicUsize::new(0));
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let reader = AlwaysReadyOneByteReader {
            bytes: input,
            position: 0,
            ready_reads: ready_reads.clone(),
            bytes_read: bytes_read.clone(),
        };
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut sniff = Box::pin(CompressionFormat::sniff(reader));

        for poll_number in 0..10 {
            let before = ready_reads.load(Ordering::Relaxed);
            let mut cx = Context::from_waker(&waker);
            assert!(
                sniff.as_mut().poll(&mut cx).is_pending(),
                "poll {poll_number} must yield at the ready-read budget"
            );
            let reads_this_poll = ready_reads.load(Ordering::Relaxed) - before;
            assert_eq!(
                reads_this_poll, SNIFF_YIELD_AFTER_READY_READS,
                "poll {poll_number} exceeded the ready-read budget"
            );
        }
        assert!(
            bytes_read.load(Ordering::Relaxed) > TAR_HEADER_LEN,
            "manual polls must progress beyond TAR lookahead into the shared-frame payload"
        );
        assert_eq!(wake_counter.0.load(Ordering::Relaxed), 10, "every voluntary yield must arrange a repoll");

        let (format, sniffed) = sniff.await.expect("bounded inspection should eventually complete");
        assert_eq!(format, CompressionFormat::Lz4);
        let mut decoder = format.get_decoder(sniffed).expect("LZ4 decoder should be created");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("LZ4 stream should survive bounded inspection");
        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_sniff_shared_frame_preserves_underlying_read_error() {
        let mut bytes = shared_skippable_frame(0, b"abcd");
        bytes.truncate(bytes.len() - 2);
        let err = CompressionFormat::sniff(ErrorAfterBytes { bytes, position: 0 })
            .await
            .expect_err("underlying read failure must escape inspection");

        assert!(matches!(
            err,
            ZipError::InspectStream(ref source)
                if source.kind() == io::ErrorKind::ConnectionReset && source.to_string() == "sentinel inspect failure"
        ));
    }

    #[tokio::test]
    async fn test_sniff_shared_frame_without_lz4_or_zstd_successor_is_unknown() {
        let mut input = shared_skippable_frame(0, b"metadata");
        input.extend_from_slice(b"raw tar bytes");

        let (format, mut sniffed) = CompressionFormat::sniff(std::io::Cursor::new(input))
            .await
            .expect("complete shared frame should be inspectable");
        assert_eq!(format, CompressionFormat::Unknown);
        let mut replayed = Vec::new();
        sniffed
            .read_to_end(&mut replayed)
            .await
            .expect("successor bytes should replay");
        assert_eq!(replayed, b"raw tar bytes");
    }

    #[tokio::test]
    async fn test_get_decoder_round_trips_xz_stream_with_memory_limit() {
        let encoded = encode_xz(b"payload").await;
        let (format, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(encoded))
            .await
            .expect("XZ magic should be inspected");
        assert_eq!(format, CompressionFormat::Xz);

        let mut decoder = format.get_decoder(sniffed).expect("XZ decoder should be created");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("XZ decode should succeed within the memory limit");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_get_decoder_rejects_xz_dictionary_over_memory_limit() {
        const LZMA2_MAX_DICTIONARY_PROPERTY: u8 = 40;

        let encoded = xz_with_dictionary_property(b"payload", LZMA2_MAX_DICTIONARY_PROPERTY).await;
        let mut decoder = CompressionFormat::Xz
            .get_decoder(std::io::Cursor::new(encoded))
            .expect("XZ decoder should be created before inspecting the stream header");
        let mut decoded = Vec::new();
        let err = decoder
            .read_to_end(&mut decoded)
            .await
            .expect_err("hostile XZ dictionary request must exceed the decoder memory limit");

        assert!(decoded.is_empty(), "decoder must reject the hostile dictionary before producing output");
        assert!(
            err.to_string().contains("memory limit"),
            "hostile XZ dictionary should fail at the decoder memory boundary: {err}"
        );
    }

    #[tokio::test]
    async fn test_get_decoder_accepts_xz_64_mib_dictionary() {
        const LZMA2_64_MIB_DICTIONARY_PROPERTY: u8 = 28;

        let encoded = xz_with_dictionary_property(b"payload", LZMA2_64_MIB_DICTIONARY_PROPERTY).await;
        let mut decoder = CompressionFormat::Xz
            .get_decoder(std::io::Cursor::new(encoded))
            .expect("XZ decoder should be created before inspecting the stream header");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("the memory limit must retain preset-9-compatible dictionary headroom");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_get_decoder_accepts_zstd_window_at_limit() {
        let encoded = encode_zstd_with_window(b"payload", ZSTD_DECODER_MAX_WINDOW_LOG).await;
        let mut decoder = CompressionFormat::Zstd
            .get_decoder(std::io::Cursor::new(encoded))
            .expect("Zstd decoder should be created before inspecting the frame header");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect("16 MiB Zstd windows must remain compatible");

        assert_eq!(decoded, b"payload");
    }

    #[tokio::test]
    async fn test_get_decoder_rejects_zstd_window_over_limit() {
        let encoded = encode_zstd_with_window(b"payload", ZSTD_DECODER_MAX_WINDOW_LOG + 1).await;
        let mut decoder = CompressionFormat::Zstd
            .get_decoder(std::io::Cursor::new(encoded))
            .expect("Zstd decoder should be created before inspecting the frame header");
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .await
            .expect_err("Zstd windows larger than 16 MiB must be rejected");

        assert!(decoded.is_empty(), "decoder must reject the oversized window before producing output");
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
