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

//! Bounded asynchronous decoder for the S2/Snappy framed stream format.

use minlz::{crc::crc, decode, decode_into, decode_len};
use pin_project_lite::pin_project;
use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

const S2_MAGIC_BODY: &[u8] = b"S2sTwO";
const SNAPPY_MAGIC_BODY: &[u8] = b"sNaPpY";
const CHUNK_TYPE_COMPRESSED_DATA: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED_DATA: u8 = 0x01;
const CHUNK_TYPE_PADDING: u8 = 0xfe;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;
const CHECKSUM_SIZE: usize = 4;
const CHUNK_HEADER_LEN: usize = 4;
const MAX_READY_READS_PER_POLL: usize = 64;
const MAX_CHUNKS_PER_POLL: usize = 64;
const MAX_INPUT_BYTES_PER_POLL: usize = 256 * 1024;
const MAX_SNAPPY_DECOMPRESSED_BLOCK_SIZE: usize = 64 << 10;
const MAX_FRAMED_CHUNK_SIZE: usize = (1 << 24) - 1;
const MAX_LEGACY_S2_DECOMPRESSED_BLOCK_SIZE: usize = 1 << 24;

// This is checksum size + klauspost/s2 MaxEncodedLen(4 MiB).
// MaxEncodedLen adds a four-byte varint and four-byte literal header. The Go
// reader keeps this encoded-input cap even for Snappy frames, then applies the
// tighter 64 KiB limit to the decoded size.
const MAX_S2_COMPRESSED_CHUNK_SIZE: usize = CHECKSUM_SIZE + MAX_S2_DECOMPRESSED_BLOCK_SIZE + 4 + 4;

/// S2 writers, including minio-go's Snowball writer, cap a decoded block at
/// 4 MiB. Enforcing the framing limit before allocation prevents a tiny block
/// length varint from requesting the block codec's much larger generic limit.
pub const MAX_S2_DECOMPRESSED_BLOCK_SIZE: usize = 4 << 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameMode {
    Uninitialized,
    S2,
    Snappy,
    MidstreamS2,
}

impl FrameMode {
    fn max_compressed_chunk_size(self, s2_limit: usize) -> usize {
        match self {
            Self::Snappy => MAX_S2_COMPRESSED_CHUNK_SIZE,
            Self::Uninitialized | Self::S2 | Self::MidstreamS2 => s2_limit,
        }
    }

    fn max_decompressed_block_size(self, s2_limit: usize) -> usize {
        match self {
            Self::Snappy => MAX_SNAPPY_DECOMPRESSED_BLOCK_SIZE,
            Self::Uninitialized | Self::S2 | Self::MidstreamS2 => s2_limit,
        }
    }
}

pin_project! {
    /// Decode an S2 or Snappy framed stream without blocking the async reader.
    #[derive(Debug)]
    pub struct S2Decoder<R> {
        #[pin]
        inner: R,
        output: Vec<u8>,
        output_pos: usize,
        finished: bool,
        poisoned: bool,
        header_buf: [u8; CHUNK_HEADER_LEN],
        header_read: usize,
        chunk_type: u8,
        chunk_buf: Vec<u8>,
        chunk_len: usize,
        chunk_read: usize,
        reading_chunk: bool,
        skipping_chunk: bool,
        frame_mode: FrameMode,
        max_s2_compressed_chunk_size: usize,
        max_s2_decompressed_block_size: usize,
    }
}

impl<R> S2Decoder<R> {
    pub fn new(inner: R) -> Self {
        Self::with_limits(
            inner,
            FrameMode::Uninitialized,
            MAX_S2_COMPRESSED_CHUNK_SIZE,
            MAX_S2_DECOMPRESSED_BLOCK_SIZE,
        )
    }

    /// Create a decoder positioned at a trusted S2 data-chunk boundary.
    ///
    /// Indexed range reads start after the stream identifier. General stream
    /// consumers should use [`S2Decoder::new`] so a missing identifier remains
    /// an error.
    pub fn new_at_chunk_boundary(inner: R) -> Self {
        Self::with_limits(
            inner,
            FrameMode::MidstreamS2,
            MAX_S2_COMPRESSED_CHUNK_SIZE,
            MAX_S2_DECOMPRESSED_BLOCK_SIZE,
        )
    }

    /// Create a bounded decoder for rio-v2 data written before its block-size
    /// API was capped at 4 MiB.
    ///
    /// This compatibility mode accepts decoded S2 blocks up to 16 MiB and an
    /// encoded chunk up to the format's 24-bit framing limit. New streams and
    /// general S2 consumers should use the stricter constructors above.
    pub fn new_at_legacy_chunk_boundary(inner: R) -> Self {
        Self::with_limits(
            inner,
            FrameMode::MidstreamS2,
            MAX_FRAMED_CHUNK_SIZE,
            MAX_LEGACY_S2_DECOMPRESSED_BLOCK_SIZE,
        )
    }

    fn with_limits(
        inner: R,
        frame_mode: FrameMode,
        max_s2_compressed_chunk_size: usize,
        max_s2_decompressed_block_size: usize,
    ) -> Self {
        Self {
            inner,
            output: Vec::new(),
            output_pos: 0,
            finished: false,
            poisoned: false,
            header_buf: [0u8; CHUNK_HEADER_LEN],
            header_read: 0,
            chunk_type: 0,
            chunk_buf: Vec::new(),
            chunk_len: 0,
            chunk_read: 0,
            reading_chunk: false,
            skipping_chunk: false,
            frame_mode,
            max_s2_compressed_chunk_size,
            max_s2_decompressed_block_size,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R> AsyncRead for S2Decoder<R>
where
    R: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        let mut ready_reads = 0;
        let mut completed_chunks = 0;
        let mut input_bytes = 0;

        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if *this.poisoned {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "S2 decoder previously failed")));
        }

        if *this.output_pos < this.output.len() {
            let to_copy = min(buf.remaining(), this.output.len() - *this.output_pos);
            buf.put_slice(&this.output[*this.output_pos..*this.output_pos + to_copy]);
            *this.output_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        if *this.finished {
            return Poll::Ready(Ok(()));
        }

        loop {
            if ready_reads >= MAX_READY_READS_PER_POLL
                || completed_chunks >= MAX_CHUNKS_PER_POLL
                || input_bytes >= MAX_INPUT_BYTES_PER_POLL
            {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            if !*this.reading_chunk {
                while *this.header_read < CHUNK_HEADER_LEN {
                    let remaining_poll_bytes = MAX_INPUT_BYTES_PER_POLL - input_bytes;
                    if ready_reads >= MAX_READY_READS_PER_POLL || remaining_poll_bytes == 0 {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    let read_end = (*this.header_read + remaining_poll_bytes).min(CHUNK_HEADER_LEN);
                    let mut read_buf = ReadBuf::new(&mut this.header_buf[*this.header_read..read_end]);
                    match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {
                            ready_reads += 1;
                            let read = read_buf.filled().len();
                            if read == 0 {
                                if *this.header_read == 0 {
                                    *this.finished = true;
                                    return Poll::Ready(Ok(()));
                                }
                                *this.poisoned = true;
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "unexpected EOF while reading S2 chunk header",
                                )));
                            }
                            input_bytes += read;
                            *this.header_read += read;
                        }
                        Poll::Ready(Err(err)) => {
                            *this.poisoned = true;
                            return Poll::Ready(Err(err));
                        }
                    }
                }
                if ready_reads >= MAX_READY_READS_PER_POLL || input_bytes >= MAX_INPUT_BYTES_PER_POLL {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                *this.chunk_type = this.header_buf[0];
                *this.chunk_len = usize::from(this.header_buf[1])
                    | (usize::from(this.header_buf[2]) << 8)
                    | (usize::from(this.header_buf[3]) << 16);
                *this.header_read = 0;

                if *this.frame_mode == FrameMode::Uninitialized && *this.chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
                    *this.poisoned = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "S2 stream identifier must be the first chunk",
                    )));
                }

                let skippable = matches!(*this.chunk_type, CHUNK_TYPE_PADDING | 0x80..=0xfd);
                let invalid_length = match *this.chunk_type {
                    CHUNK_TYPE_STREAM_IDENTIFIER => *this.chunk_len != S2_MAGIC_BODY.len(),
                    CHUNK_TYPE_COMPRESSED_DATA => {
                        *this.chunk_len < CHECKSUM_SIZE
                            || *this.chunk_len > this.frame_mode.max_compressed_chunk_size(*this.max_s2_compressed_chunk_size)
                    }
                    CHUNK_TYPE_UNCOMPRESSED_DATA => {
                        *this.chunk_len < CHECKSUM_SIZE
                            || *this.chunk_len - CHECKSUM_SIZE
                                > this
                                    .frame_mode
                                    .max_decompressed_block_size(*this.max_s2_decompressed_block_size)
                    }
                    _ if skippable => false,
                    _ => {
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown S2 chunk type: 0x{:02x}", *this.chunk_type),
                        )));
                    }
                };
                if invalid_length {
                    *this.poisoned = true;
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "invalid S2 chunk length")));
                }

                if !skippable && this.chunk_buf.len() < *this.chunk_len {
                    this.chunk_buf.resize(*this.chunk_len, 0);
                }
                *this.chunk_read = 0;
                *this.reading_chunk = true;
                *this.skipping_chunk = skippable;
            }

            while *this.chunk_read < *this.chunk_len {
                let remaining_poll_bytes = MAX_INPUT_BYTES_PER_POLL - input_bytes;
                if ready_reads >= MAX_READY_READS_PER_POLL || remaining_poll_bytes == 0 {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let mut discard = [0u8; 8192];
                let mut read_buf = if *this.skipping_chunk {
                    let remaining = *this.chunk_len - *this.chunk_read;
                    let discard_len = remaining.min(discard.len()).min(remaining_poll_bytes);
                    ReadBuf::new(&mut discard[..discard_len])
                } else {
                    let read_end = (*this.chunk_read + remaining_poll_bytes).min(*this.chunk_len);
                    ReadBuf::new(&mut this.chunk_buf[*this.chunk_read..read_end])
                };
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        ready_reads += 1;
                        let read = read_buf.filled().len();
                        if read == 0 {
                            *this.poisoned = true;
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading S2 chunk body",
                            )));
                        }
                        input_bytes += read;
                        *this.chunk_read += read;
                    }
                    Poll::Ready(Err(err)) => {
                        *this.poisoned = true;
                        return Poll::Ready(Err(err));
                    }
                }
            }

            completed_chunks += 1;
            *this.reading_chunk = false;
            if *this.skipping_chunk {
                *this.skipping_chunk = false;
                continue;
            }

            let chunk = &this.chunk_buf[..*this.chunk_len];
            match *this.chunk_type {
                CHUNK_TYPE_STREAM_IDENTIFIER => {
                    *this.frame_mode = if chunk == S2_MAGIC_BODY {
                        FrameMode::S2
                    } else if chunk == SNAPPY_MAGIC_BODY {
                        FrameMode::Snappy
                    } else {
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "invalid S2 stream identifier")));
                    };
                    continue;
                }
                CHUNK_TYPE_COMPRESSED_DATA | CHUNK_TYPE_UNCOMPRESSED_DATA => {
                    if *this.frame_mode == FrameMode::Uninitialized {
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "S2 data chunk before stream identifier",
                        )));
                    }
                    if let Err(err) = decode_chunk_into(
                        this.output,
                        chunk,
                        *this.chunk_type == CHUNK_TYPE_COMPRESSED_DATA,
                        this.frame_mode
                            .max_decompressed_block_size(*this.max_s2_decompressed_block_size),
                    ) {
                        this.output.clear();
                        *this.output_pos = 0;
                        *this.poisoned = true;
                        return Poll::Ready(Err(err));
                    }
                }
                _ => unreachable!("chunk type validated before reading its payload"),
            }

            if this.output.is_empty() {
                *this.output_pos = 0;
                continue;
            }

            *this.output_pos = 0;
            let to_copy = min(buf.remaining(), this.output.len());
            buf.put_slice(&this.output[..to_copy]);
            *this.output_pos += to_copy;
            return Poll::Ready(Ok(()));
        }
    }
}

delegate_reader_capabilities_generic_no_index!(S2Decoder<R>, inner);

fn decode_chunk_into(output: &mut Vec<u8>, chunk: &[u8], compressed: bool, max_decompressed_block_size: usize) -> io::Result<()> {
    let expected_crc = u32::from_le_bytes(
        chunk[..CHECKSUM_SIZE]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "S2 chunk smaller than checksum header"))?,
    );
    let payload = &chunk[CHECKSUM_SIZE..];
    if compressed {
        let (decoded_len, _) = decode_len(payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 length decode error: {err}")))?;
        if decoded_len > max_decompressed_block_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("S2 decompressed block size exceeds limit: size={decoded_len}, limit={max_decompressed_block_size}"),
            ));
        }
        if output.len() >= decoded_len {
            let written = decode_into(&mut output[..decoded_len], payload)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 decode error: {err}")))?;
            output.truncate(written);
        } else {
            *output =
                decode(payload).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 decode error: {err}")))?;
        }
    } else {
        output.clear();
        output.extend_from_slice(payload);
    }

    let actual_crc = crc(output);
    if actual_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("S2 CRC mismatch: expected={expected_crc:08x} actual={actual_crc:08x}"),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Wake, Waker};
    use tokio::io::AsyncReadExt;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn append_chunk_header(stream: &mut Vec<u8>, chunk_type: u8, payload_len: usize) {
        assert!(payload_len <= 0x00ff_ffff, "test chunk payload must fit the framing length");
        stream.push(chunk_type);
        stream.push(u8::try_from(payload_len & 0xff).expect("low length byte must fit u8"));
        stream.push(u8::try_from((payload_len >> 8) & 0xff).expect("middle length byte must fit u8"));
        stream.push(u8::try_from((payload_len >> 16) & 0xff).expect("high length byte must fit u8"));
    }

    fn append_chunk(stream: &mut Vec<u8>, chunk_type: u8, payload: &[u8]) {
        append_chunk_header(stream, chunk_type, payload.len());
        stream.extend_from_slice(payload);
    }

    fn append_uncompressed_chunk(stream: &mut Vec<u8>, payload: &[u8]) {
        let mut chunk = crc(payload).to_le_bytes().to_vec();
        chunk.extend_from_slice(payload);
        append_chunk(stream, CHUNK_TYPE_UNCOMPRESSED_DATA, &chunk);
    }

    fn append_compressed_chunk(stream: &mut Vec<u8>, payload: &[u8]) {
        let mut chunk = crc(payload).to_le_bytes().to_vec();
        chunk.extend_from_slice(&minlz::encode(payload));
        append_chunk(stream, CHUNK_TYPE_COMPRESSED_DATA, &chunk);
    }

    fn append_uvarint(output: &mut Vec<u8>, mut value: usize) {
        loop {
            let mut byte = u8::try_from(value & 0x7f).expect("varint byte must fit u8");
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                return;
            }
        }
    }

    const GO_S2_GOLDEN_PLAINTEXT: &[u8] = b"This is a test file with some repeated content to compress.\nThe quick brown fox jumps over the lazy dog.\nThe quick brown fox jumps over the lazy dog.\nThe quick brown fox jumps over the lazy dog.\nLorem ipsum dolor sit amet, consectetur adipiscing elit.\nLorem ipsum dolor sit amet, consectetur adipiscing elit.\nLorem ipsum dolor sit amet, consectetur adipiscing elit.\nBinary compatibility testing with S2 compression format.\nBinary compatibility testing with S2 compression format.\nBinary compatibility testing with S2 compression format.\nPerformance benchmarking and optimization verification.\nPerformance benchmarking and optimization verification.\nPerformance benchmarking and optimization verification.\n";

    // Fixed interoperability fixture generated with the same dependency and
    // option as minio-go PutObjectsSnowball:
    //   minio-go commit 0e78d3f18efe14e352e20d3a262b99df97b516b8
    //   github.com/klauspost/compress v1.19.2
    //   s2.NewWriter(dst, s2.WriterBetterCompression())
    // Generator body: create that writer on os.Stdout, io.Copy from os.Stdin,
    // then Close; invoke as `go run generator.go < input | xxd -p -c 100000`.
    // The exact input is GO_S2_GOLDEN_PLAINTEXT, so this test never creates its
    // expected stream through minlz.
    const GO_S2_GOLDEN_HEX: &str = "ff06000053327354774f00120100f8de94fbc105f06654686973206973206120746573742066696c65207769746820736f6d6520726570656174656420636f6e74656e7420746f20636f6d70726573732e0a54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f67112d15004c684c6f72656d20697073756d20646f6c6f722073697420616d65742c01b85c73656374657475722061646970697363696e6720656c697411391500641442696e61727925432061746962696c697479257901952577045332356120696f6e20666f726d61113915006508506572057d306e63652062656e63686d61726b01a730616e64206f7074696d697a617401a41876657269666963050d1138150062";

    #[tokio::test]
    async fn decodes_fixed_go_s2_fixture() {
        let fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        decoder.read_to_end(&mut output).await.expect("Go S2 fixture must decode");

        assert_eq!(output, GO_S2_GOLDEN_PLAINTEXT);
    }

    #[tokio::test]
    async fn indexed_chunk_boundary_mode_decodes_a_headerless_tail() {
        let fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        let header_len = CHUNK_HEADER_LEN + S2_MAGIC_BODY.len();
        let mut decoder = S2Decoder::new_at_chunk_boundary(Cursor::new(fixture[header_len..].to_vec()));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .await
            .expect("trusted indexed tail should decode without a stream identifier");

        assert_eq!(output, GO_S2_GOLDEN_PLAINTEXT);
    }

    #[tokio::test]
    async fn strict_mode_rejects_a_headerless_data_chunk() {
        let fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        let header_len = CHUNK_HEADER_LEN + S2_MAGIC_BODY.len();
        let mut decoder = S2Decoder::new(Cursor::new(fixture[header_len..].to_vec()));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("untrusted stream must include a stream identifier");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("must be the first chunk"));
    }

    #[tokio::test]
    async fn strict_mode_rejects_skippable_chunk_before_identifier() {
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_PADDING, &[]);
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, S2_MAGIC_BODY);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("strict streams must begin with an identifier even when the first chunk is skippable");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("must be the first chunk"));
        assert_eq!(
            decoder.get_ref().position(),
            u64::try_from(CHUNK_HEADER_LEN).expect("chunk header length must fit u64")
        );
    }

    #[tokio::test]
    async fn snappy_identifier_enforces_the_64_kib_decoded_limit() {
        let payload = vec![0u8; MAX_SNAPPY_DECOMPRESSED_BLOCK_SIZE + 1];
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, SNAPPY_MAGIC_BODY);
        append_compressed_chunk(&mut fixture, &payload);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("Snappy frames must reject decoded blocks larger than 64 KiB");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("limit=65536"));
    }

    #[tokio::test]
    async fn snappy_frame_accepts_large_noncanonical_encoding_with_small_output() {
        let payload = vec![b'x'; 40_000];
        let mut encoded = Vec::with_capacity(payload.len() * 2 + 3);
        append_uvarint(&mut encoded, payload.len());
        for byte in &payload {
            encoded.push(0); // One-byte literal tag.
            encoded.push(*byte);
        }
        assert!(encoded.len() > MAX_SNAPPY_DECOMPRESSED_BLOCK_SIZE);

        let mut chunk = crc(&payload).to_le_bytes().to_vec();
        chunk.extend_from_slice(&encoded);
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, SNAPPY_MAGIC_BODY);
        append_chunk(&mut fixture, CHUNK_TYPE_COMPRESSED_DATA, &chunk);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .await
            .expect("Snappy encoded input uses the S2 reader cap while decoded output stays below 64 KiB");

        assert_eq!(output, payload);
    }

    #[tokio::test]
    async fn repeated_identifier_switches_the_frame_limit() {
        let payload = vec![b'x'; MAX_SNAPPY_DECOMPRESSED_BLOCK_SIZE + 1];
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, SNAPPY_MAGIC_BODY);
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, S2_MAGIC_BODY);
        append_uncompressed_chunk(&mut fixture, &payload);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .await
            .expect("a later S2 identifier must restore the S2 block limit");

        assert_eq!(output, payload);
    }

    #[tokio::test]
    async fn rejects_oversized_compressed_chunk_before_allocation() {
        let declared_len = MAX_S2_COMPRESSED_CHUNK_SIZE + 1;
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, S2_MAGIC_BODY);
        append_chunk_header(&mut fixture, CHUNK_TYPE_COMPRESSED_DATA, declared_len);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("oversized compressed chunks must fail from their header");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "invalid S2 chunk length");
        assert_eq!(
            decoder.get_ref().position(),
            u64::try_from(CHUNK_HEADER_LEN * 2 + S2_MAGIC_BODY.len()).expect("fixture prefix length must fit u64")
        );
        assert_eq!(decoder.chunk_buf.len(), S2_MAGIC_BODY.len());
    }

    #[tokio::test]
    async fn accepts_empty_uncompressed_and_compressed_chunks() {
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_STREAM_IDENTIFIER, S2_MAGIC_BODY);
        append_uncompressed_chunk(&mut fixture, &[]);
        append_compressed_chunk(&mut fixture, &[]);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .await
            .expect("empty data chunks are valid framed-stream no-ops");

        assert!(output.is_empty());
    }

    #[test]
    fn compressed_blocks_reuse_the_decoded_output_allocation() {
        let first_payload = vec![b'a'; 32 * 1024];
        let second_payload = vec![b'b'; first_payload.len()];
        let mut first_chunk = crc(&first_payload).to_le_bytes().to_vec();
        first_chunk.extend_from_slice(&minlz::encode(&first_payload));
        let mut second_chunk = crc(&second_payload).to_le_bytes().to_vec();
        second_chunk.extend_from_slice(&minlz::encode(&second_payload));

        let mut output = Vec::new();
        decode_chunk_into(&mut output, &first_chunk, true, MAX_S2_DECOMPRESSED_BLOCK_SIZE)
            .expect("first compressed block should decode");
        let allocation = output.as_ptr();
        decode_chunk_into(&mut output, &second_chunk, true, MAX_S2_DECOMPRESSED_BLOCK_SIZE)
            .expect("same-sized compressed block should decode into the existing allocation");

        assert_eq!(output, second_payload);
        assert_eq!(output.as_ptr(), allocation);
    }

    #[tokio::test]
    async fn rejects_decompressed_block_length_above_framing_limit() {
        let mut fixture = b"\xff\x06\x00\x00S2sTwO".to_vec();
        fixture.extend_from_slice(&[CHUNK_TYPE_COMPRESSED_DATA, 8, 0, 0]);
        fixture.extend_from_slice(&[0, 0, 0, 0]);
        fixture.extend_from_slice(&[0x81, 0x80, 0x80, 0x02]);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("oversized decoded block declaration must fail before allocation");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds limit"));
    }

    #[tokio::test]
    async fn legacy_chunk_boundary_mode_rejects_blocks_above_compatibility_limit() {
        let mut chunk = vec![0u8; CHECKSUM_SIZE];
        append_uvarint(&mut chunk, MAX_LEGACY_S2_DECOMPRESSED_BLOCK_SIZE + 1);
        let mut fixture = Vec::new();
        append_chunk(&mut fixture, CHUNK_TYPE_COMPRESSED_DATA, &chunk);

        let mut decoder = S2Decoder::new_at_legacy_chunk_boundary(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("legacy compatibility must remain bounded before allocation");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("limit=16777216"));
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn rejects_crc_mismatch() {
        let mut fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        fixture[14] ^= 0xff;

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = [0u8; 128];
        let err = decoder.read(&mut output).await.expect_err("CRC mismatch must fail");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("CRC mismatch"));
        assert!(decoder.output.is_empty(), "unverified decoded bytes must be discarded");

        let err = decoder
            .read(&mut output)
            .await
            .expect_err("a poisoned decoder must remain fail-closed on later reads");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "S2 decoder previously failed");
    }

    #[tokio::test]
    async fn rejects_truncated_chunk() {
        let mut fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        fixture.truncate(fixture.len() - 1);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder
            .read_to_end(&mut output)
            .await
            .expect_err("truncated S2 chunk must fail");

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn skips_large_extension_chunks_without_buffering_them() {
        const EXTENSION_SIZE: usize = 64 * 1024;

        let mut fixture = b"\xff\x06\x00\x00S2sTwO".to_vec();
        append_chunk_header(&mut fixture, CHUNK_TYPE_PADDING, EXTENSION_SIZE);
        fixture.resize(fixture.len() + EXTENSION_SIZE, 0);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .await
            .expect("skippable extension must be consumed");

        assert!(output.is_empty());
        assert_eq!(decoder.chunk_buf.len(), S2_MAGIC_BODY.len());
    }

    #[test]
    fn yields_after_bounded_number_of_non_data_chunks() {
        let mut fixture = b"\xff\x06\x00\x00S2sTwO".to_vec();
        for _ in 0..MAX_CHUNKS_PER_POLL {
            fixture.extend_from_slice(&[CHUNK_TYPE_PADDING, 0, 0, 0]);
        }

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut output = [0u8; 1];
        let mut read_buf = ReadBuf::new(&mut output);

        assert!(Pin::new(&mut decoder).poll_read(&mut cx, &mut read_buf).is_pending());
        assert!(read_buf.filled().is_empty());
        assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn yields_while_streaming_a_large_skippable_chunk() {
        const EXTENSION_SIZE: usize = MAX_INPUT_BYTES_PER_POLL * 2;

        let mut fixture = b"\xff\x06\x00\x00S2sTwO".to_vec();
        append_chunk_header(&mut fixture, CHUNK_TYPE_PADDING, EXTENSION_SIZE);
        fixture.resize(fixture.len() + EXTENSION_SIZE, 0);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut output = [0u8; 1];
        let mut read_buf = ReadBuf::new(&mut output);

        assert!(Pin::new(&mut decoder).poll_read(&mut cx, &mut read_buf).is_pending());
        assert!(read_buf.filled().is_empty());
        assert_eq!(
            decoder.get_ref().position(),
            u64::try_from(MAX_INPUT_BYTES_PER_POLL).expect("poll byte budget must fit u64")
        );
        assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn header_reads_respect_the_remaining_poll_byte_budget() {
        const FIRST_EXTENSION_SIZE: usize = MAX_INPUT_BYTES_PER_POLL - 15;

        let mut fixture = b"\xff\x06\x00\x00S2sTwO".to_vec();
        append_chunk_header(&mut fixture, CHUNK_TYPE_PADDING, FIRST_EXTENSION_SIZE);
        fixture.resize(fixture.len() + FIRST_EXTENSION_SIZE, 0);
        fixture.extend_from_slice(&[CHUNK_TYPE_PADDING, 0, 0, 0]);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut output = [0u8; 1];
        let mut read_buf = ReadBuf::new(&mut output);

        assert!(Pin::new(&mut decoder).poll_read(&mut cx, &mut read_buf).is_pending());
        assert!(read_buf.filled().is_empty());
        assert_eq!(
            decoder.get_ref().position(),
            u64::try_from(MAX_INPUT_BYTES_PER_POLL).expect("poll byte budget must fit u64")
        );
        assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
    }
}
