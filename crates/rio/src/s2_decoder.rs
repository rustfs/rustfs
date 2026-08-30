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

use minlz::{crc::crc, decode, decode_len};
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

/// S2 writers, including minio-go's Snowball writer, cap a decoded block at
/// 4 MiB. Enforcing the framing limit before allocation prevents a tiny block
/// length varint from requesting the block codec's much larger generic limit.
pub const MAX_S2_DECOMPRESSED_BLOCK_SIZE: usize = 4 << 20;

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
        stream_initialized: bool,
    }
}

impl<R> S2Decoder<R> {
    pub fn new(inner: R) -> Self {
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
            stream_initialized: false,
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

        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if *this.output_pos < this.output.len() {
            let to_copy = min(buf.remaining(), this.output.len() - *this.output_pos);
            buf.put_slice(&this.output[*this.output_pos..*this.output_pos + to_copy]);
            *this.output_pos += to_copy;
            if *this.output_pos == this.output.len() {
                this.output.clear();
                *this.output_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if *this.finished {
            return Poll::Ready(Ok(()));
        }
        if *this.poisoned {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "S2 decoder previously failed",
            )));
        }

        loop {
            if !*this.reading_chunk {
                while *this.header_read < CHUNK_HEADER_LEN {
                    let mut read_buf = ReadBuf::new(&mut this.header_buf[*this.header_read..]);
                    match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {
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
                            *this.header_read += read;
                        }
                        Poll::Ready(Err(err)) => {
                            *this.poisoned = true;
                            return Poll::Ready(Err(err));
                        }
                    }
                }

                *this.chunk_type = this.header_buf[0];
                *this.chunk_len = usize::from(this.header_buf[1])
                    | (usize::from(this.header_buf[2]) << 8)
                    | (usize::from(this.header_buf[3]) << 16);
                *this.header_read = 0;

                let invalid_length = match *this.chunk_type {
                    CHUNK_TYPE_STREAM_IDENTIFIER => *this.chunk_len != S2_MAGIC_BODY.len(),
                    CHUNK_TYPE_COMPRESSED_DATA => *this.chunk_len < CHECKSUM_SIZE,
                    CHUNK_TYPE_UNCOMPRESSED_DATA => {
                        *this.chunk_len < CHECKSUM_SIZE
                            || *this.chunk_len - CHECKSUM_SIZE > MAX_S2_DECOMPRESSED_BLOCK_SIZE
                    }
                    _ => false,
                };
                if invalid_length {
                    *this.poisoned = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid S2 chunk length",
                    )));
                }

                if this.chunk_buf.len() < *this.chunk_len {
                    this.chunk_buf.resize(*this.chunk_len, 0);
                }
                *this.chunk_read = 0;
                *this.reading_chunk = true;
            }

            while *this.chunk_read < *this.chunk_len {
                let mut read_buf = ReadBuf::new(&mut this.chunk_buf[*this.chunk_read..*this.chunk_len]);
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let read = read_buf.filled().len();
                        if read == 0 {
                            *this.poisoned = true;
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading S2 chunk body",
                            )));
                        }
                        *this.chunk_read += read;
                    }
                    Poll::Ready(Err(err)) => {
                        *this.poisoned = true;
                        return Poll::Ready(Err(err));
                    }
                }
            }

            let chunk = &this.chunk_buf[..*this.chunk_len];
            *this.reading_chunk = false;
            match *this.chunk_type {
                CHUNK_TYPE_STREAM_IDENTIFIER => {
                    if chunk != S2_MAGIC_BODY && chunk != SNAPPY_MAGIC_BODY {
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid S2 stream identifier",
                        )));
                    }
                    *this.stream_initialized = true;
                    continue;
                }
                CHUNK_TYPE_COMPRESSED_DATA | CHUNK_TYPE_UNCOMPRESSED_DATA => {
                    if !*this.stream_initialized {
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "S2 data chunk before stream identifier",
                        )));
                    }
                    match decode_chunk(chunk, *this.chunk_type == CHUNK_TYPE_COMPRESSED_DATA) {
                        Ok(output) => *this.output = output,
                        Err(err) => {
                            *this.poisoned = true;
                            return Poll::Ready(Err(err));
                        }
                    }
                }
                CHUNK_TYPE_PADDING | 0x80..=0xfd => continue,
                _ => {
                    *this.poisoned = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown S2 chunk type: 0x{:02x}", *this.chunk_type),
                    )));
                }
            }

            if this.output.is_empty() {
                *this.poisoned = true;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty S2 data chunk",
                )));
            }

            *this.output_pos = 0;
            let to_copy = min(buf.remaining(), this.output.len());
            buf.put_slice(&this.output[..to_copy]);
            *this.output_pos += to_copy;
            if *this.output_pos == this.output.len() {
                this.output.clear();
                *this.output_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
    }
}

delegate_reader_capabilities_generic_no_index!(S2Decoder<R>, inner);

fn decode_chunk(chunk: &[u8], compressed: bool) -> io::Result<Vec<u8>> {
    let expected_crc = u32::from_le_bytes(
        chunk[..CHECKSUM_SIZE]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "S2 chunk smaller than checksum header"))?,
    );
    let payload = &chunk[CHECKSUM_SIZE..];
    let output = if compressed {
        let (decoded_len, _) = decode_len(payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 length decode error: {err}")))?;
        if decoded_len > MAX_S2_DECOMPRESSED_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "S2 decompressed block size exceeds limit: size={decoded_len}, limit={MAX_S2_DECOMPRESSED_BLOCK_SIZE}"
                ),
            ));
        }
        decode(payload).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 decode error: {err}")))?
    } else {
        payload.to_vec()
    };

    let actual_crc = crc(&output);
    if actual_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("S2 CRC mismatch: expected={expected_crc:08x} actual={actual_crc:08x}"),
        ));
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

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
    async fn rejects_crc_mismatch() {
        let mut fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        fixture[14] ^= 0xff;

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder.read_to_end(&mut output).await.expect_err("CRC mismatch must fail");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("CRC mismatch"));
    }

    #[tokio::test]
    async fn rejects_truncated_chunk() {
        let mut fixture = hex_simd::decode_to_vec(GO_S2_GOLDEN_HEX).expect("golden fixture must be valid hex");
        fixture.truncate(fixture.len() - 1);

        let mut decoder = S2Decoder::new(Cursor::new(fixture));
        let mut output = Vec::new();
        let err = decoder.read_to_end(&mut output).await.expect_err("truncated S2 chunk must fail");

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
