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

use crate::compress_index::{Index, TryGetIndex};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use pin_project_lite::pin_project;
use rustfs_utils::{put_uvarint, put_uvarint_len};
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;

const ENCRYPTION_BLOCK_SIZE: usize = 8 * 1024;

/// Frame type bytes shared by the writer and reader.
///
/// v1 (`0x00`) frames authenticate only the ciphertext: the header (length +
/// plaintext CRC32) and the end marker sit outside the AEAD, and frames are
/// emitted per upstream read so their plaintext length varies.
///
/// v2 (`0x01`/`0x02`) frames fix both gaps while keeping the byte layout:
/// - the 8-byte header and the frame's index are bound as AEAD associated
///   data, so header tampering, frame reordering and cross-frame splicing
///   fail authentication;
/// - the final frame of a stream (or of each multipart part segment) carries
///   its own type byte, authenticated via the AAD, so truncating trailing
///   frames is detected — the unauthenticated `0xFF` end marker remains only
///   as the segment delimiter;
/// - every non-final frame carries exactly [`ENCRYPTION_BLOCK_SIZE`] plaintext
///   bytes, giving fixed-length frames a closed-form offset mapping.
const FRAME_TYPE_V1: u8 = 0x00;
const FRAME_TYPE_V2: u8 = 0x01;
const FRAME_TYPE_V2_FINAL: u8 = 0x02;
const FRAME_TYPE_END: u8 = 0xFF;

/// AEAD associated data of a v2 frame: the 8-byte header followed by the
/// frame index within its segment, little-endian.
fn v2_frame_aad(header: &[u8; 8], block_index: usize) -> [u8; 16] {
    let mut aad = [0u8; 16];
    aad[..8].copy_from_slice(header);
    aad[8..].copy_from_slice(&(block_index as u64).to_le_bytes());
    aad
}

pin_project! {
    /// A reader wrapper that encrypts data on the fly using AES-256-GCM.
    /// This is a demonstration. For production, use a secure and audited crypto library.
    pub struct EncryptReader<R> {
        #[pin]
        pub inner: R,
        cipher: Aes256Gcm,
        base_nonce: [u8; 12], // 96-bit base nonce for GCM
        buffer: Vec<u8>,
        buffer_pos: usize,
        read_buffer: Vec<u8>,
        block_index: usize,
        finished: bool,
        // v2 framing (see the frame-type constants above)
        frame_v2: bool,
        pending: usize,
        input_done: bool,
    }
}

impl<R> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("key"),
            base_nonce: nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
            read_buffer: vec![0u8; ENCRYPTION_BLOCK_SIZE],
            block_index: 0,
            finished: false,
            frame_v2: false,
            pending: 0,
            input_done: false,
        }
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        Self::new(inner, key, multipart_part_nonce(base_nonce, part_number))
    }

    /// Writer for the authenticated, fixed-frame v2 layout.
    ///
    /// Key and nonce derivation are identical to [`EncryptReader::new`]; only
    /// the frame format changes (header + frame index bound as AEAD associated
    /// data, an authenticated final frame, fixed-size non-final frames).
    pub fn new_v2(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        let mut reader = Self::new(inner, key, nonce);
        reader.frame_v2 = true;
        reader
    }

    /// Multipart writer for the v2 layout; see [`EncryptReader::new_v2`].
    pub fn new_multipart_v2(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        let mut reader = Self::new_multipart(inner, key, base_nonce, part_number);
        reader.frame_v2 = true;
        reader
    }
}

/// Build one frame: header, plaintext-length uvarint, ciphertext. For v2
/// frames the header and frame index are the AEAD associated data.
fn build_frame(
    cipher: &Aes256Gcm,
    nonce_bytes: &[u8; 12],
    type_byte: u8,
    block_index: usize,
    plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
    let nonce = Nonce::try_from(nonce_bytes.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
    let nonce = &nonce;
    let crc = {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        hasher.update(plaintext);
        hasher.finalize() as u32
    };
    let int_len = put_uvarint_len(plaintext.len() as u64);
    // Ciphertext length is plaintext + 16-byte GCM tag, known ahead of
    // encryption, so the header can be fixed before it becomes the AAD.
    let clen = int_len + plaintext.len() + 16 + 4;
    let mut header = [0u8; 8];
    header[0] = type_byte;
    header[1] = (clen & 0xFF) as u8;
    header[2] = ((clen >> 8) & 0xFF) as u8;
    header[3] = ((clen >> 16) & 0xFF) as u8;
    header[4] = (crc & 0xFF) as u8;
    header[5] = ((crc >> 8) & 0xFF) as u8;
    header[6] = ((crc >> 16) & 0xFF) as u8;
    header[7] = ((crc >> 24) & 0xFF) as u8;

    let ciphertext = match type_byte {
        FRAME_TYPE_V1 => cipher.encrypt(nonce, plaintext),
        _ => {
            let aad = v2_frame_aad(&header, block_index);
            cipher.encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
        }
    }
    .map_err(|e| Error::other(format!("encrypt error: {e}")))?;

    let mut out = Vec::with_capacity(8 + int_len + ciphertext.len());
    out.extend_from_slice(&header);
    let mut plaintext_len_buf = [0u8; 10];
    let encoded_len = put_uvarint(&mut plaintext_len_buf, plaintext.len() as u64);
    out.extend_from_slice(&plaintext_len_buf[..encoded_len]);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

impl<R> AsyncRead for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        // Serve from buffer if any
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
            buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
            *this.buffer_pos += to_copy;
            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        if *this.finished {
            return Poll::Ready(Ok(()));
        }

        if *this.frame_v2 {
            // Accumulate a full block so every non-final frame carries exactly
            // ENCRYPTION_BLOCK_SIZE plaintext bytes (fixed-length frames give
            // range reads a closed-form offset mapping).
            while !*this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE {
                let mut temp_buf = ReadBuf::new(&mut this.read_buffer[*this.pending..ENCRYPTION_BLOCK_SIZE]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            *this.input_done = true;
                        } else {
                            *this.pending += n;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }

            // A short block is only ever the stream tail; EOF exactly on a block
            // boundary emits that full block as non-final and an empty final
            // frame on the next poll, so emptiness is always authenticated.
            let is_final = *this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE;
            let type_byte = if is_final { FRAME_TYPE_V2_FINAL } else { FRAME_TYPE_V2 };
            let block_nonce = derive_block_nonce(this.base_nonce, *this.block_index);
            let mut out = build_frame(
                this.cipher,
                &block_nonce,
                type_byte,
                *this.block_index,
                &this.read_buffer[..*this.pending],
            )?;
            if is_final {
                let mut end_header = [0u8; 8];
                end_header[0] = FRAME_TYPE_END;
                out.extend_from_slice(&end_header);
                *this.finished = true;
            }
            *this.pending = 0;
            *this.block_index += 1;
            *this.buffer = out;
            *this.buffer_pos = 0;
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        // Read a fixed block size from inner.
        let mut temp_buf = ReadBuf::new(&mut this.read_buffer[..]);
        match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let n = temp_buf.filled().len();
                if n == 0 {
                    // EOF, write end header
                    let mut header = [0u8; 8];
                    header[0] = 0xFF; // type: end
                    *this.buffer = header.to_vec();
                    *this.buffer_pos = 0;
                    *this.finished = true;
                    let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
                    buf.put_slice(&this.buffer[..to_copy]);
                    *this.buffer_pos += to_copy;
                    Poll::Ready(Ok(()))
                } else {
                    // Encrypt the chunk
                    let block_nonce = derive_block_nonce(this.base_nonce, *this.block_index);
                    let nonce = Nonce::try_from(block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
                    let plaintext = &this.read_buffer[..n];
                    let plaintext_len = plaintext.len();
                    let crc = {
                        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                        hasher.update(plaintext);
                        hasher.finalize() as u32
                    };
                    let ciphertext = this
                        .cipher
                        .encrypt(&nonce, plaintext)
                        .map_err(|e| Error::other(format!("encrypt error: {e}")))?;
                    let int_len = put_uvarint_len(plaintext_len as u64);
                    let clen = int_len + ciphertext.len() + 4;
                    // Header: 8 bytes
                    // 0: type (0 = encrypted, 0xFF = end)
                    // 1-3: length (little endian u24, ciphertext length)
                    // 4-7: CRC32 of plaintext (little endian u32)
                    let mut header = [0u8; 8];
                    header[0] = 0x00; // 0 = encrypted
                    header[1] = (clen & 0xFF) as u8;
                    header[2] = ((clen >> 8) & 0xFF) as u8;
                    header[3] = ((clen >> 16) & 0xFF) as u8;
                    header[4] = (crc & 0xFF) as u8;
                    header[5] = ((crc >> 8) & 0xFF) as u8;
                    header[6] = ((crc >> 16) & 0xFF) as u8;
                    header[7] = ((crc >> 24) & 0xFF) as u8;
                    debug!(
                        "encrypt block header typ=0 len={} header={:?} plaintext_len={} ciphertext_len={}",
                        clen,
                        header,
                        plaintext_len,
                        ciphertext.len()
                    );
                    let mut out = Vec::with_capacity(8 + int_len + ciphertext.len());
                    out.extend_from_slice(&header);
                    let mut plaintext_len_buf = [0u8; 10];
                    let encoded_len = put_uvarint(&mut plaintext_len_buf, plaintext_len as u64);
                    out.extend_from_slice(&plaintext_len_buf[..encoded_len]);
                    out.extend_from_slice(&ciphertext);
                    *this.buffer = out;
                    *this.buffer_pos = 0;
                    *this.block_index += 1;
                    let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
                    buf.put_slice(&this.buffer[..to_copy]);
                    *this.buffer_pos += to_copy;
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

delegate_reader_capabilities_generic_no_index!(EncryptReader<R>, inner);

impl<R> TryGetIndex for EncryptReader<R>
where
    R: TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

pin_project! {
    /// A reader wrapper that decrypts data on the fly using AES-256-GCM.
    /// This is a demonstration. For production, use a secure and audited crypto library.
    pub struct DecryptReader<R> {
        #[pin]
        pub inner: R,
        cipher: Aes256Gcm,
        base_nonce: [u8; 12], // Base nonce recorded in object metadata
        current_nonce_base: [u8; 12], // Active base nonce for the current encrypted segment
        multipart_mode: bool,
        multipart_parts: Vec<usize>,
        current_part_index: usize,
        current_part: usize,
        block_index: usize,
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // For block framing
        header_buf: [u8; 8],
        header_read: usize,
        header_done: bool,
        ciphertext_buf: Vec<u8>,
        ciphertext_read: usize,
        ciphertext_len: usize,
        // v2 framing state (see the frame-type constants above)
        current_frame_type: u8,
        segment_frame_version: Option<u8>,
        saw_final_frame: bool,
        segment_frames: usize,
        stream_saw_v2: bool,
        segments_completed: usize,
    }
}

impl<R> DecryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("key"),
            base_nonce: nonce,
            current_nonce_base: nonce,
            multipart_mode: false,
            multipart_parts: Vec::new(),
            current_part_index: 0,
            current_part: 0,
            block_index: 0,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            header_buf: [0u8; 8],
            header_read: 0,
            header_done: false,
            ciphertext_buf: Vec::new(),
            ciphertext_read: 0,
            ciphertext_len: 0,
            current_frame_type: FRAME_TYPE_V1,
            segment_frame_version: None,
            saw_final_frame: false,
            segment_frames: 0,
            stream_saw_v2: false,
            segments_completed: 0,
        }
    }

    /// Decrypt a stream that starts at an arbitrary frame boundary of a
    /// single-part v2 object.
    ///
    /// `starting_block_index` is the absolute index of the first frame in the
    /// stream: both the per-block nonce derivation and the v2 AEAD associated
    /// data bind absolute indices, so a frame served from the middle of an
    /// object only authenticates when the caller positions the read at a true
    /// frame boundary and names that frame's index. v1 streams are never
    /// planned with a non-zero start (their frames have no closed-form
    /// positions), so this constructor is v2-only by construction.
    pub fn new_at_block(inner: R, key: [u8; 32], nonce: [u8; 12], starting_block_index: usize) -> Self {
        let mut reader = Self::new(inner, key, nonce);
        reader.block_index = starting_block_index;
        reader
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], multipart_parts: Vec<usize>) -> Self {
        let first_part = multipart_parts.first().copied().unwrap_or(1);
        let initial_nonce = derive_part_nonce(&base_nonce, first_part);

        debug!("decrypt_reader: initialized multipart mode");

        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("key"),
            base_nonce,
            current_nonce_base: initial_nonce,
            multipart_mode: true,
            multipart_parts,
            current_part_index: 0,
            current_part: first_part,
            block_index: 0,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            header_buf: [0u8; 8],
            header_read: 0,
            header_done: false,
            ciphertext_buf: Vec::new(),
            ciphertext_read: 0,
            ciphertext_len: 0,
            current_frame_type: FRAME_TYPE_V1,
            segment_frame_version: None,
            saw_final_frame: false,
            segment_frames: 0,
            stream_saw_v2: false,
            segments_completed: 0,
        }
    }
}

impl<R> AsyncRead for DecryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        loop {
            // Serve buffered plaintext first
            if *this.buffer_pos < this.buffer.len() {
                let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
                buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
                *this.buffer_pos += to_copy;
                if *this.buffer_pos == this.buffer.len() {
                    this.buffer.clear();
                    *this.buffer_pos = 0;
                }
                return Poll::Ready(Ok(()));
            }

            if *this.finished {
                return Poll::Ready(Ok(()));
            }

            if *this.ciphertext_len == 0 {
                // Read header (8 bytes) only when there is no in-flight payload.
                while !*this.header_done && *this.header_read < 8 {
                    let mut temp = [0u8; 8];
                    let mut temp_buf = ReadBuf::new(&mut temp[0..8 - *this.header_read]);
                    match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {
                            let n = temp_buf.filled().len();
                            if n == 0 {
                                if *this.header_read == 0 {
                                    // v2 segments end with an authenticated final
                                    // frame; a clean EOF before it means trailing
                                    // frames were dropped.
                                    if *this.segment_frame_version == Some(2)
                                        && !*this.saw_final_frame
                                        && *this.segment_frames > 0
                                    {
                                        return Poll::Ready(Err(Error::new(
                                            std::io::ErrorKind::UnexpectedEof,
                                            "encrypted stream truncated before its final frame",
                                        )));
                                    }
                                    // A v2 multipart stream must deliver every
                                    // listed part segment; a missing tail part
                                    // would otherwise read as a clean end.
                                    if *this.stream_saw_v2
                                        && *this.multipart_mode
                                        && *this.segments_completed < this.multipart_parts.len()
                                    {
                                        return Poll::Ready(Err(Error::new(
                                            std::io::ErrorKind::UnexpectedEof,
                                            "encrypted stream ended before all part segments were read",
                                        )));
                                    }
                                    *this.finished = true;
                                    return Poll::Ready(Ok(()));
                                }
                                return Poll::Ready(Err(Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "unexpected EOF while reading encrypted block header",
                                )));
                            }
                            this.header_buf[*this.header_read..*this.header_read + n].copy_from_slice(&temp_buf.filled()[..n]);
                            *this.header_read += n;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    }
                }

                if !*this.header_done && *this.header_read == 8 {
                    *this.header_done = true;
                }

                if !*this.header_done {
                    return Poll::Pending;
                }

                let typ = this.header_buf[0];
                let len =
                    (this.header_buf[1] as usize) | ((this.header_buf[2] as usize) << 8) | ((this.header_buf[3] as usize) << 16);
                *this.header_read = 0;
                *this.header_done = false;

                if typ == FRAME_TYPE_END {
                    // A v2 segment terminator is only valid right after the
                    // segment's authenticated final frame; anywhere earlier it
                    // marks dropped frames.
                    if *this.segment_frame_version == Some(2) && !*this.saw_final_frame {
                        return Poll::Ready(Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            "encrypted segment terminator before the final frame",
                        )));
                    }
                    *this.segments_completed += 1;
                    *this.segment_frame_version = None;
                    *this.saw_final_frame = false;
                    *this.segment_frames = 0;

                    if *this.multipart_mode {
                        let next_part = if *this.current_part_index + 1 < this.multipart_parts.len() {
                            *this.current_part_index += 1;
                            this.multipart_parts[*this.current_part_index]
                        } else {
                            *this.current_part + 1
                        };
                        debug!(
                            next_part = next_part,
                            "decrypt_reader: reached segment terminator, advancing to next part"
                        );
                        *this.current_part = next_part;
                        *this.current_nonce_base = derive_part_nonce(this.base_nonce, *this.current_part);
                        *this.block_index = 0;
                        *this.ciphertext_read = 0;
                        *this.ciphertext_len = 0;
                        continue;
                    }

                    *this.finished = true;
                    *this.block_index = 0;
                    *this.ciphertext_read = 0;
                    *this.ciphertext_len = 0;
                    continue;
                }

                let frame_version = match typ {
                    FRAME_TYPE_V1 => 1,
                    FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,
                    other => {
                        return Poll::Ready(Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown encrypted frame type {other:#04x}"),
                        )));
                    }
                };
                if *this.saw_final_frame {
                    return Poll::Ready(Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        "encrypted frame after the segment's final frame",
                    )));
                }
                match *this.segment_frame_version {
                    None => *this.segment_frame_version = Some(frame_version),
                    Some(version) if version != frame_version => {
                        return Poll::Ready(Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            "encrypted segment mixes frame format versions",
                        )));
                    }
                    Some(_) => {}
                }
                if frame_version == 2 {
                    *this.stream_saw_v2 = true;
                }
                *this.current_frame_type = typ;

                tracing::debug!(typ = typ, len = len, "decrypt block header");

                if len == 0 {
                    if frame_version == 2 {
                        // Every v2 frame — including the empty final frame — has a
                        // tagged payload; a zero length can only be a forgery.
                        return Poll::Ready(Err(Error::new(std::io::ErrorKind::InvalidData, "zero-length v2 encrypted frame")));
                    }
                    tracing::warn!("encountered zero-length encrypted block, treating as end of stream");
                    *this.finished = true;
                    *this.ciphertext_read = 0;
                    *this.ciphertext_len = 0;
                    continue;
                }

                let Some(payload_len) = len.checked_sub(4) else {
                    tracing::error!("invalid encrypted block length: typ={} len={} header={:?}", typ, len, this.header_buf);
                    return Poll::Ready(Err(Error::other("Invalid encrypted block length")));
                };

                if this.ciphertext_buf.len() < payload_len {
                    this.ciphertext_buf.resize(payload_len, 0);
                }
                *this.ciphertext_len = payload_len;
                *this.ciphertext_read = 0;
            }

            while *this.ciphertext_read < *this.ciphertext_len {
                let mut temp_buf = ReadBuf::new(&mut this.ciphertext_buf[*this.ciphertext_read..*this.ciphertext_len]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading encrypted block payload",
                            )));
                        }
                        *this.ciphertext_read += n;
                    }
                    Poll::Ready(Err(e)) => {
                        *this.ciphertext_read = 0;
                        *this.ciphertext_len = 0;
                        return Poll::Ready(Err(e));
                    }
                }
            }

            if *this.ciphertext_read < *this.ciphertext_len {
                return Poll::Pending;
            }

            let ciphertext_buf = &this.ciphertext_buf[..*this.ciphertext_len];
            // `ciphertext_buf`'s length derives from the untrusted 24-bit header length field, so
            // it can be shorter than 16 bytes. `uvarint` is safe on any slice length, so pass the
            // whole slice instead of a fixed `[0..16]` index that panics on corrupted/truncated
            // blocks shorter than 16 bytes.
            // `uvarint_len <= 0` means the length varint was empty/unterminated (0) or overflowed
            // (negative — as usize it would index far past the buffer). The `> len` bound is
            // belt-and-suspenders (a positive return is always <= buf.len()).
            let (plaintext_len, uvarint_len) = rustfs_utils::uvarint(ciphertext_buf);
            if uvarint_len <= 0 || uvarint_len as usize > ciphertext_buf.len() {
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(Error::new(std::io::ErrorKind::InvalidData, "Invalid encrypted block length prefix")));
            }
            let ciphertext = &ciphertext_buf[uvarint_len as usize..];
            let block_nonce = derive_block_nonce(this.current_nonce_base, *this.block_index);
            let nonce = Nonce::try_from(block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
            let plaintext = if *this.current_frame_type != FRAME_TYPE_V1 {
                // v2: the header and frame index are associated data, the nonce
                // derivation is exactly the modern scheme, and there are no
                // legacy fallbacks — any mismatch is tampering, not history.
                let aad = v2_frame_aad(this.header_buf, *this.block_index);
                this.cipher
                    .decrypt(
                        &nonce,
                        Payload {
                            msg: ciphertext,
                            aad: &aad,
                        },
                    )
                    .map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "v2 encrypted frame failed authentication"))?
            } else {
                let legacy_part_nonce = if *this.multipart_mode {
                    derive_legacy_part_nonce(this.base_nonce, *this.current_part)
                } else {
                    *this.base_nonce
                };
                let legacy_block_nonce = derive_block_nonce(&legacy_part_nonce, *this.block_index);
                match this.cipher.decrypt(&nonce, ciphertext) {
                    Ok(plaintext) => plaintext,
                    Err(primary_err) => {
                        let legacy_nonce =
                            Nonce::try_from(legacy_block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;

                        match this.cipher.decrypt(&legacy_nonce, ciphertext) {
                            Ok(plaintext) => plaintext,
                            Err(_) => {
                                // Accept previously written streams that reused the part nonce
                                // for every block inside a segment.
                                let legacy_part_nonce = Nonce::try_from(legacy_part_nonce.as_slice())
                                    .map_err(|_| Error::other("invalid nonce length"))?;
                                this.cipher
                                    .decrypt(&legacy_part_nonce, ciphertext)
                                    .map_err(|_| Error::other(format!("decrypt error: {primary_err}")))?
                            }
                        }
                    }
                }
            };
            if *this.current_frame_type == FRAME_TYPE_V2_FINAL {
                *this.saw_final_frame = true;
            }
            *this.segment_frames += 1;

            debug!(
                part = *this.current_part,
                plaintext_len = plaintext.len(),
                "decrypt_reader: decrypted chunk"
            );

            if plaintext.len() != plaintext_len as usize {
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(Error::other("Plaintext length mismatch")));
            }

            let expected_crc = (this.header_buf[4] as u32)
                | ((this.header_buf[5] as u32) << 8)
                | ((this.header_buf[6] as u32) << 16)
                | ((this.header_buf[7] as u32) << 24);
            let actual_crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(&plaintext);
                hasher.finalize() as u32
            };
            if actual_crc != expected_crc {
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(Error::other("CRC32 mismatch")));
            }

            *this.buffer = plaintext;
            *this.buffer_pos = 0;
            *this.block_index += 1;
            *this.ciphertext_read = 0;
            *this.ciphertext_len = 0;

            if this.buffer.is_empty() {
                // An authenticated empty frame (the v2 final frame of a
                // block-aligned segment) carries no plaintext. Keep parsing:
                // returning Ready with nothing appended would read as EOF to
                // the caller and silently drop every remaining segment.
                continue;
            }

            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            return Poll::Ready(Ok(()));
        }
    }
}

delegate_reader_capabilities_generic_no_index!(DecryptReader<R>, inner);

impl<R> TryGetIndex for DecryptReader<R>
where
    R: TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

fn derive_block_nonce(base: &[u8; 12], block_index: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, block_index)
}

pub fn multipart_part_nonce(base_nonce: [u8; 12], part_number: usize) -> [u8; 12] {
    derive_part_nonce(&base_nonce, part_number)
}

fn derive_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 4, part_number)
}

fn derive_legacy_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, part_number)
}

fn derive_nonce_offset(base: &[u8; 12], start: usize, offset: usize) -> [u8; 12] {
    let mut nonce = *base;
    let mut suffix = [0u8; 4];
    suffix.copy_from_slice(&nonce[start..start + 4]);
    let current = u32::from_be_bytes(suffix);
    let next = current.wrapping_add(offset as u32);
    nonce[start..start + 4].copy_from_slice(&next.to_be_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use crate::HardLimitReader;

    use super::*;
    use futures::StreamExt;
    use rand::{Rng, RngExt};
    use tokio::io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf};
    use tokio_util::io::ReaderStream;

    struct ChunkedCursor {
        inner: Cursor<Vec<u8>>,
        max_chunk: usize,
    }

    impl ChunkedCursor {
        fn new(data: Vec<u8>, max_chunk: usize) -> Self {
            Self {
                inner: Cursor::new(data),
                max_chunk,
            }
        }
    }

    impl AsyncRead for ChunkedCursor {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.max_chunk == 0 || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let remaining = self.inner.get_ref().len() as u64 - self.inner.position();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let to_read = remaining.min(self.max_chunk as u64).min(buf.remaining() as u64) as usize;
            let start = self.inner.position() as usize;
            let end = start + to_read;
            buf.put_slice(&self.inner.get_ref()[start..end]);
            self.inner.set_position(end as u64);
            Poll::Ready(Ok(()))
        }
    }

    struct PendingChunkedCursor {
        inner: Cursor<Vec<u8>>,
        max_chunk: usize,
        should_pending: bool,
    }

    impl PendingChunkedCursor {
        fn new(data: Vec<u8>, max_chunk: usize) -> Self {
            Self {
                inner: Cursor::new(data),
                max_chunk,
                should_pending: true,
            }
        }
    }

    impl AsyncRead for PendingChunkedCursor {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.should_pending {
                self.should_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            if self.max_chunk == 0 || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let remaining = self.inner.get_ref().len() as u64 - self.inner.position();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let to_read = remaining.min(self.max_chunk as u64).min(buf.remaining() as u64) as usize;
            let start = self.inner.position() as usize;
            let end = start + to_read;
            buf.put_slice(&self.inner.get_ref()[start..end]);
            self.inner.set_position(end as u64);
            self.should_pending = true;
            Poll::Ready(Ok(()))
        }
    }

    fn encrypt_with_legacy_nonce_reuse(data: &[u8], key: [u8; 32], nonce: [u8; 12]) -> Vec<u8> {
        let cipher = Aes256Gcm::new_from_slice(&key).expect("valid key");
        let nonce = Nonce::try_from(nonce.as_slice()).expect("valid nonce");
        let mut encrypted = Vec::new();

        for chunk in data.chunks(ENCRYPTION_BLOCK_SIZE) {
            let crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(chunk);
                hasher.finalize() as u32
            };
            let ciphertext = cipher.encrypt(&nonce, chunk).expect("legacy encrypt");
            let int_len = put_uvarint_len(chunk.len() as u64);
            let clen = int_len + ciphertext.len() + 4;
            let mut header = [0u8; 8];
            header[1] = (clen & 0xFF) as u8;
            header[2] = ((clen >> 8) & 0xFF) as u8;
            header[3] = ((clen >> 16) & 0xFF) as u8;
            header[4] = (crc & 0xFF) as u8;
            header[5] = ((crc >> 8) & 0xFF) as u8;
            header[6] = ((crc >> 16) & 0xFF) as u8;
            header[7] = ((crc >> 24) & 0xFF) as u8;
            encrypted.extend_from_slice(&header);
            let mut plaintext_len_buf = [0u8; 10];
            let encoded_len = put_uvarint(&mut plaintext_len_buf, chunk.len() as u64);
            encrypted.extend_from_slice(&plaintext_len_buf[..encoded_len]);
            encrypted.extend_from_slice(&ciphertext);
        }

        encrypted.extend_from_slice(&[0xFF, 0, 0, 0, 0, 0, 0, 0]);
        encrypted
    }

    async fn encrypt_part_with_legacy_nonce_layout(
        data: &[u8],
        key: [u8; 32],
        base_nonce: [u8; 12],
        part_number: usize,
    ) -> Vec<u8> {
        let nonce = derive_legacy_part_nonce(&base_nonce, part_number);
        let reader = BufReader::new(Cursor::new(data.to_vec()));
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");
        encrypted
    }

    fn extract_encrypted_payloads(encrypted: &[u8]) -> Vec<Vec<u8>> {
        let mut payloads = Vec::new();
        let mut pos = 0;

        while pos + 8 <= encrypted.len() {
            let header = &encrypted[pos..pos + 8];
            pos += 8;
            if header[0] == 0xFF {
                break;
            }

            let len = (header[1] as usize) | ((header[2] as usize) << 8) | ((header[3] as usize) << 16);
            let payload_len = len - 4;
            payloads.push(encrypted[pos..pos + payload_len].to_vec());
            pos += payload_len;
        }

        payloads
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_reader_aes256gcm() {
        let data = b"hello sse encrypt";
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = BufReader::new(&data[..]);
        let encrypt_reader = EncryptReader::new(reader, key, nonce);

        // Encrypt
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        // Decrypt using DecryptReader
        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(&decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_only() {
        // Encrypt some data first
        let data = b"test decrypt only";
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        // Encrypt
        let reader = BufReader::new(&data[..]);
        let encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        // Now test DecryptReader

        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(&decrypted, data);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_reader_large() {
        use rand::Rng;
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = std::io::Cursor::new(data.clone());
        let encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let reader = std::io::Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(&decrypted, &data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_large_with_small_chunks() {
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = Cursor::new(data.clone());
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let reader = ChunkedCursor::new(encrypted, 3);
        let mut decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_large_with_pending_chunks() {
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = Cursor::new(data.clone());
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let reader = PendingChunkedCursor::new(encrypted, 3);
        let mut decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_large_through_reader_stream() {
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = Cursor::new(data.clone());
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let reader = ChunkedCursor::new(encrypted, 8192);
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut stream = ReaderStream::with_capacity(Box::new(decrypt_reader), 262_144);

        let mut decrypted = Vec::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.expect("operation should succeed");
            decrypted.extend_from_slice(&bytes);
        }

        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_large_through_hard_limit_reader_stream() {
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = Cursor::new(data.clone());
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let reader = ChunkedCursor::new(encrypted, 8192);
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let limit_reader = HardLimitReader::new(decrypt_reader, size as i64);
        let mut stream = ReaderStream::with_capacity(Box::new(limit_reader), 262_144);

        let mut decrypted = Vec::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.expect("operation should succeed");
            decrypted.extend_from_slice(&bytes);
        }

        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_multipart_segments() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let part_one = vec![0xA5; 512 * 1024];
        let part_two = vec![0x5A; 256 * 1024];

        async fn encrypt_part(data: &[u8], key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Vec<u8> {
            let nonce = derive_part_nonce(&base_nonce, part_number);
            let reader = BufReader::new(Cursor::new(data.to_vec()));
            let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
            let mut encrypted = Vec::new();
            encrypt_reader
                .read_to_end(&mut encrypted)
                .await
                .expect("operation should succeed");
            encrypted
        }

        let encrypted_one = encrypt_part(&part_one, key, base_nonce, 1).await;
        let encrypted_two = encrypt_part(&part_two, key, base_nonce, 2).await;

        let mut combined = Vec::with_capacity(encrypted_one.len() + encrypted_two.len());
        combined.extend_from_slice(&encrypted_one);
        combined.extend_from_slice(&encrypted_two);

        let reader = BufReader::new(Cursor::new(combined));
        let mut decrypt_reader = DecryptReader::new_multipart(reader, key, base_nonce, vec![1, 2]);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        let mut expected = Vec::with_capacity(part_one.len() + part_two.len());
        expected.extend_from_slice(&part_one);
        expected.extend_from_slice(&part_two);

        assert_eq!(decrypted, expected);
    }

    #[tokio::test]
    async fn test_encrypt_reader_uses_distinct_nonces_per_block() {
        let data = vec![0xAB; ENCRYPTION_BLOCK_SIZE * 2];
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = Cursor::new(data);
        let mut encrypt_reader = EncryptReader::new(reader, key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("operation should succeed");

        let payloads = extract_encrypted_payloads(&encrypted);
        assert!(payloads.len() >= 2);
        assert_ne!(payloads[0], payloads[1]);
    }

    #[test]
    fn test_part_and_block_nonces_do_not_collide_across_parts() {
        let base_nonce = [0u8; 12];
        let part_one_block_one = derive_block_nonce(&derive_part_nonce(&base_nonce, 1), 1);
        let part_two_block_zero = derive_block_nonce(&derive_part_nonce(&base_nonce, 2), 0);

        assert_ne!(part_one_block_one, part_two_block_zero);
    }

    #[tokio::test]
    async fn test_decrypt_reader_accepts_legacy_single_nonce_streams() {
        let mut data = vec![0u8; ENCRYPTION_BLOCK_SIZE * 3 + 17];
        rand::rng().fill(&mut data[..]);
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let encrypted = encrypt_with_legacy_nonce_reuse(&data, key, nonce);
        let reader = Cursor::new(encrypted);
        let mut decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_reader_accepts_legacy_multipart_nonce_layout() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let part_one = vec![0x11; ENCRYPTION_BLOCK_SIZE + 97];
        let part_two = vec![0x22; ENCRYPTION_BLOCK_SIZE + 33];

        let encrypted_one = encrypt_part_with_legacy_nonce_layout(&part_one, key, base_nonce, 1).await;
        let encrypted_two = encrypt_part_with_legacy_nonce_layout(&part_two, key, base_nonce, 2).await;

        let mut combined = Vec::with_capacity(encrypted_one.len() + encrypted_two.len());
        combined.extend_from_slice(&encrypted_one);
        combined.extend_from_slice(&encrypted_two);

        let reader = BufReader::new(Cursor::new(combined));
        let mut decrypt_reader = DecryptReader::new_multipart(reader, key, base_nonce, vec![1, 2]);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("operation should succeed");

        let mut expected = Vec::with_capacity(part_one.len() + part_two.len());
        expected.extend_from_slice(&part_one);
        expected.extend_from_slice(&part_two);

        assert_eq!(decrypted, expected);
    }

    // Regression: a corrupted block header whose length yields a payload shorter than 16 bytes
    // must not panic. Header (8 bytes): [typ, len_lo, len_mid, len_hi, crc0..crc3]; payload is
    // `len - 4` bytes. Pre-fix, poll_read sliced `ciphertext_buf[0..16]` unconditionally,
    // panicking with "range end index 16 out of range for slice of length N" when N < 16.
    #[tokio::test]
    async fn test_decrypt_reader_short_block_no_panic() {
        let key = [0u8; 32];
        let nonce = [0u8; 12];

        // len = 8 -> payload_len = 4 (< 16). Provide exactly 4 payload bytes.
        let len: usize = 8;
        let mut input = vec![
            0x00u8, // typ (regular block)
            (len & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            ((len >> 16) & 0xFF) as u8,
        ];
        input.extend_from_slice(&[0u8; 4]); // crc (unused before the panic site)
        input.extend_from_slice(&[0x01u8, 0x02, 0x03, 0x04]); // 4-byte payload

        let mut decrypt_reader = DecryptReader::new(Cursor::new(input), key, nonce);
        let mut out = Vec::new();
        let res = decrypt_reader.read_to_end(&mut out).await;
        assert!(res.is_err(), "corrupted short encrypted block must return an error, not panic");
    }

    // ======================= v2 frame format =======================

    /// Byte cost of one full v2 frame: 8B header + 2B uvarint(8192) + 8192B
    /// plaintext + 16B GCM tag.
    const V2_FULL_FRAME_LEN: usize = 8 + 2 + super::ENCRYPTION_BLOCK_SIZE + 16;

    async fn v2_encrypt(data: &[u8], key: [u8; 32], nonce: [u8; 12]) -> Vec<u8> {
        let mut encrypt_reader = EncryptReader::new_v2(BufReader::new(data), key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted)
            .await
            .expect("v2 encryption succeeds");
        encrypted
    }

    async fn v2_decrypt(bytes: Vec<u8>, key: [u8; 32], nonce: [u8; 12]) -> std::io::Result<Vec<u8>> {
        let mut decrypt_reader = DecryptReader::new(Cursor::new(bytes), key, nonce);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await?;
        Ok(decrypted)
    }

    #[tokio::test]
    async fn v2_round_trips_every_boundary_length() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let block = super::ENCRYPTION_BLOCK_SIZE;
        for len in [0usize, 1, block - 1, block, block + 1, 3 * block, 3 * block + 7] {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let encrypted = v2_encrypt(&data, key, nonce).await;
            let decrypted = v2_decrypt(encrypted, key, nonce).await.expect("round trip succeeds");
            assert_eq!(decrypted, data, "length {len} must round-trip");
        }
    }

    #[tokio::test]
    async fn v2_non_final_frames_are_fixed_length() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let block = super::ENCRYPTION_BLOCK_SIZE;
        // 3 full frames + a 7-byte final frame + the 8-byte end marker.
        let tail = 7usize;
        let data: Vec<u8> = vec![0xAB; 3 * block + tail];
        let encrypted = v2_encrypt(&data, key, nonce).await;
        let final_frame_len = 8 + 1 + tail + 16;
        assert_eq!(encrypted.len(), 3 * V2_FULL_FRAME_LEN + final_frame_len + 8);
        for frame_index in 0..3 {
            assert_eq!(encrypted[frame_index * V2_FULL_FRAME_LEN], super::FRAME_TYPE_V2);
        }
        assert_eq!(encrypted[3 * V2_FULL_FRAME_LEN], super::FRAME_TYPE_V2_FINAL);
        assert_eq!(encrypted[encrypted.len() - 8], super::FRAME_TYPE_END);

        // EOF exactly on a block boundary still authenticates emptiness with an
        // empty final frame.
        let aligned: Vec<u8> = vec![0xCD; block];
        let encrypted = v2_encrypt(&aligned, key, nonce).await;
        let empty_final_len = 8 + 1 + 16;
        assert_eq!(encrypted.len(), V2_FULL_FRAME_LEN + empty_final_len + 8);
    }

    #[tokio::test]
    async fn v2_rejects_header_tampering_reordering_and_truncation() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let block = super::ENCRYPTION_BLOCK_SIZE;
        let data: Vec<u8> = (0..2 * block + 100).map(|i| (i % 249) as u8).collect();
        let encrypted = v2_encrypt(&data, key, nonce).await;

        // Flip a CRC byte in the first frame's header: the header is AAD, so
        // authentication fails even though the ciphertext is untouched.
        let mut crc_flip = encrypted.clone();
        crc_flip[5] ^= 0x01;
        v2_decrypt(crc_flip, key, nonce)
            .await
            .expect_err("header tampering must fail");

        // Rewrite the final-frame type byte to non-final: same AAD binding.
        let mut type_flip = encrypted.clone();
        let final_offset = 2 * V2_FULL_FRAME_LEN;
        assert_eq!(type_flip[final_offset], super::FRAME_TYPE_V2_FINAL);
        type_flip[final_offset] = super::FRAME_TYPE_V2;
        v2_decrypt(type_flip, key, nonce)
            .await
            .expect_err("final-flag tampering must fail");

        // Swap the two full frames: the frame index is AAD, so replaying a
        // frame at another position fails.
        let mut swapped = encrypted.clone();
        let (first, rest) = swapped.split_at_mut(V2_FULL_FRAME_LEN);
        first.swap_with_slice(&mut rest[..V2_FULL_FRAME_LEN]);
        v2_decrypt(swapped, key, nonce).await.expect_err("frame reordering must fail");

        // Drop the final frame and end marker: a clean EOF before the final
        // frame is truncation, not success.
        let truncated = encrypted[..2 * V2_FULL_FRAME_LEN].to_vec();
        v2_decrypt(truncated, key, nonce).await.expect_err("truncation must fail");

        // Fabricate an early end marker where the final frame should be.
        let mut early_end = encrypted[..2 * V2_FULL_FRAME_LEN].to_vec();
        early_end.extend_from_slice(&[super::FRAME_TYPE_END, 0, 0, 0, 0, 0, 0, 0]);
        v2_decrypt(early_end, key, nonce)
            .await
            .expect_err("forged end marker must fail");

        // Unknown frame type and forged zero-length v2 frame both fail.
        v2_decrypt(vec![0x07, 0, 0, 0, 0, 0, 0, 0], key, nonce)
            .await
            .expect_err("unknown frame type must fail");
        v2_decrypt(vec![super::FRAME_TYPE_V2, 0, 0, 0, 0, 0, 0, 0], key, nonce)
            .await
            .expect_err("zero-length v2 frame must fail");

        // The untampered stream still decrypts after all that.
        let decrypted = v2_decrypt(encrypted, key, nonce).await.expect("control decrypt succeeds");
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn v2_rejects_mixed_frame_versions_within_a_segment() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let block = super::ENCRYPTION_BLOCK_SIZE;
        let v2_stream = v2_encrypt(&vec![0x11; block], key, nonce).await;

        // First full v2 frame followed by a v1 frame header: version mixing.
        let mut mixed = v2_stream[..V2_FULL_FRAME_LEN].to_vec();
        let mut v1_reader = EncryptReader::new(BufReader::new(&[0x22u8; 64][..]), key, nonce);
        let mut v1_stream = Vec::new();
        v1_reader.read_to_end(&mut v1_stream).await.expect("v1 encryption succeeds");
        mixed.extend_from_slice(&v1_stream);
        v2_decrypt(mixed, key, nonce)
            .await
            .expect_err("mixed frame versions must fail");
    }

    #[tokio::test]
    async fn v2_multipart_round_trips_and_detects_missing_parts() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let part_one: Vec<u8> = vec![0x31; super::ENCRYPTION_BLOCK_SIZE + 11];
        let part_two: Vec<u8> = vec![0x32; 300];

        async fn encrypt_part_v2(data: &[u8], key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Vec<u8> {
            let mut reader = EncryptReader::new_multipart_v2(BufReader::new(data), key, base_nonce, part_number);
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.expect("part encryption succeeds");
            out
        }

        let encrypted_one = encrypt_part_v2(&part_one, key, base_nonce, 1).await;
        let encrypted_two = encrypt_part_v2(&part_two, key, base_nonce, 2).await;

        let mut combined = encrypted_one.clone();
        combined.extend_from_slice(&encrypted_two);
        let mut decrypt_reader = DecryptReader::new_multipart(Cursor::new(combined), key, base_nonce, vec![1, 2]);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("multipart v2 round trip succeeds");
        let mut expected = part_one.clone();
        expected.extend_from_slice(&part_two);
        assert_eq!(decrypted, expected);

        // Dropping the entire second part must not read as a clean end.
        let mut decrypt_reader = DecryptReader::new_multipart(Cursor::new(encrypted_one), key, base_nonce, vec![1, 2]);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect_err("a missing part segment must fail");
    }

    #[tokio::test]
    async fn v2_multipart_serves_segments_after_a_block_aligned_part() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        // A block-aligned first part ends in an authenticated EMPTY final
        // frame. That zero-plaintext frame must not surface as a zero-byte
        // read (EOF to the caller) — the regression dropped every segment
        // after it.
        let part_one: Vec<u8> = (0..4 * super::ENCRYPTION_BLOCK_SIZE).map(|i| (i % 253) as u8).collect();
        let part_two: Vec<u8> = (0..super::ENCRYPTION_BLOCK_SIZE + 77)
            .map(|i| ((i + 5) % 241) as u8)
            .collect();

        let mut combined = Vec::new();
        for (data, part_number) in [(&part_one, 1usize), (&part_two, 2usize)] {
            let mut reader = EncryptReader::new_multipart_v2(BufReader::new(&data[..]), key, base_nonce, part_number);
            reader.read_to_end(&mut combined).await.expect("part encryption succeeds");
        }

        let mut decrypt_reader = DecryptReader::new_multipart(Cursor::new(combined), key, base_nonce, vec![1, 2]);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("block-aligned multipart round trip succeeds");
        let mut expected = part_one.clone();
        expected.extend_from_slice(&part_two);
        assert_eq!(decrypted.len(), expected.len(), "no segment may be dropped after an empty final frame");
        assert_eq!(decrypted, expected);
    }

    #[tokio::test]
    async fn v2_decrypts_from_an_arbitrary_frame_boundary() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let block = super::ENCRYPTION_BLOCK_SIZE;
        let data: Vec<u8> = (0..4 * block + 321).map(|i| (i % 247) as u8).collect();
        let encrypted = v2_encrypt(&data, key, nonce).await;

        // Serve the ciphertext window starting at frame 2 and decrypt with the
        // matching absolute frame index: nonce and AAD both line up.
        let window = encrypted[2 * V2_FULL_FRAME_LEN..].to_vec();
        let mut decrypt_reader = DecryptReader::new_at_block(Cursor::new(window.clone()), key, nonce, 2);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect("mid-stream decrypt at a true frame boundary succeeds");
        assert_eq!(decrypted, &data[2 * block..]);

        // The same window with a wrong starting index fails authentication.
        let mut decrypt_reader = DecryptReader::new_at_block(Cursor::new(window), key, nonce, 1);
        let mut decrypted = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted)
            .await
            .expect_err("a wrong absolute frame index must fail authentication");
    }
}
