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
use rand::Rng;
pub use rustfs_config::{DEFAULT_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE, ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE};
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
///
/// A segment-nonce (`0x03`) frame opens a multipart part segment when
/// [`ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE`] is enabled (or via the
/// explicit `*_with_segment_nonce` constructors). It carries a fresh random
/// 12-byte segment nonce in the clear together with an AES-256-GCM tag over
/// the empty message binding the frame header, announced nonce, session base
/// nonce, mode, and part number as associated data. The reader derives the
/// segment's frame nonces from the announced nonce offset by `part_number`, so
/// re-uploading the same part number never reuses an AES-GCM `(key, nonce)`
/// pair. Segments without a `0x03` frame keep session-derived nonces so
/// existing objects remain readable without any configuration switch.
const FRAME_TYPE_V1: u8 = 0x00;
const FRAME_TYPE_V2: u8 = 0x01;
const FRAME_TYPE_V2_FINAL: u8 = 0x02;
const FRAME_TYPE_SEGMENT_NONCE: u8 = 0x03;
const FRAME_TYPE_END: u8 = 0xFF;

const SEGMENT_NONCE_LEN: usize = 12;
const GCM_TAG_LEN: usize = 16;
/// Header length field of a segment-nonce (`0x03`) frame: 1-byte uvarint(12),
/// 12-byte nonce, 16-byte GCM tag, and the 4 bytes every frame length accounts
/// for its header CRC field.
const SEGMENT_NONCE_FRAME_LEN: usize = 1 + SEGMENT_NONCE_LEN + GCM_TAG_LEN + 4;
/// Total serialized byte length of a segment-nonce (`0x03`) frame:
/// 8-byte header + 1-byte length prefix + 12-byte nonce + 16-byte GCM tag = 37 bytes.
pub const SEGMENT_NONCE_FRAME_TOTAL_LEN: usize = 8 + SEGMENT_NONCE_FRAME_LEN - 4;
/// Plaintext bytes carried by each full non-final v2 frame.
pub const V2_FRAME_PLAINTEXT_LEN: usize = ENCRYPTION_BLOCK_SIZE;
/// Ciphertext bytes of one full non-final v2 frame:
/// 8-byte header + 2-byte uvarint(8192) + 8192-byte plaintext + 16-byte GCM tag = 8218 bytes.
pub const V2_FULL_FRAME_LEN: usize = 8 + 2 + ENCRYPTION_BLOCK_SIZE + GCM_TAG_LEN;

/// Closed-form ciphertext byte offset of frame `block_index` within a v2
/// fixed-frame segment.
///
/// When `has_segment_nonce` is `true`, the segment opens with a
/// [`SEGMENT_NONCE_FRAME_TOTAL_LEN`]-byte (37-byte) `0x03` frame, placing
/// frame `block_index` at `37 + block_index * 8218`. When `false`, frame
/// `block_index` starts at `block_index * 8218`.
///
/// Returns `None` on arithmetic overflow.
pub const fn v2_frame_ciphertext_offset(block_index: usize, has_segment_nonce: bool) -> Option<usize> {
    let prefix = if has_segment_nonce { SEGMENT_NONCE_FRAME_TOTAL_LEN } else { 0 };
    match block_index.checked_mul(V2_FULL_FRAME_LEN) {
        Some(frames_bytes) => prefix.checked_add(frames_bytes),
        None => None,
    }
}

fn multipart_segment_nonce_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE,
            DEFAULT_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE,
                DEFAULT_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE,
            )
        })
    }
}

/// AEAD associated data of a v2 frame: the 8-byte header followed by the
/// frame index within its segment, little-endian.
fn v2_frame_aad(header: &[u8; 8], block_index: usize) -> [u8; 16] {
    let mut aad = [0u8; 16];
    aad[..8].copy_from_slice(header);
    aad[8..].copy_from_slice(&(block_index as u64).to_le_bytes());
    aad
}

#[derive(Clone, Copy)]
struct SegmentNonceAnnouncement {
    segment_nonce: [u8; 12],
    session_base_nonce: [u8; 12],
    part_number: Option<usize>,
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
        // Segment nonce waiting to be emitted as the segment's opening `0x03` frame
        pending_segment_nonce: Option<SegmentNonceAnnouncement>,
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
            pending_segment_nonce: None,
        }
    }

    /// Writer for one part of a multipart upload.
    ///
    /// When [`ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE`] is enabled, each
    /// part stream draws a fresh random 12-byte segment nonce, announces it in
    /// an authenticated [`FRAME_TYPE_SEGMENT_NONCE`] (`0x03`) opening frame
    /// bound to `(base_nonce, part_number)`, and derives its frame nonces from
    /// `multipart_part_nonce(segment_nonce, part_number)`. Re-uploading the
    /// same part number with different plaintext therefore never reuses an
    /// AES-256-GCM `(key, nonce)` pair.
    ///
    /// When the write switch is off (the default during mixed-version rolling
    /// upgrades), nonces are derived directly from `(base_nonce, part_number)`
    /// so nodes predating `0x03` read support can still decrypt newly written
    /// parts.
    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        if multipart_segment_nonce_enabled() {
            Self::new_multipart_with_segment_nonce(inner, key, base_nonce, part_number)
        } else {
            Self::new(inner, key, multipart_part_nonce(base_nonce, part_number))
        }
    }

    /// Multipart writer that always opens the segment with an authenticated
    /// random [`FRAME_TYPE_SEGMENT_NONCE`] (`0x03`) frame, regardless of
    /// [`ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE`].
    pub fn new_multipart_with_segment_nonce(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        let segment_nonce = random_segment_nonce();
        let mut reader = Self::new(inner, key, multipart_part_nonce(segment_nonce, part_number));
        reader.pending_segment_nonce = Some(SegmentNonceAnnouncement {
            segment_nonce,
            session_base_nonce: base_nonce,
            part_number: Some(part_number),
        });
        reader
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

    /// Multipart writer for the v2 layout; see [`EncryptReader::new_v2`] and
    /// [`EncryptReader::new_multipart`].
    pub fn new_multipart_v2(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        let mut reader = Self::new_multipart(inner, key, base_nonce, part_number);
        reader.frame_v2 = true;
        reader
    }

    /// Multipart writer for the v2 layout that always emits an authenticated
    /// random [`FRAME_TYPE_SEGMENT_NONCE`] (`0x03`) opening frame.
    pub fn new_multipart_v2_with_segment_nonce(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        let mut reader = Self::new_multipart_with_segment_nonce(inner, key, base_nonce, part_number);
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

/// Header of a segment-nonce (`0x03`) frame: type, length, and CRC32 of the
/// announced 12-byte nonce, laid out like a v1/v2 frame header.
fn segment_nonce_frame_header(segment_nonce: &[u8; 12]) -> [u8; 8] {
    let crc = {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        hasher.update(segment_nonce);
        hasher.finalize() as u32
    };
    let mut header = [0u8; 8];
    header[0] = FRAME_TYPE_SEGMENT_NONCE;
    header[1] = (SEGMENT_NONCE_FRAME_LEN & 0xFF) as u8;
    header[2] = ((SEGMENT_NONCE_FRAME_LEN >> 8) & 0xFF) as u8;
    header[3] = ((SEGMENT_NONCE_FRAME_LEN >> 16) & 0xFF) as u8;
    header[4..].copy_from_slice(&crc.to_le_bytes());
    header
}

/// AEAD associated data of a segment-nonce (`0x03`) frame:
/// - `[0..8]`: 8-byte header
/// - `[8..20]`: announced 12-byte `segment_nonce`
/// - `[20..32]`: 12-byte session `base_nonce` (prevents cross-object part
///   splicing under a shared key)
/// - `[32]`: mode byte (`1` for multipart, `0` for single-part)
/// - `[33..41]`: `part_number` (little-endian `u64`; `0` for single-part)
fn segment_nonce_aad(
    header: &[u8; 8],
    segment_nonce: &[u8; 12],
    session_base_nonce: &[u8; 12],
    part_number: Option<usize>,
) -> [u8; 41] {
    let mut aad = [0u8; 41];
    aad[..8].copy_from_slice(header);
    aad[8..20].copy_from_slice(segment_nonce);
    aad[20..32].copy_from_slice(session_base_nonce);
    aad[32] = u8::from(part_number.is_some());
    aad[33..41].copy_from_slice(&(part_number.unwrap_or(0) as u64).to_le_bytes());
    aad
}

/// Nonce under which a segment-nonce frame's authentication tag is computed:
/// the segment base with the block counter saturated (`u32::MAX`), which no
/// data frame in the segment is permitted to reach.
fn segment_nonce_tag_nonce(segment_base: &[u8; 12]) -> [u8; 12] {
    derive_block_nonce(segment_base, u32::MAX as usize)
}

/// Build the 37-byte segment-nonce (`0x03`) frame: 8-byte header, 1-byte
/// length uvarint (`12`), the 12-byte random nonce in the clear, and the
/// 16-byte AES-256-GCM tag of the empty message authenticating header, nonce,
/// session base nonce, mode, and part number.
fn build_segment_nonce_frame(
    cipher: &Aes256Gcm,
    segment_base: &[u8; 12],
    segment_nonce: &[u8; 12],
    session_base_nonce: &[u8; 12],
    part_number: Option<usize>,
) -> std::io::Result<Vec<u8>> {
    let header = segment_nonce_frame_header(segment_nonce);
    let tag_nonce = segment_nonce_tag_nonce(segment_base);
    let nonce = Nonce::try_from(tag_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
    let aad = segment_nonce_aad(&header, segment_nonce, session_base_nonce, part_number);
    let tag = cipher
        .encrypt(&nonce, Payload { msg: &[], aad: &aad })
        .map_err(|e| Error::other(format!("encrypt error: {e}")))?;

    let mut out = Vec::with_capacity(SEGMENT_NONCE_FRAME_TOTAL_LEN);
    out.extend_from_slice(&header);
    let mut len_buf = [0u8; 10];
    let encoded_len = put_uvarint(&mut len_buf, SEGMENT_NONCE_LEN as u64);
    out.extend_from_slice(&len_buf[..encoded_len]);
    out.extend_from_slice(segment_nonce);
    out.extend_from_slice(&tag);
    Ok(out)
}

/// Verify a segment-nonce (`0x03`) frame payload (the 29 bytes after the
/// 8-byte header) and return the derived segment base nonce.
fn parse_segment_nonce_frame(
    cipher: &Aes256Gcm,
    header: &[u8; 8],
    payload: &[u8],
    session_base_nonce: &[u8; 12],
    part_number: Option<usize>,
) -> std::io::Result<[u8; 12]> {
    let invalid = |msg: &str| Error::new(std::io::ErrorKind::InvalidData, msg.to_string());
    if header[0] != FRAME_TYPE_SEGMENT_NONCE {
        return Err(invalid("unexpected frame type for segment nonce frame"));
    }
    let declared_len = (header[1] as usize) | ((header[2] as usize) << 8) | ((header[3] as usize) << 16);
    if declared_len != SEGMENT_NONCE_FRAME_LEN || payload.len() != SEGMENT_NONCE_FRAME_LEN - 4 {
        return Err(invalid("invalid segment nonce frame length"));
    }
    let (nonce_len, uvarint_len) = rustfs_utils::uvarint(payload);
    if uvarint_len != 1 || nonce_len != SEGMENT_NONCE_LEN as u64 {
        return Err(invalid("invalid segment nonce frame length prefix"));
    }
    let body = &payload[1..];
    if body.len() != SEGMENT_NONCE_LEN + GCM_TAG_LEN {
        return Err(invalid("invalid segment nonce frame length"));
    }
    let (segment_nonce, tag) = body.split_at(SEGMENT_NONCE_LEN);
    let segment_nonce: [u8; 12] = segment_nonce
        .try_into()
        .map_err(|_| invalid("invalid segment nonce length"))?;

    let segment_base = match part_number {
        Some(part_number) => derive_part_nonce(&segment_nonce, part_number),
        None => segment_nonce,
    };
    let tag_nonce = segment_nonce_tag_nonce(&segment_base);
    let nonce = Nonce::try_from(tag_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
    let aad = segment_nonce_aad(header, &segment_nonce, session_base_nonce, part_number);
    cipher
        .decrypt(&nonce, Payload { msg: tag, aad: &aad })
        .map_err(|_| invalid("segment nonce frame failed authentication"))?;

    let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let actual_crc = {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        hasher.update(&segment_nonce);
        hasher.finalize() as u32
    };
    if actual_crc != expected_crc {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "CRC32 mismatch"));
    }
    Ok(segment_base)
}

/// Authenticate a complete 37-byte [`FRAME_TYPE_SEGMENT_NONCE`] (`0x03`)
/// opening frame and return the segment's active base nonce.
///
/// Callers performing closed-form v2 fixed-frame seeks inside a multipart part
/// read the part's first [`SEGMENT_NONCE_FRAME_TOTAL_LEN`] (37) bytes, verify
/// them with this helper against `(key, session_base_nonce, Some(part_number))`,
/// compute the target frame offset via
/// `v2_frame_ciphertext_offset(block_index, true)`, and pass the returned base
/// nonce to [`DecryptReader::new_multipart_at_block`].
///
/// # Errors
/// Returns [`std::io::ErrorKind::InvalidData`] if `frame` is not a valid,
/// authentic 37-byte segment-nonce frame for `(key, session_base_nonce,
/// part_number)`.
pub fn authenticate_segment_nonce_frame(
    frame: &[u8],
    key: [u8; 32],
    session_base_nonce: [u8; 12],
    part_number: Option<usize>,
) -> std::io::Result<[u8; 12]> {
    if frame.len() != SEGMENT_NONCE_FRAME_TOTAL_LEN {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "invalid segment nonce frame length"));
    }
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| Error::other("invalid key length"))?;
    let (header_slice, payload) = frame.split_at(8);
    let header: &[u8; 8] = header_slice
        .try_into()
        .map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "invalid segment nonce frame header"))?;
    parse_segment_nonce_frame(&cipher, header, payload, &session_base_nonce, part_number)
}

fn random_segment_nonce() -> [u8; 12] {
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    nonce
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

        // Emit the segment's authenticated random nonce announcement before frame 0.
        if let Some(announcement) = this.pending_segment_nonce.take() {
            *this.buffer = build_segment_nonce_frame(
                this.cipher,
                this.base_nonce,
                &announcement.segment_nonce,
                &announcement.session_base_nonce,
                announcement.part_number,
            )?;
            *this.buffer_pos = 0;
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
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

            if *this.block_index >= u32::MAX as usize {
                return Poll::Ready(Err(Error::other("encrypted segment exceeded maximum frame count")));
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
                    if *this.block_index >= u32::MAX as usize {
                        return Poll::Ready(Err(Error::other("encrypted segment exceeded maximum frame count")));
                    }
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

/// Read-side switch for the pre-`1.0.0-alpha.91` nonce layout, in which a whole
/// v1 segment reused the part nonce for every block.
///
/// On by default, because turning it off refuses to decrypt objects written
/// before that release. Block zero's derived nonce equals that base nonce, so
/// the layout also lets a frame encrypted at index zero authenticate anywhere
/// in its segment; the in-segment layout lock catches that as soon as a later
/// frame disagrees, but a stream that is nothing but repeats of frame zero has
/// no such later frame. A deployment with no pre-alpha.91 objects should set
/// this to `false` to remove that surface outright (backlog#2369 P2).
///
// RUSTFS_COMPAT_TODO(backlog-2369-legacy-nonce-fallback): Remove after the
// minimum supported direct-upgrade release and after migration tooling has
// rewritten every pre-alpha.91 encrypted object.
pub const ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK: &str = "RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK";
const DEFAULT_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK: bool = true;

fn legacy_nonce_fallback_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(
            ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK,
            DEFAULT_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK,
        )
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_bool(
                ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK,
                DEFAULT_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK,
            )
        })
    }
}

/// The nonce layout selected while decoding a legacy v1 segment.
///
/// A historical writer used one of these layouts consistently for every
/// block in a segment. Once a non-zero block identifies that layout, accepting
/// another layout would let an attacker replay a block encrypted at index zero.
#[derive(Clone, Copy, PartialEq, Eq)]
enum V1NonceLayout {
    Current,
    LegacyBlock,
    ReusedPart,
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
        v1_nonce_layout: Option<V1NonceLayout>,
        legacy_nonce_fallback: bool,
        // True once the current segment's active base nonce was announced via
        // a `0x03` frame (or supplied explicitly by a mid-segment seek).
        explicit_segment_nonce: bool,
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
            v1_nonce_layout: None,
            legacy_nonce_fallback: legacy_nonce_fallback_enabled(),
            explicit_segment_nonce: false,
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
            v1_nonce_layout: None,
            legacy_nonce_fallback: legacy_nonce_fallback_enabled(),
            explicit_segment_nonce: false,
        }
    }

    /// Decrypt a multipart v2 stream starting at an arbitrary frame boundary
    /// (`starting_block_index`) of the first part in `multipart_parts`.
    ///
    /// `base_nonce` is the upload's session nonce used to authenticate any
    /// subsequent parts' `0x03` frames (or derive legacy nonces for parts
    /// without one). `first_segment_base_nonce` is the active base nonce for
    /// the opening part: either the result of
    /// [`authenticate_segment_nonce_frame`] when the part starts with a 37-byte
    /// `0x03` frame, or `multipart_part_nonce(base_nonce, first_part)` when it
    /// does not.
    ///
    /// Because the opening part's base nonce is already established, the stream
    /// must begin directly at frame `starting_block_index` and any `0x03` frame
    /// in the opening segment is rejected. After the opening segment's `0xFF`
    /// end marker, subsequent parts in `multipart_parts` read their own opening
    /// `0x03` frames (or fall back to session-derived nonces) normally.
    pub fn new_multipart_at_block(
        inner: R,
        key: [u8; 32],
        base_nonce: [u8; 12],
        first_segment_base_nonce: [u8; 12],
        multipart_parts: Vec<usize>,
        starting_block_index: usize,
    ) -> Self {
        let mut reader = Self::new_multipart(inner, key, base_nonce, multipart_parts);
        reader.current_nonce_base = first_segment_base_nonce;
        reader.block_index = starting_block_index;
        reader.explicit_segment_nonce = true;
        reader
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
                                    // A segment that opened with a `0x03` frame
                                    // must contain at least one data frame or a
                                    // `0xFF` end marker; immediate EOF means the
                                    // segment was truncated right after its nonce.
                                    if *this.explicit_segment_nonce && *this.segment_frames == 0 {
                                        return Poll::Ready(Err(Error::new(
                                            std::io::ErrorKind::UnexpectedEof,
                                            "encrypted stream truncated after segment nonce frame",
                                        )));
                                    }
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
                    *this.v1_nonce_layout = None;
                    *this.explicit_segment_nonce = false;

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

                if typ == FRAME_TYPE_SEGMENT_NONCE {
                    // Only the very first frame of a segment may announce its
                    // nonce; anywhere later it would re-key frames already
                    // authenticated under another nonce.
                    if *this.block_index != 0
                        || *this.segment_frames != 0
                        || this.segment_frame_version.is_some()
                        || *this.explicit_segment_nonce
                    {
                        return Poll::Ready(Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            "segment nonce frame after the segment began",
                        )));
                    }
                    if len != SEGMENT_NONCE_FRAME_LEN {
                        return Poll::Ready(Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid segment nonce frame length",
                        )));
                    }
                    *this.current_frame_type = typ;
                    let payload_len = len - 4;
                    if this.ciphertext_buf.len() < payload_len {
                        this.ciphertext_buf.resize(payload_len, 0);
                    }
                    *this.ciphertext_len = payload_len;
                    *this.ciphertext_read = 0;
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
            if *this.current_frame_type == FRAME_TYPE_SEGMENT_NONCE {
                let part_number = (*this.multipart_mode).then_some(*this.current_part);
                let segment_base =
                    match parse_segment_nonce_frame(this.cipher, this.header_buf, ciphertext_buf, this.base_nonce, part_number) {
                        Ok(segment_base) => segment_base,
                        Err(err) => {
                            *this.ciphertext_read = 0;
                            *this.ciphertext_len = 0;
                            *this.current_frame_type = FRAME_TYPE_V1;
                            return Poll::Ready(Err(err));
                        }
                    };
                *this.current_nonce_base = segment_base;
                *this.explicit_segment_nonce = true;
                *this.current_frame_type = FRAME_TYPE_V1;
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                continue;
            }
            if *this.block_index >= u32::MAX as usize {
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "encrypted segment exceeded maximum frame count",
                )));
            }
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
            } else if *this.explicit_segment_nonce {
                // A segment that announced its nonce has no historical nonce
                // layout to fall back to, so it is locked to the current layout
                // from frame 0.
                this.cipher
                    .decrypt(&nonce, ciphertext)
                    .map_err(|error| Error::new(std::io::ErrorKind::InvalidData, format!("decrypt error: {error}")))?
            } else {
                let legacy_part_nonce = if *this.multipart_mode {
                    derive_legacy_part_nonce(this.base_nonce, *this.current_part)
                } else {
                    *this.base_nonce
                };
                let legacy_block_nonce = derive_block_nonce(&legacy_part_nonce, *this.block_index);
                let legacy_part_nonce =
                    Nonce::try_from(legacy_part_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
                let legacy_block_nonce =
                    Nonce::try_from(legacy_block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
                let layouts = [
                    (V1NonceLayout::Current, &nonce),
                    (V1NonceLayout::LegacyBlock, &legacy_block_nonce),
                    (V1NonceLayout::ReusedPart, &legacy_part_nonce),
                ];
                let selected = if *this.block_index == 0 { None } else { *this.v1_nonce_layout };
                let mut plaintext = None;
                let mut last_error = None;
                for (layout, candidate_nonce) in layouts {
                    if selected.is_some_and(|expected| expected != layout) {
                        continue;
                    }
                    if layout == V1NonceLayout::ReusedPart && !*this.legacy_nonce_fallback {
                        continue;
                    }
                    match this.cipher.decrypt(candidate_nonce, ciphertext) {
                        Ok(value) => {
                            plaintext = Some((value, layout));
                            break;
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                let (plaintext, layout) = plaintext.ok_or_else(|| {
                    Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "decrypt error: {}",
                            last_error.map_or_else(|| "nonce layout rejected".to_string(), |error| error.to_string())
                        ),
                    )
                })?;
                if *this.block_index > 0 && this.v1_nonce_layout.is_none() {
                    *this.v1_nonce_layout = Some(layout);
                }
                plaintext
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

    /// Encrypts `block_count` full v1 blocks, then overwrites frame one with a
    /// verbatim copy of frame zero. Every frame is the same length, so the
    /// stream keeps its original size and the forgery is invisible to any
    /// length check.
    async fn v1_stream_with_frame_zero_replayed_at_index_one(key: [u8; 32], nonce: [u8; 12], block_count: usize) -> Vec<u8> {
        assert!(block_count >= 2, "a replay needs at least two frames");
        let mut data = Vec::with_capacity(ENCRYPTION_BLOCK_SIZE * block_count);
        for index in 0..block_count {
            data.extend(std::iter::repeat_n(0xA1u8.wrapping_add(index as u8 * 17), ENCRYPTION_BLOCK_SIZE));
        }

        let mut encrypt_reader = EncryptReader::new(Cursor::new(data), key, nonce);
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.expect("encrypt v1 frames");

        // Header layout: [type][len:24][crc:32]; `len` counts the payload plus
        // its own 4-byte CRC field, so the frame occupies 8 + (len - 4) bytes.
        let declared_len = (encrypted[1] as usize) | ((encrypted[2] as usize) << 8) | ((encrypted[3] as usize) << 16);
        let frame_len = 8 + declared_len - 4;
        let replayed_first = encrypted[..frame_len].to_vec();
        encrypted[frame_len..frame_len * 2].copy_from_slice(&replayed_first);
        encrypted
    }

    #[tokio::test]
    async fn decrypt_reader_rejects_a_replayed_first_v1_frame() {
        let key = [0x11; 32];
        let nonce = [0x22; 12];
        let encrypted = v1_stream_with_frame_zero_replayed_at_index_one(key, nonce, 3).await;

        let mut decrypt_reader = DecryptReader::new(Cursor::new(encrypted), key, nonce);
        let error = decrypt_reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("a repeated index-zero frame must not authenticate at index one");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    /// The in-segment layout lock must not cost compatibility: every legacy v1
    /// shape the fallback chain exists for still decrypts under the default.
    #[tokio::test]
    async fn legacy_v1_streams_still_decrypt_under_the_default_fallback() {
        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK, None::<&str>)], async {
            assert!(legacy_nonce_fallback_enabled(), "the legacy nonce fallback must stay on by default");

            let mut key = [0u8; 32];
            let mut nonce = [0u8; 12];
            rand::rng().fill_bytes(&mut key);
            rand::rng().fill_bytes(&mut nonce);
            let mut data = vec![0u8; ENCRYPTION_BLOCK_SIZE * 3 + 17];
            rand::rng().fill(&mut data[..]);

            // Modern single-part stream.
            let mut encrypted = Vec::new();
            EncryptReader::new(Cursor::new(data.clone()), key, nonce)
                .read_to_end(&mut encrypted)
                .await
                .expect("modern v1 stream should encrypt");
            let mut decrypted = Vec::new();
            DecryptReader::new(Cursor::new(encrypted), key, nonce)
                .read_to_end(&mut decrypted)
                .await
                .expect("modern v1 stream should decrypt");
            assert_eq!(decrypted, data);

            // Pre-alpha.91 stream that reused the part nonce for every block.
            let legacy = encrypt_with_legacy_nonce_reuse(&data, key, nonce);
            let mut decrypted = Vec::new();
            DecryptReader::new(Cursor::new(legacy), key, nonce)
                .read_to_end(&mut decrypted)
                .await
                .expect("a reused-nonce legacy stream should still decrypt");
            assert_eq!(decrypted, data);
        })
        .await;
    }

    /// The residual after the layout lock: a stream that is nothing but repeats
    /// of frame zero has no later frame to disagree with the reused-part
    /// layout, so only turning the fallback off rejects it.
    #[tokio::test]
    async fn a_two_frame_replay_is_closed_only_by_disabling_the_legacy_fallback() {
        let key = [0x33; 32];
        let nonce = [0x44; 12];
        let encrypted = v1_stream_with_frame_zero_replayed_at_index_one(key, nonce, 2).await;

        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK, None::<&str>)], async {
            let mut forged = Vec::new();
            DecryptReader::new(Cursor::new(encrypted.clone()), key, nonce)
                .read_to_end(&mut forged)
                .await
                .expect("with the fallback on this forgery is still accepted");
            assert_eq!(forged.len(), ENCRYPTION_BLOCK_SIZE * 2);
            assert_eq!(
                &forged[..ENCRYPTION_BLOCK_SIZE],
                &forged[ENCRYPTION_BLOCK_SIZE..],
                "the accepted forgery is frame zero's plaintext twice over"
            );
        })
        .await;

        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK, Some("false"))], async {
            let error = DecryptReader::new(Cursor::new(encrypted.clone()), key, nonce)
                .read_to_end(&mut Vec::new())
                .await
                .expect_err("with the fallback off the replayed frame must not authenticate");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        })
        .await;
    }

    /// Turning the fallback off removes exactly the third layout: modern v1
    /// streams keep decrypting, pre-alpha.91 reused-nonce streams stop.
    #[tokio::test]
    async fn disabling_the_legacy_nonce_fallback_refuses_only_reused_part_nonces() {
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);
        let mut data = vec![0u8; ENCRYPTION_BLOCK_SIZE * 3 + 17];
        rand::rng().fill(&mut data[..]);

        let mut modern = Vec::new();
        EncryptReader::new(Cursor::new(data.clone()), key, nonce)
            .read_to_end(&mut modern)
            .await
            .expect("modern v1 stream should encrypt");
        let legacy = encrypt_with_legacy_nonce_reuse(&data, key, nonce);

        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK, Some("false"))], async {
            assert!(!legacy_nonce_fallback_enabled(), "the switch must be observed");

            let mut decrypted = Vec::new();
            DecryptReader::new(Cursor::new(modern), key, nonce)
                .read_to_end(&mut decrypted)
                .await
                .expect("modern v1 streams must keep decrypting with the fallback off");
            assert_eq!(decrypted, data);

            let error = DecryptReader::new(Cursor::new(legacy), key, nonce)
                .read_to_end(&mut Vec::new())
                .await
                .expect_err("the third layout must be gone when the fallback is off");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        })
        .await;
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

    // ======================= segment-nonce (0x03) frame format =======================

    async fn decrypt_multipart(
        combined: Vec<u8>,
        key: [u8; 32],
        base_nonce: [u8; 12],
        parts: Vec<usize>,
    ) -> std::io::Result<Vec<u8>> {
        let mut decrypt_reader = DecryptReader::new_multipart(Cursor::new(combined), key, base_nonce, parts);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await?;
        Ok(decrypted)
    }

    async fn encrypt_multipart_part(data: &[u8], key: [u8; 32], base_nonce: [u8; 12], part_number: usize, v2: bool) -> Vec<u8> {
        let mut out = Vec::new();
        if v2 {
            let mut reader = EncryptReader::new_multipart_v2(BufReader::new(data), key, base_nonce, part_number);
            reader.read_to_end(&mut out).await.expect("v2 part encryption succeeds");
        } else {
            let mut reader = EncryptReader::new_multipart(BufReader::new(data), key, base_nonce, part_number);
            reader.read_to_end(&mut out).await.expect("v1 part encryption succeeds");
        }
        out
    }

    async fn encrypt_multipart_part_explicit_nonce(
        data: &[u8],
        key: [u8; 32],
        base_nonce: [u8; 12],
        part_number: usize,
        v2: bool,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        if v2 {
            let mut reader =
                EncryptReader::new_multipart_v2_with_segment_nonce(BufReader::new(data), key, base_nonce, part_number);
            reader
                .read_to_end(&mut out)
                .await
                .expect("v2 segment-nonce part encryption succeeds");
        } else {
            let mut reader = EncryptReader::new_multipart_with_segment_nonce(BufReader::new(data), key, base_nonce, part_number);
            reader
                .read_to_end(&mut out)
                .await
                .expect("v1 segment-nonce part encryption succeeds");
        }
        out
    }

    async fn read_in_tiny_chunks<R: AsyncRead + Unpin>(mut reader: R, chunk_size: usize) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; chunk_size];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn multipart_segment_nonce_write_switch_defaults_off_and_honors_env() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);
        let part_data: Vec<u8> = (0..ENCRYPTION_BLOCK_SIZE + 55).map(|i| (i % 251) as u8).collect();

        // Default (unset): off for rolling-upgrade compatibility with older nodes.
        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE, None::<&str>)], async {
            assert!(
                !multipart_segment_nonce_enabled(),
                "write switch must default off for mixed-version rolling upgrades"
            );
            let enc_v1 = encrypt_multipart_part(&part_data, key, base_nonce, 1, false).await;
            assert_eq!(enc_v1[0], FRAME_TYPE_V1, "default v1 write must not emit 0x03");
            assert_eq!(
                decrypt_multipart(enc_v1, key, base_nonce, vec![1])
                    .await
                    .expect("default v1 decrypts"),
                part_data
            );

            let enc_v2 = encrypt_multipart_part(&part_data, key, base_nonce, 1, true).await;
            assert_eq!(enc_v2[0], FRAME_TYPE_V2, "default v2 write must not emit 0x03");
            assert_eq!(
                decrypt_multipart(enc_v2, key, base_nonce, vec![1])
                    .await
                    .expect("default v2 decrypts"),
                part_data
            );
        })
        .await;

        // Enabled: writes open with authenticated 37-byte 0x03 segment-nonce frames.
        let (enc_v1_03, enc_v2_03) =
            temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE, Some("true"))], async {
                assert!(multipart_segment_nonce_enabled(), "write switch must observe true");
                let enc_v1 = encrypt_multipart_part(&part_data, key, base_nonce, 1, false).await;
                assert_eq!(enc_v1[0], FRAME_TYPE_SEGMENT_NONCE);
                assert_eq!(enc_v1[1] as usize, SEGMENT_NONCE_FRAME_LEN);
                assert_eq!(enc_v1[SEGMENT_NONCE_FRAME_TOTAL_LEN], FRAME_TYPE_V1);

                let enc_v2 = encrypt_multipart_part(&part_data, key, base_nonce, 1, true).await;
                assert_eq!(enc_v2[0], FRAME_TYPE_SEGMENT_NONCE);
                assert_eq!(enc_v2[1] as usize, SEGMENT_NONCE_FRAME_LEN);
                assert_eq!(enc_v2[SEGMENT_NONCE_FRAME_TOTAL_LEN], FRAME_TYPE_V2);
                (enc_v1, enc_v2)
            })
            .await;

        // Read support is unconditional: even with the write switch explicitly false,
        // both v1 and v2 segments prefixed with 0x03 decrypt seamlessly.
        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE, Some("false"))], async {
            assert!(!multipart_segment_nonce_enabled(), "write switch must observe false");
            assert_eq!(
                decrypt_multipart(enc_v1_03, key, base_nonce, vec![1])
                    .await
                    .expect("0x03 v1 decrypts with write switch off"),
                part_data
            );
            assert_eq!(
                decrypt_multipart(enc_v2_03, key, base_nonce, vec![1])
                    .await
                    .expect("0x03 v2 decrypts with write switch off"),
                part_data
            );
        })
        .await;
    }

    #[tokio::test]
    async fn multipart_part_reupload_never_reuses_nonce_v1_and_v2() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let mut first = vec![0u8; ENCRYPTION_BLOCK_SIZE * 2 + 123];
        let mut second = vec![0u8; first.len()];
        rand::rng().fill(&mut first[..]);
        rand::rng().fill(&mut second[..]);

        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE, Some("true"))], async {
            for v2 in [false, true] {
                let encrypted_first = encrypt_multipart_part(&first, key, base_nonce, 1, v2).await;
                let encrypted_second = encrypt_multipart_part(&second, key, base_nonce, 1, v2).await;

                assert_eq!(encrypted_first[0], FRAME_TYPE_SEGMENT_NONCE);
                assert_eq!(encrypted_second[0], FRAME_TYPE_SEGMENT_NONCE);

                let payloads_first = extract_encrypted_payloads(&encrypted_first);
                let payloads_second = extract_encrypted_payloads(&encrypted_second);
                assert_eq!(payloads_first.len(), payloads_second.len());
                // Frame 0 is the 0x03 segment-nonce frame: distinct random nonces per upload.
                assert_ne!(payloads_first[0], payloads_second[0]);

                // XOR of corresponding data frame ciphertexts must not reveal the XOR of
                // the two plaintexts (no shared AES-GCM keystream).
                let mut ciphertext_xor = Vec::new();
                for (a, b) in payloads_first.iter().zip(payloads_second.iter()).skip(1) {
                    let (_, prefix) = rustfs_utils::uvarint(a);
                    let prefix = prefix as usize;
                    ciphertext_xor.extend(
                        a[prefix..a.len() - GCM_TAG_LEN]
                            .iter()
                            .zip(&b[prefix..b.len() - GCM_TAG_LEN])
                            .map(|(x, y)| x ^ y),
                    );
                }
                assert_eq!(ciphertext_xor.len(), first.len());
                let plaintext_xor: Vec<u8> = first.iter().zip(&second).map(|(x, y)| x ^ y).collect();
                let matching = ciphertext_xor.iter().zip(&plaintext_xor).filter(|(x, y)| x == y).count();
                assert!(
                    matching < first.len() / 16,
                    "v2={v2}: {matching} of {} keystream bytes repeated",
                    first.len()
                );

                // Even re-uploading identical plaintext draws a fresh segment nonce and distinct ciphertext.
                let encrypted_same = encrypt_multipart_part(&first, key, base_nonce, 1, v2).await;
                assert_ne!(
                    &encrypted_first[..SEGMENT_NONCE_FRAME_TOTAL_LEN],
                    &encrypted_same[..SEGMENT_NONCE_FRAME_TOTAL_LEN]
                );
                assert_ne!(encrypted_first, encrypted_same);

                for (encrypted, plaintext) in [
                    (encrypted_first, &first),
                    (encrypted_second, &second),
                    (encrypted_same, &first),
                ] {
                    let decrypted = decrypt_multipart(encrypted, key, base_nonce, vec![1])
                        .await
                        .expect("each re-upload of the part decrypts cleanly");
                    assert_eq!(&decrypted, plaintext);
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn multipart_segment_nonce_round_trips_boundaries_chunked_and_noncontiguous_parts() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let block = ENCRYPTION_BLOCK_SIZE;
        let lengths = [0usize, 1, 127, 128, block - 1, block, block + 1, 2 * block + 19];

        for v2 in [false, true] {
            for &len in &lengths {
                let data: Vec<u8> = (0..len).map(|i| ((i * 13 + 5) % 251) as u8).collect();
                let encrypted = encrypt_multipart_part_explicit_nonce(&data, key, base_nonce, 4, v2).await;
                assert_eq!(encrypted[0], FRAME_TYPE_SEGMENT_NONCE);

                // Round-trip through normal, 3-byte chunked, and pending-chunked readers.
                let decrypted = decrypt_multipart(encrypted.clone(), key, base_nonce, vec![4])
                    .await
                    .expect("boundary length decrypts");
                assert_eq!(decrypted, data, "v2={v2}, len={len}");

                let mut chunked_decrypt =
                    DecryptReader::new_multipart(ChunkedCursor::new(encrypted.clone(), 3), key, base_nonce, vec![4]);
                let mut chunked_out = Vec::new();
                chunked_decrypt
                    .read_to_end(&mut chunked_out)
                    .await
                    .expect("chunked decrypt succeeds");
                assert_eq!(chunked_out, data, "chunked v2={v2}, len={len}");

                let mut pending_decrypt =
                    DecryptReader::new_multipart(PendingChunkedCursor::new(encrypted, 3), key, base_nonce, vec![4]);
                let mut pending_out = Vec::new();
                pending_decrypt
                    .read_to_end(&mut pending_out)
                    .await
                    .expect("pending chunked decrypt succeeds");
                assert_eq!(pending_out, data, "pending v2={v2}, len={len}");
            }

            // Tiny output buffer on EncryptReader (3 bytes at a time, splitting the 37-byte 0x03 frame).
            let data: Vec<u8> = (0..block + 33).map(|i| (i % 239) as u8).collect();
            let encrypted_tiny = if v2 {
                read_in_tiny_chunks(
                    EncryptReader::new_multipart_v2_with_segment_nonce(Cursor::new(data.clone()), key, base_nonce, 2),
                    3,
                )
                .await
                .expect("tiny-chunk v2 encrypt")
            } else {
                read_in_tiny_chunks(
                    EncryptReader::new_multipart_with_segment_nonce(Cursor::new(data.clone()), key, base_nonce, 2),
                    3,
                )
                .await
                .expect("tiny-chunk v1 encrypt")
            };
            assert_eq!(encrypted_tiny[0], FRAME_TYPE_SEGMENT_NONCE);
            assert_eq!(
                decrypt_multipart(encrypted_tiny, key, base_nonce, vec![2])
                    .await
                    .expect("tiny-chunk encrypted stream decrypts"),
                data
            );

            // Non-contiguous part numbers [1, 3, 7], including a block-aligned middle part and empty tail.
            let p1: Vec<u8> = (0..block + 17).map(|i| (i % 251) as u8).collect();
            let p3: Vec<u8> = (0..2 * block).map(|i| ((i + 11) % 241) as u8).collect();
            let p7: Vec<u8> = Vec::new();
            let mut combined = Vec::new();
            combined.extend_from_slice(&encrypt_multipart_part_explicit_nonce(&p1, key, base_nonce, 1, v2).await);
            combined.extend_from_slice(&encrypt_multipart_part_explicit_nonce(&p3, key, base_nonce, 3, v2).await);
            combined.extend_from_slice(&encrypt_multipart_part_explicit_nonce(&p7, key, base_nonce, 7, v2).await);

            let decrypted = decrypt_multipart(combined, key, base_nonce, vec![1, 3, 7])
                .await
                .expect("non-contiguous parts decrypt");
            let mut expected = p1;
            expected.extend_from_slice(&p3);
            expected.extend_from_slice(&p7);
            assert_eq!(decrypted, expected, "non-contiguous parts v2={v2}");
        }
    }

    #[tokio::test]
    async fn multipart_segment_nonce_mixed_rolling_upgrade_segments() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let part_one: Vec<u8> = (0..ENCRYPTION_BLOCK_SIZE + 91).map(|i| (i % 251) as u8).collect();
        let part_two: Vec<u8> = (0..ENCRYPTION_BLOCK_SIZE + 43).map(|i| ((i + 9) % 239) as u8).collect();
        let mut expected = part_one.clone();
        expected.extend_from_slice(&part_two);

        // Encrypt legacy (no 0x03) parts directly.
        let mut legacy_p1_v1 = Vec::new();
        EncryptReader::new(Cursor::new(part_one.clone()), key, multipart_part_nonce(base_nonce, 1))
            .read_to_end(&mut legacy_p1_v1)
            .await
            .expect("legacy p1 v1");
        let mut legacy_p2_v1 = Vec::new();
        EncryptReader::new(Cursor::new(part_two.clone()), key, multipart_part_nonce(base_nonce, 2))
            .read_to_end(&mut legacy_p2_v1)
            .await
            .expect("legacy p2 v1");
        let mut legacy_p1_v2 = Vec::new();
        EncryptReader::new_v2(Cursor::new(part_one.clone()), key, multipart_part_nonce(base_nonce, 1))
            .read_to_end(&mut legacy_p1_v2)
            .await
            .expect("legacy p1 v2");
        let mut legacy_p2_v2 = Vec::new();
        EncryptReader::new_v2(Cursor::new(part_two.clone()), key, multipart_part_nonce(base_nonce, 2))
            .read_to_end(&mut legacy_p2_v2)
            .await
            .expect("legacy p2 v2");

        let seg_p1_v1 = encrypt_multipart_part_explicit_nonce(&part_one, key, base_nonce, 1, false).await;
        let seg_p2_v1 = encrypt_multipart_part_explicit_nonce(&part_two, key, base_nonce, 2, false).await;
        let seg_p1_v2 = encrypt_multipart_part_explicit_nonce(&part_one, key, base_nonce, 1, true).await;
        let seg_p2_v2 = encrypt_multipart_part_explicit_nonce(&part_two, key, base_nonce, 2, true).await;

        let mixes = [
            ("0x03-v1 + legacy-v1", &seg_p1_v1, &legacy_p2_v1),
            ("legacy-v1 + 0x03-v1", &legacy_p1_v1, &seg_p2_v1),
            ("0x03-v2 + legacy-v2", &seg_p1_v2, &legacy_p2_v2),
            ("legacy-v2 + 0x03-v2", &legacy_p1_v2, &seg_p2_v2),
            ("0x03-v1 + 0x03-v2", &seg_p1_v1, &seg_p2_v2),
            ("0x03-v2 + 0x03-v1", &seg_p1_v2, &seg_p2_v1),
        ];
        for (label, first_part, second_part) in mixes {
            let mut combined = first_part.clone();
            combined.extend_from_slice(second_part);
            let decrypted = decrypt_multipart(combined, key, base_nonce, vec![1, 2])
                .await
                .unwrap_or_else(|e| panic!("{label}: mixed rolling-upgrade stream must decrypt: {e}"));
            assert_eq!(decrypted, expected, "{label}");
        }
    }

    #[tokio::test]
    async fn segment_nonce_frame_security_invariants() {
        let mut key = [0u8; 32];
        let mut base_nonce_a = [0u8; 12];
        let mut base_nonce_b = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce_a);
        rand::rng().fill_bytes(&mut base_nonce_b);
        if base_nonce_a == base_nonce_b {
            base_nonce_b[0] ^= 0xFF;
        }

        let data = vec![0x5A; ENCRYPTION_BLOCK_SIZE * 2 + 5];
        let encrypted_v1 = encrypt_multipart_part_explicit_nonce(&data, key, base_nonce_a, 1, false).await;
        let encrypted_v2 = encrypt_multipart_part_explicit_nonce(&data, key, base_nonce_a, 1, true).await;

        // 1. Flipping header CRC, uvarint prefix, nonce byte, or tag byte fails authentication.
        for offset in [4, 5, 8, 9, 8 + 1 + SEGMENT_NONCE_LEN, SEGMENT_NONCE_FRAME_TOTAL_LEN - 1] {
            let mut tampered = encrypted_v1.clone();
            tampered[offset] ^= 0x01;
            let err = decrypt_multipart(tampered, key, base_nonce_a, vec![1])
                .await
                .expect_err("tampered segment nonce frame must fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "offset={offset}");
        }

        // 2. Serving part 1 in part 2's position fails authentication.
        let err = decrypt_multipart(encrypted_v1.clone(), key, base_nonce_a, vec![2])
            .await
            .expect_err("segment moved to another part number must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("segment nonce frame failed authentication"), "{err}");

        // 3. Cross-session / cross-object splicing under the same key fails authentication.
        let err = decrypt_multipart(encrypted_v1.clone(), key, base_nonce_b, vec![1])
            .await
            .expect_err("segment spliced into another upload session must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("segment nonce frame failed authentication"), "{err}");

        // 4. Cross-mode splicing (multipart segment served to single-part reader) fails authentication.
        let err = DecryptReader::new(Cursor::new(encrypted_v1.clone()), key, base_nonce_a)
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("multipart segment served to single-part reader must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // 5. Double 0x03 frame at segment start is rejected.
        let nonce_frame = encrypted_v1[..SEGMENT_NONCE_FRAME_TOTAL_LEN].to_vec();
        let mut doubled = nonce_frame.clone();
        doubled.extend_from_slice(&encrypted_v1);
        let err = decrypt_multipart(doubled, key, base_nonce_a, vec![1])
            .await
            .expect_err("second segment nonce frame at start must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("segment nonce frame after the segment began"), "{err}");

        // 6. Announcing a 0x03 frame after the first data frame is rejected.
        let first_data_frame_len = {
            let header = &encrypted_v1[SEGMENT_NONCE_FRAME_TOTAL_LEN..SEGMENT_NONCE_FRAME_TOTAL_LEN + 8];
            8 + ((header[1] as usize) | ((header[2] as usize) << 8) | ((header[3] as usize) << 16)) - 4
        };
        let mut reannounced = encrypted_v1[..SEGMENT_NONCE_FRAME_TOTAL_LEN + first_data_frame_len].to_vec();
        reannounced.extend_from_slice(&nonce_frame);
        reannounced.extend_from_slice(&encrypted_v1[SEGMENT_NONCE_FRAME_TOTAL_LEN + first_data_frame_len..]);
        let err = decrypt_multipart(reannounced, key, base_nonce_a, vec![1])
            .await
            .expect_err("segment nonce frame after frame 0 must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("segment nonce frame after the segment began"), "{err}");

        // 7. Invalid declared header lengths (including 0, 4, 32, 34, and 0xFFFFFF) fail cleanly without panic.
        for bad_len in [
            0usize,
            4,
            SEGMENT_NONCE_FRAME_LEN - 1,
            SEGMENT_NONCE_FRAME_LEN + 1,
            0x00FF_FFFF,
        ] {
            let bad_header = vec![
                FRAME_TYPE_SEGMENT_NONCE,
                (bad_len & 0xFF) as u8,
                ((bad_len >> 8) & 0xFF) as u8,
                ((bad_len >> 16) & 0xFF) as u8,
                0,
                0,
                0,
                0,
            ];
            let err = decrypt_multipart(bad_header, key, base_nonce_a, vec![1])
                .await
                .expect_err("invalid segment nonce frame length must fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert!(err.to_string().contains("invalid segment nonce frame length"), "{err}");
        }

        // 8. Truncation immediately after 0x03 (no data frame and no 0xFF terminator) fails with UnexpectedEof.
        let err = decrypt_multipart(nonce_frame.clone(), key, base_nonce_a, vec![1])
            .await
            .expect_err("EOF immediately after 0x03 frame must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("truncated after segment nonce frame"), "{err}");

        // 9. Truncation mid-0x03 payload fails with UnexpectedEof.
        let err = decrypt_multipart(nonce_frame[..15].to_vec(), key, base_nonce_a, vec![1])
            .await
            .expect_err("truncated 0x03 payload must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        // 10. Stripping the 0x03 frame from either v1 or v2 fails authentication.
        for stripped in [
            encrypted_v1[SEGMENT_NONCE_FRAME_TOTAL_LEN..].to_vec(),
            encrypted_v2[SEGMENT_NONCE_FRAME_TOTAL_LEN..].to_vec(),
        ] {
            let err = decrypt_multipart(stripped, key, base_nonce_a, vec![1])
                .await
                .expect_err("stripping the 0x03 frame must fail authentication");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }

        // 11. A 0x03 v1 segment is locked to the current nonce layout from frame 0 even when
        // RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK=true: replaying frame 0 at index 1 in a
        // 2-frame segment fails authentication.
        let two_blocks = vec![0x42; ENCRYPTION_BLOCK_SIZE * 2];
        let enc_two = encrypt_multipart_part_explicit_nonce(&two_blocks, key, base_nonce_a, 1, false).await;
        let frame0_start = SEGMENT_NONCE_FRAME_TOTAL_LEN;
        let frame0_len = {
            let h = &enc_two[frame0_start..frame0_start + 8];
            8 + ((h[1] as usize) | ((h[2] as usize) << 8) | ((h[3] as usize) << 16)) - 4
        };
        let mut replayed = enc_two[..frame0_start + frame0_len].to_vec();
        replayed.extend_from_slice(&enc_two[frame0_start..frame0_start + frame0_len]);
        replayed.extend_from_slice(&[FRAME_TYPE_END, 0, 0, 0, 0, 0, 0, 0]);
        temp_env::async_with_vars([(ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK, Some("true"))], async {
            let err = decrypt_multipart(replayed, key, base_nonce_a, vec![1])
                .await
                .expect_err("0x03 segment must never accept legacy reused-part nonce fallback");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        })
        .await;
    }

    #[tokio::test]
    async fn v2_segment_nonce_fixed_frame_layout_and_ranged_get_seek() {
        let mut key = [0u8; 32];
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut base_nonce);

        let block = V2_FRAME_PLAINTEXT_LEN;
        assert_eq!(SEGMENT_NONCE_FRAME_TOTAL_LEN, 37);
        assert_eq!(V2_FULL_FRAME_LEN, 8218);
        assert_eq!(v2_frame_ciphertext_offset(0, false), Some(0));
        assert_eq!(v2_frame_ciphertext_offset(0, true), Some(37));
        assert_eq!(v2_frame_ciphertext_offset(2, true), Some(37 + 2 * 8218));
        assert_eq!(v2_frame_ciphertext_offset(usize::MAX, true), None);

        // 3-part v2 object with 0x03 segment-nonce prefixes:
        // - Part 1: 3 full frames + 500-byte short final frame
        // - Part 2: 2 full frames (exact block multiple -> 25-byte empty final frame)
        // - Part 3: 1 full frame + 123-byte short final frame
        let part_plaintexts: Vec<Vec<u8>> = vec![
            (0..3 * block + 500).map(|i| ((i * 17 + 1) % 251) as u8).collect(),
            (0..2 * block).map(|i| ((i * 23 + 2) % 247) as u8).collect(),
            (0..block + 123).map(|i| ((i * 29 + 3) % 241) as u8).collect(),
        ];

        let mut part_ciphertexts = Vec::with_capacity(part_plaintexts.len());
        let mut combined_plaintext = Vec::new();
        let mut combined_ciphertext = Vec::new();
        let mut part_plain_starts = Vec::new();
        let mut part_cipher_starts = Vec::new();

        for (idx, pt) in part_plaintexts.iter().enumerate() {
            let part_number = idx + 1;
            part_plain_starts.push(combined_plaintext.len());
            part_cipher_starts.push(combined_ciphertext.len());

            let ct = encrypt_multipart_part_explicit_nonce(pt, key, base_nonce, part_number, true).await;

            // Verify exact fixed-frame offsets after the 37-byte 0x03 prefix.
            assert_eq!(ct[0], FRAME_TYPE_SEGMENT_NONCE);
            let full_frames = pt.len() / block;
            let tail_len = pt.len() % block;
            let final_frame_len = 8 + put_uvarint_len(tail_len as u64) + tail_len + GCM_TAG_LEN;
            let expected_len = SEGMENT_NONCE_FRAME_TOTAL_LEN + full_frames * V2_FULL_FRAME_LEN + final_frame_len + 8;
            assert_eq!(ct.len(), expected_len, "part {part_number} exact ciphertext size");
            for k in 0..full_frames {
                let off = v2_frame_ciphertext_offset(k, true).expect("frame offset fits");
                assert_eq!(ct[off], FRAME_TYPE_V2, "part {part_number} frame {k} type");
            }
            let final_off = v2_frame_ciphertext_offset(full_frames, true).expect("final frame offset fits");
            assert_eq!(ct[final_off], FRAME_TYPE_V2_FINAL, "part {part_number} final frame type");
            assert_eq!(ct[ct.len() - 8], FRAME_TYPE_END, "part {part_number} end marker");

            combined_plaintext.extend_from_slice(pt);
            combined_ciphertext.extend_from_slice(&ct);
            part_ciphertexts.push(ct);
        }

        // Empty v2 part with 0x03 prefix: 37 (0x03) + 25 (empty 0x02) + 8 (0xFF) = 70 bytes.
        let empty_v2 = encrypt_multipart_part_explicit_nonce(&[], key, base_nonce, 9, true).await;
        assert_eq!(empty_v2.len(), SEGMENT_NONCE_FRAME_TOTAL_LEN + (8 + 1 + GCM_TAG_LEN) + 8);
        assert_eq!(
            decrypt_multipart(empty_v2, key, base_nonce, vec![9])
                .await
                .expect("empty v2 0x03 part decrypts"),
            Vec::<u8>::new()
        );

        let total_plain = combined_plaintext.len();
        let p2_start = part_plain_starts[1];
        let p3_start = part_plain_starts[2];

        // Inclusive plaintext ranges covering offsets before, at, inside, and across every
        // frame boundary and every part boundary.
        let ranges: Vec<(usize, usize)> = vec![
            // Part 1 frame 0 head, interior, and exact end.
            (0, 0),
            (0, 99),
            (17, block - 1),
            // Crossing Part 1 frame 0 -> frame 1.
            (block - 1, block),
            (block - 13, block + 29),
            // Exact Part 1 frame 1 start, interior, and crossing into frame 2.
            (block, block),
            (block, block + 63),
            (block + 5, 2 * block + 100),
            // Crossing Part 1 frame 2 into short final frame 3 and ending at Part 1's last byte.
            (2 * block + 8000, p2_start - 1),
            // Entirely inside Part 1's short final frame 3.
            (3 * block, p2_start - 1),
            (3 * block + 100, p2_start - 1),
            (p2_start - 1, p2_start - 1),
            // Crossing Part 1 -> Part 2 (0x03 prefix boundary).
            (p2_start - 1, p2_start),
            (p2_start - 200, p2_start + 300),
            // Starting mid-frame in Part 1 frame 2, spanning all of block-aligned Part 2 into Part 3.
            (2 * block + 4096, p3_start + block + 50),
            // Inside block-aligned Part 2: frame 0, frame 0->1 boundary, exact frame 1, and last byte
            // (which sits right before the authenticated empty final frame).
            (p2_start, p2_start + 100),
            (p2_start + block - 1, p2_start + block),
            (p2_start + block, p3_start - 1),
            (p3_start - 1, p3_start - 1),
            // Crossing Part 2 -> Part 3.
            (p3_start - 1, p3_start),
            (p3_start - 50, total_plain - 1),
            // Inside Part 3 frame 0, frame 0->final boundary, and suffix tail ranges.
            (p3_start, p3_start + block + 10),
            (p3_start + block - 1, p3_start + block),
            (p3_start + block, total_plain - 1),
            (total_plain - 123, total_plain - 1),
            (total_plain - 1, total_plain - 1),
            // Full object range.
            (0, total_plain - 1),
        ];

        for &(start, end) in &ranges {
            let expected = &combined_plaintext[start..=end];
            let req_len = end - start + 1;

            // Locate start and end parts.
            let start_part_idx = (0..part_plaintexts.len())
                .rfind(|&i| part_plain_starts[i] <= start)
                .expect("start part exists");
            let end_part_idx = (0..part_plaintexts.len())
                .rfind(|&i| part_plain_starts[i] <= end)
                .expect("end part exists");
            let covering_parts: Vec<usize> = (start_part_idx + 1..=end_part_idx + 1).collect();

            // Strategy 1: part-boundary seek (reads from byte 0 of covering start part,
            // landing directly on its 0x03 segment-nonce frame).
            let part_seek_start = part_cipher_starts[start_part_idx];
            let part_seek_end = part_cipher_starts[end_part_idx] + part_ciphertexts[end_part_idx].len();
            let skip_in_part = start - part_plain_starts[start_part_idx];
            let decrypt_part = DecryptReader::new_multipart(
                Cursor::new(combined_ciphertext[part_seek_start..part_seek_end].to_vec()),
                key,
                base_nonce,
                covering_parts.clone(),
            );
            let mut buf = Vec::new();
            let end_hits_part_end = end + 1 == part_plain_starts[end_part_idx] + part_plaintexts[end_part_idx].len();
            if end_hits_part_end {
                // Drain to true EOF so the final authenticated v2 frame (including an
                // empty final frame on a block-aligned part) and END marker are verified.
                let mut reader = decrypt_part;
                reader
                    .read_to_end(&mut buf)
                    .await
                    .unwrap_or_else(|e| panic!("part-seek EOF range {start}..={end} failed: {e}"));
                assert_eq!(buf.len(), skip_in_part + req_len, "part-seek EOF length for {start}..={end}");
            } else {
                let mut limited = crate::LimitReader::new(decrypt_part, skip_in_part + req_len);
                limited
                    .read_to_end(&mut buf)
                    .await
                    .unwrap_or_else(|e| panic!("part-seek range {start}..={end} failed: {e}"));
            }
            assert_eq!(&buf[skip_in_part..], expected, "part-seek mismatch for range {start}..={end}");

            // Strategy 2: closed-form v2 fixed-frame sub-part seek using the 37-byte 0x03 prefix.
            let start_part_num = start_part_idx + 1;
            let start_part_ct = &part_ciphertexts[start_part_idx];
            let first_seg_base = authenticate_segment_nonce_frame(
                &start_part_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN],
                key,
                base_nonce,
                Some(start_part_num),
            )
            .expect("opening part 0x03 frame authenticates");

            let start_frame = skip_in_part / block;
            let skip_in_frame = skip_in_part % block;
            let frame_seek_rel = v2_frame_ciphertext_offset(start_frame, true).expect("start frame offset");
            let window_start = part_cipher_starts[start_part_idx] + frame_seek_rel;

            let end_in_end_part = end - part_plain_starts[end_part_idx];
            let end_frame = end_in_end_part / block;
            let end_part_full_frames = part_plaintexts[end_part_idx].len() / block;
            let end_rel = if end_frame + 1 < end_part_full_frames {
                v2_frame_ciphertext_offset(end_frame + 1, true).expect("end frame offset")
            } else {
                part_ciphertexts[end_part_idx].len()
            };
            let window_end = part_cipher_starts[end_part_idx] + end_rel;

            let decrypt_frame = DecryptReader::new_multipart_at_block(
                Cursor::new(combined_ciphertext[window_start..window_end].to_vec()),
                key,
                base_nonce,
                first_seg_base,
                covering_parts,
                start_frame,
            );
            let mut buf = Vec::new();
            if end_hits_part_end {
                let mut reader = decrypt_frame;
                reader
                    .read_to_end(&mut buf)
                    .await
                    .unwrap_or_else(|e| panic!("frame-seek EOF range {start}..={end} failed: {e}"));
                assert_eq!(buf.len(), skip_in_frame + req_len, "frame-seek EOF length for {start}..={end}");
            } else {
                let mut limited = crate::LimitReader::new(decrypt_frame, skip_in_frame + req_len);
                limited
                    .read_to_end(&mut buf)
                    .await
                    .unwrap_or_else(|e| panic!("frame-seek range {start}..={end} failed: {e}"));
            }
            assert_eq!(&buf[skip_in_frame..], expected, "frame-seek mismatch for range {start}..={end}");
        }

        // Negative seek checks:
        let part1_ct = &part_ciphertexts[0];
        let seg1_base = authenticate_segment_nonce_frame(&part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN], key, base_nonce, Some(1))
            .expect("part 1 0x03 frame authenticates");

        // 1. Seeking without accounting for the 37-byte 0x03 prefix fails closed.
        let mispositioned = part1_ct[V2_FULL_FRAME_LEN..].to_vec();
        DecryptReader::new_multipart_at_block(Cursor::new(mispositioned), key, base_nonce, seg1_base, vec![1], 1)
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("seeking without 37-byte 0x03 prefix offset must fail");

        // 2. Correct offset with wrong starting_block_index fails authentication.
        let frame1_off = v2_frame_ciphertext_offset(1, true).expect("frame 1 offset");
        DecryptReader::new_multipart_at_block(
            Cursor::new(part1_ct[frame1_off..].to_vec()),
            key,
            base_nonce,
            seg1_base,
            vec![1],
            2,
        )
        .read_to_end(&mut Vec::new())
        .await
        .expect_err("wrong starting_block_index must fail authentication");

        // 3. Passing the unstripped 0x03 frame into new_multipart_at_block (even at block 0) is rejected.
        let err = DecryptReader::new_multipart_at_block(Cursor::new(part1_ct.clone()), key, base_nonce, seg1_base, vec![1], 0)
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("new_multipart_at_block must reject a 0x03 frame in the opening segment");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("segment nonce frame after the segment began"), "{err}");

        // 4. new_at_block with starting_block_index > 0 rejects a 0x03 frame.
        let err = DecryptReader::new_at_block(Cursor::new(part1_ct.clone()), key, seg1_base, 1)
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("new_at_block at non-zero block must reject a 0x03 frame");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // 5. authenticate_segment_nonce_frame rejects wrong length, wrong part, wrong session nonce, wrong key, and tampering.
        assert_eq!(
            authenticate_segment_nonce_frame(&part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN - 1], key, base_nonce, Some(1))
                .expect_err("short slice must fail")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            authenticate_segment_nonce_frame(&part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN], key, base_nonce, Some(2))
                .expect_err("wrong part number must fail")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        let mut other_nonce = base_nonce;
        other_nonce[0] ^= 0x01;
        assert_eq!(
            authenticate_segment_nonce_frame(&part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN], key, other_nonce, Some(1))
                .expect_err("wrong session base_nonce must fail")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        let mut other_key = key;
        other_key[0] ^= 0x01;
        assert_eq!(
            authenticate_segment_nonce_frame(&part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN], other_key, base_nonce, Some(1))
                .expect_err("wrong key must fail")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        let mut tampered_prefix = part1_ct[..SEGMENT_NONCE_FRAME_TOTAL_LEN].to_vec();
        tampered_prefix[10] ^= 0x80;
        assert_eq!(
            authenticate_segment_nonce_frame(&tampered_prefix, key, base_nonce, Some(1))
                .expect_err("tampered prefix must fail")
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
