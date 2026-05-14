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

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use hmac::{Hmac, Mac};
use pin_project_lite::pin_project;
use rand::RngExt;
use rustfs_rio::{EtagResolvable, HashReaderDetector, HashReaderMut, Index, TryGetIndex, multipart_part_nonce};
use sha2::Sha256;
use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

const DARE_VERSION_20: u8 = 0x20;
const DARE_CIPHER_AES_256_GCM: u8 = 0x00;
const DARE_HEADER_SIZE: usize = 16;
const DARE_TAG_SIZE: usize = 16;
const DARE_PAYLOAD_SIZE: usize = 64 * 1024;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy)]
enum MultipartKeySource {
    LegacyNonce { base_nonce: [u8; 12] },
    ObjectKey { object_key: [u8; 32] },
}

pin_project! {
    pub struct EncryptReader<R> {
        #[pin]
        inner: R,
        cipher: Aes256Gcm,
        base_nonce: [u8; 12],
        sequence_number: u32,
        temp_buffer: Vec<u8>,
        read_buffer: Vec<u8>,
        output_buffer: Vec<u8>,
        output_pos: usize,
        finished: bool,
    }
}

impl<R> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new_with_object_key(inner: R, object_key: [u8; 32]) -> Self {
        Self::new(inner, object_key, random_stream_nonce())
    }

    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self::new_with_sequence(inner, key, nonce, 0)
    }

    pub fn new_with_sequence(inner: R, key: [u8; 32], nonce: [u8; 12], sequence_number: u32) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("valid AES-256-GCM key"),
            base_nonce: nonce,
            sequence_number,
            temp_buffer: Vec::with_capacity(DARE_PAYLOAD_SIZE + 1),
            read_buffer: vec![0u8; DARE_PAYLOAD_SIZE + 1],
            output_buffer: Vec::new(),
            output_pos: 0,
            finished: false,
        }
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        Self::new(inner, key, multipart_part_nonce(base_nonce, part_number))
    }

    pub fn new_multipart_with_object_key(inner: R, object_key: [u8; 32], part_number: u32) -> Self {
        Self::new(inner, derive_part_key(object_key, part_number), random_stream_nonce())
    }

    pub fn new_multipart_with_sequence(
        inner: R,
        key: [u8; 32],
        base_nonce: [u8; 12],
        part_number: usize,
        sequence_number: u32,
    ) -> Self {
        Self::new_with_sequence(inner, key, multipart_part_nonce(base_nonce, part_number), sequence_number)
    }
}

impl<R> AsyncRead for EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        if *this.output_pos < this.output_buffer.len() {
            let to_copy = min(buf.remaining(), this.output_buffer.len() - *this.output_pos);
            buf.put_slice(&this.output_buffer[*this.output_pos..*this.output_pos + to_copy]);
            *this.output_pos += to_copy;
            if *this.output_pos == this.output_buffer.len() {
                this.output_buffer.clear();
                *this.output_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if *this.finished {
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.temp_buffer.len() >= DARE_PAYLOAD_SIZE + 1 {
                let package = build_dare_package(
                    this.cipher,
                    *this.sequence_number,
                    *this.base_nonce,
                    &this.temp_buffer[..DARE_PAYLOAD_SIZE],
                    false,
                )?;
                let carry = this.temp_buffer.split_off(DARE_PAYLOAD_SIZE);
                *this.temp_buffer = carry;
                *this.sequence_number = this.sequence_number.wrapping_add(1);
                *this.output_buffer = package;
                break;
            }

            let remaining = DARE_PAYLOAD_SIZE + 1 - this.temp_buffer.len();
            let mut read_buf = ReadBuf::new(&mut this.read_buffer[..remaining]);
            match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                Poll::Pending => {
                    if this.temp_buffer.len() >= DARE_PAYLOAD_SIZE + 1 {
                        continue;
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        if this.temp_buffer.is_empty() {
                            *this.finished = true;
                            return Poll::Ready(Ok(()));
                        }

                        let package =
                            build_dare_package(this.cipher, *this.sequence_number, *this.base_nonce, this.temp_buffer, true)?;
                        this.temp_buffer.clear();
                        *this.sequence_number = this.sequence_number.wrapping_add(1);
                        *this.output_buffer = package;
                        *this.finished = true;
                        break;
                    }
                    this.temp_buffer.extend_from_slice(read_buf.filled());
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }

        let to_copy = min(buf.remaining(), this.output_buffer.len());
        buf.put_slice(&this.output_buffer[..to_copy]);
        *this.output_pos += to_copy;
        if *this.output_pos == this.output_buffer.len() {
            this.output_buffer.clear();
            *this.output_pos = 0;
        }

        Poll::Ready(Ok(()))
    }
}

impl<R> EtagResolvable for EncryptReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for EncryptReader<R>
where
    R: HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl<R> TryGetIndex for EncryptReader<R>
where
    R: TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

pin_project! {
    pub struct DecryptReader<R> {
        #[pin]
        inner: R,
        cipher: Aes256Gcm,
        expected_base_nonce: Option<[u8; 12]>,
        sequence_number: u32,
        multipart_parts: Vec<usize>,
        current_part_index: usize,
        multipart_mode: bool,
        multipart_key_source: Option<MultipartKeySource>,
        header_buf: [u8; DARE_HEADER_SIZE],
        header_read: usize,
        ciphertext_buf: Vec<u8>,
        ciphertext_len: usize,
        ciphertext_read: usize,
        ref_nonce: Option<[u8; 12]>,
        finalized: bool,
        finished: bool,
        output_buffer: Vec<u8>,
        output_pos: usize,
    }
}

impl<R> DecryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new_with_object_key(inner: R, object_key: [u8; 32]) -> Self {
        Self::new_with_object_key_and_sequence(inner, object_key, 0)
    }

    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self::new_with_sequence(inner, key, nonce, 0)
    }

    pub fn new_with_object_key_and_sequence(inner: R, object_key: [u8; 32], sequence_number: u32) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&object_key).expect("valid AES-256-GCM key"),
            expected_base_nonce: None,
            sequence_number,
            multipart_parts: Vec::new(),
            current_part_index: 0,
            multipart_mode: false,
            multipart_key_source: None,
            header_buf: [0u8; DARE_HEADER_SIZE],
            header_read: 0,
            ciphertext_buf: Vec::new(),
            ciphertext_len: 0,
            ciphertext_read: 0,
            ref_nonce: None,
            finalized: false,
            finished: false,
            output_buffer: Vec::new(),
            output_pos: 0,
        }
    }

    pub fn new_with_sequence(inner: R, key: [u8; 32], nonce: [u8; 12], sequence_number: u32) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("valid AES-256-GCM key"),
            expected_base_nonce: Some(nonce),
            sequence_number,
            multipart_parts: Vec::new(),
            current_part_index: 0,
            multipart_mode: false,
            multipart_key_source: None,
            header_buf: [0u8; DARE_HEADER_SIZE],
            header_read: 0,
            ciphertext_buf: Vec::new(),
            ciphertext_len: 0,
            ciphertext_read: 0,
            ref_nonce: None,
            finalized: false,
            finished: false,
            output_buffer: Vec::new(),
            output_pos: 0,
        }
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], multipart_parts: Vec<usize>) -> Self {
        Self::new_multipart_with_sequence(inner, key, base_nonce, multipart_parts, 0)
    }

    pub fn new_multipart_with_object_key(inner: R, object_key: [u8; 32], multipart_parts: Vec<usize>) -> Self {
        Self::new_multipart_with_object_key_and_sequence(inner, object_key, multipart_parts, 0)
    }

    pub fn new_multipart_with_sequence(
        inner: R,
        key: [u8; 32],
        base_nonce: [u8; 12],
        multipart_parts: Vec<usize>,
        sequence_number: u32,
    ) -> Self {
        let first_part = multipart_parts.first().copied().unwrap_or(1);
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("valid AES-256-GCM key"),
            expected_base_nonce: Some(multipart_part_nonce(base_nonce, first_part)),
            sequence_number,
            multipart_parts,
            current_part_index: 0,
            multipart_mode: true,
            multipart_key_source: Some(MultipartKeySource::LegacyNonce { base_nonce }),
            header_buf: [0u8; DARE_HEADER_SIZE],
            header_read: 0,
            ciphertext_buf: Vec::new(),
            ciphertext_len: 0,
            ciphertext_read: 0,
            ref_nonce: None,
            finalized: false,
            finished: false,
            output_buffer: Vec::new(),
            output_pos: 0,
        }
    }

    pub fn new_multipart_with_object_key_and_sequence(
        inner: R,
        object_key: [u8; 32],
        multipart_parts: Vec<usize>,
        sequence_number: u32,
    ) -> Self {
        let first_part = multipart_parts.first().copied().unwrap_or(1);
        let first_key = derive_part_key(object_key, first_part as u32);
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&first_key).expect("valid AES-256-GCM key"),
            expected_base_nonce: None,
            sequence_number,
            multipart_parts,
            current_part_index: 0,
            multipart_mode: true,
            multipart_key_source: Some(MultipartKeySource::ObjectKey { object_key }),
            header_buf: [0u8; DARE_HEADER_SIZE],
            header_read: 0,
            ciphertext_buf: Vec::new(),
            ciphertext_len: 0,
            ciphertext_read: 0,
            ref_nonce: None,
            finalized: false,
            finished: false,
            output_buffer: Vec::new(),
            output_pos: 0,
        }
    }
}

impl<R> AsyncRead for DecryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        if *this.output_pos < this.output_buffer.len() {
            let to_copy = min(buf.remaining(), this.output_buffer.len() - *this.output_pos);
            buf.put_slice(&this.output_buffer[*this.output_pos..*this.output_pos + to_copy]);
            *this.output_pos += to_copy;
            if *this.output_pos == this.output_buffer.len() {
                this.output_buffer.clear();
                *this.output_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        loop {
            if *this.finished {
                return Poll::Ready(Ok(()));
            }

            if *this.finalized {
                if !this.multipart_parts.is_empty() && *this.current_part_index + 1 < this.multipart_parts.len() {
                    let next_part = this.multipart_parts[*this.current_part_index + 1];
                    *this.current_part_index += 1;
                    match this.multipart_key_source.as_ref().copied() {
                        Some(MultipartKeySource::LegacyNonce { base_nonce }) => {
                            *this.expected_base_nonce = Some(multipart_part_nonce(base_nonce, next_part));
                        }
                        Some(MultipartKeySource::ObjectKey { object_key }) => {
                            let part_key = derive_part_key(object_key, next_part as u32);
                            *this.cipher = Aes256Gcm::new_from_slice(&part_key).expect("valid AES-256-GCM key");
                            *this.expected_base_nonce = None;
                        }
                        None => {}
                    }
                    *this.sequence_number = 0;
                    *this.ref_nonce = None;
                    *this.finalized = false;
                    continue;
                }

                *this.finished = true;
                return Poll::Ready(Ok(()));
            }

            while *this.header_read < DARE_HEADER_SIZE {
                let mut read_buf = ReadBuf::new(&mut this.header_buf[*this.header_read..]);
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            if *this.header_read == 0 {
                                *this.finished = true;
                                return Poll::Ready(Ok(()));
                            }
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading DARE header",
                            )));
                        }
                        *this.header_read += n;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                }
            }

            let header = this.header_buf;
            let payload_len = usize::from(u16::from_le_bytes([header[2], header[3]])) + 1;
            let package_len = payload_len + DARE_TAG_SIZE;
            if payload_len == 0 || payload_len > DARE_PAYLOAD_SIZE {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "invalid DARE payload size")));
            }
            if !is_final_header(*header) && payload_len != DARE_PAYLOAD_SIZE {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-final DARE package must carry a full 64KiB payload",
                )));
            }
            if this.ciphertext_buf.len() < package_len {
                this.ciphertext_buf.resize(package_len, 0);
            }
            *this.ciphertext_len = package_len;
            *this.ciphertext_read = 0;

            while *this.ciphertext_read < *this.ciphertext_len {
                let mut read_buf = ReadBuf::new(&mut this.ciphertext_buf[*this.ciphertext_read..*this.ciphertext_len]);
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading DARE ciphertext",
                            )));
                        }
                        *this.ciphertext_read += n;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                }
            }

            match open_dare_package(
                this.cipher,
                *this.sequence_number,
                *this.expected_base_nonce,
                *header,
                &this.ciphertext_buf[..*this.ciphertext_len],
                this.ref_nonce,
            ) {
                Ok((plaintext, ref_nonce, finalized)) => {
                    *this.ref_nonce = Some(ref_nonce);
                    *this.finalized = finalized;
                    *this.header_read = 0;
                    *this.ciphertext_len = 0;
                    *this.ciphertext_read = 0;
                    *this.sequence_number = this.sequence_number.wrapping_add(1);
                    *this.output_buffer = plaintext;
                    let to_copy = min(buf.remaining(), this.output_buffer.len());
                    buf.put_slice(&this.output_buffer[..to_copy]);
                    *this.output_pos += to_copy;
                    if *this.output_pos == this.output_buffer.len() {
                        this.output_buffer.clear();
                        *this.output_pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

impl<R> EtagResolvable for DecryptReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for DecryptReader<R>
where
    R: HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl<R> TryGetIndex for DecryptReader<R>
where
    R: TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

fn build_dare_package(
    cipher: &Aes256Gcm,
    sequence_number: u32,
    base_nonce: [u8; 12],
    plaintext: &[u8],
    final_package: bool,
) -> io::Result<Vec<u8>> {
    let mut header = [0u8; DARE_HEADER_SIZE];
    header[0] = DARE_VERSION_20;
    header[1] = DARE_CIPHER_AES_256_GCM;
    header[2..4].copy_from_slice(
        &u16::try_from(plaintext.len() - 1)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DARE payload too large for Version 2.0 framing"))?
            .to_le_bytes(),
    );
    header[4..16].copy_from_slice(&base_nonce);
    if final_package {
        header[4] |= 0x80;
    } else {
        header[4] &= 0x7F;
    }

    let mut package_nonce = header[4..16].try_into().expect("nonce slice");
    xor_sequence_into_nonce(&mut package_nonce, sequence_number);
    let nonce = Nonce::try_from(package_nonce.as_slice())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid DARE nonce length"))?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: plaintext,
                aad: &header[..4],
            },
        )
        .map_err(|err| io::Error::other(format!("failed to encrypt DARE package: {err}")))?;

    let mut package = Vec::with_capacity(DARE_HEADER_SIZE + ciphertext.len());
    package.extend_from_slice(&header);
    package.extend_from_slice(&ciphertext);
    Ok(package)
}

fn open_dare_package(
    cipher: &Aes256Gcm,
    sequence_number: u32,
    expected_base_nonce: Option<[u8; 12]>,
    header: [u8; DARE_HEADER_SIZE],
    ciphertext: &[u8],
    ref_nonce: &mut Option<[u8; 12]>,
) -> io::Result<(Vec<u8>, [u8; 12], bool)> {
    if header[0] != DARE_VERSION_20 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported DARE version"));
    }
    if header[1] != DARE_CIPHER_AES_256_GCM {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported DARE cipher suite"));
    }
    let header_nonce: [u8; 12] = header[4..16].try_into().expect("nonce slice");
    if let Some(expected_base_nonce) = expected_base_nonce {
        let masked_expected = apply_final_flag(expected_base_nonce, is_final_header(header));
        if header_nonce != masked_expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DARE package nonce does not match the configured stream nonce",
            ));
        }
    }

    let current_ref = ref_nonce.get_or_insert(header_nonce);
    let expected_ref = apply_final_flag(*current_ref, is_final_header(header));
    if header_nonce != expected_ref {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DARE package nonce does not match the stream reference nonce",
        ));
    }

    let mut package_nonce = header_nonce;
    xor_sequence_into_nonce(&mut package_nonce, sequence_number);
    let nonce = Nonce::try_from(package_nonce.as_slice())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid DARE nonce length"))?;
    let plaintext = cipher
        .decrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: ciphertext,
                aad: &header[..4],
            },
        )
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("DARE authentication failed: {err}")))?;

    Ok((plaintext, *current_ref, is_final_header(header)))
}

fn xor_sequence_into_nonce(nonce: &mut [u8; 12], sequence_number: u32) {
    let last = u32::from_le_bytes([nonce[8], nonce[9], nonce[10], nonce[11]]) ^ sequence_number;
    nonce[8..12].copy_from_slice(&last.to_le_bytes());
}

fn apply_final_flag(mut nonce: [u8; 12], final_package: bool) -> [u8; 12] {
    if final_package {
        nonce[0] |= 0x80;
    } else {
        nonce[0] &= 0x7F;
    }
    nonce
}

fn is_final_header(header: [u8; DARE_HEADER_SIZE]) -> bool {
    header[4] & 0x80 != 0
}

pub fn derive_part_key(object_key: [u8; 32], part_number: u32) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(&object_key).expect("HMAC-SHA256 accepts 32-byte object keys");
    mac.update(&part_number.to_le_bytes());

    let mut part_key = [0u8; 32];
    part_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    part_key
}

fn random_stream_nonce() -> [u8; 12] {
    let mut nonce = [0u8; 12];
    rand::rng().fill(&mut nonce);
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex::encode as hex_encode;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    const DARE_PACKAGE_SIZE: usize = DARE_HEADER_SIZE + DARE_PAYLOAD_SIZE + DARE_TAG_SIZE;

    #[tokio::test]
    async fn decrypt_reader_can_start_from_non_zero_sequence_number() {
        let plaintext = vec![0xAB; DARE_PAYLOAD_SIZE * 2 + 19];
        let key_bytes = [0x44u8; 32];
        let base_nonce = [0x66u8; 12];

        let mut encrypted = Vec::new();
        EncryptReader::new(Cursor::new(plaintext.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt plaintext");

        let tail = encrypted[DARE_PACKAGE_SIZE..].to_vec();
        let mut decrypted = Vec::new();
        DecryptReader::new_with_sequence(Cursor::new(tail), key_bytes, base_nonce, 1)
            .read_to_end(&mut decrypted)
            .await
            .expect("decrypt tail packages with non-zero sequence");

        assert_eq!(decrypted, plaintext[DARE_PAYLOAD_SIZE..]);
    }

    #[tokio::test]
    async fn singlepart_object_key_roundtrip_uses_header_nonce() {
        let object_key = [0x51u8; 32];
        let plaintext = vec![0xAC; DARE_PAYLOAD_SIZE + 17];

        let mut encrypted = Vec::new();
        EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt object-key singlepart stream");

        let mut decrypted = Vec::new();
        DecryptReader::new_with_object_key(Cursor::new(encrypted), object_key)
            .read_to_end(&mut decrypted)
            .await
            .expect("decrypt object-key singlepart stream");

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn derive_part_key_matches_minio_test_vectors() {
        assert_eq!(
            hex_encode(derive_part_key([0u8; 32], 0)),
            "aa7855e13839dd767cd5da7c1ff5036540c9264b7a803029315e55375287b4af"
        );
        assert_eq!(
            hex_encode(derive_part_key([0u8; 32], 1)),
            "a3e7181c6eed030fd52f79537c56c4d07da92e56d374ff1dd2043350785b37d8"
        );
        assert_eq!(
            hex_encode(derive_part_key([0u8; 32], 10_000)),
            "f86e65c396ed52d204ee44bd1a0bbd86eb8b01b7354e67a3b3ae0e34dd5bd115"
        );
    }

    #[tokio::test]
    async fn multipart_object_key_roundtrip_resets_sequence_per_part() {
        let object_key = [0x19u8; 32];
        let part_one_plaintext = vec![0xA1; DARE_PAYLOAD_SIZE + 31];
        let part_two_plaintext = vec![0xB2; DARE_PAYLOAD_SIZE * 2 + 7];

        let mut encrypted_one = Vec::new();
        EncryptReader::new_multipart_with_object_key(Cursor::new(part_one_plaintext.clone()), object_key, 1)
            .read_to_end(&mut encrypted_one)
            .await
            .expect("encrypt multipart part one with object key");

        let mut encrypted_two = Vec::new();
        EncryptReader::new_multipart_with_object_key(Cursor::new(part_two_plaintext.clone()), object_key, 2)
            .read_to_end(&mut encrypted_two)
            .await
            .expect("encrypt multipart part two with object key");

        let mut encrypted = encrypted_one.clone();
        encrypted.extend_from_slice(&encrypted_two);

        let mut decrypted = Vec::new();
        DecryptReader::new_multipart_with_object_key(Cursor::new(encrypted), object_key, vec![1, 2])
            .read_to_end(&mut decrypted)
            .await
            .expect("decrypt multipart object-key stream");

        let mut expected = part_one_plaintext;
        expected.extend_from_slice(&part_two_plaintext);

        assert_eq!(decrypted, expected);
        assert_ne!(encrypted_one, encrypted_two[..encrypted_one.len()]);
    }
}
