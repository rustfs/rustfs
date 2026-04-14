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
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use pin_project_lite::pin_project;
use rustfs_utils::{put_uvarint, put_uvarint_len};
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;

const ENCRYPTION_BLOCK_SIZE: usize = 8 * 1024;

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
        }
    }
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
                    // 4-7: CRC32 of ciphertext (little endian u32)
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
        }
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12]) -> Self {
        let first_part = 1;
        let initial_nonce = derive_part_nonce(&base_nonce, first_part);

        debug!("decrypt_reader: initialized multipart mode");

        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("key"),
            base_nonce,
            current_nonce_base: initial_nonce,
            multipart_mode: true,
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

            // Read header (8 bytes)
            while !*this.header_done && *this.header_read < 8 {
                let mut temp = [0u8; 8];
                let mut temp_buf = ReadBuf::new(&mut temp[0..8 - *this.header_read]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            if *this.header_read == 0 {
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
            let crc = (this.header_buf[4] as u32)
                | ((this.header_buf[5] as u32) << 8)
                | ((this.header_buf[6] as u32) << 16)
                | ((this.header_buf[7] as u32) << 24);

            *this.header_read = 0;
            *this.header_done = false;

            if typ == 0xFF {
                if *this.multipart_mode {
                    debug!(
                        next_part = *this.current_part + 1,
                        "decrypt_reader: reached segment terminator, advancing to next part"
                    );
                    *this.current_part += 1;
                    *this.current_nonce_base = derive_part_nonce(this.base_nonce, *this.current_part);
                    *this.block_index = 0;
                    *this.ciphertext_read = 0;
                    *this.ciphertext_len = 0;
                    continue;
                }

                *this.finished = true;
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                continue;
            }

            tracing::debug!(typ = typ, len = len, "decrypt block header");

            if len == 0 {
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
            let (plaintext_len, uvarint_len) = rustfs_utils::uvarint(&ciphertext_buf[0..16]);
            let ciphertext = &ciphertext_buf[uvarint_len as usize..];
            let block_nonce = derive_block_nonce(this.current_nonce_base, *this.block_index);
            let nonce = Nonce::try_from(block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
            let legacy_part_nonce = if *this.multipart_mode {
                derive_legacy_part_nonce(this.base_nonce, *this.current_part)
            } else {
                *this.base_nonce
            };
            let legacy_block_nonce = derive_block_nonce(&legacy_part_nonce, *this.block_index);
            let plaintext = match this.cipher.decrypt(&nonce, ciphertext) {
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
            };

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

            let actual_crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(&plaintext);
                hasher.finalize() as u32
            };
            if actual_crc != crc {
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(Error::other("CRC32 mismatch")));
            }

            *this.buffer = plaintext;
            *this.buffer_pos = 0;
            *this.block_index += 1;
            *this.ciphertext_read = 0;
            *this.ciphertext_len = 0;

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();
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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        // Decrypt using DecryptReader
        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        // Now test DecryptReader

        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let reader = std::io::Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let reader = ChunkedCursor::new(encrypted, 3);
        let mut decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let reader = ChunkedCursor::new(encrypted, 8192);
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let mut stream = ReaderStream::with_capacity(Box::new(decrypt_reader), 262_144);

        let mut decrypted = Vec::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap();
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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let reader = ChunkedCursor::new(encrypted, 8192);
        let decrypt_reader = DecryptReader::new(reader, key, nonce);
        let limit_reader = HardLimitReader::new(decrypt_reader, size as i64);
        let mut stream = ReaderStream::with_capacity(Box::new(limit_reader), 262_144);

        let mut decrypted = Vec::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap();
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
            encrypt_reader.read_to_end(&mut encrypted).await.unwrap();
            encrypted
        }

        let encrypted_one = encrypt_part(&part_one, key, base_nonce, 1).await;
        let encrypted_two = encrypt_part(&part_two, key, base_nonce, 2).await;

        let mut combined = Vec::with_capacity(encrypted_one.len() + encrypted_two.len());
        combined.extend_from_slice(&encrypted_one);
        combined.extend_from_slice(&encrypted_two);

        let reader = BufReader::new(Cursor::new(combined));
        let mut decrypt_reader = DecryptReader::new_multipart(reader, key, base_nonce);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

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
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

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
        let mut decrypt_reader = DecryptReader::new_multipart(reader, key, base_nonce);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

        let mut expected = Vec::with_capacity(part_one.len() + part_two.len());
        expected.extend_from_slice(&part_one);
        expected.extend_from_slice(&part_two);

        assert_eq!(decrypted, expected);
    }
}
