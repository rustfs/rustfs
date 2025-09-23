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

use crate::HashReaderDetector;
use crate::HashReaderMut;
use crate::compress_index::{Index, TryGetIndex};
use crate::{EtagResolvable, Reader};
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use pin_project_lite::pin_project;
use rustfs_utils::{put_uvarint, put_uvarint_len};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    /// A reader wrapper that encrypts data on the fly using AES-256-GCM.
    /// This is a demonstration. For production, use a secure and audited crypto library.
    #[derive(Debug)]
    pub struct EncryptReader<R> {
        #[pin]
        pub inner: R,
        key: [u8; 32],   // AES-256-GCM key
        nonce: [u8; 12], // 96-bit nonce for GCM
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
    }
}

impl<R> EncryptReader<R>
where
    R: Reader,
{
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            key,
            nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
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
        // Read a fixed block size from inner
        let block_size = 8 * 1024;
        let mut temp = vec![0u8; block_size];
        let mut temp_buf = ReadBuf::new(&mut temp);
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
                    let cipher = Aes256Gcm::new_from_slice(this.key).expect("key");
                    let nonce = Nonce::from_slice(this.nonce);
                    let plaintext = &temp_buf.filled()[..n];
                    let plaintext_len = plaintext.len();
                    let crc = crc32fast::hash(plaintext);
                    let ciphertext = cipher
                        .encrypt(nonce, plaintext)
                        .map_err(|e| std::io::Error::other(format!("encrypt error: {e}")))?;
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
                    println!(
                        "encrypt block header typ=0 len={} header={:?} plaintext_len={} ciphertext_len={}",
                        clen,
                        header,
                        plaintext_len,
                        ciphertext.len()
                    );
                    let mut out = Vec::with_capacity(8 + int_len + ciphertext.len());
                    out.extend_from_slice(&header);
                    let mut plaintext_len_buf = vec![0u8; int_len];
                    put_uvarint(&mut plaintext_len_buf, plaintext_len as u64);
                    out.extend_from_slice(&plaintext_len_buf);
                    out.extend_from_slice(&ciphertext);
                    *this.buffer = out;
                    *this.buffer_pos = 0;
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
    R: EtagResolvable + HashReaderDetector,
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
    /// A reader wrapper that decrypts data on the fly using AES-256-GCM.
    /// This is a demonstration. For production, use a secure and audited crypto library.
#[derive(Debug)]
    pub struct DecryptReader<R> {
        #[pin]
        pub inner: R,
        key: [u8; 32],   // AES-256-GCM key
        nonce: [u8; 12], // 96-bit nonce for GCM
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // For block framing
        header_buf: [u8; 8],
        header_read: usize,
        header_done: bool,
        ciphertext_buf: Option<Vec<u8>>,
        ciphertext_read: usize,
        ciphertext_len: usize,
    }
}

impl<R> DecryptReader<R>
where
    R: Reader,
{
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            key,
            nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            header_buf: [0u8; 8],
            header_read: 0,
            header_done: false,
            ciphertext_buf: None,
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
                            *this.finished = true;
                            return Poll::Ready(Ok(()));
                        }
                        this.header_buf[*this.header_read..*this.header_read + n].copy_from_slice(&temp_buf.filled()[..n]);
                        *this.header_read += n;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }

                if *this.header_read < 8 {
                    return Poll::Pending;
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
                *this.finished = true;
                continue;
            }

            tracing::debug!(typ = typ, len = len, "decrypt block header");

            if len == 0 {
                tracing::warn!("encountered zero-length encrypted block, treating as end of stream");
                *this.finished = true;
                this.ciphertext_buf.take();
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                continue;
            }

            let Some(payload_len) = len.checked_sub(4) else {
                tracing::error!("invalid encrypted block length: typ={} len={} header={:?}", typ, len, this.header_buf);
                return Poll::Ready(Err(std::io::Error::other("Invalid encrypted block length")));
            };

            if this.ciphertext_buf.is_none() {
                *this.ciphertext_buf = Some(vec![0u8; payload_len]);
                *this.ciphertext_len = payload_len;
                *this.ciphertext_read = 0;
            }

            let ciphertext_buf = this.ciphertext_buf.as_mut().unwrap();
            while *this.ciphertext_read < *this.ciphertext_len {
                let mut temp_buf = ReadBuf::new(&mut ciphertext_buf[*this.ciphertext_read..]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            break;
                        }
                        *this.ciphertext_read += n;
                    }
                    Poll::Ready(Err(e)) => {
                        this.ciphertext_buf.take();
                        *this.ciphertext_read = 0;
                        *this.ciphertext_len = 0;
                        return Poll::Ready(Err(e));
                    }
                }
            }

            if *this.ciphertext_read < *this.ciphertext_len {
                return Poll::Pending;
            }

            let (plaintext_len, uvarint_len) = rustfs_utils::uvarint(&ciphertext_buf[0..16]);
            let ciphertext = &ciphertext_buf[uvarint_len as usize..];

            let cipher = Aes256Gcm::new_from_slice(this.key).expect("key");
            let nonce = Nonce::from_slice(this.nonce);
            let plaintext = cipher
                .decrypt(nonce, ciphertext)
                .map_err(|e| std::io::Error::other(format!("decrypt error: {e}")))?;

            if plaintext.len() != plaintext_len as usize {
                this.ciphertext_buf.take();
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(std::io::Error::other("Plaintext length mismatch")));
            }

            let actual_crc = crc32fast::hash(&plaintext);
            if actual_crc != crc {
                this.ciphertext_buf.take();
                *this.ciphertext_read = 0;
                *this.ciphertext_len = 0;
                return Poll::Ready(Err(std::io::Error::other("CRC32 mismatch")));
            }

            *this.buffer = plaintext;
            *this.buffer_pos = 0;
            this.ciphertext_buf.take();
            *this.ciphertext_read = 0;
            *this.ciphertext_len = 0;

            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            return Poll::Ready(Ok(()));
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
    R: EtagResolvable + HashReaderDetector,
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::WarpReader;

    use super::*;
    use rand::RngCore;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_encrypt_decrypt_reader_aes256gcm() {
        let data = b"hello sse encrypt";
        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let reader = BufReader::new(&data[..]);
        let encrypt_reader = EncryptReader::new(WarpReader::new(reader), key, nonce);

        // Encrypt
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        // Decrypt using DecryptReader
        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(WarpReader::new(reader), key, nonce);
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
        let encrypt_reader = EncryptReader::new(WarpReader::new(reader), key, nonce);
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        // Now test DecryptReader

        let reader = Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(WarpReader::new(reader), key, nonce);
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
        let encrypt_reader = EncryptReader::new(WarpReader::new(reader), key, nonce);
        let mut encrypt_reader = encrypt_reader;
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let reader = std::io::Cursor::new(encrypted.clone());
        let decrypt_reader = DecryptReader::new(WarpReader::new(reader), key, nonce);
        let mut decrypt_reader = decrypt_reader;
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

        assert_eq!(&decrypted, &data);
    }
}
