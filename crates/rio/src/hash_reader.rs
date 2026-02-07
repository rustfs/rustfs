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

//! HashReader implementation with generic support
//!
//! This module provides a generic `HashReader<R>` that can wrap any type implementing
//! `AsyncRead + Unpin + Send + Sync + 'static + EtagResolvable`.
//!
//! ## Migration from the original Reader enum
//!
//! The original `HashReader::new` method that worked with the `Reader` enum
//! has been replaced with a generic approach. To preserve the original logic:
//!
//! ### Original logic (before generics):
//! ```ignore
//! // Original code would do:
//! // 1. Check if inner is already a HashReader
//! // 2. If size > 0, wrap with HardLimitReader  
//! // 3. If !diskable_md5, wrap with EtagReader
//! // 4. Create HashReader with the wrapped reader
//!
//! let reader = HashReader::new(inner, size, actual_size, etag, diskable_md5)?;
//! ```
//!
//! ### New generic approach:
//! ```rust
//! use rustfs_rio::{HashReader, HardLimitReader, EtagReader};
//! use tokio::io::BufReader;
//! use std::io::Cursor;
//! use rustfs_rio::WarpReader;
//!
//! # tokio_test::block_on(async {
//! let data = b"hello world";
//! let reader = BufReader::new(Cursor::new(&data[..]));
//! let reader = Box::new(WarpReader::new(reader));
//! let size = data.len() as i64;
//! let actual_size = size;
//! let etag = None;
//! let diskable_md5 = false;
//!
//! // Method 1: Simple creation (recommended for most cases)
//! let hash_reader = HashReader::new(reader, size, actual_size, etag.clone(), None, diskable_md5).unwrap();
//!
//! // Method 2: With manual wrapping to recreate original logic
//! let reader2 = BufReader::new(Cursor::new(&data[..]));
//! let reader2 = Box::new(WarpReader::new(reader2));
//! let wrapped_reader: Box<dyn rustfs_rio::Reader> = if size > 0 {
//!     if !diskable_md5 {
//!         // Wrap with both HardLimitReader and EtagReader
//!         let hard_limit = HardLimitReader::new(reader2, size);
//!         Box::new(EtagReader::new(Box::new(hard_limit), etag.clone()))
//!     } else {
//!         // Only wrap with HardLimitReader
//!         Box::new(HardLimitReader::new(reader2, size))
//!     }
//! } else if !diskable_md5 {
//!     // Only wrap with EtagReader
//!     Box::new(EtagReader::new(reader2, etag.clone()))
//! } else {
//!     // No wrapping needed
//!     reader2
//! };
//! let hash_reader2 = HashReader::new(wrapped_reader, size, actual_size, etag.clone(), None, diskable_md5).unwrap();
//! # });
//! ```
//!
//! ## HashReader Detection
//!
//! The `HashReaderDetector` trait allows detection of existing HashReader instances:
//!
//! ```rust
//! use rustfs_rio::{HashReader, HashReaderDetector};
//! use tokio::io::BufReader;
//! use std::io::Cursor;
//! use rustfs_rio::WarpReader;
//!
//! # tokio_test::block_on(async {
//! let data = b"test";
//! let reader = BufReader::new(Cursor::new(&data[..]));
//! let hash_reader = HashReader::new(Box::new(WarpReader::new(reader)), 4, 4, None, None,false).unwrap();
//!
//! // Check if a type is a HashReader
//! assert!(hash_reader.is_hash_reader());
//!
//! // Use new for compatibility (though it's simpler to use new() directly)
//! let reader2 = BufReader::new(Cursor::new(&data[..]));
//! let result = HashReader::new(Box::new(WarpReader::new(reader2)), 4, 4, None, None, false);
//! assert!(result.is_ok());
//! # });
//! ```

use crate::Checksum;
use crate::ChecksumHasher;
use crate::ChecksumType;
use crate::Sha256Hasher;
use crate::compress_index::{Index, TryGetIndex};
use crate::get_content_checksum;
use crate::{EtagReader, EtagResolvable, HardLimitReader, HashReaderDetector, Reader, WarpReader};
use base64::Engine;
use base64::engine::general_purpose;
use http::HeaderMap;
use pin_project_lite::pin_project;
use s3s::TrailingHeaders;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Write;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::error;

/// Trait for mutable operations on HashReader
pub trait HashReaderMut {
    fn into_inner(self) -> Box<dyn Reader>;
    fn take_inner(&mut self) -> Box<dyn Reader>;
    fn bytes_read(&self) -> u64;
    fn checksum(&self) -> &Option<String>;
    fn set_checksum(&mut self, checksum: Option<String>);
    fn size(&self) -> i64;
    fn set_size(&mut self, size: i64);
    fn actual_size(&self) -> i64;
    fn set_actual_size(&mut self, actual_size: i64);
    fn content_hash(&self) -> &Option<Checksum>;
    fn content_sha256(&self) -> &Option<String>;
    fn get_trailer(&self) -> Option<&TrailingHeaders>;
    fn set_trailer(&mut self, trailer: Option<TrailingHeaders>);
}

pin_project! {

    pub struct HashReader {
        #[pin]
        pub inner: Box<dyn Reader>,
        pub size: i64,
        checksum: Option<String>,
        pub actual_size: i64,
        pub diskable_md5: bool,
        bytes_read: u64,
        content_hash: Option<Checksum>,
        content_hasher: Option<Box<dyn ChecksumHasher>>,
        content_sha256: Option<String>,
        content_sha256_hasher: Option<Sha256Hasher>,
        checksum_on_finish: bool,

        trailer_s3s: Option<TrailingHeaders>,

    }

}

impl HashReader {
    /// Used for transformation layers (compression/encryption)
    pub const SIZE_PRESERVE_LAYER: i64 = -1;
    pub fn new(
        mut inner: Box<dyn Reader>,
        size: i64,
        actual_size: i64,
        md5hex: Option<String>,
        sha256hex: Option<String>,
        diskable_md5: bool,
    ) -> std::io::Result<Self> {
        // Get the innermost HashReader
        if size != Self::SIZE_PRESERVE_LAYER
            && let Some(existing_hash_reader) = inner.as_hash_reader_mut()
        {
            if existing_hash_reader.bytes_read() > 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Cannot create HashReader from an already read HashReader",
                ));
            }

            if let Some(checksum) = existing_hash_reader.checksum()
                && let Some(ref md5) = md5hex
                && checksum != md5
            {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "HashReader checksum mismatch"));
            }

            if existing_hash_reader.size() > 0 && size > 0 && existing_hash_reader.size() != size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("HashReader size mismatch: expected {}, got {}", existing_hash_reader.size(), size),
                ));
            }

            existing_hash_reader.set_checksum(md5hex.clone());

            if existing_hash_reader.size() < 0 && size >= 0 {
                existing_hash_reader.set_size(size);
            }

            if existing_hash_reader.actual_size() <= 0 && actual_size >= 0 {
                existing_hash_reader.set_actual_size(actual_size);
            }

            let size = existing_hash_reader.size();
            let actual_size = existing_hash_reader.actual_size();
            let content_hash = existing_hash_reader.content_hash().clone();
            let content_hasher = existing_hash_reader
                .content_hash()
                .clone()
                .map(|hash| hash.checksum_type.hasher().unwrap());
            let content_sha256 = existing_hash_reader.content_sha256().clone();
            let content_sha256_hasher = existing_hash_reader.content_sha256().clone().map(|_| Sha256Hasher::new());
            let inner = existing_hash_reader.take_inner();

            Ok(Self {
                inner,
                size,
                checksum: md5hex.clone(),
                actual_size,
                diskable_md5,
                bytes_read: 0,
                content_sha256,
                content_sha256_hasher,
                content_hash,
                content_hasher,
                checksum_on_finish: false,
                trailer_s3s: existing_hash_reader.get_trailer().cloned(),
            })
        } else {
            if size > 0 {
                let hr = HardLimitReader::new(inner, size);
                inner = Box::new(hr);

                if !diskable_md5 && !inner.is_hash_reader() {
                    let er = EtagReader::new(inner, md5hex.clone());
                    inner = Box::new(er);
                }
            } else if size != Self::SIZE_PRESERVE_LAYER && !diskable_md5 {
                let er = EtagReader::new(inner, md5hex.clone());
                inner = Box::new(er);
            }

            Ok(Self {
                inner,
                size,
                checksum: md5hex,
                actual_size,
                diskable_md5,
                bytes_read: 0,
                content_hash: None,
                content_hasher: None,
                content_sha256: sha256hex.clone(),
                content_sha256_hasher: sha256hex.map(|_| Sha256Hasher::new()),
                checksum_on_finish: false,
                trailer_s3s: None,
            })
        }
    }

    pub fn into_inner(self) -> Box<dyn Reader> {
        self.inner
    }

    /// Update HashReader parameters
    pub fn update_params(&mut self, size: i64, actual_size: i64, etag: Option<String>) {
        if self.size < 0 && size >= 0 {
            self.size = size;
        }

        if self.actual_size <= 0 && actual_size > 0 {
            self.actual_size = actual_size;
        }

        if etag.is_some() {
            self.checksum = etag;
        }
    }

    pub fn size(&self) -> i64 {
        self.size
    }
    pub fn actual_size(&self) -> i64 {
        self.actual_size
    }

    pub fn add_checksum_from_s3s(
        &mut self,
        headers: &HeaderMap,
        trailing_headers: Option<TrailingHeaders>,
        ignore_value: bool,
    ) -> Result<(), std::io::Error> {
        let cs = get_content_checksum(headers)?;

        if ignore_value {
            return Ok(());
        }

        if let Some(checksum) = cs {
            if checksum.checksum_type.trailing() {
                self.trailer_s3s = trailing_headers.clone();
            }

            self.content_hash = Some(checksum.clone());

            return self.add_non_trailing_checksum(Some(checksum), ignore_value);
        }

        Ok(())
    }

    pub fn add_checksum_no_trailer(&mut self, header: &HeaderMap, ignore_value: bool) -> Result<(), std::io::Error> {
        let cs = get_content_checksum(header)?;

        if let Some(checksum) = cs {
            self.content_hash = Some(checksum.clone());

            return self.add_non_trailing_checksum(Some(checksum), ignore_value);
        }
        Ok(())
    }

    pub fn add_non_trailing_checksum(&mut self, checksum: Option<Checksum>, ignore_value: bool) -> Result<(), std::io::Error> {
        if let Some(checksum) = checksum {
            self.content_hash = Some(checksum.clone());

            if ignore_value {
                return Ok(());
            }

            if let Some(hasher) = checksum.checksum_type.hasher() {
                self.content_hasher = Some(hasher);
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum type"));
            }

            tracing::debug!("add_non_trailing_checksum checksum={checksum:?}");
        }
        Ok(())
    }

    pub fn checksum(&self) -> Option<Checksum> {
        if self
            .content_hash
            .as_ref()
            .is_none_or(|v| !v.checksum_type.is_set() || !v.valid())
        {
            return None;
        }
        self.content_hash.clone()
    }
    pub fn content_crc_type(&self) -> Option<ChecksumType> {
        self.content_hash.as_ref().map(|v| v.checksum_type)
    }

    pub fn content_crc(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(checksum) = self.content_hash.as_ref() {
            if !checksum.valid() || checksum.checksum_type.is(ChecksumType::NONE) {
                return map;
            }

            if checksum.checksum_type.trailing() {
                if let Some(trailer) = self.trailer_s3s.as_ref()
                    && let Some(Some(checksum_str)) = trailer.read(|headers| {
                        checksum
                            .checksum_type
                            .key()
                            .and_then(|key| headers.get(key).and_then(|value| value.to_str().ok().map(|s| s.to_string())))
                    })
                {
                    map.insert(checksum.checksum_type.to_string(), checksum_str);
                }
                return map;
            }

            map.insert(checksum.checksum_type.to_string(), checksum.encoded.clone());

            return map;
        }
        map
    }
}

impl HashReaderMut for HashReader {
    fn into_inner(self) -> Box<dyn Reader> {
        self.inner
    }

    fn take_inner(&mut self) -> Box<dyn Reader> {
        // Replace inner with an empty reader to move it out safely while keeping self valid
        mem::replace(&mut self.inner, Box::new(WarpReader::new(Cursor::new(Vec::new()))))
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    fn checksum(&self) -> &Option<String> {
        &self.checksum
    }

    fn set_checksum(&mut self, checksum: Option<String>) {
        self.checksum = checksum;
    }

    fn size(&self) -> i64 {
        self.size
    }

    fn set_size(&mut self, size: i64) {
        self.size = size;
    }

    fn actual_size(&self) -> i64 {
        self.actual_size
    }

    fn set_actual_size(&mut self, actual_size: i64) {
        self.actual_size = actual_size;
    }

    fn content_hash(&self) -> &Option<Checksum> {
        &self.content_hash
    }

    fn content_sha256(&self) -> &Option<String> {
        &self.content_sha256
    }

    fn get_trailer(&self) -> Option<&TrailingHeaders> {
        self.trailer_s3s.as_ref()
    }

    fn set_trailer(&mut self, trailer: Option<TrailingHeaders>) {
        self.trailer_s3s = trailer;
    }
}

impl AsyncRead for HashReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();

        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let data = &buf.filled()[before..];
                let filled = data.len();

                *this.bytes_read += filled as u64;

                if filled > 0 {
                    // Update SHA256 hasher
                    if let Some(hasher) = this.content_sha256_hasher
                        && let Err(e) = hasher.write_all(data)
                    {
                        error!("SHA256 hasher write error, error={:?}", e);
                        return Poll::Ready(Err(std::io::Error::other(e)));
                    }

                    // Update content hasher
                    if let Some(hasher) = this.content_hasher
                        && let Err(e) = hasher.write_all(data)
                    {
                        return Poll::Ready(Err(std::io::Error::other(e)));
                    }
                }

                if filled == 0 && !*this.checksum_on_finish {
                    // check SHA256
                    if let (Some(hasher), Some(expected_sha256)) = (this.content_sha256_hasher, this.content_sha256) {
                        let sha256 = hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower);
                        if sha256 != *expected_sha256 {
                            error!("SHA256 mismatch, expected={:?}, actual={:?}", expected_sha256, sha256);
                            return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "SHA256 mismatch")));
                        }
                    }

                    // check content hasher
                    if let (Some(hasher), Some(expected_content_hash)) = (this.content_hasher, this.content_hash) {
                        if expected_content_hash.checksum_type.trailing()
                            && let Some(trailer) = this.trailer_s3s.as_ref()
                            && let Some(Some(checksum_str)) = trailer.read(|headers| {
                                expected_content_hash
                                    .checksum_type
                                    .key()
                                    .and_then(|key| headers.get(key).and_then(|value| value.to_str().ok().map(|s| s.to_string())))
                            })
                        {
                            expected_content_hash.encoded = checksum_str;
                            expected_content_hash.raw = general_purpose::STANDARD
                                .decode(&expected_content_hash.encoded)
                                .map_err(|_| std::io::Error::other("Invalid base64 checksum"))?;

                            if expected_content_hash.raw.is_empty() {
                                return Poll::Ready(Err(std::io::Error::other("Content hash mismatch")));
                            }
                        }

                        let content_hash = hasher.finalize();

                        if content_hash != expected_content_hash.raw {
                            let expected_hex = hex_simd::encode_to_string(&expected_content_hash.raw, hex_simd::AsciiCase::Lower);
                            let actual_hex = hex_simd::encode_to_string(content_hash, hex_simd::AsciiCase::Lower);
                            error!(
                                "Content hash mismatch, type={:?}, encoded={:?}, expected={:?}, actual={:?}",
                                expected_content_hash.checksum_type, expected_content_hash.encoded, expected_hex, actual_hex
                            );
                            // Use ChecksumMismatch error so that API layer can return BadDigest
                            let checksum_err = crate::errors::ChecksumMismatch {
                                want: expected_hex,
                                got: actual_hex,
                            };
                            return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, checksum_err)));
                        }
                    }

                    *this.checksum_on_finish = true;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

impl EtagResolvable for HashReader {
    fn try_resolve_etag(&mut self) -> Option<String> {
        if self.diskable_md5 {
            return None;
        }
        if let Some(etag) = self.inner.try_resolve_etag() {
            return Some(etag);
        }
        // If no etag from inner and we have a stored checksum, return it
        self.checksum.clone()
    }
}

impl HashReaderDetector for HashReader {
    fn is_hash_reader(&self) -> bool {
        true
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        Some(self)
    }
}

impl TryGetIndex for HashReader {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DecryptReader, WarpReader, encrypt_reader};
    use rand::RngExt;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_hashreader_wrapping_logic() {
        let data = b"hello world";
        let size = data.len() as i64;
        let actual_size = size;
        let etag = None;

        // Test 1: Simple creation
        let reader1 = BufReader::new(Cursor::new(&data[..]));
        let reader1 = Box::new(WarpReader::new(reader1));
        let hash_reader1 = HashReader::new(reader1, size, actual_size, etag.clone(), None, false).unwrap();
        assert_eq!(hash_reader1.size(), size);
        assert_eq!(hash_reader1.actual_size(), actual_size);

        // Test 2: With HardLimitReader wrapping
        let reader2 = BufReader::new(Cursor::new(&data[..]));
        let reader2 = Box::new(WarpReader::new(reader2));
        let hard_limit = HardLimitReader::new(reader2, size);
        let hard_limit = Box::new(hard_limit);
        let hash_reader2 = HashReader::new(hard_limit, size, actual_size, etag.clone(), None, false).unwrap();
        assert_eq!(hash_reader2.size(), size);
        assert_eq!(hash_reader2.actual_size(), actual_size);

        // Test 3: With EtagReader wrapping
        let reader3 = BufReader::new(Cursor::new(&data[..]));
        let reader3 = Box::new(WarpReader::new(reader3));
        let etag_reader = EtagReader::new(reader3, etag.clone());
        let etag_reader = Box::new(etag_reader);
        let hash_reader3 = HashReader::new(etag_reader, size, actual_size, etag.clone(), None, false).unwrap();
        assert_eq!(hash_reader3.size(), size);
        assert_eq!(hash_reader3.actual_size(), actual_size);
    }

    #[tokio::test]
    async fn test_hashreader_etag_basic() {
        let data = b"hello hashreader";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, None, false).unwrap();
        let mut buf = Vec::new();
        let _ = hash_reader.read_to_end(&mut buf).await.unwrap();
        // Since we removed EtagReader integration, etag might be None
        let _etag = hash_reader.try_resolve_etag();
        // Just check that we can call etag() without error
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_hashreader_diskable_md5() {
        let data = b"no etag";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        let mut hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, None, true).unwrap();
        let mut buf = Vec::new();
        let _ = hash_reader.read_to_end(&mut buf).await.unwrap();
        // Etag should be None when diskable_md5 is true
        let etag = hash_reader.try_resolve_etag();
        assert!(etag.is_none());
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_hashreader_new_logic() {
        let data = b"test data";
        let reader = BufReader::new(Cursor::new(&data[..]));
        let reader = Box::new(WarpReader::new(reader));
        // Create a HashReader first
        let hash_reader =
            HashReader::new(reader, data.len() as i64, data.len() as i64, Some("test_etag".to_string()), None, false).unwrap();
        let hash_reader = Box::new(WarpReader::new(hash_reader));
        // Now try to create another HashReader from the existing one using new
        let result = HashReader::new(
            hash_reader,
            data.len() as i64,
            data.len() as i64,
            Some("test_etag".to_string()),
            None,
            false,
        );

        assert!(result.is_ok());
        let final_reader = result.unwrap();
        assert_eq!(final_reader.checksum, Some("test_etag".to_string()));
        assert_eq!(final_reader.size(), data.len() as i64);
    }

    #[tokio::test]
    async fn test_for_wrapping_readers() {
        use crate::{CompressReader, DecompressReader};
        use md5::{Digest, Md5};
        use rand::Rng;
        use rustfs_utils::compress::CompressionAlgorithm;

        // Generate 1MB random data
        let size = 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);

        let mut hasher = Md5::new();
        hasher.update(&data);

        let hex = faster_hex::hex_string(hasher.finalize().as_slice());
        let expected = hex.to_string();

        println!("expected: {expected}");

        let reader = Cursor::new(data.clone());
        let reader = BufReader::new(reader);

        // Enable compression test
        let is_compress = true;
        let size = data.len() as i64;
        let actual_size = data.len() as i64;

        let reader = Box::new(WarpReader::new(reader));
        // Create HashReader
        let mut hr = HashReader::new(reader, size, actual_size, Some(expected.clone()), None, false).unwrap();

        // If compression is enabled, compress data first
        let compressed_data = if is_compress {
            let mut compressed_buf = Vec::new();
            let compress_reader = CompressReader::new(hr, CompressionAlgorithm::Gzip);
            let mut compress_reader = compress_reader;
            compress_reader.read_to_end(&mut compressed_buf).await.unwrap();

            println!("Original size: {}, Compressed size: {}", data.len(), compressed_buf.len());

            compressed_buf
        } else {
            // If not compressing, read original data directly
            let mut buf = Vec::new();
            hr.read_to_end(&mut buf).await.unwrap();
            buf
        };

        let mut key = [0u8; 32];
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut key);
        rand::rng().fill_bytes(&mut nonce);

        let is_encrypt = true;

        if is_encrypt {
            // Encrypt compressed data
            let encrypt_reader = encrypt_reader::EncryptReader::new(WarpReader::new(Cursor::new(compressed_data)), key, nonce);
            let mut encrypted_data = Vec::new();
            let mut encrypt_reader = encrypt_reader;
            encrypt_reader.read_to_end(&mut encrypted_data).await.unwrap();

            println!("Encrypted size: {}", encrypted_data.len());

            // Decrypt data
            let decrypt_reader = DecryptReader::new(WarpReader::new(Cursor::new(encrypted_data)), key, nonce);
            let mut decrypt_reader = decrypt_reader;
            let mut decrypted_data = Vec::new();
            decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();

            if is_compress {
                // If compression was used, decompress is needed
                let decompress_reader =
                    DecompressReader::new(WarpReader::new(Cursor::new(decrypted_data)), CompressionAlgorithm::Gzip);
                let mut decompress_reader = decompress_reader;
                let mut final_data = Vec::new();
                decompress_reader.read_to_end(&mut final_data).await.unwrap();

                println!("Final decompressed size: {}", final_data.len());
                assert_eq!(final_data.len() as i64, actual_size);
                assert_eq!(&final_data, &data);
            } else {
                // Without compression we can compare the decrypted bytes directly
                assert_eq!(decrypted_data.len() as i64, actual_size);
                assert_eq!(&decrypted_data, &data);
            }
            return;
        }

        // When encryption is disabled, only handle compression/decompression
        if is_compress {
            let decompress_reader =
                DecompressReader::new(WarpReader::new(Cursor::new(compressed_data)), CompressionAlgorithm::Gzip);
            let mut decompress_reader = decompress_reader;
            let mut decompressed = Vec::new();
            decompress_reader.read_to_end(&mut decompressed).await.unwrap();

            assert_eq!(decompressed.len() as i64, actual_size);
            assert_eq!(&decompressed, &data);
        } else {
            assert_eq!(compressed_data.len() as i64, actual_size);
            assert_eq!(&compressed_data, &data);
        }

        // Validate the etag (compression alters the payload, so this may require adjustments)
        println!("Test completed successfully with compression: {is_compress}, encryption: {is_encrypt}");
    }

    #[tokio::test]
    async fn test_compression_with_compressible_data() {
        use crate::{CompressReader, DecompressReader};
        use rustfs_utils::compress::CompressionAlgorithm;

        // Create highly compressible data (repeated pattern)
        let pattern = b"Hello, World! This is a test pattern that should compress well. ";
        let repeat_count = 16384; // 16K repetitions
        let mut data = Vec::new();
        for _ in 0..repeat_count {
            data.extend_from_slice(pattern);
        }

        println!("Original data size: {} bytes", data.len());

        let reader = BufReader::new(Cursor::new(data.clone()));
        let reader = Box::new(WarpReader::new(reader));
        let hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, None, false).unwrap();

        // Test compression
        let compress_reader = CompressReader::new(hash_reader, CompressionAlgorithm::Gzip);
        let mut compressed_data = Vec::new();
        let mut compress_reader = compress_reader;
        compress_reader.read_to_end(&mut compressed_data).await.unwrap();

        println!("Compressed data size: {} bytes", compressed_data.len());
        println!("Compression ratio: {:.2}%", (compressed_data.len() as f64 / data.len() as f64) * 100.0);

        // Verify compression actually reduced size for this compressible data
        assert!(compressed_data.len() < data.len(), "Compression should reduce size for repetitive data");

        // Test decompression
        let decompress_reader = DecompressReader::new(Cursor::new(compressed_data), CompressionAlgorithm::Gzip);
        let mut decompressed_data = Vec::new();
        let mut decompress_reader = decompress_reader;
        decompress_reader.read_to_end(&mut decompressed_data).await.unwrap();

        // Verify decompressed data matches original
        assert_eq!(decompressed_data.len(), data.len());
        assert_eq!(&decompressed_data, &data);

        println!("Compression/decompression test passed successfully!");
    }

    #[tokio::test]
    async fn test_compression_algorithms() {
        use crate::{CompressReader, DecompressReader};
        use rustfs_utils::compress::CompressionAlgorithm;

        let data = b"This is test data for compression algorithm testing. ".repeat(1000);
        println!("Testing with {} bytes of data", data.len());

        let algorithms = vec![
            CompressionAlgorithm::Gzip,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Zstd,
        ];

        for algorithm in algorithms {
            println!("\nTesting algorithm: {algorithm:?}");

            let reader = BufReader::new(Cursor::new(data.clone()));
            let reader = Box::new(WarpReader::new(reader));
            let hash_reader = HashReader::new(reader, data.len() as i64, data.len() as i64, None, None, false).unwrap();

            // Compress
            let compress_reader = CompressReader::new(hash_reader, algorithm);
            let mut compressed_data = Vec::new();
            let mut compress_reader = compress_reader;
            compress_reader.read_to_end(&mut compressed_data).await.unwrap();

            println!(
                "  Compressed size: {} bytes (ratio: {:.2}%)",
                compressed_data.len(),
                (compressed_data.len() as f64 / data.len() as f64) * 100.0
            );

            // Decompress
            let decompress_reader = DecompressReader::new(Cursor::new(compressed_data), algorithm);
            let mut decompressed_data = Vec::new();
            let mut decompress_reader = decompress_reader;
            decompress_reader.read_to_end(&mut decompressed_data).await.unwrap();

            // Verify
            assert_eq!(decompressed_data.len(), data.len());
            assert_eq!(&decompressed_data, &data);
            println!("  ✓ Algorithm {algorithm:?} test passed");
        }
    }
}
