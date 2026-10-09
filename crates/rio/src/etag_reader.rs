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
use crate::{BadDigest, EtagResolvable, HashReaderDetector, HashReaderMut};
use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use pin_project_lite::pin_project;
use sha2::Sha256;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::error;

pin_project! {
    pub struct  EtagReader<R> {
        #[pin]
        pub inner: R,
        pub md5: Md5,
        pub finished: bool,
        pub checksum: Option<String>,
        resolved_etag: Option<String>,
        encrypted_etag_mac: Option<Hmac<Sha256>>,
    }
}

impl<R> EtagReader<R> {
    pub fn new(inner: R, checksum: Option<String>) -> Self {
        Self {
            inner,
            md5: Md5::new(),
            finished: false,
            checksum,
            resolved_etag: None,
            encrypted_etag_mac: None,
        }
    }

    /// Protect the content fingerprint with the object's encryption material.
    /// Plaintext checksum verification remains the inner reader's responsibility.
    pub fn new_encrypted(inner: R, key: [u8; 32], nonce: [u8; 12]) -> std::io::Result<Self> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&key).map_err(std::io::Error::other)?;
        mac.update(b"RustFS:encrypted-ETag:v1\0");
        mac.update(&nonce);
        let mut reader = Self::new(inner, None);
        reader.encrypted_etag_mac = Some(mac);
        Ok(reader)
    }

    /// Get the final md5 value (etag) as a hex string, only compute once.
    /// Can be called multiple times, always returns the same result after finished.
    pub fn get_etag(&mut self) -> String {
        if let Some(etag) = &self.resolved_etag {
            return etag.clone();
        }

        let etag = self.md5.clone().finalize().to_vec();
        let etag = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
        self.resolved_etag = Some(etag.clone());
        etag
    }
}

impl<R> AsyncRead for EtagReader<R>
where
    R: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        if *this.finished {
            return Poll::Ready(Ok(()));
        }

        let orig_filled = buf.filled().len();
        let poll = this.inner.as_mut().poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let filled = &buf.filled()[orig_filled..];
            if !filled.is_empty() {
                this.md5.update(filled);
            } else {
                // EOF
                *this.finished = true;
                let etag = if let Some(etag) = this.resolved_etag.as_ref() {
                    etag.clone()
                } else {
                    let etag = this.md5.clone().finalize().to_vec();
                    let etag = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
                    *this.resolved_etag = Some(etag.clone());
                    etag
                };

                if let Some(checksum) = this.checksum
                    && *checksum != etag
                {
                    error!("Checksum mismatch, expected={:?}, actual={:?}", checksum, etag);
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        BadDigest {
                            expected_md5: checksum.clone(),
                            calculated_md5: etag,
                        },
                    )));
                }
            }
        }
        poll
    }
}

impl<R> EtagResolvable for EtagReader<R> {
    fn is_etag_reader(&self) -> bool {
        true
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        if let Some(mut mac) = self.encrypted_etag_mac.clone() {
            if !self.finished {
                return None;
            }
            mac.update(self.get_etag().as_bytes());
            let protected = mac.finalize().into_bytes();
            return Some(hex_simd::encode_to_string(&protected[..16], hex_simd::AsciiCase::Lower));
        }
        // EtagReader provides its own etag, not delegating to inner
        if let Some(checksum) = &self.checksum {
            Some(checksum.clone())
        } else if self.finished {
            Some(self.get_etag())
        } else {
            None
        }
    }
}

impl<R> HashReaderDetector for EtagReader<R>
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

impl<R> TryGetIndex for EtagReader<R>
where
    R: TryGetIndex,
{
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn encrypted_etags_hide_plaintext_fingerprints_including_empty_objects() {
        for data in [b"".as_slice(), b"Salary 2026: 85000 EUR".as_slice()] {
            let md5 = hex_simd::encode_to_string(Md5::digest(data), hex_simd::AsciiCase::Lower);
            let mut tags = Vec::new();
            for (key, nonce) in [([1; 32], [1; 12]), ([2; 32], [1; 12]), ([1; 32], [2; 12])] {
                let mut reader = EtagReader::new_encrypted(Cursor::new(data), key, nonce).expect("create protected ETag reader");
                assert!(reader.try_resolve_etag().is_none());
                let mut output = Vec::new();
                reader.read_to_end(&mut output).await.expect("read unchanged plaintext");
                assert_eq!(output, data);
                let tag = reader.try_resolve_etag().expect("resolve protected ETag after EOF");
                assert_eq!(tag.len(), 32);
                assert_ne!(tag, md5);
                assert_eq!(reader.try_resolve_etag().as_ref(), Some(&tag));
                tags.push(tag);
            }
            assert_ne!(tags[0], tags[1], "different keys must not link identical content");
            assert_ne!(tags[0], tags[2], "different objects under a legacy key must not link identical content");
        }
    }

    #[tokio::test]
    async fn encrypted_etag_reader_preserves_plaintext_digest_rejection() {
        let plaintext = crate::HashReader::from_stream(
            Cursor::new(b"actual plaintext"),
            16,
            16,
            Some("00000000000000000000000000000000".to_owned()),
            None,
            false,
        )
        .expect("create checked plaintext reader");
        let mut reader = EtagReader::new_encrypted(plaintext, [1; 32], [2; 12]).expect("create protected ETag reader");
        let err = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("wrong plaintext MD5 must still be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_etag_reader_basic() {
        let data = b"hello world";
        let mut hasher = Md5::new();
        hasher.update(data);
        let hex = faster_hex::hex_string(hasher.finalize().as_slice());
        let expected = hex.to_string();
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, None);

        let mut buf = Vec::new();
        let n = etag_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);

        let etag = etag_reader.try_resolve_etag();
        assert_eq!(etag, Some(expected));
    }

    #[tokio::test]
    async fn test_etag_reader_empty() {
        let data = b"";
        let mut hasher = Md5::new();
        hasher.update(data);
        let hex = faster_hex::hex_string(hasher.finalize().as_slice());
        let expected = hex.to_string();
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, None);

        let mut buf = Vec::new();
        let n = etag_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert!(buf.is_empty());

        let etag = etag_reader.try_resolve_etag();
        assert_eq!(etag, Some(expected));
    }

    #[tokio::test]
    async fn test_etag_reader_multiple_get() {
        let data = b"abc123";
        let mut hasher = Md5::new();
        hasher.update(data);
        let hex = faster_hex::hex_string(hasher.finalize().as_slice());
        let expected = hex.to_string();
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, None);

        let mut buf = Vec::new();
        let _ = etag_reader.read_to_end(&mut buf).await.unwrap();

        // Call etag multiple times, should always return the same result
        let etag1 = { etag_reader.try_resolve_etag() };
        let etag2 = { etag_reader.try_resolve_etag() };
        assert_eq!(etag1, Some(expected.clone()));
        assert_eq!(etag2, Some(expected.clone()));
    }

    #[tokio::test]
    async fn test_etag_reader_not_finished() {
        let data = b"abc123";
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, None);

        // Do not read to end, etag should be None
        let mut buf = [0u8; 2];
        let _ = etag_reader.read(&mut buf).await.unwrap();
        assert_eq!(etag_reader.try_resolve_etag(), None);
    }

    #[tokio::test]
    async fn test_etag_reader_large_data() {
        // Generate 3MB random data
        let size = 3 * 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut hasher = Md5::new();
        hasher.update(&data);
        let cloned_data = data.clone();
        let hex = faster_hex::hex_string(hasher.finalize().as_slice());
        let expected = hex.to_string();
        let reader = Cursor::new(data.clone());
        let mut etag_reader = EtagReader::new(reader, None);
        let mut buf = Vec::new();
        let n = etag_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, size);
        assert_eq!(&buf, &cloned_data);

        let etag = etag_reader.try_resolve_etag();
        assert_eq!(etag, Some(expected));
    }

    #[tokio::test]
    async fn test_etag_reader_checksum_match() {
        let data = b"checksum test data";
        let mut hasher = Md5::new();
        hasher.update(data);
        let expected = hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower);
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, Some(expected.clone()));

        let mut buf = Vec::new();
        let n = etag_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
        // Verification passed, etag should equal expected
        assert_eq!(etag_reader.try_resolve_etag(), Some(expected));
    }

    #[tokio::test]
    async fn test_etag_reader_checksum_mismatch() {
        let data = b"checksum test data";
        let wrong_checksum = "deadbeefdeadbeefdeadbeefdeadbeef".to_string();
        let calculated_md5 = hex_simd::encode_to_string(Md5::digest(data), hex_simd::AsciiCase::Lower);
        let reader = BufReader::new(&data[..]);
        let mut etag_reader = EtagReader::new(reader, Some(wrong_checksum.clone()));

        let mut buf = Vec::new();
        // Verification failed, should return InvalidData error
        let err = etag_reader.read_to_end(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let digest = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<BadDigest>())
            .expect("checksum mismatch should preserve the BadDigest type");
        assert_eq!(digest.expected_md5, wrong_checksum);
        assert_eq!(digest.calculated_md5, calculated_md5);
    }
}
