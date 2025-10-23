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
use crate::{EtagResolvable, HashReaderDetector, HashReaderMut, Reader};
use md5::{Digest, Md5};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::error;

pin_project! {
    pub struct  EtagReader {
        #[pin]
        pub inner: Box<dyn Reader>,
        pub md5: Md5,
        pub finished: bool,
        pub checksum: Option<String>,
    }
}

impl EtagReader {
    pub fn new(inner: Box<dyn Reader>, checksum: Option<String>) -> Self {
        Self {
            inner,
            md5: Md5::new(),
            finished: false,
            checksum,
        }
    }

    /// Get the final md5 value (etag) as a hex string, only compute once.
    /// Can be called multiple times, always returns the same result after finished.
    pub fn get_etag(&mut self) -> String {
        let etag = self.md5.clone().finalize().to_vec();
        hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower)
    }
}

impl AsyncRead for EtagReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        let orig_filled = buf.filled().len();
        let poll = this.inner.as_mut().poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let filled = &buf.filled()[orig_filled..];
            if !filled.is_empty() {
                this.md5.update(filled);
            } else {
                // EOF
                *this.finished = true;
                if let Some(checksum) = this.checksum {
                    let etag = this.md5.clone().finalize().to_vec();
                    let etag_hex = hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower);
                    if *checksum != etag_hex {
                        error!("Checksum mismatch, expected={:?}, actual={:?}", checksum, etag_hex);
                        return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Checksum mismatch")));
                    }
                }
            }
        }
        poll
    }
}

impl EtagResolvable for EtagReader {
    fn is_etag_reader(&self) -> bool {
        true
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
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

impl HashReaderDetector for EtagReader {
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl TryGetIndex for EtagReader {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use crate::WarpReader;

    use super::*;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_etag_reader_basic() {
        let data = b"hello world";
        let mut hasher = Md5::new();
        hasher.update(data);
        let expected = format!("{:x}", hasher.finalize());
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
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
        let expected = format!("{:x}", hasher.finalize());
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
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
        let expected = format!("{:x}", hasher.finalize());
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
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
        let reader = Box::new(WarpReader::new(reader));
        let mut etag_reader = EtagReader::new(reader, None);

        // Do not read to end, etag should be None
        let mut buf = [0u8; 2];
        let _ = etag_reader.read(&mut buf).await.unwrap();
        assert_eq!(etag_reader.try_resolve_etag(), None);
    }

    #[tokio::test]
    async fn test_etag_reader_large_data() {
        use rand::Rng;
        // Generate 3MB random data
        let size = 3 * 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let mut hasher = Md5::new();
        hasher.update(&data);

        let cloned_data = data.clone();

        let expected = format!("{:x}", hasher.finalize());

        let reader = Cursor::new(data.clone());
        let reader = Box::new(WarpReader::new(reader));
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
        let reader = Box::new(WarpReader::new(reader));
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
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
        let mut etag_reader = EtagReader::new(reader, Some(wrong_checksum.clone()));

        let mut buf = Vec::new();
        // Verification failed, should return InvalidData error
        let err = etag_reader.read_to_end(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
