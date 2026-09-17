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
use pin_project_lite::pin_project;
use rustfs_utils::hash::Md5Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::error;

pin_project! {
    pub struct  EtagReader<R> {
        #[pin]
        pub inner: R,
        // `Some` until EOF; taken (consumed) exactly once when the stream ends.
        // `Md5Stream` has no snapshot/clone: the digest exists only after EOF.
        md5: Option<Md5Stream>,
        pub finished: bool,
        pub checksum: Option<String>,
        resolved_etag: Option<String>,
    }
}

impl<R> EtagReader<R> {
    pub fn new(inner: R, checksum: Option<String>) -> Self {
        Self {
            inner,
            md5: Some(Md5Stream::new()),
            finished: false,
            checksum,
            resolved_etag: None,
        }
    }

    /// The final md5 value (etag) as a hex string.
    ///
    /// `None` until the inner stream has reached EOF: the hasher is consumed
    /// exactly once at that point, so there is no partial digest to hand out
    /// earlier. After EOF this returns the same cached value every time.
    pub fn get_etag(&self) -> Option<String> {
        self.resolved_etag.clone()
    }

    /// Runs exactly once, on the poll that observes EOF: `finished` is set on
    /// that same poll and every later poll returns early, so `md5` is always
    /// `Some` here. Hashing nothing on the impossible `None` beats panicking
    /// inside the data path.
    fn resolve_at_eof(md5: &mut Option<Md5Stream>, resolved_etag: &mut Option<String>) -> String {
        let digest = md5.take().unwrap_or_default().finalize();
        let etag = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
        *resolved_etag = Some(etag.clone());
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
                if let Some(md5) = this.md5.as_mut() {
                    md5.update(filled);
                }
            } else {
                // EOF
                *this.finished = true;
                let etag = Self::resolve_at_eof(this.md5, this.resolved_etag);

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
        // EtagReader provides its own etag, not delegating to inner
        if let Some(checksum) = &self.checksum {
            Some(checksum.clone())
        } else if self.finished {
            self.get_etag()
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
    // RustCrypto md-5 stays a dev-dependency: the expected values must come
    // from an implementation independent of the one under test.
    use md5::{Digest, Md5};
    use rand::RngExt;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

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
        assert_eq!(etag_reader.get_etag(), None, "no partial digest before EOF");

        // Reading the rest resolves it, and the value is stable afterwards.
        let mut rest = Vec::new();
        etag_reader.read_to_end(&mut rest).await.unwrap();
        let expected = faster_hex::hex_string(Md5::digest(data).as_slice());
        assert_eq!(etag_reader.get_etag(), Some(expected.clone()));
        assert_eq!(etag_reader.try_resolve_etag(), Some(expected));
    }

    /// The body stream never hands EtagReader the object in one piece; the
    /// digest must not depend on how the inner reader splits its reads.
    #[tokio::test]
    async fn test_etag_reader_small_inner_reads_match_one_shot() {
        let size = 3 * 64 * 1024 + 77;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let expected = faster_hex::hex_string(Md5::digest(&data).as_slice());

        // BufReader with a tiny capacity forces many short poll_read fills.
        let inner = BufReader::with_capacity(61, Cursor::new(data.clone()));
        let mut etag_reader = EtagReader::new(inner, None);
        let mut out = Vec::new();
        let mut chunk = [0u8; 61];
        loop {
            let n = etag_reader.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(out, data);
        assert_eq!(etag_reader.try_resolve_etag(), Some(expected));
    }

    /// Reads after EOF are a no-op and never disturb the resolved etag.
    #[tokio::test]
    async fn test_etag_reader_reads_after_eof_are_stable() {
        let data = b"stable after eof";
        let expected = faster_hex::hex_string(Md5::digest(data).as_slice());
        let mut etag_reader = EtagReader::new(BufReader::new(&data[..]), None);
        let mut buf = Vec::new();
        etag_reader.read_to_end(&mut buf).await.unwrap();
        for _ in 0..3 {
            let mut extra = [0u8; 8];
            assert_eq!(etag_reader.read(&mut extra).await.unwrap(), 0);
            assert_eq!(etag_reader.get_etag(), Some(expected.clone()));
        }
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
        assert_eq!(digest.calculated_md5, calculated_md5.clone());
        // The digest was resolved before the comparison, so it stays observable
        // after the error and the reader stays at EOF.
        assert_eq!(etag_reader.get_etag(), Some(calculated_md5));
        assert!(etag_reader.finished);
    }

    /// An inner reader that yields part of the body, then one `Err`, then the
    /// rest. EtagReader must surface the error, must not treat it as EOF, and
    /// must keep hashing correctly once the inner reader recovers.
    struct FlakyInner {
        data: Vec<u8>,
        pos: usize,
        fail_at: usize,
        failed: bool,
    }

    impl AsyncRead for FlakyInner {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.pos >= self.fail_at && !self.failed {
                self.failed = true;
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "transient")));
            }
            let end = (self.pos + buf.remaining().min(7)).min(self.data.len());
            buf.put_slice(&self.data[self.pos..end]);
            self.pos = end;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_etag_reader_inner_error_is_not_eof_and_state_survives() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 31 % 251) as u8).collect();
        let expected = faster_hex::hex_string(Md5::digest(&data).as_slice());
        let mut etag_reader = EtagReader::new(
            FlakyInner {
                data: data.clone(),
                pos: 0,
                fail_at: 300,
                failed: false,
            },
            None,
        );

        let mut out = Vec::new();
        let mut chunk = [0u8; 64];
        let mut saw_error = false;
        loop {
            match etag_reader.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&chunk[..n]),
                Err(err) => {
                    assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
                    assert!(!etag_reader.finished, "an inner error must not finish the reader");
                    assert_eq!(etag_reader.get_etag(), None);
                    saw_error = true;
                }
            }
        }
        assert!(saw_error, "the inner reader must have failed once");
        assert_eq!(out, data);
        assert_eq!(etag_reader.try_resolve_etag(), Some(expected));
    }

    /// Interleaved `Pending` from the inner reader (a slow network body) must
    /// neither be treated as data nor as EOF.
    #[tokio::test]
    async fn test_etag_reader_survives_pending_between_chunks() {
        let data = b"pending between chunks keeps the digest intact";
        let expected = faster_hex::hex_string(Md5::digest(data).as_slice());
        let inner = tokio_test::io::Builder::new()
            .read(&data[..10])
            .wait(std::time::Duration::from_millis(5))
            .read(&data[10..20])
            .wait(std::time::Duration::from_millis(5))
            .read(&data[20..])
            .build();
        let mut etag_reader = EtagReader::new(inner, None);
        let mut out = Vec::new();
        etag_reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);
        assert_eq!(etag_reader.try_resolve_etag(), Some(expected));
    }
}
