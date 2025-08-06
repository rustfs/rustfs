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

//! LimitReader: a wrapper for AsyncRead that limits the total number of bytes read.
//!
//! # Example
//! ```
//! use tokio::io::{AsyncReadExt, BufReader};
//! use rustfs_rio::LimitReader;
//!
//! #[tokio::main]
//! async fn main() {
//!  let data = b"hello world";
//!       let reader = BufReader::new(&data[..]);
//!      let mut limit_reader = LimitReader::new(reader, data.len());
//!
//!      let mut buf = Vec::new();
//!      let n = limit_reader.read_to_end(&mut buf).await.unwrap();
//!      assert_eq!(n, data.len());
//!      assert_eq!(&buf, data);
//! }
//! ```

use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{EtagResolvable, HashReaderDetector, HashReaderMut, TryGetIndex};

pin_project! {
    #[derive(Debug)]
    pub struct LimitReader<R> {
        #[pin]
        pub inner: R,
        limit: usize,
        read: usize,
    }
}

/// A wrapper for AsyncRead that limits the total number of bytes read.
impl<R> LimitReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create a new LimitReader wrapping `inner`, with a total read limit of `limit` bytes.
    pub fn new(inner: R, limit: usize) -> Self {
        Self { inner, limit, read: 0 }
    }
}

impl<R> AsyncRead for LimitReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        let remaining = this.limit.saturating_sub(*this.read);
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        let orig_remaining = buf.remaining();
        let allowed = remaining.min(orig_remaining);
        if allowed == 0 {
            return Poll::Ready(Ok(()));
        }
        if allowed == orig_remaining {
            let before_size = buf.filled().len();
            let poll = this.inner.as_mut().poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &poll {
                let n = buf.filled().len() - before_size;
                *this.read += n;
            }
            poll
        } else {
            let mut temp = vec![0u8; allowed];
            let mut temp_buf = ReadBuf::new(&mut temp);
            let poll = this.inner.as_mut().poll_read(cx, &mut temp_buf);
            if let Poll::Ready(Ok(())) = &poll {
                let n = temp_buf.filled().len();
                buf.put_slice(temp_buf.filled());
                *this.read += n;
            }
            poll
        }
    }
}

impl<R> EtagResolvable for LimitReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for LimitReader<R>
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

impl<R> TryGetIndex for LimitReader<R> where R: AsyncRead + Unpin + Send + Sync {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_limit_reader_exact() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let mut limit_reader = LimitReader::new(reader, data.len());

        let mut buf = Vec::new();
        let n = limit_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_limit_reader_less_than_data() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let mut limit_reader = LimitReader::new(reader, 5);

        let mut buf = Vec::new();
        let n = limit_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn test_limit_reader_zero() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let mut limit_reader = LimitReader::new(reader, 0);

        let mut buf = Vec::new();
        let n = limit_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn test_limit_reader_multiple_reads() {
        let data = b"abcdefghij";
        let reader = BufReader::new(&data[..]);
        let mut limit_reader = LimitReader::new(reader, 7);

        let mut buf1 = [0u8; 3];
        let n1 = limit_reader.read(&mut buf1).await.unwrap();
        assert_eq!(n1, 3);
        assert_eq!(&buf1, b"abc");

        let mut buf2 = [0u8; 5];
        let n2 = limit_reader.read(&mut buf2).await.unwrap();
        assert_eq!(n2, 4);
        assert_eq!(&buf2[..n2], b"defg");

        let mut buf3 = [0u8; 2];
        let n3 = limit_reader.read(&mut buf3).await.unwrap();
        assert_eq!(n3, 0);
    }

    #[tokio::test]
    async fn test_limit_reader_large_file() {
        use rand::Rng;
        // Generate a 3MB random byte array for testing
        let size = 3 * 1024 * 1024;
        let mut data = vec![0u8; size];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut limit_reader = LimitReader::new(reader, size);

        // Read data into buffer
        let mut buf = Vec::new();
        let n = limit_reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, size);
        assert_eq!(buf.len(), size);
        assert_eq!(&buf, &data);
    }
}
