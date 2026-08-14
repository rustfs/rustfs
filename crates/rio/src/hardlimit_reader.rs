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

use crate::IncompleteBody;
use pin_project_lite::pin_project;
use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    pub struct HardLimitReader<R> {
        #[pin]
        pub inner: R,
        remaining: i64,
        scratch: Vec<u8>,
    }
}

impl<R> HardLimitReader<R> {
    pub fn new(inner: R, limit: i64) -> Self {
        HardLimitReader {
            inner,
            remaining: limit,
            scratch: Vec::new(),
        }
    }
}

impl<R> AsyncRead for HardLimitReader<R>
where
    R: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
        let mut this = self.project();
        if *this.remaining < 0 {
            return Poll::Ready(Err(Error::other("input provided more bytes than specified")));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if *this.remaining == 0 {
            let mut discard = [0u8; 8192];
            let mut discard_buf = ReadBuf::new(&mut discard);
            return match this.inner.as_mut().poll_read(cx, &mut discard_buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    if discard_buf.filled().is_empty() {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(Error::other("input provided more bytes than specified")))
                    }
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            };
        }

        let remaining = match usize::try_from(*this.remaining) {
            Ok(remaining) => remaining,
            Err(_) => usize::MAX,
        };
        let allowed = remaining.min(buf.remaining());
        let read = if allowed == buf.remaining() {
            let before = buf.filled().len();
            match this.inner.as_mut().poll_read(cx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => buf.filled().len() - before,
            }
        } else {
            this.scratch.resize(allowed, 0);
            let mut scratch_buf = ReadBuf::new(&mut this.scratch[..allowed]);
            match this.inner.as_mut().poll_read(cx, &mut scratch_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    let read = scratch_buf.filled().len();
                    buf.put_slice(scratch_buf.filled());
                    read
                }
            }
        };
        if read == 0 {
            return Poll::Ready(Err(Error::new(
                std::io::ErrorKind::UnexpectedEof,
                IncompleteBody {
                    remaining: *this.remaining,
                },
            )));
        }
        let read = match i64::try_from(read) {
            Ok(read) => read,
            Err(_) => return Poll::Ready(Err(Error::other("read count exceeds i64::MAX"))),
        };
        *this.remaining -= read;
        Poll::Ready(Ok(()))
    }
}

delegate_reader_capabilities_generic!(HardLimitReader<R>, inner);

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use rustfs_utils::read_full;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_hardlimit_reader_normal() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, data.len() as i64);
        let mut r = hardlimit;
        let mut buf = Vec::new();
        let n = r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_hardlimit_reader_exact_limit() {
        let data = b"1234567890";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, 10);
        let mut r = hardlimit;
        let mut buf = Vec::new();
        let n = r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 10);
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_hardlimit_reader_exceed_limit() {
        let data = b"abcdef";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, 3);
        let mut r = hardlimit;
        let mut buf = vec![0u8; 10];
        // Reading exceeds limit, should return error
        let err = match read_full(&mut r, &mut buf).await {
            Ok(n) => {
                println!("Read {n} bytes");
                assert_eq!(n, 3);
                assert_eq!(&buf[..n], b"abc");
                None
            }
            Err(e) => Some(e),
        };

        assert!(err.is_some());

        let err = err.unwrap();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(
            err.get_ref()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .is_some_and(|source| source.to_string().contains("more bytes than specified"))
        );
    }

    #[tokio::test]
    async fn test_hardlimit_reader_empty() {
        let data = b"";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, 0);
        let mut r = hardlimit;
        let mut buf = Vec::new();
        let n = r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_hardlimit_reader_zero_capacity_read_does_not_consume_input() {
        let mut reader = HardLimitReader::new(BufReader::new(&b"abc"[..]), 3);
        let mut empty = [];

        assert_eq!(reader.read(&mut empty).await.expect("zero-capacity read should succeed"), 0);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("input should remain readable");
        assert_eq!(out, b"abc");
    }

    #[tokio::test]
    async fn test_hardlimit_reader_short_input_returns_unexpected_eof() {
        let data = b"abc";
        let reader = BufReader::new(&data[..]);
        let mut r = HardLimitReader::new(reader, 5);
        let mut buf = [0u8; 8];

        let err = read_full(&mut r, &mut buf)
            .await
            .expect_err("short input must surface unexpected eof");

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(
            err.get_ref()
                .and_then(|inner| inner.downcast_ref::<std::io::Error>())
                .and_then(|inner| inner.get_ref())
                .and_then(|inner| inner.downcast_ref::<IncompleteBody>())
                .is_some(),
            "error should retain the incomplete body marker"
        );
    }

    #[tokio::test]
    async fn test_hardlimit_reader_rejects_extra_bytes_after_limit() {
        let data = b"abcdef";
        let reader = BufReader::new(&data[..]);
        let mut r = HardLimitReader::new(reader, 3);

        let mut first = [0u8; 3];
        let n = read_full(&mut r, &mut first).await.expect("first read should consume limit");
        assert_eq!(n, 3);
        assert_eq!(&first, b"abc");

        let mut second = [0u8; 1];
        let err = read_full(&mut r, &mut second)
            .await
            .expect_err("bytes beyond the declared limit must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("more bytes than specified"));
    }

    #[tokio::test]
    async fn test_hardlimit_reader_caps_each_read_before_reporting_extra_bytes() {
        let mut reader = HardLimitReader::new(BufReader::new(&b"abcdef"[..]), 3);
        let mut out = Vec::new();

        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("bytes beyond the declared limit must be rejected");

        assert_eq!(out, b"abc");
        assert!(err.to_string().contains("more bytes than specified"));
    }
}
