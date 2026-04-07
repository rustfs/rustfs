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

use crate::{BlockReadable, BoxReadBlockFuture, Reader};
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
    }
}

impl<R> HardLimitReader<R> {
    pub fn new(inner: R, limit: i64) -> Self {
        HardLimitReader { inner, remaining: limit }
    }
}

impl<R> AsyncRead for HardLimitReader<R>
where
    R: AsyncRead,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
        if self.remaining < 0 {
            return Poll::Ready(Err(Error::other("input provided more bytes than specified")));
        }
        // Save the initial length
        let before = buf.filled().len();

        // Poll the inner reader
        let this = self.as_mut().project();
        let poll = this.inner.poll_read(cx, buf);

        if let Poll::Ready(Ok(())) = &poll {
            let after = buf.filled().len();
            let read = (after - before) as i64;
            *this.remaining -= read;
            if *this.remaining < 0 {
                return Poll::Ready(Err(Error::other("input provided more bytes than specified")));
            }
        }
        poll
    }
}

delegate_reader_capabilities_generic!(HardLimitReader<R>, inner);

impl<R> BlockReadable for HardLimitReader<R>
where
    R: Reader,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        Box::pin(async move {
            if self.remaining < 0 {
                return Err(Error::other("input provided more bytes than specified"));
            }

            let max_len = match usize::try_from(self.remaining) {
                Ok(remaining) => remaining.min(buf.len()),
                Err(_) => buf.len(),
            };

            if max_len == 0 {
                let mut probe = [0_u8; 1];
                match self.inner.read_block(&mut probe).await {
                    Ok(0) => return Ok(0),
                    Ok(n) => {
                        self.remaining -= n as i64;
                        return Err(Error::other("input provided more bytes than specified"));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(0),
                    Err(err) => return Err(err),
                }
            }

            let n = self.inner.read_block(&mut buf[..max_len]).await?;
            self.remaining -= n as i64;
            if self.remaining < 0 {
                return Err(Error::other("input provided more bytes than specified"));
            }

            Ok(n)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::{BlockReadable, WarpReader};
    use rustfs_utils::read_full;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_hardlimit_reader_normal() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, 20);
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
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[tokio::test]
    async fn test_hardlimit_reader_empty() {
        let data = b"";
        let reader = BufReader::new(&data[..]);
        let hardlimit = HardLimitReader::new(reader, 5);
        let mut r = hardlimit;
        let mut buf = Vec::new();
        let n = r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_hardlimit_reader_read_block_enforces_limit() {
        let data = b"abcdef";
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
        let mut hardlimit = HardLimitReader::new(reader, 3);

        let mut buf = [0_u8; 8];
        let n = hardlimit.read_block(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"abc");

        let err = hardlimit.read_block(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }
}
