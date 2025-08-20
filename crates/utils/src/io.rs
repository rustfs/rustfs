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

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Write all bytes from buf to writer, returning the total number of bytes written.
pub async fn write_all<W: AsyncWrite + Send + Sync + Unpin>(writer: &mut W, buf: &[u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match writer.write(&buf[total..]).await {
            Ok(0) => {
                break;
            }
            Ok(n) => total += n,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

/// Read exactly buf.len() bytes into buf, or return an error if EOF is reached before.
/// Like Go's io.ReadFull.
#[allow(dead_code)]
pub async fn read_full<R: AsyncRead + Send + Sync + Unpin>(mut reader: R, mut buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while !buf.is_empty() {
        let n = match reader.read(buf).await {
            Ok(n) => n,
            Err(e) => {
                if total == 0 {
                    return Err(e);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("read {total} bytes, error: {e}"),
                ));
            }
        };
        if n == 0 {
            if total > 0 {
                return Ok(total);
            }
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "early EOF"));
        }
        buf = &mut buf[n..];
        total += n;
    }
    Ok(total)
}

/// Encodes a u64 into buf and returns the number of bytes written.
/// Panics if buf is too small.
pub fn put_uvarint(buf: &mut [u8], x: u64) -> usize {
    let mut i = 0;
    let mut x = x;
    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i + 1
}

pub fn put_uvarint_len(x: u64) -> usize {
    let mut i = 0;
    let mut x = x;
    while x >= 0x80 {
        x >>= 7;
        i += 1;
    }
    i + 1
}

/// Decodes a u64 from buf and returns (value, number of bytes read).
/// If buf is too small, returns (0, 0).
/// If overflow, returns (0, -(n as isize)), where n is the number of bytes read.
pub fn uvarint(buf: &[u8]) -> (u64, isize) {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            // MaxVarintLen64 = 10
            return (0, -((i + 1) as isize));
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return (0, -((i + 1) as isize));
            }
            return (x | ((b as u64) << s), (i + 1) as isize);
        }
        x |= ((b & 0x7F) as u64) << s;
        s += 7;
    }
    (0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;
    use tracing::debug;

    #[tokio::test]
    async fn test_read_full_exact() {
        // let data = b"abcdef";
        let data = b"channel async callback test data!";
        let mut reader = BufReader::new(&data[..]);
        let size = data.len();

        let mut total = 0;
        let mut rev = vec![0u8; size];

        let mut count = 0;

        while total < size {
            let mut buf = [0u8; 8];
            let n = read_full(&mut reader, &mut buf).await.unwrap();
            total += n;
            rev[total - n..total].copy_from_slice(&buf[..n]);

            count += 1;
            debug!("Read progress - count: {}, total: {}, bytes read: {}", count, total, n);
        }
        assert_eq!(total, size);

        assert_eq!(&rev, data);
    }

    #[tokio::test]
    async fn test_read_full_short() {
        let data = b"abc";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = [0u8; 6];
        let n = read_full(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], data);
    }

    #[tokio::test]
    async fn test_read_full_1m() {
        let size = 1024 * 1024;
        let data = vec![42u8; size];
        let mut reader = BufReader::new(&data[..]);
        let mut buf = vec![0u8; size / 3];
        read_full(&mut reader, &mut buf).await.unwrap();
        assert_eq!(buf, data[..size / 3]);
    }

    #[test]
    fn test_put_uvarint_and_uvarint_zero() {
        let mut buf = [0u8; 16];
        let n = put_uvarint(&mut buf, 0);
        let (decoded, m) = uvarint(&buf[..n]);
        assert_eq!(decoded, 0);
        assert_eq!(m as usize, n);
    }

    #[test]
    fn test_put_uvarint_and_uvarint_max() {
        let mut buf = [0u8; 16];
        let n = put_uvarint(&mut buf, u64::MAX);
        let (decoded, m) = uvarint(&buf[..n]);
        assert_eq!(decoded, u64::MAX);
        assert_eq!(m as usize, n);
    }

    #[test]
    fn test_put_uvarint_and_uvarint_various() {
        let mut buf = [0u8; 16];
        for &v in &[1u64, 127, 128, 255, 300, 16384, u32::MAX as u64] {
            let n = put_uvarint(&mut buf, v);
            let (decoded, m) = uvarint(&buf[..n]);
            assert_eq!(decoded, v, "decode mismatch for {v}");
            assert_eq!(m as usize, n, "length mismatch for {v}");
        }
    }

    #[test]
    fn test_uvarint_incomplete() {
        let buf = [0x80u8, 0x80, 0x80];
        let (v, n) = uvarint(&buf);
        assert_eq!(v, 0);
        assert_eq!(n, 0);
    }

    #[test]
    fn test_uvarint_overflow_case() {
        let buf = [0xFFu8; 11];
        let (v, n) = uvarint(&buf);
        assert_eq!(v, 0);
        assert!(n < 0);
    }

    #[tokio::test]
    async fn test_write_all_basic() {
        let data = b"hello world!";
        let mut buf = Vec::new();
        let n = write_all(&mut buf, data).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_write_all_partial() {
        struct PartialWriter {
            inner: Vec<u8>,
            max_write: usize,
        }
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;
        impl AsyncWrite for PartialWriter {
            fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
                let n = buf.len().min(self.max_write);
                self.inner.extend_from_slice(&buf[..n]);
                Poll::Ready(Ok(n))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let data = b"abcdefghijklmnopqrstuvwxyz";
        let mut writer = PartialWriter {
            inner: Vec::new(),
            max_write: 5,
        };
        let n = write_all(&mut writer, data).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&writer.inner, data);
    }
}
