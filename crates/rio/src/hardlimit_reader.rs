use crate::compress_index::{Index, TryGetIndex};
use crate::{EtagResolvable, HashReaderDetector, HashReaderMut, Reader};
use pin_project_lite::pin_project;
use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    pub struct HardLimitReader {
        #[pin]
        pub inner: Box<dyn Reader>,
        remaining: i64,
    }
}

impl HardLimitReader {
    pub fn new(inner: Box<dyn Reader>, limit: i64) -> Self {
        HardLimitReader { inner, remaining: limit }
    }
}

impl AsyncRead for HardLimitReader {
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
            self.remaining -= read;
            if self.remaining < 0 {
                return Poll::Ready(Err(Error::other("input provided more bytes than specified")));
            }
        }
        poll
    }
}

impl EtagResolvable for HardLimitReader {
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl HashReaderDetector for HardLimitReader {
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }
    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl TryGetIndex for HardLimitReader {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::WarpReader;

    use super::*;
    use rustfs_utils::read_full;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_hardlimit_reader_normal() {
        let data = b"hello world";
        let reader = BufReader::new(&data[..]);
        let reader = Box::new(WarpReader::new(reader));
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
        let reader = Box::new(WarpReader::new(reader));
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
        let reader = Box::new(WarpReader::new(reader));
        let hardlimit = HardLimitReader::new(reader, 3);
        let mut r = hardlimit;
        let mut buf = vec![0u8; 10];
        // 读取超限，应该返回错误
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
        let reader = Box::new(WarpReader::new(reader));
        let hardlimit = HardLimitReader::new(reader, 5);
        let mut r = hardlimit;
        let mut buf = Vec::new();
        let n = r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert_eq!(&buf, data);
    }
}
