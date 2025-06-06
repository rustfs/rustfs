use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{EtagResolvable, HashReaderDetector, Reader};

pub struct WarpReader<R> {
    inner: R,
}

impl<R: AsyncRead + Unpin + Send + Sync> WarpReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for WarpReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> HashReaderDetector for WarpReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> EtagResolvable for WarpReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> Reader for WarpReader<R> {}
