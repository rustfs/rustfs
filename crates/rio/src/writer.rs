use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::HttpWriter;

pub enum Writer {
    Cursor(Cursor<Vec<u8>>),
    Http(HttpWriter),
    Other(Box<dyn AsyncWrite + Unpin + Send + Sync>),
}

impl Writer {
    /// Create a Writer::Other from any AsyncWrite + Unpin + Send type.
    pub fn from_tokio_writer<W>(w: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
    {
        Writer::Other(Box::new(w))
    }

    pub fn from_cursor(w: Cursor<Vec<u8>>) -> Self {
        Writer::Cursor(w)
    }

    pub fn from_http(w: HttpWriter) -> Self {
        Writer::Http(w)
    }

    pub fn into_cursor_inner(self) -> Option<Vec<u8>> {
        match self {
            Writer::Cursor(w) => Some(w.into_inner()),
            _ => None,
        }
    }

    pub fn as_cursor(&mut self) -> Option<&mut Cursor<Vec<u8>>> {
        match self {
            Writer::Cursor(w) => Some(w),
            _ => None,
        }
    }
    pub fn as_http(&mut self) -> Option<&mut HttpWriter> {
        match self {
            Writer::Http(w) => Some(w),
            _ => None,
        }
    }

    pub fn into_http(self) -> Option<HttpWriter> {
        match self {
            Writer::Http(w) => Some(w),
            _ => None,
        }
    }

    pub fn into_cursor(self) -> Option<Cursor<Vec<u8>>> {
        match self {
            Writer::Cursor(w) => Some(w),
            _ => None,
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_write(cx, buf),
            Writer::Http(w) => Pin::new(w).poll_write(cx, buf),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_flush(cx),
            Writer::Http(w) => Pin::new(w).poll_flush(cx),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_shutdown(cx),
            Writer::Http(w) => Pin::new(w).poll_shutdown(cx),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_shutdown(cx),
        }
    }
}

/// WriterAll wraps a Writer and ensures each write writes the entire buffer (like write_all).
pub struct WriterAll<W: AsyncWrite + Unpin> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> WriterAll<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Write the entire buffer, like write_all.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> std::io::Result<()> {
        while !buf.is_empty() {
            let n = self.inner.write(buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write whole buffer"));
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    /// Get a mutable reference to the inner writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for WriterAll<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, mut buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let mut total_written = 0;
        while !buf.is_empty() {
            // Safety: W: Unpin
            let inner_pin = Pin::new(&mut self.inner);
            match inner_pin.poll_write(cx, buf) {
                Poll::Ready(Ok(0)) => {
                    if total_written == 0 {
                        return Poll::Ready(Ok(0));
                    } else {
                        return Poll::Ready(Ok(total_written));
                    }
                }
                Poll::Ready(Ok(n)) => {
                    total_written += n;
                    buf = &buf[n..];
                }
                Poll::Ready(Err(e)) => {
                    if total_written == 0 {
                        return Poll::Ready(Err(e));
                    } else {
                        return Poll::Ready(Ok(total_written));
                    }
                }
                Poll::Pending => {
                    if total_written == 0 {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Ok(total_written));
                    }
                }
            }
        }
        Poll::Ready(Ok(total_written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
