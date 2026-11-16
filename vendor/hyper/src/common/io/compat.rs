use std::pin::Pin;
use std::task::{Context, Poll};

/// This adapts from `hyper` IO traits to the ones in Tokio.
///
/// This is currently used by `h2`, and by hyper internal unit tests.
#[derive(Debug)]
pub(crate) struct Compat<T>(pub(crate) T);

impl<T> Compat<T> {
    pub(crate) fn new(io: T) -> Self {
        Compat(io)
    }

    fn p(self: Pin<&mut Self>) -> Pin<&mut T> {
        // SAFETY: The simplest of projections. This is just
        // a wrapper, we don't do anything that would undo the projection.
        unsafe { self.map_unchecked_mut(|me| &mut me.0) }
    }
}

impl<T> tokio::io::AsyncRead for Compat<T>
where
    T: crate::rt::Read,
{
    /// `poll_read` fn implementation for `Compat<T>`.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let init = tbuf.initialized().len();
        let filled = tbuf.filled().len();
        // SAFETY:
        // 1. `tbuf.inner_mut()` returns a raw pointer/mutable slice which we wrap into
        //    a `crate::rt::ReadBuf` that is layout-compatible with the source `tokio::io::ReadBuf`.
        // 2. We explicitly restore the `init` and `filled` states from the original buffer
        //    to maintain the invariant that the new `ReadBuf` tracks the same progress.
        // 3. The underlying memory remains valid and uniquely accessible via `tbuf` for
        //    the duration of this poll operation.
        let (new_init, new_filled) = unsafe {
            let mut buf = crate::rt::ReadBuf::uninit(tbuf.inner_mut());
            buf.set_init(init);
            buf.set_filled(filled);

            match crate::rt::Read::poll_read(self.p(), cx, buf.unfilled()) {
                Poll::Ready(Ok(())) => (buf.init_len(), buf.len()),
                other => return other,
            }
        };

        let n_init = new_init - init;
        // SAFETY:
        // 1. `tbuf.assume_init(n_init)` is safe because `crate::rt::Read::poll_read`
        //    guarantees that the bytes written into the buffer were initialized.
        // 2. `tbuf.set_filled(new_filled)` is safe because `new_filled` is derived
        //    directly from the buffer state after the successful read operation.
        unsafe {
            tbuf.assume_init(n_init);
            tbuf.set_filled(new_filled);
        }

        Poll::Ready(Ok(()))
    }
}

impl<T> tokio::io::AsyncWrite for Compat<T>
where
    T: crate::rt::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        crate::rt::Write::poll_write(self.p(), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        crate::rt::Write::poll_flush(self.p(), cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        crate::rt::Write::poll_shutdown(self.p(), cx)
    }

    fn is_write_vectored(&self) -> bool {
        crate::rt::Write::is_write_vectored(&self.0)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        crate::rt::Write::poll_write_vectored(self.p(), cx, bufs)
    }
}

#[cfg(test)]
impl<T> crate::rt::Read for Compat<T>
where
    T: tokio::io::AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: crate::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(self.p(), cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
impl<T> crate::rt::Write for Compat<T>
where
    T: tokio::io::AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.p(), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.p(), cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.p(), cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.0)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.p(), cx, bufs)
    }
}
