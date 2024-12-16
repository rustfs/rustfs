use std::io::Read;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
pub enum Reader {
    #[default]
    NotUse,
    File(File),
    Buffer(VecAsyncReader),
}

impl AsyncRead for Reader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Reader::File(file) => Pin::new(file).poll_read(cx, buf),
            Reader::Buffer(buffer) => Pin::new(buffer).poll_read(cx, buf),
            Reader::NotUse => Poll::Ready(Ok(())),
        }
    }
}

#[derive(Default)]
pub enum Writer {
    #[default]
    NotUse,
    File(File),
    Buffer(VecAsyncWriter),
}

impl AsyncWrite for Writer {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Writer::File(file) => Pin::new(file).poll_write(cx, buf),
            Writer::Buffer(buff) => Pin::new(buff).poll_write(cx, buf),
            Writer::NotUse => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Writer::File(file) => Pin::new(file).poll_flush(cx),
            Writer::Buffer(buff) => Pin::new(buff).poll_flush(cx),
            Writer::NotUse => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Writer::File(file) => Pin::new(file).poll_shutdown(cx),
            Writer::Buffer(buff) => Pin::new(buff).poll_shutdown(cx),
            Writer::NotUse => Poll::Ready(Ok(())),
        }
    }
}

pub struct AsyncToSync<R> {
    inner: R,
}

impl<R: AsyncRead + Unpin> AsyncToSync<R> {
    pub fn new_reader(inner: R) -> Self {
        Self { inner }
    }
    fn read_async(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        // Poll the underlying AsyncRead to fill the ReadBuf
        match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R: AsyncWrite + Unpin> AsyncToSync<R> {
    pub fn new_writer(inner: R) -> Self {
        Self { inner }
    }
    // This function will perform a write using AsyncWrite
    fn write_async(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        match result {
            Poll::Ready(Ok(n)) => Poll::Ready(Ok(n)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    // This function will perform a flush using AsyncWrite
    fn flush_async(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
}

impl<R: AsyncRead + Unpin> Read for AsyncToSync<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        loop {
            match self.read_async(&mut cx, buf) {
                Poll::Ready(Ok(n)) => return Ok(n),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {
                    // If Pending, we need to wait for the readiness.
                    // Here, we can use an arbitrary mechanism to yield control,
                    // this might be blocking until some readiness occurs can be complex.
                    // A full blocking implementation would require an async runtime to block on.
                    std::thread::sleep(std::time::Duration::from_millis(1)); // Replace with proper waiting if needed
                }
            }
        }
    }
}

impl<W: AsyncWrite + Unpin> Write for AsyncToSync<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        loop {
            match self.write_async(&mut cx, buf) {
                Poll::Ready(Ok(n)) => return Ok(n),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {
                    // Here we are blocking and waiting for the async operation to complete.
                    std::thread::sleep(std::time::Duration::from_millis(1)); // Not efficient, see notes.
                }
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        loop {
            match self.flush_async(&mut cx) {
                Poll::Ready(Ok(())) => return Ok(()),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {
                    // Again, blocking to wait for flush.
                    std::thread::sleep(std::time::Duration::from_millis(1)); // Not efficient, see notes.
                }
            }
        }
    }
}

pub struct VecAsyncWriter {
    buffer: Vec<u8>,
}

impl VecAsyncWriter {
    /// Create a new VecAsyncWriter with an empty Vec<u8>.  
    pub fn new(buffer: Vec<u8>) -> Self {
        VecAsyncWriter { buffer }
    }

    /// Retrieve the underlying buffer.  
    pub fn get_buffer(&self) -> &[u8] {
        &self.buffer
    }
}

// Implementing AsyncWrite trait for VecAsyncWriter
impl AsyncWrite for VecAsyncWriter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let len = buf.len();

        // Assume synchronous writing for simplicity
        self.get_mut().buffer.extend_from_slice(buf);

        // Returning the length of written data
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // In this case, flushing is a no-op for a Vec<u8>
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Similar to flush, shutdown has no effect here
        Poll::Ready(Ok(()))
    }
}

pub struct VecAsyncReader {
    buffer: Vec<u8>,
    position: usize,
}

impl VecAsyncReader {
    /// Create a new VecAsyncReader with the given Vec<u8>.  
    pub fn new(buffer: Vec<u8>) -> Self {
        VecAsyncReader { buffer, position: 0 }
    }

    /// Reset the reader position.  
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

// Implementing AsyncRead trait for VecAsyncReader
impl AsyncRead for VecAsyncReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Check how many bytes are available to read
        let len = this.buffer.len();
        let bytes_available = len - this.position;

        if bytes_available == 0 {
            // If there's no more data to read, return ready with an Eof
            return Poll::Ready(Ok(()));
        }

        // Calculate how much we can read into the provided buffer
        let to_read = std::cmp::min(bytes_available, buf.remaining());

        // Write the data to the buf
        buf.put_slice(&this.buffer[this.position..this.position + to_read]);

        // Update the position
        this.position += to_read;

        // Indicate how many bytes were read
        Poll::Ready(Ok(()))
    }
}
