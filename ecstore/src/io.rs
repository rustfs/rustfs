use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
pub enum Reader {
    #[default]
    NotUse,
    File(File),
}

impl AsyncRead for Reader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Reader::File(file) => {
                let file = Pin::new(file);
                file.poll_read(cx, buf)
            }
            Reader::NotUse => Poll::Ready(Ok(())),
        }
    }
}

#[derive(Default)]
pub enum Writer {
    #[default]
    NotUse,
    File(File),
}

impl AsyncWrite for Writer {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Writer::File(file) => {
                // Create a pinned reference from the file
                let file = Pin::new(file);
                file.poll_write(cx, buf)
            }
            Writer::NotUse => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Writer::File(file) => {
                let file = Pin::new(file);
                file.poll_flush(cx)
            }
            Writer::NotUse => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Writer::File(file) => {
                let file = Pin::new(file);
                file.poll_shutdown(cx)
            }
            Writer::NotUse => Poll::Ready(Ok(())),
        }
    }
}

// #[tokio::test]
// async fn test_reader{

// }
