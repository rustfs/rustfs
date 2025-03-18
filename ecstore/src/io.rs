use futures::TryStreamExt;
use md5::Digest;
use md5::Md5;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tracing::error;
use tracing::warn;

pub type FileReader = Box<dyn AsyncRead + Send + Sync + Unpin>;
pub type FileWriter = Box<dyn AsyncWrite + Send + Sync + Unpin>;

pub const READ_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Debug)]
pub struct HttpFileWriter {
    wd: tokio::io::DuplexStream,
    err_rx: oneshot::Receiver<std::io::Error>,
}

impl HttpFileWriter {
    pub fn new(url: &str, disk: &str, volume: &str, path: &str, size: usize, append: bool) -> std::io::Result<Self> {
        let (rd, wd) = tokio::io::duplex(READ_BUFFER_SIZE);

        let (err_tx, err_rx) = oneshot::channel::<std::io::Error>();

        let body = reqwest::Body::wrap_stream(ReaderStream::with_capacity(rd, READ_BUFFER_SIZE));

        let url = url.to_owned();
        let disk = disk.to_owned();
        let volume = volume.to_owned();
        let path = path.to_owned();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            if let Err(err) = client
                .put(format!(
                    "{}/rustfs/rpc/put_file_stream?disk={}&volume={}&path={}&append={}&size={}",
                    url,
                    urlencoding::encode(&disk),
                    urlencoding::encode(&volume),
                    urlencoding::encode(&path),
                    append,
                    size
                ))
                .body(body)
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            {
                error!("HttpFileWriter put file err: {:?}", err);

                if let Err(er) = err_tx.send(err) {
                    error!("HttpFileWriter tx.send err: {:?}", er);
                }
            }
        });

        Ok(Self { wd, err_rx })
    }
}

impl AsyncWrite for HttpFileWriter {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        if let Ok(err) = self.as_mut().err_rx.try_recv() {
            return Poll::Ready(Err(err));
        }

        Pin::new(&mut self.wd).poll_write(cx, buf)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.wd).poll_flush(cx)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.wd).poll_shutdown(cx)
    }
}

pub struct HttpFileReader {
    inner: FileReader,
}

impl HttpFileReader {
    pub async fn new(url: &str, disk: &str, volume: &str, path: &str, offset: usize, length: usize) -> std::io::Result<Self> {
        let resp = reqwest::Client::new()
            .get(format!(
                "{}/rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}",
                url,
                urlencoding::encode(disk),
                urlencoding::encode(volume),
                urlencoding::encode(path),
                offset,
                length
            ))
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let inner = Box::new(StreamReader::new(resp.bytes_stream().map_err(std::io::Error::other)));

        Ok(Self { inner })
    }
}

impl AsyncRead for HttpFileReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<tokio::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

pub struct EtagReader<R> {
    inner: R,
    md5: Md5,
}

impl<R> EtagReader<R> {
    pub fn new(inner: R) -> Self {
        EtagReader { inner, md5: Md5::new() }
    }

    pub fn etag(self) -> String {
        hex_simd::encode_to_string(self.md5.finalize(), hex_simd::AsciiCase::Lower)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for EtagReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<tokio::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes = buf.filled();
                self.md5.update(bytes);

                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
