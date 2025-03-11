use crate::error::Result;
use futures::TryStreamExt;
use std::io::Cursor;
use std::pin::Pin;
use std::task::Poll;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tracing::error;
use tracing::warn;

#[derive(Debug)]
pub enum FileReader {
    Local(File),
    // Remote(RemoteFileReader),
    Buffer(Cursor<Vec<u8>>),
    Http(HttpFileReader),
}

impl AsyncRead for FileReader {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(reader) => Pin::new(reader).poll_read(cx, buf),
            Self::Buffer(reader) => Pin::new(reader).poll_read(cx, buf),
            Self::Http(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

#[derive(Debug)]
pub struct HttpFileReader {
    // client: reqwest::Client,
    // url: String,
    // disk: String,
    // volume: String,
    // path: String,
    // offset: usize,
    // length: usize,
    inner: tokio::io::DuplexStream,
    // buf: Vec<u8>,
    // pos: usize,
}

impl HttpFileReader {
    pub fn new(url: &str, disk: &str, volume: &str, path: &str, offset: usize, length: usize) -> Result<Self> {
        warn!("http read start {}", path);
        let url = url.to_owned();
        let disk = disk.to_owned();
        let volume = volume.to_owned();
        let path = path.to_owned();

        // let (reader, mut writer) = tokio::io::simplex(1024);
        let (reader, mut writer) = tokio::io::duplex(1024 * 1024 * 10);

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let resp = match client
                .get(format!(
                    "{}/rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}",
                    url,
                    urlencoding::encode(&disk),
                    urlencoding::encode(&volume),
                    urlencoding::encode(&path),
                    offset,
                    length
                ))
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            {
                Ok(resp) => resp,
                Err(err) => {
                    warn!("http file reader error: {}", err);
                    return;
                }
            };

            let mut rd = StreamReader::new(
                resp.bytes_stream()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            if let Err(err) = tokio::io::copy(&mut rd, &mut writer).await {
                error!("http file reader copy error: {}", err);
            };
        });
        Ok(Self {
            // client: reqwest::Client::new(),
            // url: url.to_string(),
            // disk: disk.to_string(),
            // volume: volume.to_string(),
            // path: path.to_string(),
            // offset,
            // length,
            inner: reader,
            // buf: Vec::new(),
            // pos: 0,
        })
    }
}

impl AsyncRead for HttpFileReader {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub enum FileWriter {
    Local(File),
    Http(HttpFileWriter),
    Buffer(Cursor<Vec<u8>>),
}

impl AsyncWrite for FileWriter {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Buffer(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Http(writer) => Pin::new(writer).poll_write(cx, buf),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_flush(cx),
            Self::Buffer(writer) => Pin::new(writer).poll_flush(cx),
            Self::Http(writer) => Pin::new(writer).poll_flush(cx),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::result::Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Buffer(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Http(writer) => Pin::new(writer).poll_shutdown(cx),
        }
    }
}

#[derive(Debug)]
pub struct HttpFileWriter {
    wd: tokio::io::WriteHalf<tokio::io::SimplexStream>,
}

impl HttpFileWriter {
    pub fn new(url: &str, disk: &str, volume: &str, path: &str, size: usize, append: bool) -> Result<Self> {
        let (rd, wd) = tokio::io::simplex(1024 * 1024 * 10);

        let body = reqwest::Body::wrap_stream(ReaderStream::new(rd));

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
            {
                error!("HttpFileWriter put file err: {:?}", err);
                // return;
            }

            // TODO: handle response

            // debug!("http write done {}", path);
        });

        Ok(Self {
            wd,
            // client: reqwest::Client::new(),
            // url: url.to_string(),
            // disk: disk.to_string(),
            // volume: volume.to_string(),
        })
    }
}

impl AsyncWrite for HttpFileWriter {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
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
