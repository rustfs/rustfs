use crate::error::Result;
use futures::TryStreamExt;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::AsyncWrite;
use tokio::sync::oneshot;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tracing::error;
use tracing::warn;

use super::FileReader;

#[derive(Debug)]
pub struct HttpFileWriter {
    wd: tokio::io::WriteHalf<tokio::io::SimplexStream>,
    err_rx: oneshot::Receiver<std::io::Error>,
}

impl HttpFileWriter {
    pub fn new(url: &str, disk: &str, volume: &str, path: &str, size: usize, append: bool) -> Result<Self> {
        let (rd, wd) = tokio::io::simplex(4096);

        let (err_tx, err_rx) = oneshot::channel::<std::io::Error>();

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
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            {
                error!("HttpFileWriter put file err: {:?}", err);

                if let Err(er) = err_tx.send(err) {
                    error!("HttpFileWriter tx.send err: {:?}", er);
                }
                // return;
            }

            // error!("http write done {}", path);
        });

        Ok(Self {
            wd,
            err_rx,
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

pub async fn new_http_reader(
    url: &str,
    disk: &str,
    volume: &str,
    path: &str,
    offset: usize,
    length: usize,
) -> Result<FileReader> {
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
        .await?;

    let inner = StreamReader::new(resp.bytes_stream().map_err(std::io::Error::other));

    Ok(Box::new(inner))
}
