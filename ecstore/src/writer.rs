use std::{io, task::Poll};

use futures::{ready, Future};
use tokio::io::{AsyncWrite, BufWriter};
use tracing::debug;
use uuid::Uuid;

use crate::disk::DiskStore;

pub struct AppendWriter<'a> {
    disk: DiskStore,
    volume: &'a str,
    path: &'a str,
}

impl<'a> AppendWriter<'a> {
    pub fn new(disk: DiskStore, volume: &'a str, path: &'a str) -> Self {
        debug!("AppendWriter new {}: {}/{}", disk.id(), volume, path);
        Self { disk, volume, path }
    }

    async fn async_write(&self, buf: &[u8]) -> Result<(), std::io::Error> {
        debug!("async_write {}: {}: {}", self.disk.id(), &self.path, buf.len());

        // self.disk
        //     .append_file(&self.volume, &self.path, buf)
        //     .await
        //     .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(())
    }
}

impl<'a> AsyncWrite for AppendWriter<'a> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let mut fut = Box::pin(self.async_write(buf));
        debug!("AsyncWrite poll_write {}, buf:{}", self.disk.id(), buf.len());

        // while let Poll::Ready(e) = fut.as_mut().poll(cx) {
        //     let a = match e {
        //         Ok(_) => {
        //             debug!("Ready ok {}", self.disk.id());
        //             Poll::Ready(Ok(buf.len()))
        //         }
        //         Err(e) => {
        //             debug!("Ready err {}", self.disk.id());
        //             Poll::Ready(Err(e))
        //         }
        //     };

        //     return a;
        // }

        // Poll::Pending

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                debug!("Pending {}", self.disk.id());
                Poll::Pending
            }
            Poll::Ready(e) => match e {
                Ok(_) => {
                    debug!("Ready ok {}", self.disk.id());
                    Poll::Ready(Ok(buf.len()))
                }
                Err(e) => {
                    debug!("Ready err {}", self.disk.id());
                    Poll::Ready(Err(e))
                }
            },
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}
