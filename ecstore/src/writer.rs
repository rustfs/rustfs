use std::{io, task::Poll};

use futures::Future;
use tokio::io::AsyncWrite;
use tracing::debug;

use crate::disk::DiskStore;

pub struct AppendWriter<'a> {
    disk: DiskStore,
    volume: &'a str,
    path: &'a str,
}

impl<'a> AppendWriter<'a> {
    pub fn new(disk: DiskStore, volume: &'a str, path: &'a str) -> Self {
        Self { disk, volume, path }
    }

    async fn async_write(&self, buf: &[u8]) -> Result<(), std::io::Error> {
        debug!("async_write {}: {}", &self.path, buf.len());

        unimplemented!()
    }
}

impl<'a> AsyncWrite for AppendWriter<'a> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let mut fut = Box::pin(self.async_write(buf));

        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Ok(buf.len())),
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
