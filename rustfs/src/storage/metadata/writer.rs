// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::storage::metadata::mx::StorageManager;
use crate::storage::metadata::types::ChunkInfo;
use bytes::Bytes;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

pub struct ChunkedWriter {
    storage_manager: Arc<dyn StorageManager>,
    chunk_size: usize,
    current_chunk: Vec<u8>,
    chunks: Vec<ChunkInfo>,
    offset: u64,
    hasher: blake3::Hasher,
    write_futures: Vec<Pin<Box<dyn Future<Output = rustfs_ecstore::error::Result<()>> + Send>>>,
    last_error: Option<std::io::Error>,
}

impl ChunkedWriter {
    pub fn new(storage_manager: Arc<dyn StorageManager>, chunk_size: usize) -> Self {
        Self {
            storage_manager,
            chunk_size,
            current_chunk: Vec::with_capacity(chunk_size),
            chunks: Vec::new(),
            offset: 0,
            hasher: blake3::Hasher::new(),
            write_futures: Vec::new(),
            last_error: None,
        }
    }

    pub fn chunks(&self) -> Vec<ChunkInfo> {
        self.chunks.clone()
    }

    pub fn content_hash(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }

    fn flush_chunk(&mut self) {
        if self.current_chunk.is_empty() {
            return;
        }

        let chunk_data = Bytes::from(self.current_chunk.clone());
        self.current_chunk.clear();

        let chunk_hash = blake3::hash(&chunk_data).to_hex().to_string();
        let chunk_size = chunk_data.len() as u64;

        self.chunks.push(ChunkInfo {
            hash: chunk_hash.clone(),
            size: chunk_size,
            offset: self.offset,
        });

        self.offset += chunk_size;
        self.hasher.update(&chunk_data);

        let mgr = self.storage_manager.clone();
        let fut = Box::pin(async move {
            if !mgr.exists(&chunk_hash).await {
                mgr.write_data(&chunk_hash, chunk_data).await?;
            }
            Ok(())
        });

        self.write_futures.push(fut);
    }
}

impl AsyncWrite for ChunkedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Some(err) = self.last_error.take() {
            return Poll::Ready(Err(err));
        }

        // Poll pending writes to keep them moving and check for errors
        let mut error = None;
        self.write_futures.retain_mut(|fut| {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => false,
                Poll::Ready(Err(e)) => {
                    error = Some(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
                    false
                }
                Poll::Pending => true,
            }
        });

        if let Some(err) = error {
            return Poll::Ready(Err(err));
        }

        // Simple implementation: append to current_chunk until full
        let remaining = self.chunk_size - self.current_chunk.len();
        let to_write = std::cmp::min(remaining, buf.len());

        self.current_chunk.extend_from_slice(&buf[..to_write]);

        if self.current_chunk.len() >= self.chunk_size {
            self.flush_chunk();
        }

        Poll::Ready(Ok(to_write))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some(err) = self.last_error.take() {
            return Poll::Ready(Err(err));
        }

        // Flush current chunk if any
        if !self.current_chunk.is_empty() {
            self.flush_chunk();
        }

        // Wait for all pending writes
        let mut all_done = true;
        let mut error = None;

        self.write_futures.retain_mut(|fut| {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => false,
                Poll::Ready(Err(e)) => {
                    error = Some(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
                    false
                }
                Poll::Pending => {
                    all_done = false;
                    true
                }
            }
        });

        if let Some(err) = error {
            return Poll::Ready(Err(err));
        }

        if all_done {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}
