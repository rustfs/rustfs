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

//! Compatibility adapter that exposes chunk streams as `AsyncRead`.

use crate::chunk::BoxChunkStream;
use bytes::Bytes;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// `AsyncRead` adapter for boxed chunk streams.
pub struct ChunkStreamReader {
    stream: BoxChunkStream,
    current: Option<Bytes>,
    offset: usize,
}

impl ChunkStreamReader {
    #[must_use]
    pub fn new(stream: BoxChunkStream) -> Self {
        Self {
            stream,
            current: None,
            offset: 0,
        }
    }
}

impl AsyncRead for ChunkStreamReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            if let Some(current) = &self.current {
                if self.offset < current.len() {
                    let remaining = &current[self.offset..];
                    let to_read = remaining.len().min(buf.remaining());
                    buf.put_slice(&remaining[..to_read]);
                    self.offset += to_read;
                    return Poll::Ready(Ok(()));
                }

                self.current = None;
                self.offset = 0;
                continue;
            }

            match self.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    let next = chunk.as_bytes();
                    if next.is_empty() {
                        continue;
                    }
                    self.current = Some(next);
                    self.offset = 0;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{IoChunk, MappedChunk, PooledChunk};
    use bytes::Bytes;
    use futures_util::stream;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_chunk_stream_reader_reads_single_chunk() {
        let stream: BoxChunkStream = Box::pin(stream::iter(vec![Ok(IoChunk::Shared(Bytes::from_static(b"hello")))]));
        let mut reader = ChunkStreamReader::new(stream);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello");
    }

    #[tokio::test]
    async fn test_chunk_stream_reader_reads_multiple_chunks() {
        let stream: BoxChunkStream = Box::pin(stream::iter(vec![
            Ok(IoChunk::Shared(Bytes::from_static(b"he"))),
            Ok(IoChunk::Mapped(MappedChunk::new(Bytes::from_static(b"llo!"), 0, 4).unwrap())),
            Ok(IoChunk::Pooled(PooledChunk::from_bytes(Bytes::from_static(b" world")).unwrap())),
        ]));
        let mut reader = ChunkStreamReader::new(stream);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello! world");
    }

    #[tokio::test]
    async fn test_chunk_stream_reader_handles_empty_stream() {
        let stream: BoxChunkStream = Box::pin(stream::iter(Vec::<io::Result<IoChunk>>::new()));
        let mut reader = ChunkStreamReader::new(stream);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn test_chunk_stream_reader_propagates_stream_error() {
        let stream: BoxChunkStream = Box::pin(stream::iter(vec![Err(io::Error::other("chunk stream failure"))]));
        let mut reader = ChunkStreamReader::new(stream);
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("chunk stream failure"));
    }
}
