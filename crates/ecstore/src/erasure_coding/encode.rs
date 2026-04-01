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

use crate::disk::error::Error;
use crate::disk::error_reduce::count_errs;
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_write_quorum_errs};
use crate::erasure_coding::BitrotWriterWrapper;
use crate::erasure_coding::Erasure;
use crate::erasure_coding::erasure::{EncodeBlockBuffer, EncodedShardBlock, EncodedShardBufferPool};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use rustfs_rio::BlockReadable;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tracing::error;

pub(crate) struct MultiWriter<'a> {
    writers: &'a mut [Option<BitrotWriterWrapper>],
    write_quorum: usize,
    errs: Vec<Option<Error>>,
}

pub(crate) struct BlockAssembler<R> {
    reader: R,
    block_buffer: EncodeBlockBuffer,
    total_bytes: usize,
}

impl<R> BlockAssembler<R>
where
    R: AsyncRead + BlockReadable + Send + Sync + Unpin + 'static,
{
    pub(crate) fn new(reader: R, block_size: usize) -> Self {
        Self {
            reader,
            block_buffer: EncodeBlockBuffer::new(block_size),
            total_bytes: 0,
        }
    }

    pub(crate) async fn next_block(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        match self.block_buffer.read_from_block(&mut self.reader).await {
            Ok(n) if n > 0 => {
                self.total_bytes += n;
                Ok(Some(self.block_buffer.filled(n).to_vec()))
            }
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if let Some(inner) = e.get_ref()
                    && rustfs_rio::is_checksum_mismatch(inner)
                {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Clone)]
pub(crate) struct ErasureChunkEncoder {
    erasure: Arc<Erasure>,
    buffer_pool: EncodedShardBufferPool,
}

impl ErasureChunkEncoder {
    pub(crate) async fn new(erasure: Arc<Erasure>) -> Self {
        let reusable_capacity = erasure.shard_size() * erasure.total_shard_count();
        Self {
            erasure,
            buffer_pool: EncodedShardBufferPool::with_prefill(reusable_capacity, 2).await,
        }
    }

    pub(crate) async fn encode_block(&self, block: &[u8]) -> std::io::Result<EncodedShardBlock> {
        let reusable_buffer = self.buffer_pool.acquire().await;
        self.erasure.encode_data_block_with_buffer(block, reusable_buffer)
    }

    pub(crate) async fn release(&self, block: EncodedShardBlock) {
        self.buffer_pool.release(block).await;
    }
}

pub(crate) trait ShardSource {
    fn shard_count(&self) -> usize;
    fn shard(&self, idx: usize) -> Bytes;
}

impl ShardSource for EncodedShardBlock {
    fn shard_count(&self) -> usize {
        self.shard_count()
    }

    fn shard(&self, idx: usize) -> Bytes {
        self.shard(idx)
    }
}

impl ShardSource for Vec<Bytes> {
    fn shard_count(&self) -> usize {
        self.len()
    }

    fn shard(&self, idx: usize) -> Bytes {
        self[idx].clone()
    }
}

impl<'a> MultiWriter<'a> {
    pub fn new(writers: &'a mut [Option<BitrotWriterWrapper>], write_quorum: usize) -> Self {
        let length = writers.len();
        MultiWriter {
            writers,
            write_quorum,
            errs: vec![None; length],
        }
    }

    async fn write_shard(writer_opt: &mut Option<BitrotWriterWrapper>, err: &mut Option<Error>, shard: Bytes) {
        match writer_opt {
            Some(writer) => {
                match writer.write(&shard).await {
                    Ok(n) => {
                        if n < shard.len() {
                            *err = Some(Error::ShortWrite);
                            *writer_opt = None; // Mark as failed
                        } else {
                            *err = None;
                        }
                    }
                    Err(e) => {
                        *err = Some(Error::from(e));
                    }
                }
            }
            None => {
                *err = Some(Error::DiskNotFound);
            }
        }
    }

    pub async fn write<T>(&mut self, data: &T) -> std::io::Result<()>
    where
        T: ShardSource,
    {
        assert_eq!(data.shard_count(), self.writers.len());

        {
            let mut futures = FuturesUnordered::new();
            for (idx, (writer_opt, err)) in self.writers.iter_mut().zip(self.errs.iter_mut()).enumerate() {
                if err.is_some() {
                    continue; // Skip if we already have an error for this writer
                }
                futures.push(Self::write_shard(writer_opt, err, data.shard(idx)));
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
            error!(
                "reduce_write_quorum_errs: {:?}, offline-disks={}/{}, errs={:?}",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len(),
                self.errs
            );
            return Err(std::io::Error::other(format!(
                "Failed to write data: {} (offline-disks={}/{})",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len()
            )));
        }

        Err(std::io::Error::other(format!(
            "Failed to write data:  (offline-disks={}/{}): {}",
            count_errs(&self.errs, &Error::DiskNotFound),
            self.writers.len(),
            self.errs
                .iter()
                .map(|e| e.as_ref().map_or("<nil>".to_string(), |e| e.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    async fn shutdown_writer(writer_opt: &mut Option<BitrotWriterWrapper>, err: &mut Option<Error>) {
        match writer_opt {
            Some(writer) => match writer.shutdown().await {
                Ok(()) => {
                    *err = None;
                }
                Err(e) => {
                    *err = Some(Error::from(e));
                    *writer_opt = None;
                }
            },
            None => {
                *err = Some(Error::DiskNotFound);
            }
        }
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        {
            let mut futures = FuturesUnordered::new();
            for (writer_opt, err) in self.writers.iter_mut().zip(self.errs.iter_mut()) {
                if err.is_some() {
                    continue;
                }
                futures.push(Self::shutdown_writer(writer_opt, err));
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
            error!(
                "reduce_write_quorum_errs during shutdown: {:?}, offline-disks={}/{}, errs={:?}",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len(),
                self.errs
            );
            return Err(std::io::Error::other(format!(
                "Failed to shutdown writers: {} (offline-disks={}/{})",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len()
            )));
        }

        Err(std::io::Error::other(format!(
            "Failed to shutdown writers: (offline-disks={}/{}): {}",
            count_errs(&self.errs, &Error::DiskNotFound),
            self.writers.len(),
            self.errs
                .iter()
                .map(|e| e.as_ref().map_or("<nil>".to_string(), |e| e.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
}

impl Erasure {
    pub async fn encode<R>(
        self: Arc<Self>,
        reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + BlockReadable + Send + Sync + Unpin + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<EncodedShardBlock>(8);
        let producer = ErasureChunkEncoder::new(self.clone()).await;
        let writer_pool = producer.clone();

        let task = tokio::spawn(async move {
            let mut assembler = BlockAssembler::new(reader, self.block_size);
            while let Some(block) = assembler.next_block().await? {
                let res = producer.encode_block(&block).await?;
                if let Err(err) = tx.send(res).await {
                    return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                }
            }

            let total = assembler.total_bytes();
            Ok((assembler.into_inner(), total))
        });

        let mut writers = MultiWriter::new(writers, quorum);

        while let Some(block) = rx.recv().await {
            if block.is_empty() {
                break;
            }
            writers.write(&block).await?;
            writer_pool.release(block).await;
        }

        let (reader, total) = task.await??;
        writers.shutdown().await?;
        Ok((reader, total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure_coding::{BitrotWriterWrapper, CustomWriter};
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    #[derive(Clone, Default)]
    struct DeferredCommitWriter {
        buffered: Vec<u8>,
        committed: Arc<Mutex<Vec<u8>>>,
    }

    impl DeferredCommitWriter {
        fn new(committed: Arc<Mutex<Vec<u8>>>) -> Self {
            Self {
                buffered: Vec::new(),
                committed,
            }
        }
    }

    impl AsyncWrite for DeferredCommitWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.buffered.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let buffered = std::mem::take(&mut self.buffered);
            let mut committed = self.committed.lock().unwrap();
            committed.extend_from_slice(&buffered);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn encode_shutdowns_writers_after_small_shards() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed.clone());
        let mut writers = vec![Some(BitrotWriterWrapper::new(
            CustomWriter::new_tokio_writer(writer),
            16,
            HashAlgorithm::HighwayHash256S,
        ))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(b"small payload".to_vec()));
        let (_reader, written) = erasure.encode(reader, &mut writers, 1).await.unwrap();

        assert_eq!(written, b"small payload".len());
        assert!(!committed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn block_assembler_splits_input_into_erasure_blocks() {
        let reader = tokio::io::BufReader::new(Cursor::new(b"abcdefghijkl".to_vec()));
        let mut assembler = BlockAssembler::new(reader, 4);

        assert_eq!(assembler.next_block().await.unwrap(), Some(b"abcd".to_vec()));
        assert_eq!(assembler.next_block().await.unwrap(), Some(b"efgh".to_vec()));
        assert_eq!(assembler.next_block().await.unwrap(), Some(b"ijkl".to_vec()));
        assert_eq!(assembler.next_block().await.unwrap(), None);
        assert_eq!(assembler.total_bytes(), 12);
    }

    #[tokio::test]
    async fn erasure_chunk_encoder_produces_full_shard_block() {
        let erasure = Arc::new(Erasure::new(2, 1, 4));
        let encoder = ErasureChunkEncoder::new(erasure.clone()).await;
        let block = encoder.encode_block(b"abcd").await.unwrap();

        assert_eq!(block.shard_count(), 3);
        assert_eq!(block.shard(0).len(), erasure.shard_size());

        encoder.release(block).await;
    }
}
