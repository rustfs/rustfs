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
use crate::disk::error_reduce::reduce_errs;
use crate::erasure_coding::{BitrotReader, Erasure};
use futures::stream::{FuturesUnordered, StreamExt};
use pin_project_lite::pin_project;
use rustfs_io_core::{IoChunk, PooledChunk};
use std::io;
use std::io::ErrorKind;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::error;

pin_project! {
pub(crate) struct ParallelReader<R> {
    #[pin]
    readers: Vec<Option<BitrotReader<R>>>,
    offset: usize,
    shard_size: usize,
    shard_file_size: usize,
    data_shards: usize,
    total_shards: usize,
}
}

impl<R> ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    // Readers should handle disk errors before being passed in, ensuring each reader reaches the available number of BitrotReaders
    pub fn new(readers: Vec<Option<BitrotReader<R>>>, e: Erasure, offset: usize, total_length: usize) -> Self {
        let shard_size = e.shard_size();
        let shard_file_size = e.shard_file_size(total_length as i64) as usize;

        let offset = (offset / e.block_size) * shard_size;

        // Ensure offset does not exceed shard_file_size

        ParallelReader {
            readers,
            offset,
            shard_size,
            shard_file_size,
            data_shards: e.data_shards,
            total_shards: e.data_shards + e.parity_shards,
        }
    }
}

impl<R> ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub async fn read(&mut self) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>) {
        // if self.readers.len() != self.total_shards {
        //     return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers"));
        // }
        let num_readers = self.readers.len();

        let shard_size = if self.offset + self.shard_size > self.shard_file_size {
            self.shard_file_size - self.offset
        } else {
            self.shard_size
        };

        if shard_size == 0 {
            return (vec![None; num_readers], vec![None; num_readers]);
        }

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_readers];
        let mut errs = vec![None; num_readers];

        let mut futures = Vec::with_capacity(self.total_shards);
        let reader_iter: std::slice::IterMut<'_, Option<BitrotReader<R>>> = self.readers.iter_mut();
        for (i, reader) in reader_iter.enumerate() {
            let future = if let Some(reader) = reader {
                Box::pin(async move {
                    let mut buf = vec![0u8; shard_size];
                    match reader.read(&mut buf).await {
                        Ok(n) => {
                            buf.truncate(n);
                            (i, Ok(buf))
                        }
                        Err(e) => (i, Err(Error::from(e))),
                    }
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = (usize, Result<Vec<u8>, Error>)> + Send>>
            } else {
                // Return FileNotFound error when reader is None
                Box::pin(async move { (i, Err(Error::FileNotFound)) })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = (usize, Result<Vec<u8>, Error>)> + Send>>
            };

            futures.push(future);
        }

        if futures.len() >= self.data_shards {
            let mut fut_iter = futures.into_iter();
            let mut sets = FuturesUnordered::new();
            for _ in 0..self.data_shards {
                if let Some(future) = fut_iter.next() {
                    sets.push(future);
                }
            }

            let mut success = 0;
            while let Some((i, result)) = sets.next().await {
                match result {
                    Ok(v) => {
                        shards[i] = Some(v);
                        success += 1;
                    }
                    Err(e) => {
                        errs[i] = Some(e);

                        if let Some(future) = fut_iter.next() {
                            sets.push(future);
                        }
                    }
                }

                if success >= self.data_shards {
                    break;
                }
            }
        }

        (shards, errs)
    }

    pub fn can_decode(&self, shards: &[Option<Vec<u8>>]) -> bool {
        shards.iter().filter(|s| s.is_some()).count() >= self.data_shards
    }
}

/// Get the total length of data blocks
fn get_data_block_len(shards: &[Option<Vec<u8>>], data_blocks: usize) -> usize {
    let mut size = 0;
    for shard in shards.iter().take(data_blocks).flatten() {
        size += shard.len();
    }

    size
}

fn block_window(
    offset: usize,
    length: usize,
    block_size: usize,
    block_index: usize,
    start_block: usize,
    end_block: usize,
) -> (usize, usize) {
    let end_remainder = offset.saturating_add(length) % block_size;
    if start_block == end_block {
        (offset % block_size, length)
    } else if block_index == start_block {
        (offset % block_size, block_size - (offset % block_size))
    } else if block_index == end_block {
        (0, if end_remainder == 0 { block_size } else { end_remainder })
    } else {
        (0, block_size)
    }
}

fn take_data_blocks_as_chunks(
    shards: &mut [Option<Vec<u8>>],
    data_blocks: usize,
    mut offset: usize,
    length: usize,
) -> io::Result<Vec<IoChunk>> {
    if get_data_block_len(shards, data_blocks) < length {
        error!("take_data_blocks_as_chunks get_data_block_len < length");
        return Err(io::Error::new(ErrorKind::UnexpectedEof, "Not enough data blocks to write"));
    }

    let mut chunks = Vec::new();
    let mut remaining = length;
    for block_op in shards.iter_mut().take(data_blocks) {
        let Some(block) = block_op.take() else {
            error!("take_data_blocks_as_chunks block_op.is_none()");
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Missing data block"));
        };

        if offset >= block.len() {
            offset -= block.len();
            continue;
        }

        let start = offset;
        offset = 0;
        let take = (block.len() - start).min(remaining);
        let chunk = if start == 0 && take == block.len() {
            IoChunk::Pooled(PooledChunk::from_vec(block))
        } else {
            IoChunk::Pooled(PooledChunk::from_vec(block).slice(start, take)?)
        };
        chunks.push(chunk);
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }

    Ok(chunks)
}

/// Write data blocks from encoded blocks to target, supporting offset and length
async fn write_data_blocks<W>(
    writer: &mut W,
    en_blocks: &[Option<Vec<u8>>],
    data_blocks: usize,
    mut offset: usize,
    length: usize,
) -> std::io::Result<usize>
where
    W: tokio::io::AsyncWrite + Send + Sync + Unpin,
{
    if get_data_block_len(en_blocks, data_blocks) < length {
        error!("write_data_blocks get_data_block_len < length");
        return Err(io::Error::new(ErrorKind::UnexpectedEof, "Not enough data blocks to write"));
    }

    let mut total_written = 0;
    let mut write_left = length;

    for block_op in &en_blocks[..data_blocks] {
        let Some(block) = block_op else {
            error!("write_data_blocks block_op.is_none()");
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Missing data block"));
        };

        if offset >= block.len() {
            offset -= block.len();
            continue;
        }

        let block_slice = &block[offset..];
        offset = 0;

        if write_left < block_slice.len() {
            writer.write_all(&block_slice[..write_left]).await.map_err(|e| {
                error!("write_data_blocks write_all err: {}", e);
                e
            })?;

            total_written += write_left;
            break;
        }

        let n = block_slice.len();

        writer.write_all(block_slice).await.map_err(|e| {
            error!("write_data_blocks write_all2 err: {}", e);
            e
        })?;

        write_left -= n;

        total_written += n;
    }

    Ok(total_written)
}

pub(crate) struct ErasureChunkDecoder<R> {
    erasure: Erasure,
    reader: ParallelReader<R>,
    offset: usize,
    length: usize,
    start_block: usize,
    end_block: usize,
    current_block: usize,
    written: usize,
    healable_error: Option<Error>,
    finished: bool,
}

impl<R> ErasureChunkDecoder<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub(crate) fn new(
        erasure: Erasure,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> io::Result<Self> {
        if readers.len() != erasure.data_shards + erasure.parity_shards {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers"));
        }

        let end_offset = offset
            .checked_add(length)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length"))?;
        if end_offset > total_length {
            return Err(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length"));
        }

        let start_block = offset / erasure.block_size;
        let end_block = if length == 0 {
            start_block
        } else {
            end_offset.saturating_sub(1) / erasure.block_size
        };
        let reader = ParallelReader::new(readers, erasure.clone(), offset, total_length);

        Ok(Self {
            erasure,
            reader,
            offset,
            length,
            start_block,
            end_block,
            current_block: start_block,
            written: 0,
            healable_error: None,
            finished: length == 0,
        })
    }

    pub(crate) async fn next_chunks(&mut self) -> io::Result<Option<Vec<IoChunk>>> {
        if self.finished {
            return Ok(None);
        }

        if self.current_block > self.end_block {
            self.finished = true;
            return Ok(None);
        }

        let block_index = self.current_block;
        self.current_block += 1;

        let (block_offset, block_length) = block_window(
            self.offset,
            self.length,
            self.erasure.block_size,
            block_index,
            self.start_block,
            self.end_block,
        );
        if block_length == 0 {
            self.finished = true;
            return Ok(None);
        }

        let (mut shards, errs) = self.reader.read().await;

        if self.healable_error.is_none()
            && let (_, Some(err)) = reduce_errs(&errs, &[])
            && (err == Error::FileNotFound || err == Error::FileCorrupt)
        {
            self.healable_error = Some(err);
        }

        if !self.reader.can_decode(&shards) {
            self.finished = true;
            error!("reconstructed chunk decoder can_decode errs: {:?}", &errs);
            return Err(Error::ErasureReadQuorum.into());
        }

        if let Err(err) = self.erasure.decode_data(&mut shards) {
            self.finished = true;
            error!("reconstructed chunk decoder decode_data err: {:?}", err);
            return Err(err);
        }

        let chunks = take_data_blocks_as_chunks(&mut shards, self.erasure.data_shards, block_offset, block_length)?;
        self.written += chunks.iter().map(IoChunk::len).sum::<usize>();
        Ok(Some(chunks))
    }

    pub(crate) fn written(&self) -> usize {
        self.written
    }

    pub(crate) fn finish_error(&self) -> Option<io::Error> {
        if self.written < self.length {
            Some(Error::LessData.into())
        } else {
            None
        }
    }

    pub(crate) fn take_healable_error(&mut self) -> Option<Error> {
        self.healable_error.take()
    }
}

pub(crate) type ReconstructedChunkDecoder<R> = ErasureChunkDecoder<R>;

impl Erasure {
    pub async fn decode<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        if readers.len() != self.data_shards + self.parity_shards {
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers")));
        }

        let Some(end_offset) = offset.checked_add(length) else {
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length")));
        };
        if end_offset > total_length {
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length")));
        }

        let mut ret_err = None;

        if length == 0 {
            return (0, ret_err);
        }

        let mut written = 0;

        let mut reader = ParallelReader::new(readers, self.clone(), offset, total_length);

        let start = offset / self.block_size;
        let end = end_offset.saturating_sub(1) / self.block_size;

        for i in start..=end {
            let (block_offset, block_length) = if start == end {
                (offset % self.block_size, length)
            } else if i == start {
                (offset % self.block_size, self.block_size - (offset % self.block_size))
            } else if i == end {
                let end_remainder = end_offset % self.block_size;
                (0, if end_remainder == 0 { self.block_size } else { end_remainder })
            } else {
                (0, self.block_size)
            };

            if block_length == 0 {
                // error!("erasure decode decode block_length == 0");
                break;
            }

            let (mut shards, errs) = reader.read().await;

            if ret_err.is_none()
                && let (_, Some(err)) = reduce_errs(&errs, &[])
                && (err == Error::FileNotFound || err == Error::FileCorrupt)
            {
                ret_err = Some(err.into());
            }

            if !reader.can_decode(&shards) {
                error!("erasure decode can_decode errs: {:?}", &errs);
                ret_err = Some(Error::ErasureReadQuorum.into());
                break;
            }

            // Decode the shards
            if let Err(e) = self.decode_data(&mut shards) {
                error!("erasure decode decode_data err: {:?}", e);
                ret_err = Some(e);
                break;
            }

            let n = match write_data_blocks(writer, &shards, self.data_shards, block_offset, block_length).await {
                Ok(n) => n,
                Err(e) => {
                    error!("erasure decode write_data_blocks err: {:?}", e);
                    ret_err = Some(e);
                    break;
                }
            };

            written += n;
        }

        if ret_err.is_some() {
            return (written, ret_err);
        }

        if written < length {
            ret_err = Some(Error::LessData.into());
        }

        (written, ret_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        disk::error::DiskError,
        erasure_coding::{BitrotReader, BitrotWriter},
    };
    use bytes::Bytes;
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_parallel_reader_normal() {
        const BLOCK_SIZE: usize = 64;
        const NUM_SHARDS: usize = 2;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            readers.push(Some(
                create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, false).await,
            ));
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            bufs.into_iter().enumerate().for_each(|(index, buf)| {
                if index < DATA_SHARDS {
                    assert!(buf.is_some());
                    let buf = buf.unwrap();
                    assert_eq!(SHARD_SIZE, buf.len());
                    assert_eq!(index as u8, buf[0]);
                } else {
                    assert!(buf.is_none());
                }
            });

            assert!(errs.iter().filter(|err| err.is_some()).count() == 0);
        }
    }

    #[tokio::test]
    async fn test_parallel_reader_with_offline_disks() {
        const OFFLINE_DISKS: usize = 2;
        const NUM_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            if i < OFFLINE_DISKS {
                // Two disks are offline
                readers.push(None);
            } else {
                readers.push(Some(
                    create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, false).await,
                ));
            }
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
            assert_eq!(OFFLINE_DISKS, errs.iter().filter(|err| err.is_some()).count());
        }
    }

    #[tokio::test]
    async fn test_parallel_reader_with_bitrots() {
        const BITROT_DISKS: usize = 2;
        const NUM_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            readers.push(Some(
                create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, i < BITROT_DISKS).await,
            ));
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
            assert_eq!(
                BITROT_DISKS,
                errs.iter()
                    .filter(|err| {
                        match err {
                            Some(DiskError::Io(err)) => {
                                err.kind() == std::io::ErrorKind::InvalidData && err.to_string().contains("bitrot")
                            }
                            _ => false,
                        }
                    })
                    .count()
            );
        }
    }

    async fn create_reader(
        shard_size: usize,
        num_shards: usize,
        value: u8,
        hash_algo: &HashAlgorithm,
        bitrot: bool,
    ) -> BitrotReader<Cursor<Vec<u8>>> {
        let len = (hash_algo.size() + shard_size) * num_shards;
        let buf = Cursor::new(vec![0u8; len]);

        let mut writer = BitrotWriter::new(buf, shard_size, hash_algo.clone());
        for _ in 0..num_shards {
            writer.write(vec![value; shard_size].as_slice()).await.unwrap();
        }

        let mut buf = writer.into_inner().into_inner();

        if bitrot {
            for i in 0..num_shards {
                // Rot one bit for each shard
                buf[i * (hash_algo.size() + shard_size)] ^= 1;
            }
        }

        let reader_cursor = Cursor::new(buf);
        BitrotReader::new(reader_cursor, shard_size, hash_algo.clone(), false)
    }

    async fn create_bitrot_reader_from_shard(
        shard: Bytes,
        shard_size: usize,
        hash_algo: &HashAlgorithm,
    ) -> BitrotReader<Cursor<Vec<u8>>> {
        let writer = Cursor::new(Vec::new());
        let mut writer = BitrotWriter::new(writer, shard_size, hash_algo.clone());
        writer.write(shard.as_ref()).await.unwrap();
        let reader_cursor = Cursor::new(writer.into_inner().into_inner());
        BitrotReader::new(reader_cursor, shard_size, hash_algo.clone(), false)
    }

    #[tokio::test]
    async fn test_erasure_chunk_decoder_reconstructs_missing_data_shard_as_pooled_chunks() {
        let erasure = Erasure::new(2, 1, 4);
        let original = b"abcd";
        let encoded = erasure.encode_data(original).unwrap();
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::None;

        let readers = vec![
            None,
            Some(create_bitrot_reader_from_shard(encoded[1].clone(), shard_size, &hash_algo).await),
            Some(create_bitrot_reader_from_shard(encoded[2].clone(), shard_size, &hash_algo).await),
        ];

        let mut decoder = ErasureChunkDecoder::new(erasure, readers, 0, original.len(), original.len()).unwrap();
        let first_batch = decoder.next_chunks().await.unwrap().unwrap();
        assert!(
            first_batch.iter().all(|chunk| matches!(chunk, IoChunk::Pooled(_))),
            "reconstructed decoder should produce pooled chunks"
        );
        let collected = first_batch
            .into_iter()
            .flat_map(|chunk| chunk.as_bytes().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(collected, original);
        assert!(decoder.next_chunks().await.unwrap().is_none());
        assert_eq!(decoder.written(), original.len());
        assert!(decoder.finish_error().is_none());
    }
}
