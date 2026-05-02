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
    if en_blocks.len() < data_blocks {
        return Err(io::Error::new(ErrorKind::InvalidInput, "data block count exceeds available shards"));
    }

    if length == 0 {
        return Ok(0);
    }

    let Some(required_len) = offset.checked_add(length) else {
        return Err(io::Error::new(ErrorKind::InvalidInput, "offset + length overflows"));
    };
    if get_data_block_len(en_blocks, data_blocks) < required_len {
        error!("write_data_blocks not enough data after offset");
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

        let write_len = write_left.min(block_slice.len());
        writer.write_all(&block_slice[..write_len]).await.map_err(|e| {
            error!("write_data_blocks write_all err: {}", e);
            e
        })?;

        total_written += write_len;
        write_left -= write_len;

        if write_left == 0 {
            return Ok(total_written);
        }
    }

    error!("write_data_blocks loop exhausted with write_left>0");
    Err(io::Error::new(ErrorKind::UnexpectedEof, "Not enough data blocks to write"))
}

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
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_write_data_blocks_writes_range_across_blocks() {
        let blocks = vec![Some(vec![1, 2, 3, 4]), Some(vec![5, 6, 7]), Some(vec![8, 9])];
        let mut out = Vec::new();

        let written = write_data_blocks(&mut out, &blocks, 3, 2, 5).await.unwrap();

        assert_eq!(written, 5);
        assert_eq!(out, vec![3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_write_data_blocks_rejects_short_data_after_offset() {
        let blocks = vec![Some(vec![1, 2, 3, 4]), Some(vec![5, 6, 7])];
        let mut out = Vec::new();

        let err = write_data_blocks(&mut out, &blocks, 2, 3, 5).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn test_write_data_blocks_rejects_invalid_data_block_count() {
        let blocks = vec![Some(vec![1, 2, 3, 4])];
        let mut out = Vec::new();

        let err = write_data_blocks(&mut out, &blocks, 2, 0, 1).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(out.is_empty());
    }

    /// Regression for upstream issue #2716: ranged GETs going through
    /// `Erasure::decode` must return the requested byte range without
    /// panicking or truncating, including when the range starts at a
    /// non-zero offset and crosses EC block boundaries.
    #[tokio::test]
    async fn test_erasure_decode_ranged_read_returns_correct_bytes() {
        const DATA_SHARDS: usize = 4;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        // 200 bytes spans 3 full blocks + 1 partial block, exercising
        // the start/middle/end branches in `Erasure::decode`.
        let total_data: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let total_len = total_data.len();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::HighwayHash256;

        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        let mut offset = 0;
        while offset < total_len {
            let end = (offset + BLOCK_SIZE).min(total_len);
            let shards = erasure.encode_data(&total_data[offset..end]).unwrap();
            for (i, shard) in shards.iter().enumerate() {
                shard_writers[i].write(shard).await.unwrap();
            }
            offset = end;
        }

        let shard_bufs: Vec<Vec<u8>> = shard_writers.into_iter().map(|w| w.into_inner().into_inner()).collect();

        // `Erasure::decode` does not seek the readers; the production caller
        // (`create_bitrot_reader`) positions each reader at the shard byte
        // offset corresponding to the request's start block. Mirror that here.
        let hash_size = hash_algo.size();
        let make_readers = |off: usize| -> Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> {
            let start_block = off / BLOCK_SIZE;
            let cursor_pos = start_block * (shard_size + hash_size);
            shard_bufs
                .iter()
                .map(|buf| {
                    let mut cursor = Cursor::new(buf.clone());
                    cursor.set_position(cursor_pos as u64);
                    Some(BitrotReader::new(cursor, shard_size, hash_algo.clone(), false))
                })
                .collect()
        };

        // (offset, length, description)
        let cases: &[(usize, usize, &str)] = &[
            (0, total_len, "full read"),
            (0, 50, "head from start, partial block"),
            (10, 30, "small range within first block"),
            (60, 80, "range crossing two block boundaries"),
            (128, 50, "range starting at block boundary"),
            (130, 10, "small range deep in middle"),
            (192, 8, "tail covering last partial block"),
        ];

        for &(off, len, desc) in cases {
            let mut output = Vec::new();
            let (written, err) = erasure.decode(&mut output, make_readers(off), off, len, total_len).await;
            assert!(err.is_none(), "{}: unexpected error: {:?}", desc, err);
            assert_eq!(written, len, "{}: written != length", desc);
            assert_eq!(output, total_data[off..off + len], "{}: bytes mismatch", desc);
        }
    }

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
}
