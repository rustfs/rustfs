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

use crate::bitrot::{BitrotReader, BitrotWriter};
use crate::disk::error::{Error, Result};
use crate::disk::error_reduce::{reduce_write_quorum_errs, OBJECT_OP_IGNORED_ERRS};
use crate::io::Etag;
use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use reed_solomon_erasure::galois_8::ReedSolomon;
use smallvec::SmallVec;
use std::any::Any;
use std::io::ErrorKind;
use std::sync::{mpsc, Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::warn;
use tracing::{error, info};
use uuid::Uuid;

use crate::disk::error::DiskError;

#[derive(Default)]
pub struct Erasure {
    data_shards: usize,
    parity_shards: usize,
    encoder: Option<ReedSolomon>,
    pub block_size: usize,
    _id: Uuid,
    _buf: Vec<u8>,
}

impl Erasure {
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        // debug!(
        //     "Erasure new data_shards {},parity_shards {} block_size {} ",
        //     data_shards, parity_shards, block_size
        // );
        let mut encoder = None;
        if parity_shards > 0 {
            encoder = Some(ReedSolomon::new(data_shards, parity_shards).unwrap());
        }

        Erasure {
            data_shards,
            parity_shards,
            block_size,
            encoder,
            _id: Uuid::new_v4(),
            _buf: vec![0u8; block_size],
        }
    }

    #[tracing::instrument(level = "info", skip(self, reader, writers))]
    pub async fn encode<S>(
        self: Arc<Self>,
        mut reader: S,
        writers: &mut [Option<BitrotWriter>],
        // block_size: usize,
        total_size: usize,
        write_quorum: usize,
    ) -> Result<(usize, String)>
    where
        S: AsyncRead + Etag + Unpin + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel(5);
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; self.block_size];
            let mut total: usize = 0;
            loop {
                if total_size > 0 {
                    let new_len = {
                        let remain = total_size - total;
                        if remain > self.block_size { self.block_size } else { remain }
                    };

                    if new_len == 0 && total > 0 {
                        break;
                    }

                    buf.resize(new_len, 0u8);
                    match reader.read_exact(&mut buf).await {
                        Ok(res) => res,
                        Err(e) => {
                            if let ErrorKind::UnexpectedEof = e.kind() {
                                break;
                            } else {
                                return Err(e.into());
                            }
                        }
                    };
                    total += buf.len();
                }
                let blocks = Arc::new(Box::pin(self.clone().encode_data(&buf)?));
                let _ = tx.send(blocks).await;
                if total_size == 0 {
                    break;
                }
            }
            let etag = reader.etag().await;
            Ok((total, etag))
        });

        while let Some(blocks) = rx.recv().await {
            let write_futures = writers.iter_mut().enumerate().map(|(i, w_op)| {
                let i_inner = i;
                let blocks_inner = blocks.clone();
                async move {
                    if let Some(w) = w_op {
                        w.write(blocks_inner[i_inner].clone()).await.err()
                    } else {
                        Some(DiskError::DiskNotFound)
                    }
                }
            });
            let errs = join_all(write_futures).await;
            let none_count = errs.iter().filter(|&x| x.is_none()).count();
            if none_count >= write_quorum {
                if total_size == 0 {
                    break;
                }
                continue;
            }

            if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                warn!("Erasure encode errs {:?}", &errs);
                return Err(err);
            }
        }
        task.await?
    }

    pub async fn decode<W>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> (usize, Option<Error>)
    where
        W: AsyncWriteExt + Send + Unpin + 'static,
    {
        if length == 0 {
            return (0, None);
        }

        let mut reader = ShardReader::new(readers, self, offset, total_length);

        // debug!("ShardReader {:?}", &reader);

        let start_block = offset / self.block_size;
        let end_block = (offset + length) / self.block_size;

        // debug!("decode block from {} to {}", start_block, end_block);

        let mut bytes_written = 0;

        for block_idx in start_block..=end_block {
            let (block_offset, block_length) = if start_block == end_block {
                (offset % self.block_size, length)
            } else if block_idx == start_block {
                let block_offset = offset % self.block_size;
                (block_offset, self.block_size - block_offset)
            } else if block_idx == end_block {
                (0, (offset + length) % self.block_size)
            } else {
                (0, self.block_size)
            };

            if block_length == 0 {
                // debug!("block_length == 0 break");
                break;
            }

            // debug!("decode {} block_offset {},block_length {} ", block_idx, block_offset, block_length);

            let mut bufs = match reader.read().await {
                Ok(bufs) => bufs,
                Err(err) => return (bytes_written, Some(err)),
            };

            if self.parity_shards > 0 {
                if let Err(err) = self.decode_data(&mut bufs) {
                    return (bytes_written, Some(err));
                }
            }

            let written_n = match self
                .write_data_blocks(writer, bufs, self.data_shards, block_offset, block_length)
                .await
            {
                Ok(n) => n,
                Err(err) => {
                    error!("write_data_blocks err {:?}", &err);
                    return (bytes_written, Some(err));
                }
            };

            bytes_written += written_n;

            // debug!("decode {} written_n {}, total_written: {} ", block_idx, written_n, bytes_written);
        }

        if bytes_written != length {
            // debug!("bytes_written != length: {} != {} ", bytes_written, length);
            return (bytes_written, Some(Error::other("erasure decode less data")));
        }

        (bytes_written, None)
    }

    async fn write_data_blocks<W>(
        &self,
        writer: &mut W,
        bufs: Vec<Option<Vec<u8>>>,
        data_blocks: usize,
        offset: usize,
        length: usize,
    ) -> Result<usize>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        if bufs.len() < data_blocks {
            return Err(Error::other("read bufs not match data_blocks"));
        }

        let data_len: usize = bufs
            .iter()
            .take(data_blocks)
            .filter(|v| v.is_some())
            .map(|v| v.as_ref().unwrap().len())
            .sum();
        if data_len < length {
            return Err(Error::other(format!("write_data_blocks data_len < length {} < {}", data_len, length)));
        }

        let mut offset = offset;

        // debug!("write_data_blocks offset {}, length {}", offset, length);

        let mut write = length;
        let mut total_written = 0;

        for opt_buf in bufs.iter().take(data_blocks) {
            let buf = opt_buf.as_ref().unwrap();

            if offset >= buf.len() {
                offset -= buf.len();
                continue;
            }

            let buf = &buf[offset..];

            offset = 0;

            // debug!("write_data_blocks write buf len {}", buf.len());

            if write < buf.len() {
                let buf = &buf[..write];

                // debug!("write_data_blocks write buf less len {}", buf.len());
                writer.write_all(buf).await?;
                // debug!("write_data_blocks write done len {}", buf.len());
                total_written += buf.len();
                break;
            }

            writer.write_all(buf).await?;
            let n = buf.len();

            // debug!("write_data_blocks write done len {}", n);
            write -= n;
            total_written += n;
        }

        Ok(total_written)
    }

    pub fn total_shard_count(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    #[tracing::instrument(level = "info", skip_all, fields(data_len=data.len()))]
    pub fn encode_data(self: Arc<Self>, data: &[u8]) -> Result<Vec<Bytes>> {
        let (shard_size, total_size) = self.need_size(data.len());

        // Generate the total length required for all shards
        let mut data_buffer = BytesMut::with_capacity(total_size);

        // Copy the source data
        data_buffer.extend_from_slice(data);
        data_buffer.resize(total_size, 0u8);

        {
            // Perform EC encoding; the results go into data_buffer
            let data_slices: SmallVec<[&mut [u8]; 16]> = data_buffer.chunks_exact_mut(shard_size).collect();

            // Only perform EC encoding when parity shards are present
            if self.parity_shards > 0 {
                self.encoder.as_ref().unwrap().encode(data_slices).map_err(Error::other)?;
            }
        }

        // Zero-copy shards: every shard references data_buffer
        let mut data_buffer = data_buffer.freeze();
        let mut shards = Vec::with_capacity(self.total_shard_count());
        for _ in 0..self.total_shard_count() {
            let shard = data_buffer.split_to(shard_size);
            shards.push(shard);
        }

        Ok(shards)
    }

    pub fn decode_data(&self, shards: &mut [Option<Vec<u8>>]) -> Result<()> {
        if self.parity_shards > 0 {
            self.encoder.as_ref().unwrap().reconstruct(shards).map_err(Error::other)?;
        }

        Ok(())
    }

    // The length per shard and the total required length
    fn need_size(&self, data_size: usize) -> (usize, usize) {
        let shard_size = self.shard_size(data_size);
        (shard_size, shard_size * (self.total_shard_count()))
    }

    // Compute each shard size
    pub fn shard_size(&self, data_size: usize) -> usize {
        data_size.div_ceil(self.data_shards)
    }
    // returns final erasure size from original size.
    pub fn shard_file_size(&self, total_size: usize) -> usize {
        if total_size == 0 {
            return 0;
        }

        let num_shards = total_size / self.block_size;
        let last_block_size = total_size % self.block_size;
        let last_shard_size = last_block_size.div_ceil(self.data_shards);
        num_shards * self.shard_size(self.block_size) + last_shard_size

        // When writing, EC pads the data so the last shard length should match
        // if last_block_size != 0 {
        //     num_shards += 1
        // }
        // num_shards * self.shard_size(self.block_size)
    }

    // where erasure reading begins.
    pub fn shard_file_offset(&self, start_offset: usize, length: usize, total_length: usize) -> usize {
        let shard_size = self.shard_size(self.block_size);
        let shard_file_size = self.shard_file_size(total_length);
        let end_shard = (start_offset + length) / self.block_size;
        let mut till_offset = end_shard * shard_size + shard_size;
        if till_offset > shard_file_size {
            till_offset = shard_file_size;
        }

        till_offset
    }

    pub async fn heal(
        &self,
        writers: &mut [Option<BitrotWriter>],
        readers: Vec<Option<BitrotReader>>,
        total_length: usize,
        _prefer: &[bool],
    ) -> Result<()> {
        info!(
            "Erasure heal, writers len: {}, readers len: {}, total_length: {}",
            writers.len(),
            readers.len(),
            total_length
        );
        if writers.len() != self.parity_shards + self.data_shards {
            return Err(Error::other("invalid argument"));
        }
        let mut reader = ShardReader::new(readers, self, 0, total_length);

        let start_block = 0;
        let mut end_block = total_length / self.block_size;
        if total_length % self.block_size != 0 {
            end_block += 1;
        }

        let mut errs = Vec::new();
        for _ in start_block..end_block {
            let mut bufs = reader.read().await?;

            if self.parity_shards > 0 {
                self.encoder.as_ref().unwrap().reconstruct(&mut bufs).map_err(Error::other)?;
            }

            let shards = bufs.into_iter().flatten().map(Bytes::from).collect::<Vec<_>>();
            if shards.len() != self.parity_shards + self.data_shards {
                return Err(Error::other("can not reconstruct data"));
            }

            for (i, w) in writers.iter_mut().enumerate() {
                if w.is_none() {
                    continue;
                }
                match w.as_mut().unwrap().write(shards[i].clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        info!("write failed, err: {:?}", e);
                        errs.push(e);
                    }
                }
            }
        }
        if !errs.is_empty() {
            return Err(errs[0].clone().into());
        }

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait Writer {
    fn as_any(&self) -> &dyn Any;
    async fn write(&mut self, buf: Bytes) -> Result<()>;
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait ReadAt {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)>;
}

pub struct ShardReader {
    readers: Vec<Option<BitrotReader>>, // Disk readers
    data_block_count: usize,            // Total number of shards
    parity_block_count: usize,
    shard_size: usize,      // Block size per shard (read one block at a time)
    shard_file_size: usize, // Total size of the shard file
    offset: usize,          // Offset within the shard
}

impl ShardReader {
    pub fn new(readers: Vec<Option<BitrotReader>>, ec: &Erasure, offset: usize, total_length: usize) -> Self {
        Self {
            readers,
            data_block_count: ec.data_shards,
            parity_block_count: ec.parity_shards,
            shard_size: ec.shard_size(ec.block_size),
            shard_file_size: ec.shard_file_size(total_length),
            offset: (offset / ec.block_size) * ec.shard_size(ec.block_size),
        }
    }

    pub async fn read(&mut self) -> Result<Vec<Option<Vec<u8>>>> {
        // let mut disks = self.readers;
        let reader_length = self.readers.len();
        // Length of the block to read
        let mut read_length = self.shard_size;
        if self.offset + read_length > self.shard_file_size {
            read_length = self.shard_file_size - self.offset
        }

        if read_length == 0 {
            return Ok(vec![None; reader_length]);
        }

        // debug!("shard reader read offset {}, shard_size {}", self.offset, read_length);

        let mut futures = Vec::with_capacity(reader_length);
        let mut errors = Vec::with_capacity(reader_length);

        let mut ress = Vec::with_capacity(reader_length);

        for disk in self.readers.iter_mut() {
            // if disk.is_none() {
            //     ress.push(None);
            //     errors.push(Some(Error::new(DiskError::DiskNotFound)));
            //     continue;
            // }

            // let disk: &mut BitrotReader = disk.as_mut().unwrap();
            let offset = self.offset;
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_at(offset, read_length).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok((res, _)) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        if !self.can_decode(&ress) {
            warn!("ec decode read ress {:?}", &ress);
            warn!("ec decode read errors {:?}", &errors);

            return Err(Error::other("shard reader read failed"));
        }

        self.offset += self.shard_size;

        Ok(ress)
    }

    fn can_decode(&self, bufs: &[Option<Vec<u8>>]) -> bool {
        let c = bufs.iter().filter(|v| v.is_some()).count();
        if self.parity_block_count > 0 {
            c >= self.data_block_count
        } else {
            c == self.data_block_count
        }
    }
}

// fn shards_to_option_shards<T: Clone>(shards: &[Vec<T>]) -> Vec<Option<Vec<T>>> {
//     let mut result = Vec::with_capacity(shards.len());

//     for v in shards.iter() {
//         let inner: Vec<T> = v.clone();
//         result.push(Some(inner));
//     }
//     result
// }

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_erasure() {
        let data_shards = 3;
        let parity_shards = 2;
        let data: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let ec = Erasure::new(data_shards, parity_shards, 1);
        let shards = Arc::new(ec).encode_data(data).unwrap();
        println!("shards:{:?}", shards);

        let mut s: Vec<_> = shards
            .iter()
            .map(|d| if d.is_empty() { None } else { Some(d.to_vec()) })
            .collect();

        // let mut s = shards_to_option_shards(&shards);

        // s[0] = None;
        s[4] = None;
        s[3] = None;

        println!("sss:{:?}", &s);

        let ec = Erasure::new(data_shards, parity_shards, 1);
        ec.decode_data(&mut s).unwrap();
        // ec.encoder.reconstruct(&mut s).unwrap();

        println!("sss:{:?}", &s);
    }
}
