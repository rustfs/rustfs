use crate::bitrot::{BitrotReader, BitrotWriter};
use crate::error::{Error, Result, StdError};
use crate::quorum::{object_op_ignored_errs, reduce_write_quorum_errs};
use bytes::Bytes;
use futures::future::join_all;
use futures::{pin_mut, Stream, StreamExt};
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::any::Any;
use std::fmt::Debug;
use std::io::ErrorKind;
use tokio::io::DuplexStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;
use tracing::warn;
// use tracing::debug;
use uuid::Uuid;

// use reader::reader::ChunkedStream;
// use crate::chunk_stream::ChunkedStream;
use crate::disk::error::DiskError;

pub struct Erasure {
    data_shards: usize,
    parity_shards: usize,
    encoder: Option<ReedSolomon>,
    pub block_size: usize,
    _id: Uuid,
    buf: Vec<u8>,
}

impl Erasure {
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        debug!(
            "Erasure new data_shards {},parity_shards {} block_size {} ",
            data_shards, parity_shards, block_size
        );
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
            buf: vec![0u8; block_size],
        }
    }

    #[tracing::instrument(level = "debug", skip(self, body, writers))]
    pub async fn encode<S>(
        &mut self,
        body: S,
        writers: &mut [Option<BitrotWriter>],
        // block_size: usize,
        total_size: usize,
        write_quorum: usize,
    ) -> Result<usize>
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + Sync,
    {
        pin_mut!(body);
        let mut reader = tokio_util::io::StreamReader::new(
            body.map(|f| f.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))),
        );

        let mut total: usize = 0;
        loop {
            let new_len = {
                let remain = total_size - total;
                if remain > self.block_size {
                    self.block_size
                } else {
                    remain
                }
            };

            if new_len == 0 {
                break;
            }

            self.buf.resize(new_len, 0u8);

            match reader.read_exact(&mut self.buf).await {
                Ok(res) => res,
                Err(e) => {
                    if let ErrorKind::UnexpectedEof = e.kind() {
                        break;
                    } else {
                        return Err(Error::new(e));
                    }
                }
            };

            total += self.buf.len();

            let blocks = self.encode_data(&self.buf)?;
            let mut errs = Vec::new();

            for (i, w_op) in writers.iter_mut().enumerate() {
                if let Some(w) = w_op {
                    match w.write(blocks[i].as_ref()).await {
                        Ok(_) => errs.push(None),
                        Err(e) => errs.push(Some(e)),
                    }
                } else {
                    errs.push(Some(Error::new(DiskError::DiskNotFound)));
                }
            }

            let none_count = errs.iter().filter(|&x| x.is_none()).count();
            if none_count >= write_quorum {
                continue;
            }

            if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
                warn!("Erasure encode errs {:?}", &errs);
                return Err(err);
            }
        }

        Ok(total)

        // // let stream = ChunkedStream::new(body, self.block_size);
        // let stream = ChunkedStream::new(body, total_size, self.block_size, false);
        // let mut total: usize = 0;
        // // let mut idx = 0;
        // pin_mut!(stream);

        // // warn!("encode start...");

        // loop {
        //     match stream.next().await {
        //         Some(result) => match result {
        //             Ok(data) => {
        //                 total += data.len();

        //                 // EOF
        //                 if data.is_empty() {
        //                     break;
        //                 }

        //                 // idx += 1;
        //                 // warn!("encode {} get data {:?}", data.len(), data.to_vec());

        //                 let blocks = self.encode_data(data.as_ref())?;

        //                 // warn!(
        //                 //     "encode shard  size: {}/{} from block_size {}, total_size {} ",
        //                 //     blocks[0].len(),
        //                 //     blocks.len(),
        //                 //     data.len(),
        //                 //     total_size
        //                 // );

        //                 let mut errs = Vec::new();

        //                 for (i, w_op) in writers.iter_mut().enumerate() {
        //                     if let Some(w) = w_op {
        //                         match w.write(blocks[i].as_ref()).await {
        //                             Ok(_) => errs.push(None),
        //                             Err(e) => errs.push(Some(e)),
        //                         }
        //                     } else {
        //                         errs.push(Some(Error::new(DiskError::DiskNotFound)));
        //                     }
        //                 }

        //                 let none_count = errs.iter().filter(|&x| x.is_none()).count();
        //                 if none_count >= write_quorum {
        //                     continue;
        //                 }

        //                 if let Some(err) = reduce_write_quorum_errs(&errs, object_op_ignored_errs().as_ref(), write_quorum) {
        //                     warn!("Erasure encode errs {:?}", &errs);
        //                     return Err(err);
        //                 }
        //             }
        //             Err(e) => {
        //                 warn!("poll result err {:?}", &e);
        //                 return Err(Error::msg(e.to_string()));
        //             }
        //         },
        //         None => {
        //             // warn!("poll empty result");
        //             break;
        //         }
        //     }
        // }

        // let _ = close_bitrot_writers(writers).await?;

        // Ok(total)
    }

    pub async fn decode(
        &self,
        writer: &mut DuplexStream,
        readers: Vec<Option<BitrotReader>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> Result<usize> {
        if length == 0 {
            return Ok(0);
        }

        let mut reader = ShardReader::new(readers, self, offset, total_length);

        // debug!("ShardReader {:?}", &reader);

        let start_block = offset / self.block_size;
        let end_block = (offset + length) / self.block_size;

        // debug!("decode block from {} to {}", start_block, end_block);

        let mut bytes_writed = 0;

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

            let mut bufs = reader.read().await?;

            if self.parity_shards > 0 {
                self.decode_data(&mut bufs)?;
            }

            let writed_n = self
                .write_data_blocks(writer, bufs, self.data_shards, block_offset, block_length)
                .await?;

            bytes_writed += writed_n;

            // debug!("decode {} writed_n {}, total_writed: {} ", block_idx, writed_n, bytes_writed);
        }

        if bytes_writed != length {
            // debug!("bytes_writed != length: {} != {} ", bytes_writed, length);
            return Err(Error::msg("erasure decode less data"));
        }

        Ok(bytes_writed)
    }

    async fn write_data_blocks(
        &self,
        writer: &mut DuplexStream,
        bufs: Vec<Option<Vec<u8>>>,
        data_blocks: usize,
        offset: usize,
        length: usize,
    ) -> Result<usize> {
        if bufs.len() < data_blocks {
            return Err(Error::msg("read bufs not match data_blocks"));
        }

        let data_len: usize = bufs
            .iter()
            .take(data_blocks)
            .filter(|v| v.is_some())
            .map(|v| v.as_ref().unwrap().len())
            .sum();
        if data_len < length {
            return Err(Error::msg(format!("write_data_blocks data_len < length {} < {}", data_len, length)));
        }

        let mut offset = offset;

        // debug!("write_data_blocks offset {}, length {}", offset, length);

        let mut write = length;
        let mut total_writed = 0;

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
                total_writed += buf.len();
                break;
            }

            writer.write_all(buf).await?;
            let n = buf.len();

            // debug!("write_data_blocks write done len {}", n);
            write -= n;
            total_writed += n;
        }

        Ok(total_writed)
    }

    pub fn total_shard_count(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    pub fn encode_data(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let (shard_size, total_size) = self.need_size(data.len());

        // 生成一个新的 所需的所有分片数据长度
        let mut data_buffer = vec![0u8; total_size];
        {
            // 复制源数据
            let (left, _) = data_buffer.split_at_mut(data.len());
            left.copy_from_slice(data);
        }

        {
            // ec encode, 结果会写进 data_buffer
            let data_slices: Vec<&mut [u8]> = data_buffer.chunks_mut(shard_size).collect();

            // partiy 数量大于0 才ec
            if self.parity_shards > 0 {
                self.encoder.as_ref().unwrap().encode(data_slices)?;
            }
        }

        // 分片
        let mut shards = Vec::with_capacity(self.total_shard_count());

        let slices: Vec<&[u8]> = data_buffer.chunks(shard_size).collect();

        for &d in slices.iter() {
            shards.push(d.to_vec());
        }

        Ok(shards)
    }

    pub fn decode_data(&self, shards: &mut [Option<Vec<u8>>]) -> Result<()> {
        if self.parity_shards > 0 {
            self.encoder.as_ref().unwrap().reconstruct(shards)?;
        }

        Ok(())
    }

    // 每个分片长度，所需要的总长度
    fn need_size(&self, data_size: usize) -> (usize, usize) {
        let shard_size = self.shard_size(data_size);
        (shard_size, shard_size * (self.total_shard_count()))
    }

    // 算出每个分片大小
    pub fn shard_size(&self, data_size: usize) -> usize {
        (data_size + self.data_shards - 1) / self.data_shards
    }
    // returns final erasure size from original size.
    pub fn shard_file_size(&self, total_size: usize) -> usize {
        if total_size == 0 {
            return 0;
        }

        let num_shards = total_size / self.block_size;
        let last_block_size = total_size % self.block_size;
        let last_shard_size = (last_block_size + self.data_shards - 1) / self.data_shards;
        num_shards * self.shard_size(self.block_size) + last_shard_size

        // // 因为写入的时候ec需要补全，所以最后一个长度应该也是一样的
        // if last_block_size != 0 {
        //     num_shards += 1
        // }
        // num_shards * self.shard_size(self.block_size)
    }

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
}

#[async_trait::async_trait]
pub trait Writer {
    fn as_any(&self) -> &dyn Any;
    async fn write(&mut self, buf: &[u8]) -> Result<()>;
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait ReadAt: Debug {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)>;
}

#[derive(Debug)]
pub struct ShardReader {
    readers: Vec<Option<BitrotReader>>, // 磁盘
    data_block_count: usize,            // 总的分片数量
    parity_block_count: usize,
    shard_size: usize,      // 每个分片的块大小 一次读取一块
    shard_file_size: usize, // 分片文件总长度
    offset: usize,          // 在分片中的offset
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
            if disk.is_none() {
                ress.push(None);
                errors.push(Some(Error::new(DiskError::DiskNotFound)));
                continue;
            }

            let disk: &mut BitrotReader = disk.as_mut().unwrap();
            futures.push(disk.read_at(self.offset, read_length));
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

            return Err(Error::msg("shard reader read faild"));
        }

        self.offset += self.shard_size;

        Ok(ress)
    }

    fn can_decode(&self, bufs: &[Option<Vec<u8>>]) -> bool {
        let c = bufs.iter().filter(|v| v.is_some()).count();
        if self.parity_block_count > 0 {
            c > self.data_block_count
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
        let shards = ec.encode_data(data).unwrap();
        println!("shards:{:?}", shards);

        let mut s: Vec<_> = shards
            .iter()
            .map(|d| if d.is_empty() { None } else { Some(d.clone()) })
            .collect();

        // let mut s = shards_to_option_shards(&shards);

        // s[0] = None;
        s[4] = None;
        s[3] = None;

        println!("sss:{:?}", &s);

        ec.decode_data(&mut s).unwrap();
        // ec.encoder.reconstruct(&mut s).unwrap();

        println!("sss:{:?}", &s);
    }
}
