use anyhow::anyhow;
use anyhow::Error;
use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use futures::{Stream, StreamExt};
use reed_solomon_erasure::galois_8::ReedSolomon;
use s3s::dto::StreamingBlob;
use s3s::StdError;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::DuplexStream;
use tracing::debug;
use tracing::warn;
// use tracing::debug;
use uuid::Uuid;

use crate::chunk_stream::ChunkedStream;
use crate::disk_api::DiskError;
use crate::disk_api::FileReader;

pub struct Erasure {
    data_shards: usize,
    parity_shards: usize,
    encoder: ReedSolomon,
    block_size: usize,
    id: Uuid,
}

impl Erasure {
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        Erasure {
            data_shards,
            parity_shards,
            block_size,
            encoder: ReedSolomon::new(data_shards, parity_shards).unwrap(),
            id: Uuid::new_v4(),
        }
    }

    pub async fn encode<S, W>(
        &self,
        body: S,
        writers: &mut Vec<W>,
        // block_size: usize,
        total_size: usize,
        _write_quorum: usize,
    ) -> Result<usize>
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
        W: AsyncWrite + Unpin,
    {
        let mut stream = ChunkedStream::new(body, total_size, self.block_size, true);
        let mut total: usize = 0;
        let mut idx = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    let blocks = self.encode_data(data.as_ref())?;

                    warn!("encode shard size: {}/{} from block_size {} ", blocks[0].len(), blocks.len(), data.len());

                    let mut errs = Vec::new();
                    idx += 1;
                    for (i, w) in writers.iter_mut().enumerate() {
                        total += blocks[i].len();

                        // debug!(
                        //     "{} {}-{} encode write {} , total:{}, readed:{}",
                        //     self.id,
                        //     idx,
                        //     i,
                        //     blocks[i].len(),
                        //     data_size,
                        //     total
                        // );

                        match w.write_all(blocks[i].as_ref()).await {
                            Ok(_) => errs.push(None),
                            Err(e) => errs.push(Some(e)),
                        }
                    }

                    // debug!("{} encode_data write errs:{:?}", self.id, errs);
                    // // TODO: reduceWriteQuorumErrs
                    // for err in errs.iter() {
                    //     if err.is_some() {
                    //         return Err(Error::msg("message"));
                    //     }
                    // }
                }
                Err(e) => return Err(anyhow!(e)),
            }
        }

        warn!(" encode_data done shard block num {}", idx);

        Ok(total)

        // loop {
        //     match rd.next().await {
        //         Some(res) => todo!(),
        //         None => todo!(),
        //     }
        // }
    }

    pub async fn decode(
        &self,
        writer: &mut DuplexStream,
        readers: Vec<Option<FileReader>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> Result<usize> {
        if length == 0 {
            return Ok(0);
        }

        let mut reader = ShardReader::new(readers, self, offset, total_length);

        let start_block = offset / self.block_size;
        let end_block = (offset + length) / self.block_size;

        warn!("decode block from {} to {}", start_block, end_block);

        let mut bytes_writed = 0;
        for block_idx in start_block..=end_block {
            let mut block_offset = 0;
            let mut block_length = 0;
            if start_block == end_block {
                block_offset = offset % self.block_size;
                block_length = length;
            } else if block_idx == start_block {
                block_offset = offset % self.block_size;
                block_length = self.block_size - block_offset;
            } else if block_idx == end_block {
                block_offset = 0;
                block_length = (offset + length) % self.block_size;
            } else {
                block_offset = 0;
                block_length = self.block_size;
            }

            if block_length == 0 {
                break;
            }

            warn!("decode block_offset {},block_length {} ", block_offset, block_length);

            let mut bufs = reader.read().await?;

            self.decode_data(&mut bufs)?;

            let writed_n = self
                .write_data_blocks(writer, bufs, self.data_shards, block_offset, block_length)
                .await?;

            bytes_writed += writed_n;
        }

        if bytes_writed != length {
            warn!("bytes_writed != length ");
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

        debug!("write_data_blocks offset {}, length {}", offset, length);

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

            // if write < buf.len() {}

            let n = writer.write(buf).await?;

            write -= n;
            total_writed += n;
        }

        Ok(total_writed)
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

            self.encoder.encode(data_slices)?;
        }

        // 分片
        let mut shards = Vec::with_capacity(self.encoder.total_shard_count());

        let slices: Vec<&[u8]> = data_buffer.chunks(shard_size).collect();

        for &d in slices.iter() {
            shards.push(d.to_vec());
        }

        Ok(shards)
    }

    pub fn decode_data(&self, shards: &mut Vec<Option<Vec<u8>>>) -> Result<()> {
        self.encoder.reconstruct(shards)?;
        Ok(())
    }

    // 每个分片长度，所需要的总长度
    fn need_size(&self, data_size: usize) -> (usize, usize) {
        let shard_size = self.shard_size(data_size);
        (shard_size, shard_size * (self.encoder.total_shard_count()))
    }

    // 算出每个分片大小
    fn shard_size(&self, data_size: usize) -> usize {
        (data_size + self.encoder.data_shard_count() - 1) / self.encoder.data_shard_count()
    }
    // returns final erasure size from original size.
    fn shard_file_size(&self, total_size: usize) -> usize {
        if total_size == 0 {
            return 0;
        }

        let mut num_shards = total_size / self.block_size;
        let last_block_size = total_size % self.block_size;
        // let last_shard_size = (last_block_size + self.data_shards - 1) / self.data_shards;
        // num_shards * self.shard_size(self.block_size) + last_shard_size

        // 因为写入的时候ec需要补全，所以最后一个长度应该也是一样的
        if last_block_size != 0 {
            num_shards += 1
        }
        num_shards * self.shard_size(self.block_size)
    }
}

pub trait ReadAt {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)>;
}

pub struct ShardReader {
    readers: Vec<Option<FileReader>>, // 磁盘
    data_block_count: usize,          // 总的分片数量
    shard_size: usize,                // 每个分片的块大小 一次读取一块
    shard_file_size: usize,           // 分片文件总长度
    offset: usize,                    // 在分片中的offset
}

impl ShardReader {
    pub fn new(readers: Vec<Option<FileReader>>, ec: &Erasure, offset: usize, total_length: usize) -> Self {
        Self {
            readers,
            data_block_count: ec.encoder.data_shard_count(),
            shard_size: ec.shard_size(ec.block_size),
            shard_file_size: ec.shard_file_size(total_length),
            offset: (offset / ec.block_size) * ec.shard_size(ec.block_size),
        }
    }

    pub async fn read(&mut self) -> Result<Vec<Option<Vec<u8>>>> {
        // let mut disks = self.readers;

        warn!("shard reader read offset {}, shard_size {}", self.offset, self.shard_size);

        let reader_length = self.readers.len();

        let mut futures = Vec::with_capacity(reader_length);
        let mut errors = Vec::with_capacity(reader_length);

        let mut ress = Vec::with_capacity(reader_length);

        for disk in self.readers.iter_mut() {
            if disk.is_none() {
                ress.push(None);
                errors.push(Some(Error::new(DiskError::DiskNotFound)));
                continue;
            }

            let disk: &mut FileReader = disk.as_mut().unwrap();
            futures.push(disk.read_at(self.offset, self.shard_size));
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

        // debug!("ec decode read ress {:?}", &ress);
        debug!("ec decode read errors {:?}", &errors);

        if !self.can_decode(&ress) {
            return Err(Error::msg("shard reader read faild"));
        }

        self.offset += self.shard_size;

        Ok(ress)
    }
    fn can_decode(&self, bufs: &Vec<Option<Vec<u8>>>) -> bool {
        bufs.iter().filter(|v| v.is_some()).count() > self.data_block_count
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
