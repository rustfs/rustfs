use super::BitrotReader;
use super::Erasure;
use crate::disk::error::Error;
use crate::disk::error_reduce::reduce_errs;
use futures::future::join_all;
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
    // readers传入前应处理disk错误，确保每个reader达到可用数量的BitrotReader
    pub fn new(readers: Vec<Option<BitrotReader<R>>>, e: Erasure, offset: usize, total_length: usize) -> Self {
        let shard_size = e.shard_size();
        let shard_file_size = e.shard_file_size(total_length as i64) as usize;

        let offset = (offset / e.block_size) * shard_size;

        // 确保offset不超过shard_file_size

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

        let shard_size = if self.offset + self.shard_size > self.shard_file_size {
            self.shard_file_size - self.offset
        } else {
            self.shard_size
        };

        if shard_size == 0 {
            return (vec![None; self.readers.len()], vec![None; self.readers.len()]);
        }

        // 使用并发读取所有分片
        let mut read_futs = Vec::with_capacity(self.readers.len());

        for (i, opt_reader) in self.readers.iter_mut().enumerate() {
            let future = if let Some(reader) = opt_reader.as_mut() {
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
                // reader是None时返回FileNotFound错误
                Box::pin(async move { (i, Err(Error::FileNotFound)) })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = (usize, Result<Vec<u8>, Error>)> + Send>>
            };
            read_futs.push(future);
        }

        let results = join_all(read_futs).await;

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; self.readers.len()];
        let mut errs = vec![None; self.readers.len()];

        for (i, shard) in results.into_iter() {
            match shard {
                Ok(data) => {
                    if !data.is_empty() {
                        shards[i] = Some(data);
                    }
                }
                Err(e) => {
                    // error!("Error reading shard {}: {}", i, e);
                    errs[i] = Some(e);
                }
            }
        }

        self.offset += shard_size;

        (shards, errs)
    }

    pub fn can_decode(&self, shards: &[Option<Vec<u8>>]) -> bool {
        shards.iter().filter(|s| s.is_some()).count() >= self.data_shards
    }
}

/// 获取数据块总长度
fn get_data_block_len(shards: &[Option<Vec<u8>>], data_blocks: usize) -> usize {
    let mut size = 0;
    for shard in shards.iter().take(data_blocks).flatten() {
        size += shard.len();
    }

    size
}

/// 将编码块中的数据块写入目标，支持 offset 和 length
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
        if block_op.is_none() {
            error!("write_data_blocks block_op.is_none()");
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Missing data block"));
        }

        let block = block_op.as_ref().unwrap();

        if offset >= block.len() {
            offset -= block.len();
            continue;
        }

        let block_slice = &block[offset..];
        offset = 0;

        if write_left < block.len() {
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

        if offset + length > total_length {
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length")));
        }

        let mut ret_err = None;

        if length == 0 {
            return (0, ret_err);
        }

        let mut written = 0;

        let mut reader = ParallelReader::new(readers, self.clone(), offset, total_length);

        let start = offset / self.block_size;
        let end = (offset + length) / self.block_size;

        for i in start..=end {
            let (block_offset, block_length) = if start == end {
                (offset % self.block_size, length)
            } else if i == start {
                (offset % self.block_size, self.block_size - (offset % self.block_size))
            } else if i == end {
                (0, (offset + length) % self.block_size)
            } else {
                (0, self.block_size)
            };

            if block_length == 0 {
                // error!("erasure decode decode block_length == 0");
                break;
            }

            let (mut shards, errs) = reader.read().await;

            if ret_err.is_none() {
                if let (_, Some(err)) = reduce_errs(&errs, &[]) {
                    if err == Error::FileNotFound || err == Error::FileCorrupt {
                        ret_err = Some(err.into());
                    }
                }
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

        if written < length {
            ret_err = Some(Error::LessData.into());
        }

        (written, ret_err)
    }
}
