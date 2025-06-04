use bytes::{Bytes, BytesMut};
use reed_solomon_erasure::galois_8::ReedSolomon;
// use rustfs_rio::Reader;
use smallvec::SmallVec;
use std::io;
use std::io::ErrorKind;
use tracing::error;
use tracing::warn;
use uuid::Uuid;

/// Erasure coding utility for data reliability using Reed-Solomon codes.
///
/// This struct provides encoding and decoding of data into data and parity shards.
/// It supports splitting data into multiple shards, generating parity for fault tolerance,
/// and reconstructing lost shards.
///
/// # Fields
/// - `data_shards`: Number of data shards.
/// - `parity_shards`: Number of parity shards.
/// - `encoder`: Optional ReedSolomon encoder instance.
/// - `block_size`: Block size for each shard.
/// - `_id`: Unique identifier for the erasure instance.
/// - `_buf`: Internal buffer for block operations.
///
/// # Example
/// ```
/// use erasure_coding::Erasure;
/// let erasure = Erasure::new(4, 2, 8);
/// let data = b"hello world";
/// let shards = erasure.encode_data(data).unwrap();
/// // Simulate loss and recovery...
/// ```

#[derive(Default, Clone)]
pub struct Erasure {
    pub data_shards: usize,
    pub parity_shards: usize,
    encoder: Option<ReedSolomon>,
    pub block_size: usize,
    _id: Uuid,
    _buf: Vec<u8>,
}

impl Erasure {
    /// Create a new Erasure instance.
    ///
    /// # Arguments
    /// * `data_shards` - Number of data shards.
    /// * `parity_shards` - Number of parity shards.
    /// * `block_size` - Block size for each shard.
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        let encoder = if parity_shards > 0 {
            Some(ReedSolomon::new(data_shards, parity_shards).unwrap())
        } else {
            None
        };

        Erasure {
            data_shards,
            parity_shards,
            block_size,
            encoder,
            _id: Uuid::new_v4(),
            _buf: vec![0u8; block_size],
        }
    }

    /// Encode data into data and parity shards.
    ///
    /// # Arguments
    /// * `data` - The input data to encode.
    ///
    /// # Returns
    /// A vector of encoded shards as `Bytes`.
    #[tracing::instrument(level = "info", skip_all, fields(data_len=data.len()))]
    pub fn encode_data(&self, data: &[u8]) -> io::Result<Vec<Bytes>> {
        // let shard_size = self.shard_size();
        // let total_size = shard_size * self.total_shard_count();

        // 数据切片数量
        let per_shard_size = data.len().div_ceil(self.data_shards);
        // 总需求大小
        let need_total_size = per_shard_size * self.total_shard_count();

        // Create a new buffer with the required total length for all shards
        let mut data_buffer = BytesMut::with_capacity(need_total_size);

        // Copy source data
        data_buffer.extend_from_slice(data);
        data_buffer.resize(need_total_size, 0u8);

        {
            // EC encode, the result will be written into data_buffer
            let data_slices: SmallVec<[&mut [u8]; 16]> = data_buffer.chunks_exact_mut(per_shard_size).collect();

            // Only do EC if parity_shards > 0
            if self.parity_shards > 0 {
                if let Some(encoder) = self.encoder.as_ref() {
                    encoder.encode(data_slices).map_err(|e| {
                        error!("encode data error: {:?}", e);
                        io::Error::new(ErrorKind::Other, format!("encode data error {:?}", e))
                    })?;
                } else {
                    warn!("parity_shards > 0, but encoder is None");
                }
            }
        }

        // Zero-copy split, all shards reference data_buffer
        let mut data_buffer = data_buffer.freeze();
        let mut shards = Vec::with_capacity(self.total_shard_count());
        for _ in 0..self.total_shard_count() {
            let shard = data_buffer.split_to(per_shard_size);
            shards.push(shard);
        }

        Ok(shards)
    }

    /// Decode and reconstruct missing shards in-place.
    ///
    /// # Arguments
    /// * `shards` - Mutable slice of optional shard data. Missing shards should be `None`.
    ///
    /// # Returns
    /// Ok if reconstruction succeeds, error otherwise.
    pub fn decode_data(&self, shards: &mut [Option<Vec<u8>>]) -> io::Result<()> {
        if self.parity_shards > 0 {
            if let Some(encoder) = self.encoder.as_ref() {
                encoder.reconstruct(shards).map_err(|e| {
                    error!("decode data error: {:?}", e);
                    io::Error::new(ErrorKind::Other, format!("decode data error {:?}", e))
                })?;
            } else {
                warn!("parity_shards > 0, but encoder is None");
            }
        }

        Ok(())
    }

    /// Get the total number of shards (data + parity).
    pub fn total_shard_count(&self) -> usize {
        self.data_shards + self.parity_shards
    }
    // /// Calculate the shard size and total size for a given data size.
    // // Returns (shard_size, total_size) for the given data size
    // fn need_size(&self, data_size: usize) -> (usize, usize) {
    //     let shard_size = self.shard_size(data_size);
    //     (shard_size, shard_size * (self.total_shard_count()))
    // }

    /// Calculate the size of each shard.
    pub fn shard_size(&self) -> usize {
        self.block_size.div_ceil(self.data_shards)
    }
    /// Calculate the total erasure file size for a given original size.
    // Returns the final erasure size from the original size
    pub fn shard_file_size(&self, total_length: usize) -> usize {
        if total_length == 0 {
            return 0;
        }

        let num_shards = total_length / self.block_size;
        let last_block_size = total_length % self.block_size;
        let last_shard_size = last_block_size.div_ceil(self.data_shards);
        num_shards * self.shard_size() + last_shard_size
    }

    /// Calculate the offset in the erasure file where reading begins.
    // Returns the offset in the erasure file where reading begins
    pub fn shard_file_offset(&self, start_offset: usize, length: usize, total_length: usize) -> usize {
        let shard_size = self.shard_size();
        let shard_file_size = self.shard_file_size(total_length);
        let end_shard = (start_offset + length) / self.block_size;
        let mut till_offset = end_shard * shard_size + shard_size;
        if till_offset > shard_file_size {
            till_offset = shard_file_size;
        }

        till_offset
    }

    /// Encode all data from a rustfs_rio::Reader in blocks, calling an async callback for each encoded block.
    /// This method is async and returns the reader and total bytes read after all blocks are processed.
    ///
    /// # Arguments
    /// * `reader` - A rustfs_rio::Reader to read data from.
    /// * `mut on_block` - Async callback: FnMut(Result<Vec<Bytes>, std::io::Error>) -> Future<Output=Result<(), E>> + Send
    ///
    /// # Returns
    /// Result<(reader, total_bytes_read), E> after all data has been processed or on callback error.
    pub async fn encode_stream_callback_async<F, Fut, E, R>(
        self: std::sync::Arc<Self>,
        reader: &mut R,
        mut on_block: F,
    ) -> Result<usize, E>
    where
        R: rustfs_rio::Reader + Send + Sync + Unpin,
        F: FnMut(std::io::Result<Vec<Bytes>>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), E>> + Send,
    {
        let block_size = self.block_size;
        let mut total = 0;
        loop {
            let mut buf = vec![0u8; block_size];
            match rustfs_utils::read_full(&mut *reader, &mut buf).await {
                Ok(n) if n > 0 => {
                    total += n;
                    let res = self.encode_data(&buf[..n]);
                    on_block(res).await?
                }
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    on_block(Err(e)).await?;
                    break;
                }
            }
            buf.clear();
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_file_size_cases() {
        let erasure = Erasure::new(4, 2, 8);

        // Case 1: total_length == 0
        assert_eq!(erasure.shard_file_size(0), 0);

        // Case 2: total_length < block_size
        assert_eq!(erasure.shard_file_size(5), 2); // 5 div_ceil 4 = 2

        // Case 3: total_length == block_size
        assert_eq!(erasure.shard_file_size(8), 2);

        // Case 4: total_length > block_size, not aligned
        assert_eq!(erasure.shard_file_size(13), 4); // 8/8=1, last=5, 5 div_ceil 4=2, 1*2+2=4

        // Case 5: total_length > block_size, aligned
        assert_eq!(erasure.shard_file_size(16), 4); // 16/8=2, last=0, 2*2+0=4

        assert_eq!(erasure.shard_file_size(1248739), 312185); // 1248739/8=156092, last=3, 3 div_ceil 4=1, 156092*2+1=312185

        assert_eq!(erasure.shard_file_size(43), 11); // 43/8=5, last=3, 3 div_ceil 4=1, 5*2+1=11
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 8;
        let erasure = Erasure::new(data_shards, parity_shards, block_size);
        // let data = b"hello erasure coding!";
        let data = b"channel async callback test data!";
        let shards = erasure.encode_data(data).unwrap();
        // Simulate the loss of one shard
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
        shards_opt[2] = None;
        // Decode
        erasure.decode_data(&mut shards_opt).unwrap();
        // Recover original data
        let mut recovered = Vec::new();
        for shard in shards_opt.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        recovered.truncate(data.len());
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_encode_all_zero_data() {
        let data_shards = 3;
        let parity_shards = 2;
        let block_size = 6;
        let erasure = Erasure::new(data_shards, parity_shards, block_size);
        let data = vec![0u8; block_size];
        let shards = erasure.encode_data(&data).unwrap();
        assert_eq!(shards.len(), data_shards + parity_shards);
        let total_len: usize = shards.iter().map(|b| b.len()).sum();
        assert_eq!(total_len, erasure.shard_size() * (data_shards + parity_shards));
    }

    #[test]
    fn test_shard_size_and_file_size() {
        let erasure = Erasure::new(4, 2, 8);
        assert_eq!(erasure.shard_file_size(33), 9);
        assert_eq!(erasure.shard_file_size(0), 0);
    }

    #[test]
    fn test_shard_file_offset() {
        let erasure = Erasure::new(4, 2, 8);
        let offset = erasure.shard_file_offset(0, 16, 32);
        assert!(offset > 0);
    }

    #[test]
    fn test_encode_decode_large_1m() {
        // Test encoding and decoding 1MB data, simulating the loss of 2 shards
        let data_shards = 6;
        let parity_shards = 3;
        let block_size = 128 * 1024; // 128KB
        let erasure = Erasure::new(data_shards, parity_shards, block_size);
        let data = vec![0x5Au8; 1024 * 1024]; // 1MB fixed content
        let shards = erasure.encode_data(&data).unwrap();
        // Simulate the loss of 2 shards
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
        shards_opt[1] = None;
        shards_opt[7] = None;
        // Decode
        erasure.decode_data(&mut shards_opt).unwrap();
        // Recover original data
        let mut recovered = Vec::new();
        for shard in shards_opt.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        recovered.truncate(data.len());
        assert_eq!(&recovered, &data);
    }

    #[tokio::test]
    async fn test_encode_stream_callback_async_error_propagation() {
        use std::sync::Arc;
        use tokio::io::BufReader;
        use tokio::sync::mpsc;
        let data_shards = 3;
        let parity_shards = 3;
        let block_size = 8;
        let erasure = Arc::new(Erasure::new(data_shards, parity_shards, block_size));
        let data = b"async stream callback error propagation!123";
        let mut rio_reader = BufReader::new(&data[..]);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(8);
        let erasure_clone = erasure.clone();
        let mut call_count = 0;
        let handle = tokio::spawn(async move {
            let result = erasure_clone
                .encode_stream_callback_async::<_, _, &'static str, _>(&mut rio_reader, move |res| {
                    let tx = tx.clone();
                    call_count += 1;
                    async move {
                        if call_count == 2 {
                            Err("user error")
                        } else {
                            let shards = res.unwrap();
                            tx.send(shards).await.unwrap();
                            Ok(())
                        }
                    }
                })
                .await;
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), "user error");
        });
        let mut all_blocks = Vec::new();
        while let Some(block) = rx.recv().await {
            println!("Received block: {:?}", block[0].len());
            all_blocks.push(block);
        }
        handle.await.unwrap();
        // 只处理了第一个 block
        assert_eq!(all_blocks.len(), 1);
        // 对第一个 block 使用 decode_data 修复并校验
        let block = &all_blocks[0];
        let mut shards_opt: Vec<Option<Vec<u8>>> = block.iter().map(|b| Some(b.to_vec())).collect();
        // 模拟丢失一个分片
        shards_opt[0] = None;
        erasure.decode_data(&mut shards_opt).unwrap();

        let mut recovered = Vec::new();
        for shard in shards_opt.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        // 只恢复第一个 block 的原始数据
        let block_data_len = std::cmp::min(block_size, data.len());
        recovered.truncate(block_data_len);
        assert_eq!(&recovered, &data[..block_data_len]);
    }

    #[tokio::test]
    async fn test_encode_stream_callback_async_channel_decode() {
        use std::sync::Arc;
        use tokio::io::BufReader;
        use tokio::sync::mpsc;
        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 8;
        let erasure = Arc::new(Erasure::new(data_shards, parity_shards, block_size));
        let data = b"channel async callback test data!";
        let mut rio_reader = BufReader::new(&data[..]);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(8);
        let erasure_clone = erasure.clone();
        let handle = tokio::spawn(async move {
            erasure_clone
                .encode_stream_callback_async::<_, _, (), _>(&mut rio_reader, move |res| {
                    let tx = tx.clone();
                    async move {
                        let shards = res.unwrap();
                        tx.send(shards).await.unwrap();
                        Ok(())
                    }
                })
                .await
                .unwrap();
        });
        let mut all_blocks = Vec::new();
        while let Some(block) = rx.recv().await {
            all_blocks.push(block);
        }
        handle.await.unwrap();
        // 对每个 block，模拟丢失一个分片并恢复
        let mut recovered = Vec::new();
        for block in &all_blocks {
            let mut shards_opt: Vec<Option<Vec<u8>>> = block.iter().map(|b| Some(b.to_vec())).collect();
            // 模拟丢失一个分片
            shards_opt[0] = None;
            erasure.decode_data(&mut shards_opt).unwrap();
            for shard in shards_opt.iter().take(data_shards) {
                recovered.extend_from_slice(shard.as_ref().unwrap());
            }
        }
        recovered.truncate(data.len());
        assert_eq!(&recovered, data);
    }
}
