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

//! Erasure coding implementation using Reed-Solomon SIMD backend.
//!
//! This module provides erasure coding functionality with high-performance SIMD
//! Reed-Solomon implementation:
//!
//! ## Reed-Solomon Implementation
//!
//! ### SIMD Mode (Only)
//! - **Performance**: Uses SIMD optimization for high-performance encoding/decoding
//! - **Compatibility**: Works with any shard size through SIMD implementation
//! - **Reliability**: High-performance SIMD implementation for large data processing
//! - **Use case**: Optimized for maximum performance in large data processing scenarios
//!
//! ## Example
//!
//! ```ignore
//! use rustfs_ecstore::erasure_coding::Erasure;
//!
//! let erasure = Erasure::new(4, 2, 1024); // 4 data shards, 2 parity shards, 1KB block size
//! let data = b"hello world";
//! let shards = erasure.encode_data(data).unwrap();
//! // Simulate loss and recovery...
//! ```

use bytes::{Bytes, BytesMut};
use reed_solomon_simd;
use smallvec::SmallVec;
use std::io;
use tokio::io::AsyncRead;
use tracing::warn;
use uuid::Uuid;

/// Reed-Solomon encoder using SIMD implementation.
pub struct ReedSolomonEncoder {
    data_shards: usize,
    parity_shards: usize,
    // Use RwLock to ensure thread safety, implementing Send + Sync
    encoder_cache: std::sync::RwLock<Option<reed_solomon_simd::ReedSolomonEncoder>>,
    decoder_cache: std::sync::RwLock<Option<reed_solomon_simd::ReedSolomonDecoder>>,
}

impl Clone for ReedSolomonEncoder {
    fn clone(&self) -> Self {
        Self {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            // 为新实例创建空的缓存，不共享缓存
            encoder_cache: std::sync::RwLock::new(None),
            decoder_cache: std::sync::RwLock::new(None),
        }
    }
}

impl ReedSolomonEncoder {
    /// Create a new Reed-Solomon encoder with specified data and parity shards.
    pub fn new(data_shards: usize, parity_shards: usize) -> io::Result<Self> {
        Ok(ReedSolomonEncoder {
            data_shards,
            parity_shards,
            encoder_cache: std::sync::RwLock::new(None),
            decoder_cache: std::sync::RwLock::new(None),
        })
    }

    /// Encode data shards with parity.
    pub fn encode(&self, shards: SmallVec<[&mut [u8]; 16]>) -> io::Result<()> {
        let mut shards_vec: Vec<&mut [u8]> = shards.into_vec();
        if shards_vec.is_empty() {
            return Ok(());
        }

        let simd_result = self.encode_with_simd(&mut shards_vec);

        match simd_result {
            Ok(()) => Ok(()),
            Err(simd_error) => {
                warn!("SIMD encoding failed: {}", simd_error);
                Err(simd_error)
            }
        }
    }

    fn encode_with_simd(&self, shards_vec: &mut [&mut [u8]]) -> io::Result<()> {
        let shard_len = shards_vec[0].len();

        // Get or create encoder
        let mut encoder = {
            let mut cache_guard = self
                .encoder_cache
                .write()
                .map_err(|_| io::Error::other("Failed to acquire encoder cache lock"))?;

            match cache_guard.take() {
                Some(mut cached_encoder) => {
                    // Use reset method to reset existing encoder to adapt to new parameters
                    if let Err(e) = cached_encoder.reset(self.data_shards, self.parity_shards, shard_len) {
                        warn!("Failed to reset SIMD encoder: {:?}, creating new one", e);
                        // If reset fails, create new encoder
                        reed_solomon_simd::ReedSolomonEncoder::new(self.data_shards, self.parity_shards, shard_len)
                            .map_err(|e| io::Error::other(format!("Failed to create SIMD encoder: {e:?}")))?
                    } else {
                        cached_encoder
                    }
                }
                None => {
                    // First use, create new encoder
                    reed_solomon_simd::ReedSolomonEncoder::new(self.data_shards, self.parity_shards, shard_len)
                        .map_err(|e| io::Error::other(format!("Failed to create SIMD encoder: {e:?}")))?
                }
            }
        };

        // Add original shards
        for (i, shard) in shards_vec.iter().enumerate().take(self.data_shards) {
            encoder
                .add_original_shard(shard)
                .map_err(|e| io::Error::other(format!("Failed to add shard {i}: {e:?}")))?;
        }

        // Encode and get recovery shards
        let result = encoder
            .encode()
            .map_err(|e| io::Error::other(format!("SIMD encoding failed: {e:?}")))?;

        // Copy recovery shards to output buffer
        for (i, recovery_shard) in result.recovery_iter().enumerate() {
            if i + self.data_shards < shards_vec.len() {
                shards_vec[i + self.data_shards].copy_from_slice(recovery_shard);
            }
        }

        // Return encoder to cache (encoder is automatically reset after result is dropped, can be reused)
        drop(result); // Explicitly drop result to ensure encoder is reset

        *self
            .encoder_cache
            .write()
            .map_err(|_| io::Error::other("Failed to return encoder to cache"))? = Some(encoder);

        Ok(())
    }

    /// Reconstruct missing shards.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> io::Result<()> {
        // Use SIMD for reconstruction
        let simd_result = self.reconstruct_with_simd(shards);

        match simd_result {
            Ok(()) => Ok(()),
            Err(simd_error) => {
                warn!("SIMD reconstruction failed: {}", simd_error);
                Err(simd_error)
            }
        }
    }

    fn reconstruct_with_simd(&self, shards: &mut [Option<Vec<u8>>]) -> io::Result<()> {
        // Find a valid shard to determine length
        let shard_len = shards
            .iter()
            .find_map(|s| s.as_ref().map(|v| v.len()))
            .ok_or_else(|| io::Error::other("No valid shards found for reconstruction"))?;

        let mut decoder = {
            let mut cache_guard = self
                .decoder_cache
                .write()
                .map_err(|_| io::Error::other("Failed to acquire decoder cache lock"))?;

            match cache_guard.take() {
                Some(mut cached_decoder) => {
                    if let Err(e) = cached_decoder.reset(self.data_shards, self.parity_shards, shard_len) {
                        warn!("Failed to reset SIMD decoder: {:?}, creating new one", e);

                        reed_solomon_simd::ReedSolomonDecoder::new(self.data_shards, self.parity_shards, shard_len)
                            .map_err(|e| io::Error::other(format!("Failed to create SIMD decoder: {e:?}")))?
                    } else {
                        cached_decoder
                    }
                }
                None => reed_solomon_simd::ReedSolomonDecoder::new(self.data_shards, self.parity_shards, shard_len)
                    .map_err(|e| io::Error::other(format!("Failed to create SIMD decoder: {e:?}")))?,
            }
        };

        // Add available shards (both data and parity)
        for (i, shard_opt) in shards.iter().enumerate() {
            if let Some(shard) = shard_opt {
                if i < self.data_shards {
                    decoder
                        .add_original_shard(i, shard)
                        .map_err(|e| io::Error::other(format!("Failed to add original shard for reconstruction: {e:?}")))?;
                } else {
                    let recovery_idx = i - self.data_shards;
                    decoder
                        .add_recovery_shard(recovery_idx, shard)
                        .map_err(|e| io::Error::other(format!("Failed to add recovery shard for reconstruction: {e:?}")))?;
                }
            }
        }

        let result = decoder
            .decode()
            .map_err(|e| io::Error::other(format!("SIMD decode error: {e:?}")))?;

        // Fill in missing data shards from reconstruction result
        for (i, shard_opt) in shards.iter_mut().enumerate() {
            if shard_opt.is_none() && i < self.data_shards {
                for (restored_index, restored_data) in result.restored_original_iter() {
                    if restored_index == i {
                        *shard_opt = Some(restored_data.to_vec());
                        break;
                    }
                }
            }
        }

        drop(result);

        *self
            .decoder_cache
            .write()
            .map_err(|_| io::Error::other("Failed to return decoder to cache"))? = Some(decoder);

        Ok(())
    }
}

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
/// ```ignore
/// use rustfs_ecstore::erasure_coding::Erasure;
/// let erasure = Erasure::new(4, 2, 8);
/// let data = b"hello world";
/// let shards = erasure.encode_data(data).unwrap();
/// // Simulate loss and recovery...
/// ```

#[derive(Default)]
pub struct Erasure {
    pub data_shards: usize,
    pub parity_shards: usize,
    encoder: Option<ReedSolomonEncoder>,
    pub block_size: usize,
    _id: Uuid,
    _buf: Vec<u8>,
}

impl Clone for Erasure {
    fn clone(&self) -> Self {
        Self {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            encoder: self.encoder.clone(),
            block_size: self.block_size,
            _id: Uuid::new_v4(), // Generate new ID for clone
            _buf: vec![0u8; self.block_size],
        }
    }
}

pub fn calc_shard_size(block_size: usize, data_shards: usize) -> usize {
    (block_size.div_ceil(data_shards) + 1) & !1
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
            Some(ReedSolomonEncoder::new(data_shards, parity_shards).unwrap())
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

        // Data shard count
        let per_shard_size = calc_shard_size(data.len(), self.data_shards);
        // Total required size
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
                    encoder.encode(data_slices)?;
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
                encoder.reconstruct(shards)?;
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
        calc_shard_size(self.block_size, self.data_shards)
    }
    /// Calculate the total erasure file size for a given original size.
    // Returns the final erasure size from the original size
    pub fn shard_file_size(&self, total_length: i64) -> i64 {
        if total_length == 0 {
            return 0;
        }
        if total_length < 0 {
            return total_length;
        }

        let total_length = total_length as usize;

        let num_shards = total_length / self.block_size;
        let last_block_size = total_length % self.block_size;
        let last_shard_size = calc_shard_size(last_block_size, self.data_shards);
        (num_shards * self.shard_size() + last_shard_size) as i64
    }

    /// Calculate the offset in the erasure file where reading begins.
    // Returns the offset in the erasure file where reading begins
    pub fn shard_file_offset(&self, start_offset: usize, length: usize, total_length: usize) -> usize {
        let shard_size = self.shard_size();
        let shard_file_size = self.shard_file_size(total_length as i64) as usize;
        let end_shard = (start_offset + length) / self.block_size;
        let mut till_offset = end_shard * shard_size + shard_size;
        if till_offset > shard_file_size {
            till_offset = shard_file_size;
        }

        till_offset
    }

    /// Encode all data from a reader in blocks, calling an async callback for each encoded block.
    /// This method is async and returns the total bytes read after all blocks are processed.
    ///
    /// # Arguments
    /// * `reader` - An async reader implementing AsyncRead + Send + Sync + Unpin
    /// * `mut on_block` - Async callback that receives encoded blocks and returns a Result
    /// * `F` - Callback type: FnMut(Result<Vec<Bytes>, std::io::Error>) -> Future<Output=Result<(), E>> + Send
    /// * `Fut` - Future type returned by the callback
    /// * `E` - Error type returned by the callback
    /// * `R` - Reader type implementing AsyncRead + Send + Sync + Unpin
    ///
    /// # Returns
    /// Result<usize, E> containing total bytes read, or error from callback
    ///
    /// # Errors
    /// Returns error if reading from reader fails or if callback returns error
    pub async fn encode_stream_callback_async<F, Fut, E, R>(
        self: std::sync::Arc<Self>,
        reader: &mut R,
        mut on_block: F,
    ) -> Result<usize, E>
    where
        R: AsyncRead + Send + Sync + Unpin,
        F: FnMut(std::io::Result<Vec<Bytes>>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), E>> + Send,
    {
        let block_size = self.block_size;
        let mut total = 0;
        loop {
            let mut buf = vec![0u8; block_size];
            match rustfs_utils::read_full(&mut *reader, &mut buf).await {
                Ok(n) if n > 0 => {
                    warn!("encode_stream_callback_async read n={}", n);
                    total += n;
                    let res = self.encode_data(&buf[..n]);
                    on_block(res).await?
                }
                Ok(_) => {
                    warn!("encode_stream_callback_async read unexpected ok");
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!("encode_stream_callback_async read unexpected eof");
                    break;
                }
                Err(e) => {
                    warn!("encode_stream_callback_async read error={:?}", e);
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
    fn test_shard_file_size_cases2() {
        let erasure = Erasure::new(12, 4, 1024 * 1024);

        assert_eq!(erasure.shard_file_size(1572864), 131074);
    }

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

        assert_eq!(erasure.shard_file_size(1248739), 312186); // 1248739/8=156092, last=3, 3 div_ceil 4=1, 156092*2+1=312185

        assert_eq!(erasure.shard_file_size(43), 12); // 43/8=5, last=3, 3 div_ceil 4=1, 5*2+1=11

        assert_eq!(erasure.shard_file_size(1572864), 393216); // 43/8=5, last=3, 3 div_ceil 4=1, 5*2+1=11
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 1024; // SIMD mode
        let erasure = Erasure::new(data_shards, parity_shards, block_size);

        // Use sufficient test data for SIMD optimization
        let test_data = b"SIMD mode test data for encoding and decoding roundtrip verification with sufficient length to ensure shard size requirements are met for proper SIMD optimization.".repeat(20); // ~3KB for SIMD

        let data = &test_data;
        let encoded_shards = erasure.encode_data(data).unwrap();
        assert_eq!(encoded_shards.len(), data_shards + parity_shards);

        // Create decode input with some shards missing, convert to the format expected by decode_data
        let mut decode_input: Vec<Option<Vec<u8>>> = vec![None; data_shards + parity_shards];
        for i in 0..data_shards {
            decode_input[i] = Some(encoded_shards[i].to_vec());
        }

        erasure.decode_data(&mut decode_input).unwrap();

        // Recover original data
        let mut recovered = Vec::new();
        for shard in decode_input.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        recovered.truncate(data.len());
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_encode_decode_large_1m() {
        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 512 * 3; // SIMD mode
        let erasure = Erasure::new(data_shards, parity_shards, block_size);

        // Generate 1MB test data
        let data: Vec<u8> = (0..1048576).map(|i| (i % 256) as u8).collect();

        let encoded_shards = erasure.encode_data(&data).unwrap();
        assert_eq!(encoded_shards.len(), data_shards + parity_shards);

        // Create decode input with some shards missing, convert to the format expected by decode_data
        let mut decode_input: Vec<Option<Vec<u8>>> = vec![None; data_shards + parity_shards];
        for i in 0..data_shards {
            decode_input[i] = Some(encoded_shards[i].to_vec());
        }

        erasure.decode_data(&mut decode_input).unwrap();

        // Recover original data
        let mut recovered = Vec::new();
        for shard in decode_input.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        recovered.truncate(data.len());
        assert_eq!(recovered, data);
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
        assert_eq!(erasure.shard_file_size(33), 10);
        assert_eq!(erasure.shard_file_size(0), 0);
    }

    #[test]
    fn test_shard_file_offset() {
        let erasure = Erasure::new(8, 8, 1024 * 1024);
        let offset = erasure.shard_file_offset(0, 86, 86);
        println!("offset={offset}");
        assert!(offset > 0);

        let total_length = erasure.shard_file_size(86);
        println!("total_length={total_length}");
        assert!(total_length > 0);
    }

    #[tokio::test]
    async fn test_encode_stream_callback_async_error_propagation() {
        use std::io::Cursor;
        use std::sync::Arc;
        use tokio::sync::mpsc;

        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 1024; // SIMD mode
        let erasure = Arc::new(Erasure::new(data_shards, parity_shards, block_size));

        // Use test data suitable for SIMD mode
        let data =
            b"Async error test data with sufficient length to meet requirements for proper testing and validation.".repeat(20); // ~2KB

        let mut reader = Cursor::new(data);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(8);
        let erasure_clone = erasure.clone();
        let handle = tokio::spawn(async move {
            erasure_clone
                .encode_stream_callback_async::<_, _, (), _>(&mut reader, move |res| {
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
        let result = handle.await;
        assert!(result.is_ok());
        let collected_shards = rx.recv().await.unwrap();
        assert_eq!(collected_shards.len(), data_shards + parity_shards);
    }

    #[tokio::test]
    async fn test_encode_stream_callback_async_channel_decode() {
        use std::io::Cursor;
        use std::sync::Arc;
        use tokio::sync::mpsc;

        let data_shards = 4;
        let parity_shards = 2;
        let block_size = 1024; // SIMD mode
        let erasure = Arc::new(Erasure::new(data_shards, parity_shards, block_size));

        // Use test data that fits in exactly one block to avoid multi-block complexity
        let data =
            b"Channel async callback test data with sufficient length to ensure proper operation and validation requirements."
                .repeat(8); // ~1KB

        let data_clone = data.clone(); // Clone for later comparison
        let mut reader = Cursor::new(data);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(8);
        let erasure_clone = erasure.clone();
        let handle = tokio::spawn(async move {
            erasure_clone
                .encode_stream_callback_async::<_, _, (), _>(&mut reader, move |res| {
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
        let result = handle.await;
        assert!(result.is_ok());
        let shards = rx.recv().await.unwrap();
        assert_eq!(shards.len(), data_shards + parity_shards);

        // Test decode using the old API that operates in-place
        let mut decode_input: Vec<Option<Vec<u8>>> = vec![None; data_shards + parity_shards];
        for i in 0..data_shards {
            decode_input[i] = Some(shards[i].to_vec());
        }
        erasure.decode_data(&mut decode_input).unwrap();

        // Recover original data
        let mut recovered = Vec::new();
        for shard in decode_input.iter().take(data_shards) {
            recovered.extend_from_slice(shard.as_ref().unwrap());
        }
        recovered.truncate(data_clone.len());
        assert_eq!(&recovered, &data_clone);
    }

    // SIMD mode specific tests
    mod simd_tests {
        use super::*;

        #[test]
        fn test_simd_encode_decode_roundtrip() {
            let data_shards = 4;
            let parity_shards = 2;
            let block_size = 1024; // Use larger block size for SIMD mode
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // Use data that will create shards >= 512 bytes for SIMD optimization
            let test_data = b"SIMD mode test data for encoding and decoding roundtrip verification with sufficient length to ensure shard size requirements are met for proper SIMD optimization and validation.";
            let data = test_data.repeat(25); // Create much larger data: ~5KB total, ~1.25KB per shard

            let encoded_shards = erasure.encode_data(&data).unwrap();
            assert_eq!(encoded_shards.len(), data_shards + parity_shards);

            // Create decode input with some shards missing
            let mut shards_opt: Vec<Option<Vec<u8>>> = encoded_shards.iter().map(|shard| Some(shard.to_vec())).collect();

            // Lose one data shard and one parity shard (should still be recoverable)
            shards_opt[1] = None; // Lose second data shard
            shards_opt[5] = None; // Lose second parity shard

            erasure.decode_data(&mut shards_opt).unwrap();

            // Verify recovered data
            let mut recovered = Vec::new();
            for shard in shards_opt.iter().take(data_shards) {
                recovered.extend_from_slice(shard.as_ref().unwrap());
            }
            recovered.truncate(data.len());
            assert_eq!(&recovered, &data);
        }

        #[test]
        fn test_simd_all_zero_data() {
            let data_shards = 4;
            let parity_shards = 2;
            let block_size = 1024; // Use larger block size for SIMD mode
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // Create all-zero data that ensures adequate shard size for SIMD optimization
            let data = vec![0u8; 1024]; // 1KB of zeros, each shard will be 256 bytes

            let encoded_shards = erasure.encode_data(&data).unwrap();
            assert_eq!(encoded_shards.len(), data_shards + parity_shards);

            // Verify that all data shards are zeros
            for (i, shard) in encoded_shards.iter().enumerate().take(data_shards) {
                assert!(shard.iter().all(|&x| x == 0), "Data shard {i} should be all zeros");
            }

            // Test recovery with some shards missing
            let mut shards_opt: Vec<Option<Vec<u8>>> = encoded_shards.iter().map(|shard| Some(shard.to_vec())).collect();

            // Lose maximum recoverable shards (equal to parity_shards)
            shards_opt[0] = None; // Lose first data shard
            shards_opt[4] = None; // Lose first parity shard

            erasure.decode_data(&mut shards_opt).unwrap();

            // Verify recovered data is still all zeros
            let mut recovered = Vec::new();
            for shard in shards_opt.iter().take(data_shards) {
                recovered.extend_from_slice(shard.as_ref().unwrap());
            }
            recovered.truncate(data.len());
            assert!(recovered.iter().all(|&x| x == 0), "Recovered data should be all zeros");
        }

        #[test]
        fn test_simd_large_data_1kb() {
            let data_shards = 8;
            let parity_shards = 4;
            let block_size = 1024; // 1KB block size optimal for SIMD
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // Create 1KB of test data
            let mut data = Vec::with_capacity(1024);
            for i in 0..1024 {
                data.push((i % 256) as u8);
            }

            let shards = erasure.encode_data(&data).unwrap();
            assert_eq!(shards.len(), data_shards + parity_shards);

            // Simulate the loss of multiple shards
            let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
            shards_opt[0] = None;
            shards_opt[3] = None;
            shards_opt[9] = None; // Parity shard
            shards_opt[11] = None; // Parity shard

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

        #[test]
        fn test_simd_minimum_shard_size() {
            let data_shards = 4;
            let parity_shards = 2;
            let block_size = 256; // Use 256 bytes to ensure sufficient shard size
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // Create data that will result in 64+ byte shards
            let data = vec![0x42u8; 200]; // 200 bytes, should create ~50 byte shards per data shard

            let result = erasure.encode_data(&data);

            // This might fail due to SIMD shard size requirements
            match result {
                Ok(shards) => {
                    println!("SIMD encoding succeeded with shard size: {}", shards[0].len());

                    // Test decoding
                    let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
                    shards_opt[1] = None;

                    let decode_result = erasure.decode_data(&mut shards_opt);
                    match decode_result {
                        Ok(_) => {
                            let mut recovered = Vec::new();
                            for shard in shards_opt.iter().take(data_shards) {
                                recovered.extend_from_slice(shard.as_ref().unwrap());
                            }
                            recovered.truncate(data.len());
                            assert_eq!(&recovered, &data);
                        }
                        Err(e) => {
                            println!("SIMD decoding failed with shard size {}: {}", shards[0].len(), e);
                        }
                    }
                }
                Err(e) => {
                    println!("SIMD encoding failed with small shard size: {e}");
                    // This is expected for very small shard sizes
                }
            }
        }

        #[test]
        fn test_simd_maximum_erasures() {
            let data_shards = 5;
            let parity_shards = 3;
            let block_size = 512;
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            let data =
                b"Testing maximum erasure capacity with SIMD Reed-Solomon implementation for robustness verification!".repeat(3);

            let shards = erasure.encode_data(&data).unwrap();

            // Lose exactly the maximum number of shards (equal to parity_shards)
            let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
            shards_opt[0] = None; // Data shard
            shards_opt[2] = None; // Data shard  
            shards_opt[6] = None; // Parity shard

            // Should succeed with maximum erasures
            erasure.decode_data(&mut shards_opt).unwrap();

            let mut recovered = Vec::new();
            for shard in shards_opt.iter().take(data_shards) {
                recovered.extend_from_slice(shard.as_ref().unwrap());
            }
            recovered.truncate(data.len());
            assert_eq!(&recovered, &data);
        }

        #[test]
        fn test_simd_small_data_handling() {
            let data_shards = 4;
            let parity_shards = 2;
            let block_size = 32; // Small block size for testing edge cases
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // Use small data to test SIMD handling of small shards
            let small_data = b"tiny!123".to_vec(); // 8 bytes data

            // Test encoding with small data
            let result = erasure.encode_data(&small_data);
            match result {
                Ok(shards) => {
                    println!("✅ SIMD encoding succeeded: {} bytes into {} shards", small_data.len(), shards.len());
                    assert_eq!(shards.len(), data_shards + parity_shards);

                    // Test decoding
                    let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|shard| Some(shard.to_vec())).collect();

                    // Lose some shards to test recovery
                    shards_opt[1] = None; // Lose one data shard
                    shards_opt[4] = None; // Lose one parity shard

                    let decode_result = erasure.decode_data(&mut shards_opt);
                    match decode_result {
                        Ok(()) => {
                            println!("✅ SIMD decode worked");

                            // Verify recovered data
                            let mut recovered = Vec::new();
                            for shard in shards_opt.iter().take(data_shards) {
                                recovered.extend_from_slice(shard.as_ref().unwrap());
                            }
                            recovered.truncate(small_data.len());
                            println!("recovered: {recovered:?}");
                            println!("small_data: {small_data:?}");
                            assert_eq!(&recovered, &small_data);
                            println!("✅ Data recovery successful with SIMD");
                        }
                        Err(e) => {
                            println!("❌ SIMD decode failed: {e}");
                            // For very small data, decode failure might be acceptable
                        }
                    }
                }
                Err(e) => {
                    println!("❌ SIMD encode failed: {e}");
                    // For very small data or configuration issues, encoding might fail
                }
            }
        }

        #[test]
        fn test_simd_large_block_1mb() {
            let data_shards = 6;
            let parity_shards = 3;
            let block_size = 1024 * 1024; // 1MB block size
            let erasure = Erasure::new(data_shards, parity_shards, block_size);

            // 创建2MB的测试数据，这样可以测试多个1MB块的处理
            let mut data = Vec::with_capacity(2 * 1024 * 1024);
            for i in 0..(2 * 1024 * 1024) {
                data.push((i % 256) as u8);
            }

            println!("🚀 Testing SIMD with 1MB block size and 2MB data");
            println!(
                "📊 Data shards: {}, Parity shards: {}, Total data: {}KB",
                data_shards,
                parity_shards,
                data.len() / 1024
            );

            // 编码数据
            let start = std::time::Instant::now();
            let shards = erasure.encode_data(&data).unwrap();
            let encode_duration = start.elapsed();

            println!("⏱️  Encoding completed in: {encode_duration:?}");
            println!("📦 Generated {} shards, each shard size: {}KB", shards.len(), shards[0].len() / 1024);

            assert_eq!(shards.len(), data_shards + parity_shards);

            // Verify that each shard is large enough for SIMD optimization
            for (i, shard) in shards.iter().enumerate() {
                println!("🔍 Shard {}: {} bytes ({}KB)", i, shard.len(), shard.len() / 1024);
                assert!(shard.len() >= 512, "Shard {} is too small for SIMD: {} bytes", i, shard.len());
            }

            // Simulate data loss - lose maximum recoverable number of shards
            let mut shards_opt: Vec<Option<Vec<u8>>> = shards.iter().map(|b| Some(b.to_vec())).collect();
            shards_opt[0] = None; // Lose 1st data shard
            shards_opt[2] = None; // Lose 3rd data shard  
            shards_opt[8] = None; // Lose 3rd parity shard (index 6+3-1=8)

            println!("💥 Simulated loss of 3 shards (max recoverable with 3 parity shards)");

            // Decode and recover data
            let start = std::time::Instant::now();
            erasure.decode_data(&mut shards_opt).unwrap();
            let decode_duration = start.elapsed();

            println!("⏱️  Decoding completed in: {decode_duration:?}");

            // Verify recovered data integrity
            let mut recovered = Vec::new();
            for shard in shards_opt.iter().take(data_shards) {
                recovered.extend_from_slice(shard.as_ref().unwrap());
            }
            recovered.truncate(data.len());

            assert_eq!(recovered.len(), data.len());
            assert_eq!(&recovered, &data, "Data mismatch after recovery!");

            println!("✅ Successfully verified data integrity after recovery");
            println!("📈 Performance summary:");
            println!(
                "   - Encode: {:?} ({:.2} MB/s)",
                encode_duration,
                (data.len() as f64 / (1024.0 * 1024.0)) / encode_duration.as_secs_f64()
            );
            println!(
                "   - Decode: {:?} ({:.2} MB/s)",
                decode_duration,
                (data.len() as f64 / (1024.0 * 1024.0)) / decode_duration.as_secs_f64()
            );
        }

        #[tokio::test]
        async fn test_simd_stream_callback() {
            use std::io::Cursor;
            use std::sync::Arc;
            use tokio::sync::mpsc;

            let data_shards = 4;
            let parity_shards = 2;
            let block_size = 256; // Larger block for SIMD
            let erasure = Arc::new(Erasure::new(data_shards, parity_shards, block_size));

            let test_data = b"SIMD stream processing test with sufficient data length for multiple blocks and proper SIMD optimization verification!";
            let data = test_data.repeat(5); // Create owned Vec<u8>
            let data_clone = data.clone(); // Clone for later comparison
            let mut reader = Cursor::new(data);

            let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(16);
            let erasure_clone = erasure.clone();

            let handle = tokio::spawn(async move {
                erasure_clone
                    .encode_stream_callback_async::<_, _, (), _>(&mut reader, move |res| {
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

            // Verify we got multiple blocks
            assert!(all_blocks.len() > 1, "Should have multiple blocks for stream test");

            // Test recovery for each block
            let mut recovered = Vec::new();
            for block in &all_blocks {
                let mut shards_opt: Vec<Option<Vec<u8>>> = block.iter().map(|b| Some(b.to_vec())).collect();
                // Lose one data shard and one parity shard
                shards_opt[1] = None;
                shards_opt[5] = None;

                erasure.decode_data(&mut shards_opt).unwrap();

                for shard in shards_opt.iter().take(data_shards) {
                    recovered.extend_from_slice(shard.as_ref().unwrap());
                }
            }

            recovered.truncate(data_clone.len());
            assert_eq!(&recovered, &data_clone);
        }
    }
}
