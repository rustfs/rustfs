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

use crate::disk::error::{Error, Result};
use crate::erasure::coding::BitrotReader;
use crate::erasure::coding::BitrotWriterWrapper;
use crate::erasure::coding::decode::ParallelReader;
use crate::erasure::coding::encode::MultiWriter;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tracing::{info, warn};

impl super::Erasure {
    pub async fn heal<R>(
        &self,
        writers: &mut [Option<BitrotWriterWrapper>],
        readers: Vec<Option<BitrotReader<R>>>,
        total_length: usize,
        _prefer: &[bool],
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + Sync,
    {
        info!(
            "Erasure heal, writers len: {}, readers len: {}, total_length: {}",
            writers.len(),
            readers.len(),
            total_length
        );
        if writers.len() != self.parity_shards + self.data_shards {
            return Err(Error::other("invalid argument"));
        }
        let mut reader = ParallelReader::new(readers, self.clone(), 0, total_length);

        let start_block = 0;
        let mut end_block = total_length / self.block_size;
        if !total_length.is_multiple_of(self.block_size) {
            end_block += 1;
        }

        let available_writers = writers.iter().filter(|w| w.is_some()).count();
        let write_quorum = available_writers.max(1);
        let mut writers = MultiWriter::new(writers, write_quorum);

        for _ in start_block..end_block {
            let (mut shards, errs) = reader.read().await;

            // Check if we have enough shards to reconstruct data
            // We need at least data_shards available shards (data + parity combined)
            let available_shards = errs.iter().filter(|e| e.is_none()).count();
            if available_shards < self.data_shards {
                warn!(
                    required_data_shards = self.data_shards,
                    available_shards,
                    total_shards = errs.len(),
                    errors = ?errs,
                    "Erasure heal read quorum unavailable"
                );
                return Err(Error::ErasureReadQuorum);
            }

            if self.parity_shards > 0 {
                self.decode_data_and_parity(&mut shards)?;
            }

            let shards = shards
                .into_iter()
                .map(|s| Bytes::from(s.unwrap_or_default()))
                .collect::<Vec<_>>();

            writers.write(shards).await?;
        }

        writers.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::coding::{CustomWriter, Erasure};
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;

    #[tokio::test]
    async fn heal_reconstructs_missing_parity_shard() {
        let erasure = Erasure::new(2, 2, 64);
        let data = b"heal should write a rebuilt parity shard";
        let encoded = erasure.encode_data(data).expect("encode should succeed");
        let missing_parity = erasure.data_shards;

        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index == missing_parity {
                    None
                } else {
                    Some(BitrotReader::new(
                        Cursor::new(shard.to_vec()),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                        false,
                    ))
                }
            })
            .collect::<Vec<_>>();

        let mut writers = (0..erasure.total_shard_count())
            .map(|index| {
                if index == missing_parity {
                    Some(BitrotWriterWrapper::new(
                        CustomWriter::new_inline_buffer(),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect("heal should rebuild parity");

        let healed = writers[missing_parity]
            .take()
            .expect("parity writer should remain")
            .into_inline_data()
            .expect("inline writer should retain data");
        assert_eq!(healed, encoded[missing_parity].to_vec());
    }

    #[tokio::test]
    async fn heal_reconstructs_missing_data_shard_across_multiple_blocks() {
        let erasure = Erasure::new(3, 2, 96);
        let data = (0..erasure.block_size * 2 + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let encoded = erasure.encode_data(&data).expect("encode should succeed");
        let missing_data = 1;

        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index == missing_data {
                    None
                } else {
                    Some(BitrotReader::new(
                        Cursor::new(shard.to_vec()),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                        false,
                    ))
                }
            })
            .collect::<Vec<_>>();

        let mut writers = (0..erasure.total_shard_count())
            .map(|index| {
                if index == missing_data {
                    Some(BitrotWriterWrapper::new(
                        CustomWriter::new_inline_buffer(),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect("heal should rebuild data");

        let healed = writers[missing_data]
            .take()
            .expect("data writer should remain")
            .into_inline_data()
            .expect("inline writer should retain data");
        assert_eq!(healed, encoded[missing_data].to_vec());
    }

    #[tokio::test]
    async fn heal_returns_read_quorum_when_available_shards_are_insufficient() {
        let erasure = Erasure::new(3, 2, 64);
        let data = b"heal should fail before decode when too few shards are readable";
        let encoded = erasure.encode_data(data).expect("encode should succeed");

        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index < 2 {
                    Some(BitrotReader::new(
                        Cursor::new(shard.to_vec()),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                        false,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut writers = (0..erasure.total_shard_count()).map(|_| None).collect::<Vec<_>>();

        let err = erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect_err("heal should fail when available shards are below data shards");

        assert!(matches!(err, Error::ErasureReadQuorum));
    }
}
