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

use crate::disk::disk_store::get_object_disk_read_timeout;
use crate::disk::error::{Error, Result};
use crate::erasure::coding::BitrotReader;
use crate::erasure::coding::BitrotWriterWrapper;
use crate::erasure::coding::encode::MultiWriter;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::io;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::{info, warn};

async fn read_heal_shards<R>(
    readers: &mut [Option<BitrotReader<R>>],
    shard_size: usize,
    read_timeout: Duration,
) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>)
where
    R: AsyncRead + Unpin + Send + Sync,
{
    let num_readers = readers.len();
    let mut shards = vec![None; num_readers];
    let mut errs = vec![None; num_readers];
    let mut retire_readers = Vec::new();

    if shard_size == 0 {
        return (shards, errs);
    }

    {
        let mut futures = FuturesUnordered::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let Some(reader) = reader else {
                errs[index] = Some(Error::FileNotFound);
                continue;
            };

            futures.push(Box::pin(async move {
                let mut buf = vec![0; shard_size];
                let read_result = if read_timeout.is_zero() {
                    reader.read(&mut buf).await
                } else {
                    match tokio::time::timeout(read_timeout, reader.read(&mut buf)).await {
                        Ok(result) => result,
                        Err(_) => {
                            return (
                                index,
                                Err(Error::from(io::Error::new(ErrorKind::TimedOut, "heal shard read timed out"))),
                                true,
                            );
                        }
                    }
                };

                match read_result {
                    Ok(n) => {
                        buf.truncate(n);
                        (index, Ok(buf), false)
                    }
                    Err(err) => {
                        let should_retire = err.kind() == ErrorKind::TimedOut;
                        (index, Err(Error::from(err)), should_retire)
                    }
                }
            }));
        }

        while let Some((index, result, should_retire)) = futures.next().await {
            match result {
                Ok(shard) => {
                    shards[index] = Some(shard);
                }
                Err(err) => {
                    errs[index] = Some(err);
                    if should_retire {
                        retire_readers.push(index);
                    }
                }
            }
        }
    }

    for index in retire_readers {
        readers[index] = None;
        warn!(shard_index = index, "retiring timed-out heal shard reader");
    }

    (shards, errs)
}

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
        let mut readers = readers;

        let start_block = 0;
        let mut end_block = total_length / self.block_size;
        if !total_length.is_multiple_of(self.block_size) {
            end_block += 1;
        }

        let available_writers = writers.iter().filter(|w| w.is_some()).count();
        let write_quorum = available_writers.max(1);
        let mut writers = MultiWriter::new(writers, write_quorum);
        let read_timeout = get_object_disk_read_timeout();
        let shard_file_size = self.shard_file_size(total_length as i64) as usize;

        for block_index in start_block..end_block {
            let shard_offset = block_index * self.shard_size();
            let shard_size = self.shard_size().min(shard_file_size.saturating_sub(shard_offset));
            let (mut shards, errs) = read_heal_shards(&mut readers, shard_size, read_timeout).await;

            // Data reads may use the first read quorum, but heal writes must only
            // proceed when the source set is strong enough to validate itself.
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

            let missing_data_source = shards.iter().take(self.data_shards).any(|shard| shard.is_none());
            let required_shards = if missing_data_source && self.parity_shards > 0 {
                self.data_shards + 1
            } else {
                self.data_shards
            };
            if available_shards < required_shards {
                return Err(Error::other(format!(
                    "can not reconstruct data: not enough verified heal source shards (need {}, have {}) {errs:?}",
                    required_shards, available_shards
                )));
            }

            let source_parity = shards
                .iter()
                .enumerate()
                .skip(self.data_shards)
                .filter_map(|(index, shard)| shard.as_ref().map(|shard| (index, shard.clone())))
                .collect::<Vec<_>>();
            if self.parity_shards > 0 {
                self.decode_data_and_parity(&mut shards)?;
                if !source_parity.is_empty() && !self.verify_data_and_parity(&shards)? {
                    return Err(Error::other("can not reconstruct data: inconsistent heal source shards"));
                }
                for (index, source) in source_parity {
                    let Some(rebuilt) = shards[index].as_ref() else {
                        return Err(Error::other("can not reconstruct data: missing rebuilt parity shard"));
                    };
                    if rebuilt != &source {
                        return Err(Error::other("can not reconstruct data: inconsistent heal source shards"));
                    }
                }
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

    #[tokio::test]
    async fn heal_rejects_inconsistent_sources_before_writing_data_shard() {
        let erasure = Erasure::new(2, 2, 64);
        let data = b"heal must not rebuild data from a stale parity shard";
        let encoded = erasure.encode_data(data).expect("encode should succeed");
        let missing_data = 1;
        let corrupt_parity = erasure.data_shards;

        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index == missing_data {
                    return None;
                }

                let mut shard = shard.to_vec();
                if index == corrupt_parity {
                    shard[0] ^= 0x5a;
                }

                Some(BitrotReader::new(Cursor::new(shard), erasure.shard_size(), HashAlgorithm::None, false))
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

        let err = erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect_err("heal should reject inconsistent source shards");
        assert!(err.to_string().contains("inconsistent heal source shards"));

        let written = writers[missing_data]
            .take()
            .expect("data writer should remain")
            .into_inline_data()
            .expect("inline writer should retain data");
        assert!(written.is_empty(), "heal must fail before writing rebuilt data");
    }
}
