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

        // Heal is best-effort per target disk. A single replacement disk failing to
        // write one block must not abort healing on the other healthy replacement
        // disks: requiring every target to succeed (write_quorum == available_writers)
        // let one flapping disk block redundancy recovery for the whole object and
        // kept the erasure set under-protected indefinitely. Upstream MinIO uses
        // writeQuorum == 1 for heal so partial progress commits; MultiWriter already
        // marks a failed writer as None (see encode.rs `write_shard`) and the ops
        // layer (set_disk/ops/heal.rs) then drops the failed writer while committing
        // the survivors. Reconstruction correctness is still guaranteed by the
        // read-side quorum check and parity cross-checks below, which are unaffected.
        let write_quorum = 1;
        let mut writers = MultiWriter::new(writers, write_quorum);
        let read_timeout = get_object_disk_read_timeout();
        let shard_file_size = self.shard_file_size(total_length as i64) as usize;

        for block_index in start_block..end_block {
            let shard_offset = block_index * self.shard_size();
            let shard_size = self.shard_size().min(shard_file_size.saturating_sub(shard_offset));
            let (mut shards, errs) = read_heal_shards(&mut readers, shard_size, read_timeout).await;

            // Every source shard is already bitrot-verified by its reader, so any
            // data_shards survivors are sufficient to reconstruct — requiring more
            // would make objects unhealable after losing exactly parity_shards disks,
            // the failure EC is sized to tolerate. The parity cross-checks below stay
            // opportunistic: they run whenever surplus source shards exist.
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
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    /// An `AsyncWrite` that accepts writes until `fail_at` bytes have been
    /// written, then fails every subsequent write. Used to simulate a
    /// replacement disk that starts failing on a mid-object block.
    struct FailAfterBytes {
        written: usize,
        fail_at: usize,
    }

    impl AsyncWrite for FailAfterBytes {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            if self.written >= self.fail_at {
                return Poll::Ready(Err(io::Error::other("injected heal write failure")));
            }
            self.written += buf.len();
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

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

    // Regression for backlog#947 (ECA-06): a single replacement disk failing to
    // write a mid-object block must not abort healing on the other healthy
    // replacement disks. Before the fix, write_quorum == available_writers made
    // MultiWriter::write return Err on the first per-writer failure, so heal
    // aborted the whole object and left the healthy targets unhealed.
    #[tokio::test]
    async fn heal_completes_healthy_targets_when_one_target_disk_fails_midway() {
        // 2 data + 3 parity: two data shards are readable sources, the three
        // parity positions are empty replacement disks to be healed.
        let erasure = Erasure::new(2, 3, 64);
        // Multiple blocks so the injected failure lands on a mid-object block.
        let data = (0..erasure.block_size * 3)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let encoded = erasure.encode_data(&data).expect("encode should succeed");

        // Only the two data shards survive as readable sources.
        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index < erasure.data_shards {
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

        // Parity index that will fail mid-way; the other two parity targets stay healthy.
        let failing_target = erasure.data_shards + 1;
        let mut writers = (0..erasure.total_shard_count())
            .map(|index| {
                if index < erasure.data_shards {
                    // Source positions get no writer.
                    None
                } else if index == failing_target {
                    // Fail after the first block's shard has been written.
                    Some(BitrotWriterWrapper::new(
                        CustomWriter::new_tokio_writer(FailAfterBytes {
                            written: 0,
                            fail_at: erasure.shard_size(),
                        }),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                    ))
                } else {
                    Some(BitrotWriterWrapper::new(
                        CustomWriter::new_inline_buffer(),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                    ))
                }
            })
            .collect::<Vec<_>>();

        erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect("heal must succeed on the healthy targets despite one failing disk");

        // The failing target was dropped (marked None) so the ops layer skips it.
        assert!(writers[failing_target].is_none(), "failing target writer must be dropped, not committed");

        // Every healthy replacement target is fully healed.
        for index in erasure.data_shards..erasure.total_shard_count() {
            if index == failing_target {
                continue;
            }
            let healed = writers[index]
                .take()
                .expect("healthy target writer should remain")
                .into_inline_data()
                .expect("inline writer should retain data");
            assert_eq!(healed, encoded[index].to_vec(), "healthy target {index} must be fully healed");
        }
    }

    // Regression for backlog#947: when every replacement disk fails to write,
    // heal must return Err (nothing is falsely reported as healed) so the ops
    // layer cleans up the tmp staging directory instead of committing.
    #[tokio::test]
    async fn heal_fails_when_all_target_disks_fail_midway() {
        let erasure = Erasure::new(2, 3, 64);
        let data = (0..erasure.block_size * 3)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let encoded = erasure.encode_data(&data).expect("encode should succeed");

        let readers = encoded
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                if index < erasure.data_shards {
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

        let mut writers = (0..erasure.total_shard_count())
            .map(|index| {
                if index < erasure.data_shards {
                    None
                } else {
                    // Every replacement target fails after the first block.
                    Some(BitrotWriterWrapper::new(
                        CustomWriter::new_tokio_writer(FailAfterBytes {
                            written: 0,
                            fail_at: erasure.shard_size(),
                        }),
                        erasure.shard_size(),
                        HashAlgorithm::None,
                    ))
                }
            })
            .collect::<Vec<_>>();

        erasure
            .heal(&mut writers, readers, data.len(), &[])
            .await
            .expect_err("heal must fail when all replacement disks fail");
    }
}
