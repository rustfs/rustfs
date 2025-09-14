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

use super::BitrotWriterWrapper;
use super::Erasure;
use crate::disk::error::Error;
use crate::disk::error_reduce::count_errs;
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_write_quorum_errs};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::sync::Arc;
use std::vec;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tracing::{debug, error};

// Environment variable keys (kept internal) for tuning without changing API surface.
const ENV_EC_CHANNEL_MIN: &str = "RUSTFS_EC_ENCODE_QUEUE_MIN";
const ENV_EC_CHANNEL_MAX: &str = "RUSTFS_EC_ENCODE_QUEUE_MAX";
#[allow(dead_code)]
const ENV_EC_BLOCKS_IN_FLIGHT: &str = "RUSTFS_EC_BLOCKS_IN_FLIGHT"; // future use
const ENV_EC_DISABLE_ADAPTIVE: &str = "RUSTFS_EC_ADAPTIVE_DISABLE";

fn adaptive_channel_depth(object_hint: Option<i64>, block_size: usize) -> usize {
    if std::env::var(ENV_EC_DISABLE_ADAPTIVE).is_ok() {
        return 8; // legacy fixed depth
    }
    // Default bounds
    let default_min = 8usize;
    let default_max = 32usize;

    let min_depth = std::env::var(ENV_EC_CHANNEL_MIN)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_min);
    let max_depth = std::env::var(ENV_EC_CHANNEL_MAX)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= min_depth)
        .unwrap_or(default_max);

    let hint = object_hint.unwrap_or(-1);
    if hint <= 0 {
        return min_depth;
    }

    // Estimate number of blocks; clamp between min_depth and max_depth.
    let blocks = ((hint as usize) / block_size).max(1);
    if blocks < min_depth {
        min_depth
    } else if blocks > max_depth {
        max_depth
    } else {
        blocks
    }
}

pub(crate) struct MultiWriter<'a> {
    writers: &'a mut [Option<BitrotWriterWrapper>],
    write_quorum: usize,
    errs: Vec<Option<Error>>,
}

impl<'a> MultiWriter<'a> {
    pub fn new(writers: &'a mut [Option<BitrotWriterWrapper>], write_quorum: usize) -> Self {
        let length = writers.len();
        MultiWriter {
            writers,
            write_quorum,
            errs: vec![None; length],
        }
    }

    async fn write_shard(writer_opt: &mut Option<BitrotWriterWrapper>, err: &mut Option<Error>, shard: &Bytes) {
        match writer_opt {
            Some(writer) => {
                match writer.write(shard).await {
                    Ok(n) => {
                        if n < shard.len() {
                            *err = Some(Error::ShortWrite);
                            *writer_opt = None; // Mark as failed
                        } else {
                            *err = None;
                        }
                    }
                    Err(e) => {
                        *err = Some(Error::from(e));
                    }
                }
            }
            None => {
                *err = Some(Error::DiskNotFound);
            }
        }
    }

    pub async fn write(&mut self, data: Vec<Bytes>) -> std::io::Result<()> {
        assert_eq!(data.len(), self.writers.len());

        {
            let mut futures = FuturesUnordered::new();
            for ((writer_opt, err), shard) in self.writers.iter_mut().zip(self.errs.iter_mut()).zip(data.iter()) {
                if err.is_some() {
                    continue; // Skip if we already have an error for this writer
                }
                futures.push(Self::write_shard(writer_opt, err, shard));
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
            error!(
                "reduce_write_quorum_errs: {:?}, offline-disks={}/{}, errs={:?}",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len(),
                self.errs
            );
            return Err(std::io::Error::other(format!(
                "Failed to write data: {} (offline-disks={}/{})",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len()
            )));
        }

        Err(std::io::Error::other(format!(
            "Failed to write data:  (offline-disks={}/{}): {}",
            count_errs(&self.errs, &Error::DiskNotFound),
            self.writers.len(),
            self.errs
                .iter()
                .map(|e| e.as_ref().map_or("<nil>".to_string(), |e| e.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    pub async fn _shutdown(&mut self) -> std::io::Result<()> {
        for writer in self.writers.iter_mut().flatten() {
            writer.shutdown().await?;
        }
        Ok(())
    }
}

impl Erasure {
    pub async fn encode<R>(
        self: Arc<Self>,
        mut reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // Derive adaptive channel depth from object size hint if HashReader exposes size via actual_size().
        // We cannot access size directly from generic R; rely on environment hint passed by caller through quorum heuristic.
        // For now, pass None (future: plumb size). Keeping existing behaviour when unknown.
        let chan_cap = adaptive_channel_depth(None, self.block_size);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(chan_cap);
        debug!(capacity = chan_cap, "erasure encode channel capacity decided");

        let task = tokio::spawn(async move {
            let block_size = self.block_size;
            let mut total = 0;
            let mut buf = vec![0u8; block_size];
            loop {
                match rustfs_utils::read_full(&mut reader, &mut buf).await {
                    Ok(n) if n > 0 => {
                        total += n;
                        let res = self.encode_data(&buf[..n])?;
                        if let Err(err) = tx.send(res).await {
                            return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                        }
                    }
                    Ok(_) => break,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            Ok((reader, total))
        });

        let mut writers = MultiWriter::new(writers, quorum);

        while let Some(block) = rx.recv().await {
            if block.is_empty() {
                break;
            }
            writers.write(block).await?;
        }

        let (reader, total) = task.await??;
        // writers.shutdown().await?;
        Ok((reader, total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_encode_adaptive_channel_basic() {
        let e = Arc::new(Erasure::new(4, 2, 1024));
        let data = vec![0u8; 10 * 1024];
        let cursor = Cursor::new(data.clone());

        // Prepare in-memory writers (inline bitrot writers) by constructing custom wrappers.
        // We reuse create_bitrot_writer via inline path (no disk) to simulate writers.
        use crate::bitrot::create_bitrot_writer;
        use rustfs_utils::HashAlgorithm;

        let mut writers: Vec<Option<BitrotWriterWrapper>> = Vec::new();
        for _ in 0..6 {
            let w = create_bitrot_writer(true, None, "v", "p", 0, 1024, HashAlgorithm::HighwayHash256)
                .await
                .unwrap();
            writers.push(Some(w));
        }

        let quorum = 4; // data shards
        let (_r, total) = e.encode(cursor, &mut writers, quorum).await.unwrap();
        assert!(total > 0);
    }
}
