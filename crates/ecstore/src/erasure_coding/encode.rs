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
                        debug!(shard_len=shard.len(), error=?e, "bitrot writer write error");
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
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(8);

        let task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let block_size = self.block_size;
            let mut total = 0usize;
            let mut acc: Vec<u8> = Vec::with_capacity(block_size);
            let mut tmp = vec![0u8; 64 * 1024]; // read granularity 64KB
            loop {
                // Fill accumulator until full block or EOF
                if acc.len() < block_size {
                    let need = block_size - acc.len();
                    let chunk = if need < tmp.len() { &mut tmp[..need] } else { &mut tmp[..] };
                    let n = reader.read(chunk).await?;
                    if n == 0 {
                        // EOF
                        if !acc.is_empty() {
                            total += acc.len();
                            debug!(flush_len = acc.len(), total_bytes = total, "erasure encode final short block");
                            let res = self.encode_data(&acc)?;
                            if let Err(err) = tx.send(res).await {
                                return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                            }
                        }
                        break;
                    }
                    acc.extend_from_slice(&chunk[..n]);
                }
                if acc.len() == block_size {
                    total += acc.len();
                    debug!(block_bytes = acc.len(), total_bytes = total, "erasure encode full block");
                    let res = self.encode_data(&acc)?;
                    if let Err(err) = tx.send(res).await {
                        return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                    }
                    acc.clear();
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

        // Finalize all writers explicitly after receiving all blocks
        for w in writers.writers.iter_mut().flatten() {
            w.finalize();
        }

        let (reader, total) = task.await??;
        debug!(total_encoded_bytes = total, "erasure encode finished");
        // writers.shutdown().await?;
        Ok((reader, total))
    }
}
