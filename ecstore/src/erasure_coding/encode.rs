use super::BitrotWriterWrapper;
use super::Erasure;
use crate::disk::error::Error;
use crate::disk::error_reduce::count_errs;
use crate::disk::error_reduce::{OBJECT_OP_IGNORED_ERRS, reduce_write_quorum_errs};
use bytes::Bytes;
use std::sync::Arc;
use std::vec;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

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

    #[allow(clippy::needless_range_loop)]
    pub async fn write(&mut self, data: Vec<Bytes>) -> std::io::Result<()> {
        for i in 0..self.writers.len() {
            if self.errs[i].is_some() {
                continue; // Skip if we already have an error for this writer
            }

            let writer_opt = &mut self.writers[i];
            let shard = &data[i];

            if let Some(writer) = writer_opt {
                match writer.write(shard).await {
                    Ok(n) => {
                        if n < shard.len() {
                            self.errs[i] = Some(Error::ShortWrite);
                            self.writers[i] = None; // Mark as failed
                        } else {
                            self.errs[i] = None;
                        }
                    }
                    Err(e) => {
                        self.errs[i] = Some(Error::from(e));
                    }
                }
            } else {
                self.errs[i] = Some(Error::DiskNotFound);
            }
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count > self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
            return Err(std::io::Error::other(format!(
                "Failed to write data: {} (offline-disks={}/{})",
                write_err,
                count_errs(&self.errs, &Error::DiskNotFound),
                self.writers.len()
            )));
        }

        Err(std::io::Error::other(format!(
            "Failed to write data:  (offline-disks={}/{})",
            count_errs(&self.errs, &Error::DiskNotFound),
            self.writers.len()
        )))
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
            let block_size = self.block_size;
            let mut total = 0;
            loop {
                let mut buf = vec![0u8; block_size];
                match rustfs_utils::read_full(&mut reader, &mut buf).await {
                    Ok(n) if n > 0 => {
                        total += n;
                        let res = self.encode_data(&buf[..n])?;
                        if let Err(err) = tx.send(res).await {
                            return Err(std::io::Error::other(format!("Failed to send encoded data : {}", err)));
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
                buf.clear();
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

        Ok((reader, total))
    }
}
