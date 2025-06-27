use super::BitrotReader;
use super::BitrotWriterWrapper;
use super::decode::ParallelReader;
use crate::disk::error::{Error, Result};
use crate::erasure_coding::encode::MultiWriter;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tracing::info;

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
        if total_length % self.block_size != 0 {
            end_block += 1;
        }

        for _ in start_block..end_block {
            let (mut shards, errs) = reader.read().await;

            if errs.iter().filter(|e| e.is_none()).count() < self.data_shards {
                return Err(Error::other(format!("can not reconstruct data: not enough data shards {errs:?}")));
            }

            if self.parity_shards > 0 {
                self.decode_data(&mut shards)?;
            }

            let shards = shards
                .into_iter()
                .map(|s| Bytes::from(s.unwrap_or_default()))
                .collect::<Vec<_>>();

            let mut writers = MultiWriter::new(writers, self.data_shards);
            writers.write(shards).await?;
        }

        Ok(())
    }
}
