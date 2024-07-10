use anyhow::anyhow;
use anyhow::Error;
use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use reed_solomon_erasure::galois_8::ReedSolomon;
use s3s::StdError;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use uuid::Uuid;

use crate::chunk_stream::ChunkedStream;

pub struct Erasure {
    // data_shards: usize,
    // parity_shards: usize,
    encoder: ReedSolomon,
    id: Uuid,
}

impl Erasure {
    pub fn new(data_shards: usize, parity_shards: usize) -> Self {
        Erasure {
            // data_shards,
            // parity_shards,
            encoder: ReedSolomon::new(data_shards, parity_shards).unwrap(),
            id: Uuid::new_v4(),
        }
    }

    pub async fn encode<S, W>(
        &self,
        body: S,
        writers: &mut Vec<W>,
        block_size: usize,
        data_size: usize,
        _write_quorum: usize,
    ) -> Result<usize>
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
        W: AsyncWrite + Unpin,
    {
        let mut stream = ChunkedStream::new(body, data_size, block_size, true);
        let mut total: usize = 0;
        let mut idx = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    let blocks = self.encode_data(data.as_ref())?;

                    let mut errs = Vec::new();
                    idx += 1;
                    for (i, w) in writers.iter_mut().enumerate() {
                        total += blocks[i].len();

                        debug!(
                            "{} {}-{} encode write {} , total:{}, readed:{}",
                            self.id,
                            idx,
                            i,
                            blocks[i].len(),
                            data_size,
                            total
                        );

                        match w.write(blocks[i].as_ref()).await {
                            Ok(_) => errs.push(None),
                            Err(e) => errs.push(Some(e)),
                        }
                    }

                    debug!("{} encode_data write errs:{:?}", self.id, errs);
                    // TODO: reduceWriteQuorumErrs
                    for err in errs.iter() {
                        if err.is_some() {
                            return Err(Error::msg("message"));
                        }
                    }
                }
                Err(e) => return Err(anyhow!(e)),
            }
        }

        debug!("{} encode_data done  {}", self.id, total);

        Ok(total)

        // loop {
        //     match rd.next().await {
        //         Some(res) => todo!(),
        //         None => todo!(),
        //     }
        // }
    }

    pub fn encode_data(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let (shard_size, total_size) = self.need_size(data.len());

        let mut data_buffer = vec![0u8; total_size];
        {
            let (left, _) = data_buffer.split_at_mut(data.len());
            left.copy_from_slice(data);
        }

        {
            let data_slices: Vec<&mut [u8]> = data_buffer.chunks_mut(shard_size).collect();

            self.encoder.encode(data_slices)?;
        }

        // Ok(data_buffer)

        let mut shards = Vec::with_capacity(self.encoder.total_shard_count());

        let slices: Vec<&[u8]> = data_buffer.chunks(shard_size).collect();

        for &d in slices.iter() {
            shards.push(d.to_vec());
        }

        Ok(shards)
    }

    pub fn decode_data(&self, shards: &mut Vec<Option<Vec<u8>>>) -> Result<()> {
        self.encoder.reconstruct(shards)?;
        Ok(())
    }

    // 每个分片长度，所需要的总长度
    fn need_size(&self, data_size: usize) -> (usize, usize) {
        let shard_size = self.shard_size(data_size);
        (shard_size, shard_size * (self.encoder.total_shard_count()))
    }

    fn shard_size(&self, data_size: usize) -> usize {
        (data_size + self.encoder.data_shard_count() - 1) / self.encoder.data_shard_count()
    }
}

// fn shards_to_option_shards<T: Clone>(shards: &[Vec<T>]) -> Vec<Option<Vec<T>>> {
//     let mut result = Vec::with_capacity(shards.len());

//     for v in shards.iter() {
//         let inner: Vec<T> = v.clone();
//         result.push(Some(inner));
//     }
//     result
// }

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_erasure() {
        let data_shards = 3;
        let parity_shards = 2;
        let data: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let ec = Erasure::new(data_shards, parity_shards);
        let shards = ec.encode_data(data).unwrap();
        println!("shards:{:?}", shards);

        let mut s: Vec<_> = shards
            .iter()
            .map(|d| if d.is_empty() { None } else { Some(d.clone()) })
            .collect();

        // let mut s = shards_to_option_shards(&shards);

        // s[0] = None;
        s[4] = None;
        s[3] = None;

        println!("sss:{:?}", &s);

        ec.decode_data(&mut s).unwrap();
        // ec.encoder.reconstruct(&mut s).unwrap();

        println!("sss:{:?}", &s);
    }
}
