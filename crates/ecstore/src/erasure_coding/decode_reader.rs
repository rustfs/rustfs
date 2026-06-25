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

use crate::disk::error::Error as DiskError;
use crate::erasure_codec::bridge::ErasureDecodeEngine;
use crate::get_diagnostics::{GET_OBJECT_PATH_CODEC_STREAMING, GET_STAGE_EMIT, GET_STAGE_RECONSTRUCT, GET_STAGE_STRIPE_READ};
use crate::set_disk::shard_source::{ShardStripeSource, StripeReadState};
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};

type FillFuture<S, W> = Pin<Box<dyn Future<Output = FillResult<S, W>> + Send>>;

struct FillResult<S, W> {
    source: S,
    workspace: W,
    result: io::Result<Option<Vec<u8>>>,
}

pub(crate) struct ErasureDecodeReader<S, E>
where
    E: ErasureDecodeEngine,
{
    source: Option<S>,
    engine: E,
    workspace: Option<E::Workspace>,
    output_buf: Vec<u8>,
    output_pos: usize,
    remaining: usize,
    fill: Option<FillFuture<S, E::Workspace>>,
}

impl<S, E> ErasureDecodeReader<S, E>
where
    S: ShardStripeSource + Send + 'static,
    E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(source: S, engine: E, total_length: usize) -> io::Result<Self> {
        if engine.data_shards() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires data shards"));
        }
        if engine.block_size() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires non-zero block size"));
        }

        let shard_len = engine.block_size().div_ceil(engine.data_shards());
        let workspace = engine.prepare_workspace(shard_len)?;

        Ok(Self {
            source: Some(source),
            engine,
            workspace: Some(workspace),
            output_buf: Vec::new(),
            output_pos: 0,
            remaining: total_length,
            fill: None,
        })
    }

    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fill.is_none() {
            let Some(mut source) = self.source.take() else {
                return Poll::Ready(Err(io::Error::new(ErrorKind::BrokenPipe, "erasure reader source missing")));
            };
            let Some(mut workspace) = self.workspace.take() else {
                self.source = Some(source);
                return Poll::Ready(Err(io::Error::new(ErrorKind::BrokenPipe, "erasure reader workspace missing")));
            };

            let engine = self.engine.clone();
            let remaining = self.remaining;
            self.fill = Some(Box::pin(async move {
                let stripe_read_stage_start = Instant::now();
                let state = source.read_next_stripe().await;
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_STRIPE_READ,
                    stripe_read_stage_start.elapsed().as_secs_f64(),
                );
                let result = decode_stripe(&engine, &mut workspace, state, remaining);
                FillResult {
                    source,
                    workspace,
                    result,
                }
            }));
        }

        let fill = self
            .fill
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::BrokenPipe, "erasure reader fill future missing"))?;
        let FillResult {
            source,
            workspace,
            result,
        } = ready!(fill.as_mut().poll(cx));

        self.source = Some(source);
        self.workspace = Some(workspace);
        self.fill = None;

        match result {
            Ok(Some(buf)) => {
                if buf.is_empty() && self.remaining > 0 {
                    return Poll::Ready(Err(DiskError::LessData.into()));
                }
                rustfs_io_metrics::record_get_object_reader_stripe(GET_OBJECT_PATH_CODEC_STREAMING);
                rustfs_io_metrics::record_get_object_reader_bytes(GET_OBJECT_PATH_CODEC_STREAMING, buf.len());
                self.remaining -= buf.len();
                self.output_buf = buf;
                self.output_pos = 0;
                Poll::Ready(Ok(()))
            }
            Ok(None) => {
                if self.remaining == 0 {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(DiskError::LessData.into()))
                }
            }
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

impl<S, E> Unpin for ErasureDecodeReader<S, E> where E: ErasureDecodeEngine {}

impl<S, E> AsyncRead for ErasureDecodeReader<S, E>
where
    S: ShardStripeSource + Send + 'static,
    E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            if self.output_pos < self.output_buf.len() {
                let available = &self.output_buf[self.output_pos..];
                let copy_len = available.len().min(buf.remaining());
                buf.put_slice(&available[..copy_len]);
                self.output_pos += copy_len;
                return Poll::Ready(Ok(()));
            }

            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            ready!(self.poll_fill(cx))?;
        }
    }
}

pub(crate) struct SyncErasureDecodeReader<R> {
    inner: Mutex<R>,
}

impl<R> SyncErasureDecodeReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl<R> AsyncRead for SyncErasureDecodeReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(_) => return Poll::Ready(Err(io::Error::other("erasure decode reader lock poisoned"))),
        };
        Pin::new(&mut *inner).poll_read(cx, buf)
    }
}

fn decode_stripe<E>(
    engine: &E,
    workspace: &mut E::Workspace,
    state: StripeReadState,
    remaining: usize,
) -> io::Result<Option<Vec<u8>>>
where
    E: ErasureDecodeEngine,
{
    if state.slots().is_empty() {
        return Ok(None);
    }
    if !state.can_decode() {
        return Err(DiskError::ErasureReadQuorum.into());
    }

    let (mut shards, _errs) = state.into_parts();
    let reconstruct_stage_start = Instant::now();
    if let Err(err) = engine.reconstruct_into(&mut shards, workspace) {
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_RECONSTRUCT,
            reconstruct_stage_start.elapsed().as_secs_f64(),
        );
        return Err(err);
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        GET_OBJECT_PATH_CODEC_STREAMING,
        GET_STAGE_RECONSTRUCT,
        reconstruct_stage_start.elapsed().as_secs_f64(),
    );

    if shards.len() < engine.data_shards() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "decoded stripe has fewer shards than data shard count",
        ));
    }

    let emit_stage_start = Instant::now();
    let mut output = Vec::with_capacity(engine.block_size().min(remaining));
    for shard in shards.iter().take(engine.data_shards()) {
        if output.len() >= remaining {
            break;
        }
        let Some(shard) = shard else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "decoded stripe is missing a data shard"));
        };
        let copy_len = shard.len().min(remaining - output.len());
        output.extend_from_slice(&shard[..copy_len]);
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        GET_OBJECT_PATH_CODEC_STREAMING,
        GET_STAGE_EMIT,
        emit_stage_start.elapsed().as_secs_f64(),
    );

    Ok(Some(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure_codec::bridge::LegacyEcDecodeEngine;
    use crate::erasure_coding::Erasure;
    use crate::set_disk::shard_source::StripeReadState;
    use std::collections::VecDeque;
    use tokio::io::AsyncReadExt;

    struct VecStripeSource {
        stripes: VecDeque<StripeReadState>,
        read_quorum: usize,
    }

    #[async_trait::async_trait]
    impl ShardStripeSource for VecStripeSource {
        async fn read_next_stripe(&mut self) -> StripeReadState {
            self.stripes
                .pop_front()
                .unwrap_or_else(|| StripeReadState::new(Vec::new(), self.read_quorum))
        }
    }

    fn source_from_data(erasure: &Erasure, data: &[u8], missing_indexes: &[usize]) -> VecStripeSource {
        let read_quorum = erasure.data_shards;
        let stripes = data
            .chunks(erasure.block_size)
            .map(|chunk| {
                let shards = erasure
                    .encode_data(chunk)
                    .expect("test stripe should encode")
                    .into_iter()
                    .enumerate()
                    .map(|(index, shard)| {
                        if missing_indexes.contains(&index) {
                            None
                        } else {
                            Some(shard.to_vec())
                        }
                    })
                    .collect();
                StripeReadState::from_parts(shards, Vec::new(), read_quorum)
            })
            .collect();

        VecStripeSource { stripes, read_quorum }
    }

    async fn decode_all(erasure: Erasure, data: &[u8], missing_indexes: &[usize]) -> io::Result<Vec<u8>> {
        let source = source_from_data(&erasure, data, missing_indexes);
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, data.len())?;
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await?;
        Ok(decoded)
    }

    #[tokio::test]
    async fn erasure_decode_reader_reads_single_stripe() {
        let erasure = Erasure::new(4, 2, 64);
        let data = b"single stripe decode reader output";

        let decoded = decode_all(erasure, data, &[])
            .await
            .expect("single stripe reader should decode");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_reads_multiple_stripes() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..150u16).map(|value| value.to_le_bytes()[0]).collect::<Vec<_>>();

        let decoded = decode_all(erasure, &data, &[])
            .await
            .expect("multi stripe reader should decode");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_stops_at_eof_for_empty_object() {
        let erasure = Erasure::new(4, 2, 32);
        let source = source_from_data(&erasure, &[], &[]);
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, 0).expect("empty reader should be constructed");
        let mut decoded = Vec::new();

        let read = reader
            .read_to_end(&mut decoded)
            .await
            .expect("empty reader should finish without reading stripes");

        assert_eq!(read, 0);
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_reconstructs_missing_data_shard() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..120u16)
            .map(|value| value.wrapping_mul(17).to_le_bytes()[0])
            .collect::<Vec<_>>();

        let decoded = decode_all(erasure, &data, &[1])
            .await
            .expect("reader should reconstruct one missing data shard");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_reports_short_source() {
        let erasure = Erasure::new(4, 2, 32);
        let source = VecStripeSource {
            stripes: VecDeque::new(),
            read_quorum: erasure.data_shards,
        };
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, 1).expect("reader should be constructed");
        let mut decoded = Vec::new();

        let err = reader
            .read_to_end(&mut decoded)
            .await
            .expect_err("reader should reject EOF before requested length");

        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(decoded.is_empty());
    }
}
