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
use crate::disk::error::Error;
use crate::disk::error_reduce::reduce_errs;
use crate::erasure_codec::workspace::ShardBufferPool;
use crate::erasure_coding::{BitrotReader, Erasure};
use crate::get_diagnostics::{
    GET_OBJECT_PATH_LEGACY_DUPLEX, GET_SHARD_READ_ERROR_MISSING, GET_SHARD_READ_ERROR_NONE, GET_SHARD_READ_OUTCOME_ERROR,
    GET_SHARD_READ_OUTCOME_MISSING, GET_SHARD_READ_OUTCOME_SUCCESS, GET_SHARD_ROLE_DATA, GET_SHARD_ROLE_PARITY, GET_STAGE_EMIT,
    GET_STAGE_RANGE, GET_STAGE_RECONSTRUCT, GET_STAGE_STRIPE_READ, GET_STAGE_STRIPE_READ_FIRST_SHARD,
    GET_STAGE_STRIPE_READ_QUORUM, GetObjectFailureReason, classify_io_error, record_get_object_pipeline_failure,
};
use crate::set_disk::shard_source::{ShardReadCost, ShardStripeSource, StripeReadState};
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use pin_project_lite::pin_project;
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::{error, warn};

type ShardReadFuture<'a> = Pin<Box<dyn Future<Output = (usize, ShardReadCost, Result<Vec<u8>, Error>, bool)> + Send + 'a>>;

#[derive(Default)]
struct ShardReadCostCounts {
    local: usize,
    same_node: usize,
    remote: usize,
    unknown: usize,
}

impl ShardReadCostCounts {
    fn record(&mut self, cost: ShardReadCost) {
        match cost {
            ShardReadCost::Local => self.local += 1,
            ShardReadCost::SameNode => self.same_node += 1,
            ShardReadCost::Remote => self.remote += 1,
            ShardReadCost::Unknown => self.unknown += 1,
        }
    }

    fn low_cost(&self) -> usize {
        self.local + self.same_node
    }
}
fn shard_role(index: usize, data_shards: usize) -> &'static str {
    if index < data_shards {
        GET_SHARD_ROLE_DATA
    } else {
        GET_SHARD_ROLE_PARITY
    }
}

fn read_shard<'a, R>(
    index: usize,
    read_cost: ShardReadCost,
    reader: &'a mut Option<BitrotReader<R>>,
    recycled_buf: Option<Vec<u8>>,
    shard_size: usize,
    data_shards: usize,
    read_timeout: Duration,
    metrics_path: Option<&'static str>,
) -> ShardReadFuture<'a>
where
    R: AsyncRead + Unpin + Send + Sync + 'a,
{
    let role = shard_role(index, data_shards);
    if let Some(reader) = reader {
        Box::pin(async move {
            let mut buf = recycled_buf.unwrap_or_else(|| vec![0; shard_size]);
            debug_assert_eq!(buf.len(), shard_size);
            let read_start = Instant::now();
            let read_result = if read_timeout.is_zero() {
                reader.read(&mut buf).await
            } else {
                match tokio::time::timeout(read_timeout, reader.read(&mut buf)).await {
                    Ok(result) => result,
                    Err(_) => {
                        let timeout_error = io::Error::new(ErrorKind::TimedOut, "shard read timed out");
                        let error_class = classify_io_error(&timeout_error).as_str();
                        if let Some(path) = metrics_path {
                            rustfs_io_metrics::record_get_object_shard_read_observation(
                                path,
                                index,
                                role,
                                read_cost.as_str(),
                                GET_SHARD_READ_OUTCOME_ERROR,
                                error_class,
                                0,
                                read_start.elapsed().as_secs_f64(),
                                reader.last_verify_duration().as_secs_f64(),
                            );
                        }
                        return (index, read_cost, Err(Error::from(timeout_error)), true);
                    }
                }
            };

            match read_result {
                Ok(n) => {
                    buf.truncate(n);
                    if let Some(path) = metrics_path {
                        rustfs_io_metrics::record_get_object_shard_read_observation(
                            path,
                            index,
                            role,
                            read_cost.as_str(),
                            GET_SHARD_READ_OUTCOME_SUCCESS,
                            GET_SHARD_READ_ERROR_NONE,
                            n,
                            read_start.elapsed().as_secs_f64(),
                            reader.last_verify_duration().as_secs_f64(),
                        );
                    }
                    (index, read_cost, Ok(buf), false)
                }
                Err(e) => {
                    let verify_duration_secs = reader.last_verify_duration().as_secs_f64();
                    let error_class = classify_io_error(&e).as_str();
                    let should_retire = e.kind() == ErrorKind::TimedOut;
                    if let Some(path) = metrics_path {
                        rustfs_io_metrics::record_get_object_shard_read_observation(
                            path,
                            index,
                            role,
                            read_cost.as_str(),
                            GET_SHARD_READ_OUTCOME_ERROR,
                            error_class,
                            0,
                            read_start.elapsed().as_secs_f64(),
                            verify_duration_secs,
                        );
                    }
                    (index, read_cost, Err(Error::from(e)), should_retire)
                }
            }
        })
    } else {
        Box::pin(async move {
            if let Some(path) = metrics_path {
                rustfs_io_metrics::record_get_object_shard_read_observation(
                    path,
                    index,
                    role,
                    read_cost.as_str(),
                    GET_SHARD_READ_OUTCOME_MISSING,
                    GET_SHARD_READ_ERROR_MISSING,
                    0,
                    0.0,
                    0.0,
                );
            }
            (index, read_cost, Err(Error::FileNotFound), false)
        })
    }
}

pin_project! {
pub(crate) struct ParallelReader<R> {
    #[pin]
    readers: Vec<Option<BitrotReader<R>>>,
    offset: usize,
    shard_size: usize,
    shard_file_size: usize,
    data_shards: usize,
    total_shards: usize,
    metrics_path: Option<&'static str>,
    read_costs: Vec<ShardReadCost>,
    read_timeout: Duration,
    verify_reconstruction: bool,
    // Request-scoped shard buffers keyed by shard index. Keeping ownership in
    // `ParallelReader` avoids dropping unused parity/backup slot buffers between stripes.
    buffers: ShardBufferPool,
}
}

impl<R> ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    // Readers should handle disk errors before being passed in, ensuring each reader reaches the available number of BitrotReaders
    pub fn new(readers: Vec<Option<BitrotReader<R>>>, e: Erasure, offset: usize, total_length: usize) -> Self {
        Self::new_with_metrics_path_read_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            None,
            get_object_disk_read_timeout(),
            false,
        )
    }

    pub fn new_with_metrics_path(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        metrics_path: Option<&'static str>,
    ) -> Self {
        Self::new_with_metrics_path_read_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            metrics_path,
            get_object_disk_read_timeout(),
            false,
        )
    }

    pub fn new_with_metrics_path_and_read_costs(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        metrics_path: Option<&'static str>,
        read_costs: Vec<ShardReadCost>,
    ) -> Self {
        Self::new_with_metrics_path_read_costs_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            metrics_path,
            read_costs,
            get_object_disk_read_timeout(),
            false,
        )
    }

    fn new_for_decode(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        metrics_path: Option<&'static str>,
    ) -> Self {
        Self::new_with_metrics_path_read_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            metrics_path,
            get_object_disk_read_timeout(),
            true,
        )
    }

    fn new_with_read_timeout(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        read_timeout: Duration,
    ) -> Self {
        Self::new_with_metrics_path_read_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            None,
            read_timeout,
            false,
        )
    }

    fn new_with_metrics_path_read_timeout_and_reconstruction_verification(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        metrics_path: Option<&'static str>,
        read_timeout: Duration,
        verify_reconstruction: bool,
    ) -> Self {
        let read_costs = vec![ShardReadCost::Unknown; readers.len()];
        Self::new_with_metrics_path_read_costs_timeout_and_reconstruction_verification(
            readers,
            e,
            offset,
            total_length,
            metrics_path,
            read_costs,
            read_timeout,
            verify_reconstruction,
        )
    }

    fn new_with_metrics_path_read_costs_timeout_and_reconstruction_verification(
        readers: Vec<Option<BitrotReader<R>>>,
        e: Erasure,
        offset: usize,
        total_length: usize,
        metrics_path: Option<&'static str>,
        mut read_costs: Vec<ShardReadCost>,
        read_timeout: Duration,
        verify_reconstruction: bool,
    ) -> Self {
        let shard_size = e.shard_size();
        let shard_file_size = e.shard_file_size(total_length as i64) as usize;

        let offset = (offset / e.block_size) * shard_size;
        read_costs.resize(readers.len(), ShardReadCost::Unknown);
        read_costs.truncate(readers.len());

        // Ensure offset does not exceed shard_file_size

        ParallelReader {
            readers,
            offset,
            shard_size,
            shard_file_size,
            data_shards: e.data_shards,
            total_shards: e.data_shards + e.parity_shards,
            metrics_path,
            read_costs,
            read_timeout,
            verify_reconstruction,
            buffers: ShardBufferPool::new(e.data_shards + e.parity_shards),
        }
    }
}

fn record_shard_read_result(
    shards: &mut [Option<Vec<u8>>],
    errs: &mut [Option<Error>],
    retire_readers: &mut Vec<usize>,
    success: &mut usize,
    successful_costs: &mut ShardReadCostCounts,
    i: usize,
    read_cost: ShardReadCost,
    result: Result<Vec<u8>, Error>,
    should_retire: bool,
) -> bool {
    match result {
        Ok(v) => {
            shards[i] = Some(v);
            successful_costs.record(read_cost);
            *success += 1;
            false
        }
        Err(e) => {
            errs[i] = Some(e);
            if should_retire {
                retire_readers.push(i);
            }
            true
        }
    }
}

fn retire_abandoned_readers(errs: &mut [Option<Error>], retire_readers: &mut Vec<usize>, active_readers: &[bool]) {
    for (i, active) in active_readers.iter().enumerate() {
        if !*active {
            continue;
        }

        if errs[i].is_none() {
            errs[i] = Some(Error::from(io::Error::new(ErrorKind::TimedOut, "shard read abandoned after read quorum")));
        }
        retire_readers.push(i);
        warn!(shard_index = i, "retiring in-flight shard reader after read quorum");
    }
}

fn shard_read_hedge_delay(read_timeout: Duration) -> Duration {
    if read_timeout.is_zero() {
        Duration::ZERO
    } else {
        read_timeout.min(Duration::from_millis(100))
    }
}

impl<R> ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub async fn read(&mut self) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>) {
        // if self.readers.len() != self.total_shards {
        //     return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers"));
        // }
        let num_readers = self.readers.len();

        let shard_size = if self.offset + self.shard_size > self.shard_file_size {
            self.shard_file_size - self.offset
        } else {
            self.shard_size
        };

        if shard_size == 0 {
            return (vec![None; num_readers], vec![None; num_readers]);
        }

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_readers];
        let mut errs = vec![None; num_readers];
        let low_cost_available = self
            .readers
            .iter()
            .enumerate()
            .filter(|(index, reader)| {
                reader.is_some()
                    && self
                        .read_costs
                        .get(*index)
                        .copied()
                        .unwrap_or(ShardReadCost::Unknown)
                        .is_low_cost()
            })
            .count();
        let mut successful_costs = ShardReadCostCounts::default();

        self.buffers.ensure_slots(num_readers);

        let mut retire_readers = Vec::new();
        let mut unavailable_data_sources = self
            .readers
            .iter()
            .take(self.data_shards)
            .map(|reader| reader.is_none())
            .collect::<Vec<_>>();
        let reader_iter: std::slice::IterMut<'_, Option<BitrotReader<R>>> = self.readers.iter_mut();
        if num_readers >= self.data_shards {
            let mut reader_iter = reader_iter.enumerate();
            let mut sets = FuturesUnordered::new();
            let mut active_readers = vec![false; num_readers];
            let stripe_read_start = Instant::now();
            let mut scheduled = 0usize;
            for _ in 0..self.data_shards {
                if let Some((i, reader)) = reader_iter.next() {
                    let has_reader = reader.is_some();
                    // Only claim a request-scoped buffer when a shard will actually be read.
                    let recycled_buf = if has_reader {
                        Some(self.buffers.take(i, shard_size))
                    } else {
                        None
                    };
                    let read_cost = self.read_costs.get(i).copied().unwrap_or(ShardReadCost::Unknown);
                    scheduled += 1;
                    active_readers[i] = has_reader;
                    sets.push(read_shard(
                        i,
                        read_cost,
                        reader,
                        recycled_buf,
                        shard_size,
                        self.data_shards,
                        self.read_timeout,
                        self.metrics_path,
                    ));
                }
            }

            let mut success = 0;
            let mut completed = 0usize;
            let mut failed = 0usize;
            let mut first_shard_recorded = false;
            let mut pending = sets.len();
            let mut scheduled_all = false;
            let verification_success_target = self.total_shards.min(self.data_shards + 1);
            let parity_shards = self.total_shards.saturating_sub(self.data_shards);
            loop {
                let item = if !scheduled_all {
                    let hedge_sleep = tokio::time::sleep(shard_read_hedge_delay(self.read_timeout));
                    tokio::pin!(hedge_sleep);
                    tokio::select! {
                        item = sets.next() => item,
                        _ = &mut hedge_sleep => {
                            for (next_i, next_reader) in reader_iter.by_ref() {
                                let has_reader = next_reader.is_some();
                                let recycled_buf = if has_reader {
                                    Some(self.buffers.take(next_i, shard_size))
                                } else {
                                    None
                                };
                                let next_read_cost = self.read_costs.get(next_i).copied().unwrap_or(ShardReadCost::Unknown);
                                scheduled += 1;
                                active_readers[next_i] = has_reader;
                                pending += 1;
                                sets.push(read_shard(
                                    next_i,
                                    next_read_cost,
                                    next_reader,
                                    recycled_buf,
                                    shard_size,
                                    self.data_shards,
                                    self.read_timeout,
                                    self.metrics_path,
                                ));
                            }
                            scheduled_all = true;
                            continue;
                        }
                    }
                } else {
                    sets.next().await
                };

                let Some((i, read_cost, result, should_retire)) = item else {
                    break;
                };

                pending = pending.saturating_sub(1);
                active_readers[i] = false;
                completed += 1;
                if !first_shard_recorded {
                    if let Some(path) = self.metrics_path {
                        rustfs_io_metrics::record_get_object_stage_duration(
                            path,
                            GET_STAGE_STRIPE_READ_FIRST_SHARD,
                            stripe_read_start.elapsed().as_secs_f64(),
                        );
                    }
                    first_shard_recorded = true;
                }

                let result_is_err = record_shard_read_result(
                    &mut shards,
                    &mut errs,
                    &mut retire_readers,
                    &mut success,
                    &mut successful_costs,
                    i,
                    read_cost,
                    result,
                    should_retire,
                );
                if self.verify_reconstruction && i < self.data_shards && result_is_err {
                    unavailable_data_sources[i] = true;
                }
                if result_is_err {
                    failed += 1;
                    if let Some((next_i, next_reader)) = reader_iter.next() {
                        let has_reader = next_reader.is_some();
                        let recycled_buf = if has_reader {
                            Some(self.buffers.take(next_i, shard_size))
                        } else {
                            None
                        };
                        let next_read_cost = self.read_costs.get(next_i).copied().unwrap_or(ShardReadCost::Unknown);
                        scheduled += 1;
                        active_readers[next_i] = has_reader;
                        pending += 1;
                        sets.push(read_shard(
                            next_i,
                            next_read_cost,
                            next_reader,
                            recycled_buf,
                            shard_size,
                            self.data_shards,
                            self.read_timeout,
                            self.metrics_path,
                        ));
                    } else {
                        scheduled_all = true;
                    }
                }

                let mut missing_data_sources = unavailable_data_sources.iter().filter(|missing| **missing).count();
                if self.verify_reconstruction && success >= self.data_shards {
                    for (idx, active) in active_readers.iter().take(self.data_shards).enumerate() {
                        if *active && !unavailable_data_sources[idx] {
                            missing_data_sources += 1;
                        }
                    }
                }

                let needs_reconstruction_verification = self.verify_reconstruction
                    && verification_success_target > self.data_shards
                    && missing_data_sources > 0
                    && missing_data_sources < parity_shards;

                let target_success = if needs_reconstruction_verification {
                    verification_success_target
                } else {
                    self.data_shards
                };

                while success + pending < target_success {
                    if let Some((next_i, next_reader)) = reader_iter.next() {
                        let has_reader = next_reader.is_some();
                        let recycled_buf = if has_reader {
                            Some(self.buffers.take(next_i, shard_size))
                        } else {
                            None
                        };
                        scheduled += 1;
                        active_readers[next_i] = has_reader;
                        pending += 1;
                        sets.push(read_shard(
                            next_i,
                            next_reader,
                            recycled_buf,
                            shard_size,
                            self.data_shards,
                            self.read_timeout,
                            self.metrics_path,
                        ));
                    } else {
                        scheduled_all = true;
                        break;
                    }
                }

                if success >= target_success {
                    break;
                }

                if success >= self.data_shards && (!needs_reconstruction_verification || success + pending < target_success) {
                    break;
                }

                if success + pending < target_success {
                    break;
                }
            }

            if success >= self.data_shards {
                while let Some(Some((i, read_cost, result, should_retire))) = sets.next().now_or_never() {
                    active_readers[i] = false;
                    completed += 1;
                    if record_shard_read_result(
                        &mut shards,
                        &mut errs,
                        &mut retire_readers,
                        &mut success,
                        &mut successful_costs,
                        i,
                        read_cost,
                        result,
                        should_retire,
                    ) {
                        failed += 1;
                    }
                }
                retire_abandoned_readers(&mut errs, &mut retire_readers, &active_readers);
            }

            if let Some(path) = self.metrics_path {
                rustfs_io_metrics::record_get_object_stage_duration(
                    path,
                    GET_STAGE_STRIPE_READ_QUORUM,
                    stripe_read_start.elapsed().as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_shard_read_fanout(path, scheduled, completed, success, failed);
            }
        }

        if let Some(path) = self.metrics_path {
            rustfs_io_metrics::record_get_object_shard_read_cost_summary(
                path,
                successful_costs.local,
                successful_costs.same_node,
                successful_costs.remote,
                successful_costs.unknown,
                low_cost_available,
                successful_costs.low_cost(),
                self.data_shards,
                low_cost_available >= self.data_shards,
            );
        }

        for i in retire_readers {
            self.readers[i] = None;
        }

        (shards, errs)
    }

    pub fn recycle_shards(&mut self, shards: &mut [Option<Vec<u8>>]) {
        for (i, reader) in self.readers.iter().enumerate() {
            if reader.is_some()
                && let Some(buf) = shards.get_mut(i).and_then(Option::take)
            {
                self.buffers.put(i, buf);
            }
        }
    }

    pub fn can_decode(&self, shards: &[Option<Vec<u8>>]) -> bool {
        shards.iter().filter(|s| s.is_some()).count() >= self.data_shards
    }
}

#[async_trait::async_trait]
impl<R> ShardStripeSource for ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    async fn read_next_stripe(&mut self) -> StripeReadState {
        let read_quorum = self.data_shards;
        let (shards, errors) = ParallelReader::read(self).await;
        StripeReadState::from_parts_with_read_costs(shards, errors, &self.read_costs, read_quorum)
    }
}

fn decode_and_verify_reconstructed_shards(erasure: &Erasure, shards: &mut [Option<Vec<u8>>]) -> io::Result<()> {
    let missing_data_source = shards.iter().take(erasure.data_shards).any(|shard| shard.is_none());
    let available_shards = shards.iter().filter(|shard| shard.is_some()).count();
    let source_parity = if missing_data_source && available_shards > erasure.data_shards {
        shards
            .iter()
            .enumerate()
            .skip(erasure.data_shards)
            .filter_map(|(index, shard)| shard.as_ref().map(|shard| (index, shard.clone())))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if source_parity.is_empty() {
        return erasure.decode_data(shards);
    }

    erasure.decode_data_and_parity(shards)?;
    for (index, source) in source_parity {
        let Some(rebuilt) = shards[index].as_ref() else {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "missing rebuilt parity shard after read verification",
            ));
        };
        if rebuilt != &source {
            warn!(
                shard_index = index,
                data_shards = erasure.data_shards,
                parity_shards = erasure.parity_shards,
                "erasure decode rejected inconsistent read source shards"
            );
            return Err(io::Error::new(ErrorKind::InvalidData, "inconsistent read source shards"));
        }
    }

    Ok(())
}

/// Get the total length of data blocks
fn get_data_block_len(shards: &[Option<Vec<u8>>], data_blocks: usize) -> usize {
    let mut size = 0;
    for shard in shards.iter().take(data_blocks).flatten() {
        size += shard.len();
    }

    size
}

/// Write data blocks from encoded blocks to target, supporting offset and length
async fn write_data_blocks<W>(
    writer: &mut W,
    en_blocks: &[Option<Vec<u8>>],
    data_blocks: usize,
    mut offset: usize,
    length: usize,
) -> std::io::Result<usize>
where
    W: tokio::io::AsyncWrite + Send + Sync + Unpin,
{
    if en_blocks.len() < data_blocks {
        let reason = GetObjectFailureReason::RangeOrLengthInvalid;
        record_get_object_pipeline_failure(GET_STAGE_RANGE, reason);
        error!(
            data_blocks,
            available_shards = en_blocks.len(),
            stage = GET_STAGE_RANGE,
            reason = reason.as_str(),
            "Write data blocks received fewer shards than data blocks"
        );
        return Err(io::Error::new(ErrorKind::InvalidInput, "data block count exceeds available shards"));
    }

    if length == 0 {
        return Ok(0);
    }

    let Some(required_len) = offset.checked_add(length) else {
        let reason = GetObjectFailureReason::RangeOrLengthInvalid;
        record_get_object_pipeline_failure(GET_STAGE_RANGE, reason);
        error!(
            offset,
            length,
            stage = GET_STAGE_RANGE,
            reason = reason.as_str(),
            "Write data blocks offset and length overflow"
        );
        return Err(io::Error::new(ErrorKind::InvalidInput, "offset + length overflows"));
    };
    let data_len = get_data_block_len(en_blocks, data_blocks);
    if data_len < required_len {
        let reason = GetObjectFailureReason::ShortRead;
        error!(
            data_blocks,
            data_len,
            required_len,
            offset,
            length,
            stage = GET_STAGE_EMIT,
            reason = reason.as_str(),
            "Write data blocks had insufficient data after offset"
        );
        record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
        return Err(io::Error::new(ErrorKind::UnexpectedEof, "Not enough data blocks to write"));
    }

    let mut total_written = 0;
    let mut write_left = length;

    for block_op in &en_blocks[..data_blocks] {
        let Some(block) = block_op else {
            let reason = GetObjectFailureReason::ShortRead;
            error!(
                data_blocks,
                offset,
                length,
                stage = GET_STAGE_EMIT,
                reason = reason.as_str(),
                "Write data blocks found a missing data shard"
            );
            record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Missing data block"));
        };

        if offset >= block.len() {
            offset -= block.len();
            continue;
        }

        let block_slice = &block[offset..];
        offset = 0;

        let write_len = write_left.min(block_slice.len());
        let write_stage_start = Instant::now();
        if let Err(e) = writer.write_all(&block_slice[..write_len]).await {
            rustfs_io_metrics::record_get_object_duplex_backpressure_duration(write_stage_start.elapsed().as_secs_f64());
            let reason = classify_io_error(&e);
            record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
            error!(
                write_len,
                total_written,
                write_left,
                stage = GET_STAGE_EMIT,
                reason = reason.as_str(),
                error = ?e,
                "Write data blocks failed to emit bytes"
            );
            return Err(e);
        }
        rustfs_io_metrics::record_get_object_duplex_backpressure_duration(write_stage_start.elapsed().as_secs_f64());

        total_written += write_len;
        write_left -= write_len;

        if write_left == 0 {
            return Ok(total_written);
        }
    }

    let reason = GetObjectFailureReason::ShortRead;
    error!(
        total_written,
        write_left,
        length,
        stage = GET_STAGE_EMIT,
        reason = reason.as_str(),
        "Write data blocks exhausted data before completing requested length"
    );
    record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
    Err(io::Error::new(ErrorKind::UnexpectedEof, "Not enough data blocks to write"))
}

impl Erasure {
    pub async fn decode<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        self.decode_inner(writer, readers, offset, length, total_length, None).await
    }

    pub(crate) async fn decode_with_read_costs<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
        read_costs: Vec<ShardReadCost>,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        self.decode_inner(writer, readers, offset, length, total_length, Some(read_costs))
            .await
    }

    async fn decode_inner<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
        read_costs: Option<Vec<ShardReadCost>>,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        if readers.len() != self.data_shards + self.parity_shards {
            record_get_object_pipeline_failure(GET_STAGE_RANGE, GetObjectFailureReason::RangeOrLengthInvalid);
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers")));
        }

        let Some(end_offset) = offset.checked_add(length) else {
            record_get_object_pipeline_failure(GET_STAGE_RANGE, GetObjectFailureReason::RangeOrLengthInvalid);
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length")));
        };
        if end_offset > total_length {
            record_get_object_pipeline_failure(GET_STAGE_RANGE, GetObjectFailureReason::RangeOrLengthInvalid);
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "offset + length exceeds total length")));
        }

        let mut ret_err = None;

        if length == 0 {
            return (0, ret_err);
        }

        let mut written = 0;

        let mut reader = if let Some(read_costs) = read_costs {
            ParallelReader::new_with_metrics_path_read_costs_timeout_and_reconstruction_verification(
                readers,
                self.clone(),
                offset,
                total_length,
                Some(GET_OBJECT_PATH_LEGACY_DUPLEX),
                read_costs,
                get_object_disk_read_timeout(),
                true,
            )
        } else {
            ParallelReader::new_for_decode(readers, self.clone(), offset, total_length, Some(GET_OBJECT_PATH_LEGACY_DUPLEX))
        };

        let start = offset / self.block_size;
        let end = end_offset.saturating_sub(1) / self.block_size;

        for i in start..=end {
            let (block_offset, block_length) = if start == end {
                (offset % self.block_size, length)
            } else if i == start {
                (offset % self.block_size, self.block_size - (offset % self.block_size))
            } else if i == end {
                let end_remainder = end_offset % self.block_size;
                (0, if end_remainder == 0 { self.block_size } else { end_remainder })
            } else {
                (0, self.block_size)
            };

            if block_length == 0 {
                // error!("erasure decode decode block_length == 0");
                break;
            }

            let stripe_read_stage_start = Instant::now();
            let (mut shards, errs) = reader.read().await;
            rustfs_io_metrics::record_get_object_stage_duration(
                "legacy_duplex",
                "stripe_read",
                stripe_read_stage_start.elapsed().as_secs_f64(),
            );

            if ret_err.is_none()
                && let (_, Some(err)) = reduce_errs(&errs, &[])
                && (err == Error::FileNotFound || err == Error::FileCorrupt)
            {
                ret_err = Some(err.into());
            }

            if !reader.can_decode(&shards) {
                let reason = GetObjectFailureReason::ReadQuorum;
                error!(
                    data_shards = self.data_shards,
                    total_shards = self.data_shards + self.parity_shards,
                    available_shards = shards.iter().filter(|shard| shard.is_some()).count(),
                    block_offset,
                    block_length,
                    stage = GET_STAGE_STRIPE_READ,
                    reason = reason.as_str(),
                    errors = ?errs,
                    "Erasure decode could not gather enough shards"
                );
                record_get_object_pipeline_failure(GET_STAGE_STRIPE_READ, reason);
                ret_err = Some(Error::ErasureReadQuorum.into());
                break;
            }

            // Decode the shards. If this stripe needed parity to reconstruct a
            // missing data shard and an extra source shard was available, verify
            // the reconstructed data against that source before streaming bytes.
            let reconstruct_stage_start = Instant::now();
            if let Err(e) = decode_and_verify_reconstructed_shards(self, &mut shards) {
                rustfs_io_metrics::record_get_object_stage_duration(
                    "legacy_duplex",
                    "reconstruct",
                    reconstruct_stage_start.elapsed().as_secs_f64(),
                );
                let reason = GetObjectFailureReason::DecodeError;
                error!(
                    data_shards = self.data_shards,
                    total_shards = self.data_shards + self.parity_shards,
                    block_offset,
                    block_length,
                    stage = GET_STAGE_RECONSTRUCT,
                    reason = reason.as_str(),
                    error = ?e,
                    "Erasure shard reconstruction failed"
                );
                record_get_object_pipeline_failure(GET_STAGE_RECONSTRUCT, reason);
                ret_err = Some(e);
                break;
            }
            rustfs_io_metrics::record_get_object_stage_duration(
                "legacy_duplex",
                "reconstruct",
                reconstruct_stage_start.elapsed().as_secs_f64(),
            );

            let emit_stage_start = Instant::now();
            let n = match write_data_blocks(writer, &shards, self.data_shards, block_offset, block_length).await {
                Ok(n) => {
                    rustfs_io_metrics::record_get_object_stage_duration(
                        "legacy_duplex",
                        "emit",
                        emit_stage_start.elapsed().as_secs_f64(),
                    );
                    n
                }
                Err(e) => {
                    rustfs_io_metrics::record_get_object_stage_duration(
                        "legacy_duplex",
                        "emit",
                        emit_stage_start.elapsed().as_secs_f64(),
                    );
                    error!(
                        block_offset,
                        block_length,
                        bytes_written = written,
                        stage = GET_STAGE_EMIT,
                        reason = classify_io_error(&e).as_str(),
                        error = ?e,
                        "Erasure decode failed to emit reconstructed data"
                    );
                    ret_err = Some(e);
                    break;
                }
            };

            written += n;

            // Hand active-reader buffers back so the next stripe can reuse them
            // without retaining offline shard data that cannot be read again.
            reader.recycle_shards(&mut shards);
        }

        if ret_err.is_some() {
            return (written, ret_err);
        }

        if written < length {
            ret_err = Some(Error::LessData.into());
        }

        (written, ret_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        disk::error::DiskError,
        erasure_coding::{BitrotReader, BitrotWriter},
    };
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    enum TestShardReader {
        Ready(Cursor<Vec<u8>>),
        Pending,
        PartialThenPending { data: Vec<u8>, emitted: bool },
        TimedOut,
    }

    impl AsyncRead for TestShardReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            match &mut *self {
                TestShardReader::Ready(cursor) => Pin::new(cursor).poll_read(cx, buf),
                TestShardReader::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TestShardReader::PartialThenPending { data, emitted } => {
                    if *emitted {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }

                    let len = data.len().min(buf.remaining());
                    buf.put_slice(&data[..len]);
                    *emitted = true;
                    Poll::Ready(Ok(()))
                }
                TestShardReader::TimedOut => Poll::Ready(Err(io::Error::new(ErrorKind::TimedOut, "test shard read timed out"))),
            }
        }
    }

    #[tokio::test]
    async fn test_write_data_blocks_writes_range_across_blocks() {
        let blocks = vec![Some(vec![1, 2, 3, 4]), Some(vec![5, 6, 7]), Some(vec![8, 9])];
        let mut out = Vec::new();

        let written = write_data_blocks(&mut out, &blocks, 3, 2, 5).await.unwrap();

        assert_eq!(written, 5);
        assert_eq!(out, vec![3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_write_data_blocks_rejects_short_data_after_offset() {
        let blocks = vec![Some(vec![1, 2, 3, 4]), Some(vec![5, 6, 7])];
        let mut out = Vec::new();

        let err = write_data_blocks(&mut out, &blocks, 2, 3, 5).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn test_write_data_blocks_rejects_invalid_data_block_count() {
        let blocks = vec![Some(vec![1, 2, 3, 4])];
        let mut out = Vec::new();

        let err = write_data_blocks(&mut out, &blocks, 2, 0, 1).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(out.is_empty());
    }

    /// Regression for upstream issue #2716: ranged GETs going through
    /// `Erasure::decode` must return the requested byte range without
    /// panicking or truncating, including when the range starts at a
    /// non-zero offset and crosses EC block boundaries.
    #[tokio::test]
    async fn test_erasure_decode_ranged_read_returns_correct_bytes() {
        const DATA_SHARDS: usize = 4;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        // 200 bytes spans 3 full blocks + 1 partial block, exercising
        // the start/middle/end branches in `Erasure::decode`.
        let total_data: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let total_len = total_data.len();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::HighwayHash256;

        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        let mut offset = 0;
        while offset < total_len {
            let end = (offset + BLOCK_SIZE).min(total_len);
            let shards = erasure.encode_data(&total_data[offset..end]).unwrap();
            for (i, shard) in shards.iter().enumerate() {
                shard_writers[i].write(shard).await.unwrap();
            }
            offset = end;
        }

        let shard_bufs: Vec<Vec<u8>> = shard_writers.into_iter().map(|w| w.into_inner().into_inner()).collect();

        // `Erasure::decode` does not seek the readers; the production caller
        // (`create_bitrot_reader`) positions each reader at the shard byte
        // offset corresponding to the request's start block. Mirror that here.
        let hash_size = hash_algo.size();
        let make_readers = |off: usize| -> Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> {
            let start_block = off / BLOCK_SIZE;
            let cursor_pos = start_block * (shard_size + hash_size);
            shard_bufs
                .iter()
                .map(|buf| {
                    let mut cursor = Cursor::new(buf.clone());
                    cursor.set_position(cursor_pos as u64);
                    Some(BitrotReader::new(cursor, shard_size, hash_algo.clone(), false))
                })
                .collect()
        };

        // (offset, length, description)
        let cases: &[(usize, usize, &str)] = &[
            (0, total_len, "full read"),
            (0, 50, "head from start, partial block"),
            (10, 30, "small range within first block"),
            (60, 80, "range crossing two block boundaries"),
            (128, 50, "range starting at block boundary"),
            (130, 10, "small range deep in middle"),
            (192, 8, "tail covering last partial block"),
        ];

        for &(off, len, desc) in cases {
            let mut output = Vec::new();
            let (written, err) = erasure.decode(&mut output, make_readers(off), off, len, total_len).await;
            assert!(err.is_none(), "{}: unexpected error: {:?}", desc, err);
            assert_eq!(written, len, "{}: written != length", desc);
            assert_eq!(output, total_data[off..off + len], "{}: bytes mismatch", desc);
        }
    }

    /// Guards the shard-buffer reuse in `ParallelReader`: a multi-stripe
    /// `Erasure::decode` that reconstructs missing data shards on every stripe
    /// must still return byte-exact output. Reconstructed and parity buffers are
    /// recycled into the next stripe, so this catches stale-byte leaks or a
    /// missing resize between stripes (including the short final stripe). Run
    /// with bitrot verification both on and off.
    #[tokio::test]
    async fn test_erasure_decode_with_missing_shards_reuses_buffers() {
        const DATA_SHARDS: usize = 4;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        // 200 bytes => 3 full blocks + 1 partial: several stripes are decoded
        // through the same `ParallelReader`, so its buffers are reused.
        let total_data: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let total_len = total_data.len();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::HighwayHash256;

        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        let mut offset = 0;
        while offset < total_len {
            let end = (offset + BLOCK_SIZE).min(total_len);
            let shards = erasure.encode_data(&total_data[offset..end]).unwrap();
            for (i, shard) in shards.iter().enumerate() {
                shard_writers[i].write(shard).await.unwrap();
            }
            offset = end;
        }

        let shard_bufs: Vec<Vec<u8>> = shard_writers.into_iter().map(|w| w.into_inner().into_inner()).collect();

        // Drop two data shards: each stripe must be reconstructed from the
        // remaining data + parity shards, and those reconstructed buffers are
        // what gets recycled.
        let missing = [0usize, 2usize];
        for verify in [true, false] {
            let readers: Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> = shard_bufs
                .iter()
                .enumerate()
                .map(|(i, buf)| {
                    if missing.contains(&i) {
                        None
                    } else {
                        Some(BitrotReader::new(Cursor::new(buf.clone()), shard_size, hash_algo.clone(), !verify))
                    }
                })
                .collect();

            let mut output = Vec::new();
            let (written, err) = erasure.decode(&mut output, readers, 0, total_len, total_len).await;
            assert!(err.is_none(), "verify={verify}: unexpected error: {err:?}");
            assert_eq!(written, total_len, "verify={verify}: short write");
            assert_eq!(output, total_data, "verify={verify}: reconstructed bytes mismatch");
        }
    }

    #[tokio::test]
    async fn test_erasure_decode_rejects_inconsistent_reconstruction_sources() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let data: Vec<u8> = (0..BLOCK_SIZE as u8).collect();
        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let shard_size = erasure.shard_size();
        let encoded = erasure.encode_data(&data).expect("encode should succeed");
        let mut corrupt_parity = encoded[DATA_SHARDS].to_vec();

        corrupt_parity[0] ^= 0x80;
        let readers = vec![
            None,
            Some(BitrotReader::new(
                Cursor::new(encoded[1].to_vec()),
                shard_size,
                HashAlgorithm::None,
                false,
            )),
            Some(BitrotReader::new(Cursor::new(corrupt_parity), shard_size, HashAlgorithm::None, false)),
            Some(BitrotReader::new(
                Cursor::new(encoded[DATA_SHARDS + 1].to_vec()),
                shard_size,
                HashAlgorithm::None,
                false,
            )),
        ];

        let mut output = Vec::new();
        let (written, err) = erasure.decode(&mut output, readers, 0, data.len(), data.len()).await;

        assert_eq!(written, 0);
        let err = err.expect("inconsistent parity sources must fail the read");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
        assert!(output.is_empty());
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_erasure_decode_preserves_compressed_stream_near_block_boundary() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 1024 * 1024;

        use crate::rio::CompressReader;
        use rustfs_utils::CompressionAlgorithm;
        use tokio::io::AsyncReadExt;

        let plaintext_size = 8 * BLOCK_SIZE + 123;
        let plaintext = (0..plaintext_size)
            .scan(0x9e37_79b9_7f4a_7c15u64, |state, _| {
                *state ^= *state << 7;
                *state ^= *state >> 9;
                *state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
                Some((*state >> 32) as u8)
            })
            .collect::<Vec<_>>();

        let mut compressor = CompressReader::new(Cursor::new(plaintext), CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compressor.read_to_end(&mut compressed).await.unwrap();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::HighwayHash256;

        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        for block in compressed.chunks(BLOCK_SIZE) {
            let shards = erasure.encode_data(block).unwrap();
            for (i, shard) in shards.iter().enumerate() {
                shard_writers[i].write(shard).await.unwrap();
            }
        }

        let shard_bufs: Vec<Vec<u8>> = shard_writers.into_iter().map(|w| w.into_inner().into_inner()).collect();
        let readers = shard_bufs
            .iter()
            .map(|buf| Some(BitrotReader::new(Cursor::new(buf.clone()), shard_size, hash_algo.clone(), false)))
            .collect();

        let mut decoded = Vec::new();
        let (written, err) = erasure
            .decode(&mut decoded, readers, 0, compressed.len(), compressed.len())
            .await;

        assert!(err.is_none(), "unexpected decode error: {err:?}");
        assert_eq!(written, compressed.len());
        assert_eq!(decoded, compressed);
    }

    #[tokio::test]
    async fn test_parallel_reader_normal() {
        const BLOCK_SIZE: usize = 64;
        const NUM_SHARDS: usize = 2;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            readers.push(Some(
                create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, false).await,
            ));
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            bufs.into_iter().enumerate().for_each(|(index, buf)| {
                if index < DATA_SHARDS {
                    assert!(buf.is_some());
                    let buf = buf.unwrap();
                    assert_eq!(SHARD_SIZE, buf.len());
                    assert_eq!(index as u8, buf[0]);
                } else {
                    assert!(buf.is_none());
                }
            });

            assert!(errs.iter().filter(|err| err.is_some()).count() == 0);
        }
    }

    #[tokio::test]
    async fn test_parallel_reader_with_offline_disks() {
        const OFFLINE_DISKS: usize = 2;
        const NUM_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            if i < OFFLINE_DISKS {
                // Two disks are offline
                readers.push(None);
            } else {
                readers.push(Some(
                    create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, false).await,
                ));
            }
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
            assert_eq!(OFFLINE_DISKS, errs.iter().filter(|err| err.is_some()).count());
        }
    }

    #[tokio::test]
    async fn test_parallel_reader_with_bitrots() {
        const BITROT_DISKS: usize = 2;
        const NUM_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 8;
        const PARITY_SHARDS: usize = 4;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let reader_offset = 0;
        let mut readers = vec![];
        for i in 0..(DATA_SHARDS + PARITY_SHARDS) {
            readers.push(Some(
                create_reader(SHARD_SIZE, NUM_SHARDS, (i % 256) as u8, &HashAlgorithm::HighwayHash256, i < BITROT_DISKS).await,
            ));
        }

        let erausre = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erausre, reader_offset, NUM_SHARDS * BLOCK_SIZE);

        for _ in 0..NUM_SHARDS {
            let (bufs, errs) = parallel_reader.read().await;

            assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
            assert_eq!(
                BITROT_DISKS,
                errs.iter()
                    .filter(|err| {
                        match err {
                            Some(DiskError::Io(err)) => {
                                err.kind() == std::io::ErrorKind::InvalidData && err.to_string().contains("bitrot")
                            }
                            _ => false,
                        }
                    })
                    .count()
            );
        }
    }

    #[tokio::test]
    async fn test_parallel_reader_replaces_timed_out_shard_with_parity() {
        const NUM_SHARDS: usize = 1;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 1;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let hash_algo = HashAlgorithm::None;
        let readers = vec![
            Some(BitrotReader::new(TestShardReader::TimedOut, SHARD_SIZE, hash_algo.clone(), false)),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![1_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo.clone(),
                false,
            )),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![2_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo,
                false,
            )),
        ];

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader = ParallelReader::new(readers, erasure, 0, NUM_SHARDS * BLOCK_SIZE);

        let (bufs, errs) = parallel_reader.read().await;

        assert!(matches!(&errs[0], Some(DiskError::Io(err)) if err.kind() == ErrorKind::TimedOut));
        assert!(parallel_reader.readers[0].is_none());
        assert!(bufs[0].is_none());
        assert!(bufs[1].is_some());
        assert!(bufs[2].is_some());
        assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
    }

    #[tokio::test]
    async fn test_parallel_reader_uses_parity_without_waiting_for_pending_shard() {
        const NUM_SHARDS: usize = 1;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 1;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let hash_algo = HashAlgorithm::None;
        let readers = vec![
            Some(BitrotReader::new(TestShardReader::Pending, SHARD_SIZE, hash_algo.clone(), false)),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![1_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo.clone(),
                false,
            )),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![2_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo,
                false,
            )),
        ];

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader =
            ParallelReader::new_with_read_timeout(readers, erasure, 0, NUM_SHARDS * BLOCK_SIZE, Duration::from_secs(60));

        let (bufs, errs) = tokio::time::timeout(Duration::from_millis(500), parallel_reader.read())
            .await
            .expect("parallel reader should use ready parity without waiting for pending data shard");

        assert!(matches!(&errs[0], Some(DiskError::Io(err)) if err.kind() == ErrorKind::TimedOut));
        assert!(parallel_reader.readers[0].is_none());
        assert!(bufs[0].is_none());
        assert!(bufs[1].is_some());
        assert!(bufs[2].is_some());
        assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
    }

    #[tokio::test]
    async fn test_parallel_reader_retires_partially_read_shard_after_quorum() {
        const NUM_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 1;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let hash_algo = HashAlgorithm::None;
        let readers = vec![
            Some(BitrotReader::new(
                TestShardReader::PartialThenPending {
                    data: vec![9_u8; SHARD_SIZE / 2],
                    emitted: false,
                },
                SHARD_SIZE,
                hash_algo.clone(),
                false,
            )),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![1_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo.clone(),
                false,
            )),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![2_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo,
                false,
            )),
        ];

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader =
            ParallelReader::new_with_read_timeout(readers, erasure, 0, NUM_SHARDS * BLOCK_SIZE, Duration::from_secs(60));

        let (bufs, errs) = tokio::time::timeout(Duration::from_millis(500), parallel_reader.read())
            .await
            .expect("parallel reader should use parity without waiting for a partially read shard");

        assert!(matches!(&errs[0], Some(DiskError::Io(err)) if err.kind() == ErrorKind::TimedOut));
        assert!(parallel_reader.readers[0].is_none());
        assert!(bufs[0].is_none());
        assert!(bufs[1].is_some());
        assert!(bufs[2].is_some());
        assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());

        let (next_bufs, next_errs) = tokio::time::timeout(Duration::from_millis(500), parallel_reader.read())
            .await
            .expect("retired partially read shard should not block the next stripe");

        assert!(matches!(&next_errs[0], Some(DiskError::FileNotFound)));
        assert!(next_bufs[0].is_none());
        assert!(next_bufs[1].is_some());
        assert!(next_bufs[2].is_some());
        assert_eq!(DATA_SHARDS, next_bufs.iter().filter(|buf| buf.is_some()).count());
    }

    #[tokio::test]
    async fn test_parallel_reader_retires_pending_shard_when_quorum_needs_it() {
        const NUM_SHARDS: usize = 1;
        const BLOCK_SIZE: usize = 64;
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 1;
        const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

        let hash_algo = HashAlgorithm::None;
        let readers = vec![
            Some(BitrotReader::new(TestShardReader::Pending, SHARD_SIZE, hash_algo.clone(), false)),
            Some(BitrotReader::new(
                TestShardReader::Ready(Cursor::new(vec![1_u8; SHARD_SIZE * NUM_SHARDS])),
                SHARD_SIZE,
                hash_algo,
                false,
            )),
            None,
        ];

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let mut parallel_reader =
            ParallelReader::new_with_read_timeout(readers, erasure, 0, NUM_SHARDS * BLOCK_SIZE, Duration::from_millis(20));

        let started = std::time::Instant::now();
        let (bufs, errs) = parallel_reader.read().await;

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(&errs[0], Some(DiskError::Io(err)) if err.kind() == ErrorKind::TimedOut));
        assert!(matches!(&errs[2], Some(DiskError::FileNotFound)));
        assert!(parallel_reader.readers[0].is_none());
        assert!(bufs[0].is_none());
        assert!(bufs[1].is_some());
        assert!(bufs[2].is_none());
        assert!(bufs.iter().filter(|buf| buf.is_some()).count() < DATA_SHARDS);
    }

    async fn create_reader(
        shard_size: usize,
        num_shards: usize,
        value: u8,
        hash_algo: &HashAlgorithm,
        bitrot: bool,
    ) -> BitrotReader<Cursor<Vec<u8>>> {
        let len = (hash_algo.size() + shard_size) * num_shards;
        let buf = Cursor::new(vec![0u8; len]);

        let mut writer = BitrotWriter::new(buf, shard_size, hash_algo.clone());
        for _ in 0..num_shards {
            writer.write(vec![value; shard_size].as_slice()).await.unwrap();
        }

        let mut buf = writer.into_inner().into_inner();

        if bitrot {
            for i in 0..num_shards {
                // Rot one bit for each shard
                buf[i * (hash_algo.size() + shard_size)] ^= 1;
            }
        }

        let reader_cursor = Cursor::new(buf);
        BitrotReader::new(reader_cursor, shard_size, hash_algo.clone(), false)
    }
}
