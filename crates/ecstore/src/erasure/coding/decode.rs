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

use crate::diagnostics::get::{
    GET_OBJECT_PATH_LEGACY_DUPLEX, GET_SHARD_READ_ERROR_MISSING, GET_SHARD_READ_ERROR_NONE, GET_SHARD_READ_OUTCOME_ERROR,
    GET_SHARD_READ_OUTCOME_MISSING, GET_SHARD_READ_OUTCOME_SUCCESS, GET_SHARD_ROLE_DATA, GET_SHARD_ROLE_PARITY, GET_STAGE_EMIT,
    GET_STAGE_RANGE, GET_STAGE_RECONSTRUCT, GET_STAGE_STRIPE_READ, GET_STAGE_STRIPE_READ_FIRST_SHARD,
    GET_STAGE_STRIPE_READ_QUORUM, GetObjectFailureReason, classify_io_error, get_stage_timer_if_enabled,
    record_get_object_pipeline_failure, record_get_stage_duration_if_enabled,
};
use crate::disk::disk_store::get_object_disk_read_timeout;
use crate::disk::error::Error;
use crate::disk::error_reduce::reduce_errs;
use crate::erasure::codec::workspace::ShardBufferPool;
use crate::erasure::coding::{BitrotReader, Erasure};
use crate::io_support::bitrot::DeferredReaderStripeHandle;
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

const ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING: &str = "RUSTFS_SHARD_LOCALITY_SCHEDULING";
const ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE: &str = "RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE";
const SHARD_LOCALITY_SCHEDULING_OFF: &str = "off";
const SHARD_LOCALITY_SCHEDULING_OBSERVE: &str = "observe";
const SHARD_LOCALITY_SCHEDULING_ON: &str = "on";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShardLocalitySchedulingMode {
    Off,
    Observe,
    On,
}

impl ShardLocalitySchedulingMode {
    fn is_on(self) -> bool {
        matches!(self, ShardLocalitySchedulingMode::On)
    }
}

fn parse_shard_locality_scheduling_mode(value: &str) -> ShardLocalitySchedulingMode {
    match value.trim() {
        value if value.eq_ignore_ascii_case(SHARD_LOCALITY_SCHEDULING_OFF) => ShardLocalitySchedulingMode::Off,
        value if value.eq_ignore_ascii_case(SHARD_LOCALITY_SCHEDULING_OBSERVE) => ShardLocalitySchedulingMode::Observe,
        value if value.eq_ignore_ascii_case(SHARD_LOCALITY_SCHEDULING_ON) => ShardLocalitySchedulingMode::On,
        _ => ShardLocalitySchedulingMode::Off,
    }
}

fn get_shard_locality_scheduling_mode() -> ShardLocalitySchedulingMode {
    if let Some(value) = rustfs_utils::get_env_opt_str(ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING) {
        return parse_shard_locality_scheduling_mode(&value);
    }

    if rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, false) {
        return ShardLocalitySchedulingMode::On;
    }

    ShardLocalitySchedulingMode::Off
}

fn get_shard_locality_preference_enabled() -> bool {
    get_shard_locality_scheduling_mode().is_on()
}

pub(crate) fn should_collect_shard_read_costs() -> bool {
    // `observe` mode only feeds the stage-metrics histograms, so skip the
    // cost-collection overhead when those metrics cannot be reported anyway.
    rustfs_io_metrics::get_stage_metrics_enabled() || get_shard_locality_scheduling_mode().is_on()
}

/// Number of stripes to prefetch in the legacy decode path.
/// When > 1, stripe reads are batched to overlap disk I/O with decode.
/// Default: 1 (no prefetch, current behavior).
/// Set via environment variable RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT.
const ENV_RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT: &str = "RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT";
const DEFAULT_RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT: usize = 1;

/// Get the stripe prefetch count from environment variable.
fn get_decode_stripe_prefetch_count() -> usize {
    rustfs_utils::get_env_usize(
        ENV_RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT,
        DEFAULT_RUSTFS_GET_DECODE_STRIPE_PREFETCH_COUNT,
    )
}

/// Enable overlapping bitrot verification with stripe decode.
/// When enabled, bitrot verification for stripe N+1 runs concurrently
/// with decode of stripe N, reducing total pipeline latency.
/// Default: false (sequential behavior, current implementation).
const ENV_RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE: &str = "RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE";
const DEFAULT_RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE: bool = false;

/// Enable the data-shards-only lockstep GET read (backlog#923).
/// When enabled, the reconstruction-verifying (lockstep) GET path reads only
/// the data shards per stripe while the object is healthy; parity slots stay
/// unopened deferred readers that are engaged — realigned to the failing
/// stripe via their stripe handle — only when a data shard is missing or dies.
/// This halves per-GET read bytes, IOPS, and bitrot hashing on healthy
/// objects with 2+2 layouts.
/// Default: false (current behavior: every live shard reader is read on every
/// stripe).
const ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE: &str = "RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE";
const DEFAULT_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE: bool = false;

/// Whether the data-shards-only lockstep GET read is enabled (backlog#923).
pub(crate) fn get_lockstep_data_shards_only_enabled() -> bool {
    rustfs_utils::get_env_bool(
        ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE,
        DEFAULT_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE,
    )
}

/// Get whether bitrot-decode overlap is enabled.
fn is_bitrot_decode_overlap_enabled() -> bool {
    rustfs_utils::get_env_bool(
        ENV_RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE,
        DEFAULT_RUSTFS_GET_BITROT_DECODE_OVERLAP_ENABLE,
    )
}

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
fn shard_read_launch_rank(cost: ShardReadCost) -> u8 {
    match cost {
        ShardReadCost::Local => 0,
        ShardReadCost::SameNode => 1,
        ShardReadCost::Remote => 2,
        ShardReadCost::Unknown => 3,
    }
}

fn shard_read_launch_order(read_costs: &[ShardReadCost], num_readers: usize, locality_preference_enabled: bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..num_readers).collect();
    if locality_preference_enabled {
        order.sort_by_key(|index| {
            (
                shard_read_launch_rank(read_costs.get(*index).copied().unwrap_or(ShardReadCost::Unknown)),
                *index,
            )
        });
    }
    order
}

enum ReaderLaunchIter<'a, R> {
    Original(std::iter::Enumerate<std::slice::IterMut<'a, Option<BitrotReader<R>>>>),
    Locality(std::vec::IntoIter<(usize, &'a mut Option<BitrotReader<R>>)>),
}

impl<'a, R> ReaderLaunchIter<'a, R> {
    fn new(readers: &'a mut [Option<BitrotReader<R>>], read_costs: &[ShardReadCost], locality_preference_enabled: bool) -> Self {
        if !locality_preference_enabled {
            return Self::Original(readers.iter_mut().enumerate());
        }

        let mut ordered_readers: Vec<_> = readers.iter_mut().enumerate().collect();
        ordered_readers.sort_by_key(|(index, _)| {
            (
                shard_read_launch_rank(read_costs.get(*index).copied().unwrap_or(ShardReadCost::Unknown)),
                *index,
            )
        });
        Self::Locality(ordered_readers.into_iter())
    }
}

impl<'a, R> Iterator for ReaderLaunchIter<'a, R> {
    type Item = (usize, &'a mut Option<BitrotReader<R>>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Original(iter) => iter.next(),
            Self::Locality(iter) => iter.next(),
        }
    }
}

fn shard_role(index: usize, data_shards: usize) -> &'static str {
    if index < data_shards {
        GET_SHARD_ROLE_DATA
    } else {
        GET_SHARD_ROLE_PARITY
    }
}

#[allow(clippy::too_many_arguments)]
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
            let read_start = metrics_path.map(|_| Instant::now());
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
                                read_start.map_or(0.0, |read_start| read_start.elapsed().as_secs_f64()),
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
                            read_start.map_or(0.0, |read_start| read_start.elapsed().as_secs_f64()),
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
                            read_start.map_or(0.0, |read_start| read_start.elapsed().as_secs_f64()),
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
    locality_preference_enabled: bool,
    // Request-scoped shard buffers keyed by shard index. Keeping ownership in
    // `ParallelReader` avoids dropping unused parity/backup slot buffers between stripes.
    buffers: ShardBufferPool,
    // Lockstep-path state (verify_reconstruction == true). `engaged[i]` marks
    // readers that participate in each stripe read: all data slots from the
    // start, parity slots only once a data shard is missing/dead. Unengaged
    // parity stays an unopened deferred reader; `deferred_handles[i]` realigns
    // it to the current stripe when it is engaged mid-object (backlog#923).
    engaged: Vec<bool>,
    deferred_handles: Vec<Option<DeferredReaderStripeHandle>>,
    stripe_index: usize,
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

    pub(crate) fn new_with_metrics_path_and_reconstruction_verification(
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

    pub(crate) fn new_with_metrics_path_read_costs_and_reconstruction_verification(
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

    #[allow(clippy::too_many_arguments)]
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
        let metrics_path = metrics_path.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());
        let shard_size = e.shard_size();
        let shard_file_size = e.shard_file_size(total_length as i64) as usize;

        let offset = (offset / e.block_size) * shard_size;
        read_costs.resize(readers.len(), ShardReadCost::Unknown);
        read_costs.truncate(readers.len());

        // Ensure offset does not exceed shard_file_size

        // Default (gate off): every slot is engaged, i.e. the lockstep path
        // reads all live readers on every stripe — the pre-backlog#923
        // behavior. With the gate on, only data slots start engaged; parity is
        // engaged on demand, stripe-aligned through its deferred handle.
        let data_shards_only = get_lockstep_data_shards_only_enabled();
        let engaged = (0..readers.len())
            .map(|index| !data_shards_only || index < e.data_shards)
            .collect();
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
            locality_preference_enabled: get_shard_locality_preference_enabled(),
            buffers: ShardBufferPool::new(e.data_shards + e.parity_shards),
            engaged,
            deferred_handles: Vec::new(),
            stripe_index: 0,
        }
    }

    /// Attach the per-slot deferred stripe handles produced during bitrot
    /// reader setup. Only parity slots are consulted: a handle lets the
    /// lockstep path open a parity shard aligned to the stripe where a data
    /// shard failed, instead of reading every parity shard on every stripe
    /// (backlog#923).
    pub(crate) fn with_deferred_parity_handles(mut self, mut handles: Vec<Option<DeferredReaderStripeHandle>>) -> Self {
        handles.resize_with(self.readers.len(), || None);
        handles.truncate(self.readers.len());
        self.deferred_handles = handles;
        self
    }
}

#[allow(clippy::too_many_arguments)]
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

fn shard_read_hedge_delay(read_timeout: Duration) -> Option<Duration> {
    if read_timeout.is_zero() {
        None
    } else {
        Some(read_timeout.min(Duration::from_millis(100)))
    }
}

fn shard_locality_remote_avoid_potential(remote_scheduled: usize, low_cost_available: usize, data_shards: usize) -> usize {
    let theoretical_remote_needed = data_shards.saturating_sub(low_cost_available);
    remote_scheduled.saturating_sub(theoretical_remote_needed)
}

#[allow(clippy::too_many_arguments)]
fn record_scheduled_read_cost(
    read_cost: ShardReadCost,
    locality_preference_enabled: bool,
    low_cost_available: usize,
    data_shards: usize,
    count_remote_as_fallback: bool,
    local_preferred: &mut usize,
    remote_scheduled: &mut usize,
    fallback_to_remote: &mut usize,
) {
    if read_cost.is_low_cost() {
        if locality_preference_enabled {
            *local_preferred += 1;
        }
    } else if read_cost.is_remote() {
        *remote_scheduled += 1;
        if locality_preference_enabled && (count_remote_as_fallback || low_cost_available < data_shards) {
            *fallback_to_remote += 1;
        }
    }
}

impl<R> ParallelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub async fn read(&mut self) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>) {
        // On the reconstruction-verifying GET path, read every live shard reader
        // in lockstep so all readers advance one block per stripe and stay
        // mutually aligned. The adaptive data-first path below only reads
        // `data_shards` readers per stripe and pulls in a parity reader as a
        // substitute on demand; a parity reader first used mid-object is still
        // positioned at its stream start (block 0) and returns an earlier stripe
        // than the data shards, producing "inconsistent read source shards" and
        // truncating large-object GETs under concurrency (backlog#832).
        if self.verify_reconstruction {
            return self.read_lockstep().await;
        }
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

        // Advance to the next stripe so the following read() computes the correct
        // (possibly shorter, final) stripe length. `offset` previously never
        // advanced, so every stripe after the first reused the first stripe's
        // geometry; combined with BitrotReader now rejecting short reads, the
        // per-stripe expected length must be exact (backlog#799 B2). `self.offset`
        // is only read above to derive `shard_size`, so advancing here is safe.
        self.offset += shard_size;

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_readers];
        let mut errs = vec![None; num_readers];
        let read_costs = self.read_costs.as_slice();
        let locality_preference_enabled = self.locality_preference_enabled;
        let low_cost_available = self
            .readers
            .iter()
            .enumerate()
            .filter(|(index, reader)| {
                reader.is_some()
                    && read_costs
                        .get(*index)
                        .copied()
                        .unwrap_or(ShardReadCost::Unknown)
                        .is_low_cost()
            })
            .count();
        let remote_available = self
            .readers
            .iter()
            .enumerate()
            .filter(|(index, reader)| {
                reader.is_some() && read_costs.get(*index).copied().unwrap_or(ShardReadCost::Unknown).is_remote()
            })
            .count();
        let mut successful_costs = ShardReadCostCounts::default();
        let mut local_preferred = 0usize;
        let mut fallback_to_remote = 0usize;
        let mut remote_scheduled = 0usize;

        self.buffers.ensure_slots(num_readers);

        let mut retire_readers = Vec::new();
        let mut unavailable_data_sources = self
            .readers
            .iter()
            .take(self.data_shards)
            .map(|reader| reader.is_none())
            .collect::<Vec<_>>();
        if num_readers >= self.data_shards {
            let mut reader_iter = ReaderLaunchIter::new(&mut self.readers, read_costs, locality_preference_enabled);
            let mut sets = FuturesUnordered::new();
            let mut active_readers = vec![false; num_readers];
            let stripe_read_start = self.metrics_path.map(|_| Instant::now());
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
                    let read_cost = read_costs.get(i).copied().unwrap_or(ShardReadCost::Unknown);
                    record_scheduled_read_cost(
                        read_cost,
                        locality_preference_enabled,
                        low_cost_available,
                        self.data_shards,
                        false,
                        &mut local_preferred,
                        &mut remote_scheduled,
                        &mut fallback_to_remote,
                    );
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
                    match shard_read_hedge_delay(self.read_timeout) {
                        Some(hedge_delay) => {
                            let hedge_sleep = tokio::time::sleep(hedge_delay);
                            tokio::pin!(hedge_sleep);
                            tokio::select! {
                                item = sets.next() => item,
                                _ = &mut hedge_sleep => {
                                    if let Some((next_i, next_reader)) = reader_iter.next() {
                                        let has_reader = next_reader.is_some();
                                        let recycled_buf = if has_reader {
                                            Some(self.buffers.take(next_i, shard_size))
                                        } else {
                                            None
                                        };
                                        let next_read_cost = read_costs.get(next_i).copied().unwrap_or(ShardReadCost::Unknown);
                                        record_scheduled_read_cost(
                                            next_read_cost,
                                            locality_preference_enabled,
                                            low_cost_available,
                                            self.data_shards,
                                            true,
                                            &mut local_preferred,
                                            &mut remote_scheduled,
                                            &mut fallback_to_remote,
                                        );
                                        active_readers[next_i] = has_reader;
                                        scheduled += 1;
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
                                    continue;
                                }
                            }
                        }
                        None => sets.next().await,
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
                        record_get_stage_duration_if_enabled(path, GET_STAGE_STRIPE_READ_FIRST_SHARD, stripe_read_start);
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
                        let next_read_cost = read_costs.get(next_i).copied().unwrap_or(ShardReadCost::Unknown);
                        record_scheduled_read_cost(
                            next_read_cost,
                            locality_preference_enabled,
                            low_cost_available,
                            self.data_shards,
                            true,
                            &mut local_preferred,
                            &mut remote_scheduled,
                            &mut fallback_to_remote,
                        );
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
                        let next_read_cost = read_costs.get(next_i).copied().unwrap_or(ShardReadCost::Unknown);
                        record_scheduled_read_cost(
                            next_read_cost,
                            locality_preference_enabled,
                            low_cost_available,
                            self.data_shards,
                            true,
                            &mut local_preferred,
                            &mut remote_scheduled,
                            &mut fallback_to_remote,
                        );
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
                record_get_stage_duration_if_enabled(path, GET_STAGE_STRIPE_READ_QUORUM, stripe_read_start);
                rustfs_io_metrics::record_get_object_shard_read_fanout(path, scheduled, completed, success, failed);
                if locality_preference_enabled {
                    let remote_avoided = remote_available.saturating_sub(remote_scheduled);
                    rustfs_io_metrics::record_get_object_shard_locality_policy(
                        path,
                        local_preferred,
                        remote_avoided,
                        fallback_to_remote,
                    );
                } else {
                    let remote_avoid_potential =
                        shard_locality_remote_avoid_potential(remote_scheduled, low_cost_available, self.data_shards);
                    rustfs_io_metrics::record_get_object_shard_locality_observe_only(
                        path,
                        remote_scheduled,
                        remote_avoid_potential,
                    );
                    rustfs_io_metrics::record_get_object_shard_locality_policy_disabled(path);
                }
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

    /// Lockstep stripe read for the reconstruction-verifying GET path.
    ///
    /// Reads every *engaged* shard reader exactly once per stripe and waits for
    /// all of them, so all engaged readers advance by one block per stripe and
    /// remain mutually block-aligned. This preserves the alignment invariant
    /// that removed the "inconsistent read source shards" desync (backlog#832):
    /// a reader must never contribute a block from a different stripe than the
    /// rest of the set.
    ///
    /// While the object is healthy only the data shards are engaged, so a
    /// healthy GET reads exactly `data_shards` shards per stripe instead of all
    /// `data + parity` shards (backlog#923). Parity slots stay unopened
    /// deferred readers; when a data shard is missing or dies at stripe `k`,
    /// enough parity readers are engaged — realigned to stripe `k` through
    /// their [`DeferredReaderStripeHandle`] — to restore the decode quorum
    /// *plus one extra shard* so reconstruction verification still has a
    /// source to check against (`erasure.rs` only verifies when
    /// `available_shards > data_shards`).
    ///
    /// Any reader that errors is retired for the rest of the object — data and
    /// newly engaged parity alike: a streaming shard read that failed mid-block
    /// can no longer be trusted to be aligned, so re-reading it on a later
    /// stripe would reintroduce the desync. A parity reader that cannot be
    /// realigned (no pending deferred handle) is likewise retired instead of
    /// being read out of position.
    async fn read_lockstep(&mut self) -> (Vec<Option<Vec<u8>>>, Vec<Option<Error>>) {
        let num_readers = self.readers.len();
        let shard_size = if self.offset + self.shard_size > self.shard_file_size {
            self.shard_file_size - self.offset
        } else {
            self.shard_size
        };

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; num_readers];
        let mut errs: Vec<Option<Error>> = vec![None; num_readers];
        if shard_size == 0 {
            return (shards, errs);
        }

        // Advance to the next stripe (see the matching note in `read`); the
        // lockstep path must track stripe geometry identically (backlog#799 B2).
        self.offset += shard_size;
        let stripe_index = self.stripe_index;
        self.stripe_index += 1;

        self.buffers.ensure_slots(num_readers);

        // Engage parity up front when data slots are already known to be
        // missing (offline at setup or retired on an earlier stripe), so the
        // substitute reads run in parallel with the surviving data reads.
        let missing_data_readers = self.readers.iter().take(self.data_shards).filter(|r| r.is_none()).count();
        if missing_data_readers > 0 {
            // One extra engaged parity beyond the reconstruction quorum keeps
            // reconstruction verification effective: with exactly
            // `data_shards` available shards the verification would silently
            // turn itself off.
            let want = missing_data_readers + 1;
            let mut have = (self.data_shards..num_readers)
                .filter(|&i| self.engaged[i] && self.readers[i].is_some())
                .count();
            for idx in self.data_shards..num_readers {
                if have >= want {
                    break;
                }
                if self.readers[idx].is_some() && !self.engaged[idx] && self.try_engage_parity(idx, stripe_index) {
                    have += 1;
                }
            }
        }

        // Pre-claim per-slot buffers so the `self.readers` borrow below stays
        // disjoint from `self.buffers`.
        let participating: Vec<bool> = (0..num_readers)
            .map(|i| self.engaged[i] && self.readers[i].is_some())
            .collect();
        let mut bufs: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_readers);
        for (i, participates) in participating.iter().enumerate() {
            bufs.push(if *participates {
                Some(self.buffers.take(i, shard_size))
            } else {
                None
            });
        }

        let data_shards = self.data_shards;
        let read_timeout = self.read_timeout;
        let metrics_path = self.metrics_path;
        let read_costs = self.read_costs.clone();
        let locality_preference_enabled = self.locality_preference_enabled;
        let stripe_read_start = metrics_path.map(|_| Instant::now());

        let mut retire_readers = Vec::new();
        let mut scheduled = 0usize;
        let mut success = 0usize;
        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut first_shard_recorded = false;
        // Scope the reader borrow (held by `reader_iter`/`sets`) so it is released
        // before the retirement pass mutates `self.readers` below.
        {
            let mut sets = FuturesUnordered::new();
            let reader_iter = ReaderLaunchIter::new(&mut self.readers, &read_costs, locality_preference_enabled);
            for (i, reader) in reader_iter {
                if reader.is_none() || !participating[i] {
                    continue;
                }
                let read_cost = read_costs.get(i).copied().unwrap_or(ShardReadCost::Unknown);
                let recycled_buf = bufs[i].take();
                scheduled += 1;
                sets.push(read_shard(
                    i,
                    read_cost,
                    reader,
                    recycled_buf,
                    shard_size,
                    data_shards,
                    read_timeout,
                    metrics_path,
                ));
            }

            while let Some((i, _read_cost, result, _should_retire)) = sets.next().await {
                completed += 1;
                if !first_shard_recorded {
                    if let Some(path) = metrics_path {
                        record_get_stage_duration_if_enabled(path, GET_STAGE_STRIPE_READ_FIRST_SHARD, stripe_read_start);
                    }
                    first_shard_recorded = true;
                }
                match result {
                    Ok(v) => {
                        shards[i] = Some(v);
                        success += 1;
                    }
                    Err(e) => {
                        errs[i] = Some(e);
                        retire_readers.push(i);
                        failed += 1;
                    }
                }
            }
        }

        // A data shard may have died during this stripe's reads. The unengaged
        // parity readers are still unopened, so they can be aligned to *this*
        // stripe and read now: engage them one at a time until the stripe has
        // `data_shards + 1` successful shards (decode quorum plus one
        // reconstruction-verification source) or parity runs out.
        loop {
            let success_now = shards.iter().filter(|shard| shard.is_some()).count();
            let data_shard_missing = shards.iter().take(data_shards).any(|shard| shard.is_none());
            if !data_shard_missing || success_now > data_shards {
                break;
            }
            let Some(idx) = (data_shards..num_readers).find(|&i| self.readers[i].is_some() && !self.engaged[i]) else {
                break;
            };
            if !self.try_engage_parity(idx, stripe_index) {
                continue;
            }
            let read_cost = read_costs.get(idx).copied().unwrap_or(ShardReadCost::Unknown);
            let recycled_buf = Some(self.buffers.take(idx, shard_size));
            scheduled += 1;
            let (i, _read_cost, result, _should_retire) = read_shard(
                idx,
                read_cost,
                &mut self.readers[idx],
                recycled_buf,
                shard_size,
                data_shards,
                read_timeout,
                metrics_path,
            )
            .await;
            completed += 1;
            match result {
                Ok(v) => {
                    shards[i] = Some(v);
                    success += 1;
                }
                Err(e) => {
                    errs[i] = Some(e);
                    retire_readers.push(i);
                    failed += 1;
                }
            }
        }

        if let Some(path) = metrics_path {
            record_get_stage_duration_if_enabled(path, GET_STAGE_STRIPE_READ_QUORUM, stripe_read_start);
            rustfs_io_metrics::record_get_object_shard_read_fanout(path, scheduled, completed, success, failed);
        }

        for i in retire_readers {
            self.readers[i] = None;
        }

        (shards, errs)
    }

    /// Attempt to bring an as-yet-unread parity reader into the lockstep read
    /// set at `stripe_index`.
    ///
    /// At stripe 0 every reader is still positioned at the stream start, so
    /// engagement is trivially aligned. Past stripe 0 the parity reader must
    /// still be an unopened deferred reader: its pending open offset is
    /// advanced by `stripe_index` bitrot blocks (the `bitrot_encoded_range`
    /// geometry) so its first read returns the current stripe. A parity reader
    /// that cannot be realigned is retired for the rest of the object,
    /// mirroring the retire-on-error rule: reading it would return an earlier
    /// stripe and reintroduce the backlog#832 desync.
    fn try_engage_parity(&mut self, idx: usize, stripe_index: usize) -> bool {
        if stripe_index == 0 {
            self.engaged[idx] = true;
            return true;
        }
        let advanced = self
            .deferred_handles
            .get(idx)
            .and_then(|handle| handle.as_ref())
            .is_some_and(|handle| handle.advance_stripes(stripe_index));
        if advanced {
            self.engaged[idx] = true;
            true
        } else {
            warn!(
                shard_index = idx,
                stripe_index, "retiring parity reader that cannot be aligned to the current stripe"
            );
            self.readers[idx] = None;
            false
        }
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
        let write_stage_start = get_stage_timer_if_enabled(rustfs_io_metrics::get_stage_metrics_enabled());
        if let Err(e) = writer.write_all(&block_slice[..write_len]).await {
            if let Some(write_stage_start) = write_stage_start {
                rustfs_io_metrics::record_get_object_duplex_backpressure_duration(write_stage_start.elapsed().as_secs_f64());
            }
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
        if let Some(write_stage_start) = write_stage_start {
            rustfs_io_metrics::record_get_object_duplex_backpressure_duration(write_stage_start.elapsed().as_secs_f64());
        }

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
        self.decode_inner(writer, readers, offset, length, total_length, None, Vec::new())
            .await
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
        self.decode_inner(writer, readers, offset, length, total_length, Some(read_costs), Vec::new())
            .await
    }

    /// GET decode entry point that also carries the deferred-parity stripe
    /// handles from bitrot reader setup, so unengaged parity readers can be
    /// opened aligned to the stripe where a data shard fails (backlog#923).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn decode_with_stripe_handles<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
        read_costs: Option<Vec<ShardReadCost>>,
        deferred_handles: Vec<Option<DeferredReaderStripeHandle>>,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        self.decode_inner(writer, readers, offset, length, total_length, read_costs, deferred_handles)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn decode_inner<W, R>(
        &self,
        writer: &mut W,
        readers: Vec<Option<BitrotReader<R>>>,
        offset: usize,
        length: usize,
        total_length: usize,
        read_costs: Option<Vec<ShardReadCost>>,
        deferred_handles: Vec<Option<DeferredReaderStripeHandle>>,
    ) -> (usize, Option<std::io::Error>)
    where
        W: AsyncWrite + Send + Sync + Unpin,
        R: AsyncRead + Unpin + Send + Sync,
    {
        if readers.len() != self.data_shards + self.parity_shards {
            record_get_object_pipeline_failure(GET_STAGE_RANGE, GetObjectFailureReason::RangeOrLengthInvalid);
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "Invalid number of readers")));
        }

        // block_size/data_shards come from on-disk metadata; a corrupt FileInfo with a
        // zero here must surface as an error, not a divide-by-zero panic on every GET.
        if self.block_size == 0 || self.data_shards == 0 {
            record_get_object_pipeline_failure(GET_STAGE_RANGE, GetObjectFailureReason::RangeOrLengthInvalid);
            return (0, Some(io::Error::new(ErrorKind::InvalidInput, "Invalid erasure coding parameters")));
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
        }
        .with_deferred_parity_handles(deferred_handles);

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

            let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
            let stripe_read_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
            let (mut shards, errs) = reader.read().await;
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_LEGACY_DUPLEX, GET_STAGE_STRIPE_READ, stripe_read_stage_start);

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
            let reconstruct_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
            if let Err(e) = self.decode_data_with_reconstruction_verification(&mut shards) {
                record_get_stage_duration_if_enabled(
                    GET_OBJECT_PATH_LEGACY_DUPLEX,
                    GET_STAGE_RECONSTRUCT,
                    reconstruct_stage_start,
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
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_LEGACY_DUPLEX, GET_STAGE_RECONSTRUCT, reconstruct_stage_start);

            let emit_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
            let n = match write_data_blocks(writer, &shards, self.data_shards, block_offset, block_length).await {
                Ok(n) => {
                    record_get_stage_duration_if_enabled(GET_OBJECT_PATH_LEGACY_DUPLEX, GET_STAGE_EMIT, emit_stage_start);
                    n
                }
                Err(e) => {
                    record_get_stage_duration_if_enabled(GET_OBJECT_PATH_LEGACY_DUPLEX, GET_STAGE_EMIT, emit_stage_start);
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
        erasure::coding::{BitrotReader, BitrotWriter},
    };
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    type BoxedShardReader = Box<dyn AsyncRead + Send + Sync + Unpin>;

    /// Counts the raw bytes pulled from a shard stream, to prove which shards
    /// a decode path actually touches (backlog#923 call-count evidence).
    struct CountingShardReader {
        inner: Cursor<Vec<u8>>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl AsyncRead for CountingShardReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = result {
                let delta = buf.filled().len() - before;
                self.bytes_read.fetch_add(delta, Ordering::SeqCst);
            }
            result
        }
    }

    /// Build a production-shaped reader set: data shards as opened stream
    /// readers, parity shards as *unopened* deferred readers carrying stripe
    /// handles (mirroring `fill_deferred_bitrot_readers`). `truncate` shortens
    /// the given data shard buffers to simulate a reader dying mid-object.
    fn readers_with_deferred_parity(
        shard_bufs: &[Vec<u8>],
        data_shards: usize,
        shard_size: usize,
        hash_algo: &HashAlgorithm,
        truncate: &[(usize, usize)],
    ) -> (Vec<Option<BitrotReader<BoxedShardReader>>>, Vec<Option<DeferredReaderStripeHandle>>) {
        use crate::io_support::bitrot::create_deferred_bitrot_reader_with_stripe_handle;

        let mut readers = Vec::with_capacity(shard_bufs.len());
        let mut handles: Vec<Option<DeferredReaderStripeHandle>> = vec![None; shard_bufs.len()];
        for (i, buf) in shard_bufs.iter().enumerate() {
            if i < data_shards {
                let bytes = truncate
                    .iter()
                    .find(|(index, _)| *index == i)
                    .map(|(_, len)| buf[..*len].to_vec())
                    .unwrap_or_else(|| buf.clone());
                readers.push(Some(BitrotReader::new(
                    Box::new(Cursor::new(bytes)) as BoxedShardReader,
                    shard_size,
                    hash_algo.clone(),
                    false,
                )));
            } else {
                let (reader, handle) = create_deferred_bitrot_reader_with_stripe_handle(
                    Some(bytes::Bytes::from(buf.clone())),
                    None,
                    "test-bucket",
                    "test-object",
                    0,
                    buf.len(),
                    shard_size,
                    hash_algo.clone(),
                    false,
                    false,
                );
                readers.push(Some(reader));
                handles[i] = Some(handle);
            }
        }
        (readers, handles)
    }

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

    /// One mid-object data-shard death scenario, shared by the gate-on and
    /// gate-off variants of `test_erasure_decode_recovers_when_data_shard_dies_midway`.
    async fn run_decode_midway_death_case(hash_algo: HashAlgorithm, context: &str) {
        const DATA_SHARDS: usize = 4;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        // 200 bytes => 3 full stripes + 1 partial: the failing data shard reads
        // the first stripe, then errors on every later stripe.
        let total_data: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let total_len = total_data.len();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_size = hash_algo.size();

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

        // Truncate data shard 0 to only its first (hash+data) block: it reads
        // the first stripe successfully, then errors (UnexpectedEof), forcing
        // every later stripe to engage the deferred parity readers aligned to
        // the failing stripe.
        let first_block_len = (hash_size + shard_size).min(shard_bufs[0].len());
        let (readers, handles) =
            readers_with_deferred_parity(&shard_bufs, DATA_SHARDS, shard_size, &hash_algo, &[(0, first_block_len)]);

        let mut output = Vec::new();
        let (written, err) = erasure
            .decode_with_stripe_handles(&mut output, readers, 0, total_len, total_len, None, handles)
            .await;
        assert!(
            err.is_none(),
            "{context}, algo={hash_algo:?}: mid-object data-shard failure must still reconstruct: {err:?}"
        );
        assert_eq!(
            written, total_len,
            "{context}, algo={hash_algo:?}: short write after mid-object shard failure"
        );
        assert_eq!(
            output, total_data,
            "{context}, algo={hash_algo:?}: reconstructed bytes mismatch (stripe desync?)"
        );
    }

    /// Regression for backlog#832 (extended for backlog#923): a data shard
    /// reader that dies partway through a multi-stripe object must still
    /// reconstruct byte-exact output, and the parity readers that fill in for
    /// it must be aligned to the current stripe. With the data-shards-only
    /// gate on, parity slots are unopened deferred readers whose stripe handle
    /// advances the pending open offset to the failing stripe — mirroring the
    /// production GET reader setup. Covers both the streaming per-block
    /// checksum format and the no-per-block-hash (`hash_size == 0`) format,
    /// where the offset mapping degrades to the identity function (merge gate
    /// for backlog#923). Also run with the gate off to lock the default
    /// (read-all-shards) behavior.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_erasure_decode_recovers_when_data_shard_dies_midway() {
        for enabled in [None, Some("true")] {
            temp_env::async_with_vars([(ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE, enabled)], async {
                let context = if enabled.is_some() { "data-shards-only" } else { "default" };
                for hash_algo in [HashAlgorithm::HighwayHash256, HashAlgorithm::None] {
                    run_decode_midway_death_case(hash_algo, context).await;
                }
            })
            .await;
        }
    }

    /// Decode a healthy 2+2 multi-stripe object through the lockstep GET path
    /// and return the raw bytes pulled from each shard stream.
    async fn healthy_lockstep_shard_bytes() -> (usize, Vec<usize>) {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        // 200 bytes => 3 full stripes + 1 partial.
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

        let counters: Vec<Arc<AtomicUsize>> = (0..total_shards).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let readers: Vec<Option<BitrotReader<CountingShardReader>>> = shard_bufs
            .iter()
            .enumerate()
            .map(|(i, buf)| {
                Some(BitrotReader::new(
                    CountingShardReader {
                        inner: Cursor::new(buf.clone()),
                        bytes_read: Arc::clone(&counters[i]),
                    },
                    shard_size,
                    hash_algo.clone(),
                    false,
                ))
            })
            .collect();

        // `Erasure::decode` is the reconstruction-verifying (lockstep) GET path.
        let mut output = Vec::new();
        let (written, err) = erasure.decode(&mut output, readers, 0, total_len, total_len).await;
        assert!(err.is_none(), "healthy decode must succeed: {err:?}");
        assert_eq!(written, total_len);
        assert_eq!(output, total_data);

        (DATA_SHARDS, counters.iter().map(|counter| counter.load(Ordering::SeqCst)).collect())
    }

    /// Merge gate for backlog#923: with the gate on, the healthy lockstep GET
    /// path must read only the data shards — parity readers stay unopened and
    /// contribute zero read bytes. This is the call-count proof of the 2x read
    /// amplification fix (2+2 layout reads 2 shards per stripe, not 4).
    #[tokio::test]
    #[serial_test::serial]
    async fn test_lockstep_healthy_get_reads_only_data_shards() {
        temp_env::async_with_vars([(ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE, Some("true"))], async {
            let (data_shards, shard_bytes) = healthy_lockstep_shard_bytes().await;
            for (i, bytes) in shard_bytes.iter().enumerate() {
                if i < data_shards {
                    assert!(*bytes > 0, "data shard {i} must be read on the healthy path");
                } else {
                    assert_eq!(
                        *bytes, 0,
                        "healthy GET must not read parity shard {i} (2x read amplification, backlog#923)"
                    );
                }
            }
        })
        .await;
    }

    /// Compatibility lock: with the gate off (default), the lockstep path
    /// keeps the pre-backlog#923 behavior and reads every live shard —
    /// including parity — on every stripe.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_lockstep_default_reads_all_shards_per_stripe() {
        temp_env::async_with_vars([(ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE, None::<&str>)], async {
            let (_data_shards, shard_bytes) = healthy_lockstep_shard_bytes().await;
            for (i, bytes) in shard_bytes.iter().enumerate() {
                assert!(*bytes > 0, "default lockstep behavior must read shard {i} on every stripe");
            }
        })
        .await;
    }

    /// Merge gate for backlog#923: reconstruction verification must stay
    /// active when parity is engaged mid-object. When a data shard dies at
    /// stripe k, the lockstep path engages one parity reader beyond the decode
    /// quorum (`erasure.rs` only verifies when `available > data_shards`), so
    /// a parity shard whose content is inconsistent with the surviving data —
    /// while still passing its own bitrot hash — must be detected and fail the
    /// read instead of silently corrupting the reconstructed output. Run with
    /// the gate on and off: the rejection must hold in both modes.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_erasure_decode_rejects_inconsistent_parity_engaged_midstream() {
        for enabled in [None, Some("true")] {
            temp_env::async_with_vars([(ENV_RUSTFS_GET_LOCKSTEP_DATA_SHARDS_ONLY_ENABLE, enabled)], async {
                run_inconsistent_parity_midstream_case(if enabled.is_some() { "data-shards-only" } else { "default" }).await;
            })
            .await;
        }
    }

    async fn run_inconsistent_parity_midstream_case(context: &str) {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let total_data: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let total_len = total_data.len();

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let total_shards = DATA_SHARDS + PARITY_SHARDS;
        let shard_size = erasure.shard_size();
        let hash_algo = HashAlgorithm::HighwayHash256;
        let hash_size = hash_algo.size();

        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        let mut offset = 0;
        let mut stripe = 0usize;
        while offset < total_len {
            let end = (offset + BLOCK_SIZE).min(total_len);
            let shards = erasure.encode_data(&total_data[offset..end]).unwrap();
            for (i, shard) in shards.iter().enumerate() {
                // Corrupt the first parity shard's payload for every stripe
                // after the first, *before* it is bitrot-hashed: the shard
                // passes its own hash check but is inconsistent with the
                // erasure-coded set.
                if i == DATA_SHARDS && stripe >= 1 {
                    let mut corrupted = shard.to_vec();
                    corrupted[0] ^= 0x80;
                    shard_writers[i].write(&corrupted).await.unwrap();
                } else {
                    shard_writers[i].write(shard).await.unwrap();
                }
            }
            offset = end;
            stripe += 1;
        }

        let shard_bufs: Vec<Vec<u8>> = shard_writers.into_iter().map(|w| w.into_inner().into_inner()).collect();

        // Data shard 0 dies after stripe 0, forcing parity engagement at stripe 1.
        let first_block_len = (hash_size + shard_size).min(shard_bufs[0].len());
        let (readers, handles) =
            readers_with_deferred_parity(&shard_bufs, DATA_SHARDS, shard_size, &hash_algo, &[(0, first_block_len)]);

        let mut output = Vec::new();
        let (_written, err) = erasure
            .decode_with_stripe_handles(&mut output, readers, 0, total_len, total_len, None, handles)
            .await;

        let err = err.unwrap_or_else(|| panic!("{context}: inconsistent parity engaged mid-stream must fail the read"));
        assert_eq!(err.kind(), ErrorKind::InvalidData, "{context}");
        assert!(err.to_string().contains("inconsistent read source shards"), "{context}: {err}");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_erasure_decode_preserves_compressed_stream_near_block_boundary() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 1024 * 1024;

        use crate::io_support::rio::CompressReader;
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

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_preference_gate_defaults_disabled() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, None::<&str>),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                assert_eq!(get_shard_locality_scheduling_mode(), ShardLocalitySchedulingMode::Off);
                assert!(!get_shard_locality_preference_enabled());
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_scheduling_mode_parses_supported_values() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("observe")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                assert_eq!(get_shard_locality_scheduling_mode(), ShardLocalitySchedulingMode::Observe);
                assert!(!get_shard_locality_preference_enabled());
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                assert_eq!(get_shard_locality_scheduling_mode(), ShardLocalitySchedulingMode::On);
                assert!(get_shard_locality_preference_enabled());
                assert!(should_collect_shard_read_costs());
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("unexpected")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, Some("true")),
            ],
            || {
                assert_eq!(get_shard_locality_scheduling_mode(), ShardLocalitySchedulingMode::Off);
                assert!(!get_shard_locality_preference_enabled());
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, None::<&str>),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, Some("true")),
            ],
            || {
                assert_eq!(get_shard_locality_scheduling_mode(), ShardLocalitySchedulingMode::On);
                assert!(get_shard_locality_preference_enabled());
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_scheduling_off_does_not_collect_without_metrics() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("off")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                rustfs_io_metrics::set_get_stage_metrics_enabled(false);
                assert!(!should_collect_shard_read_costs());
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_scheduling_observe_collects_only_with_stage_metrics() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("observe")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                // Without stage metrics there is no reporting channel, so
                // observe mode must not pay the cost-collection overhead.
                rustfs_io_metrics::set_get_stage_metrics_enabled(false);
                assert!(!should_collect_shard_read_costs());

                rustfs_io_metrics::set_get_stage_metrics_enabled(true);
                assert!(should_collect_shard_read_costs());
                assert!(!get_shard_locality_preference_enabled());
                let read_costs = [ShardReadCost::Remote, ShardReadCost::Local, ShardReadCost::SameNode];
                assert_eq!(shard_read_launch_order(&read_costs, read_costs.len(), false), vec![0, 1, 2]);
                rustfs_io_metrics::set_get_stage_metrics_enabled(false);
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_scheduling_on_enables_reordering() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            || {
                assert!(get_shard_locality_preference_enabled());
            },
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_shard_locality_legacy_preference_gate_still_enables_on() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, None::<&str>),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, Some("true")),
            ],
            || {
                assert!(get_shard_locality_preference_enabled());
            },
        );
    }

    #[test]
    fn test_shard_locality_read_launch_order_is_gated() {
        let read_costs = [
            ShardReadCost::Remote,
            ShardReadCost::Local,
            ShardReadCost::Unknown,
            ShardReadCost::SameNode,
            ShardReadCost::Remote,
        ];

        assert_eq!(shard_read_launch_order(&read_costs, read_costs.len(), false), vec![0, 1, 2, 3, 4]);
        assert_eq!(shard_read_launch_order(&read_costs, read_costs.len(), true), vec![1, 3, 0, 4, 2]);
    }

    #[test]
    fn test_shard_locality_remote_avoid_potential_is_observe_only() {
        assert_eq!(shard_locality_remote_avoid_potential(2, 4, 4), 2);
        assert_eq!(shard_locality_remote_avoid_potential(2, 2, 4), 0);
        assert_eq!(shard_locality_remote_avoid_potential(3, 3, 4), 2);
    }

    #[test]
    #[serial_test::serial]
    fn parallel_reader_drops_metrics_path_when_stage_metrics_disabled() {
        let erasure = Erasure::new(2, 1, 32);
        let readers: Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> = vec![None, None, None];

        rustfs_io_metrics::set_get_stage_metrics_enabled(false);
        let reader = ParallelReader::new_with_metrics_path(readers, erasure.clone(), 0, 1, Some(GET_OBJECT_PATH_LEGACY_DUPLEX));
        assert_eq!(reader.metrics_path, None);

        let readers: Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> = vec![None, None, None];
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let reader = ParallelReader::new_with_metrics_path(readers, erasure, 0, 1, Some(GET_OBJECT_PATH_LEGACY_DUPLEX));
        assert_eq!(reader.metrics_path, Some(GET_OBJECT_PATH_LEGACY_DUPLEX));

        rustfs_io_metrics::set_get_stage_metrics_enabled(false);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_parallel_reader_local_first_avoids_remote_when_local_quorum_exists() {
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            async {
                const NUM_SHARDS: usize = 1;
                const BLOCK_SIZE: usize = 64;
                const DATA_SHARDS: usize = 4;
                const PARITY_SHARDS: usize = 2;
                const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

                let hash_algo = HashAlgorithm::HighwayHash256;
                let readers = make_test_readers(DATA_SHARDS + PARITY_SHARDS, SHARD_SIZE, NUM_SHARDS, &hash_algo, &[], &[]).await;
                let read_costs = vec![
                    ShardReadCost::Remote,
                    ShardReadCost::Remote,
                    ShardReadCost::Local,
                    ShardReadCost::SameNode,
                    ShardReadCost::Local,
                    ShardReadCost::SameNode,
                ];
                let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
                let mut parallel_reader = ParallelReader::new_with_metrics_path_and_read_costs(
                    readers,
                    erasure,
                    0,
                    NUM_SHARDS * BLOCK_SIZE,
                    None,
                    read_costs,
                );

                let (bufs, errs) = parallel_reader.read().await;

                assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
                assert!(bufs[0].is_none());
                assert!(bufs[1].is_none());
                for (index, buf) in bufs.iter().enumerate().take(DATA_SHARDS + PARITY_SHARDS).skip(2) {
                    assert_eq!(buf.as_deref(), Some(&[(index % 256) as u8; SHARD_SIZE][..]));
                }
                assert!(errs.iter().all(Option::is_none));
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_parallel_reader_local_missing_falls_back_to_remote() {
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            async {
                const NUM_SHARDS: usize = 1;
                const BLOCK_SIZE: usize = 64;
                const DATA_SHARDS: usize = 4;
                const PARITY_SHARDS: usize = 2;
                const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

                let hash_algo = HashAlgorithm::HighwayHash256;
                let readers = make_test_readers(DATA_SHARDS + PARITY_SHARDS, SHARD_SIZE, NUM_SHARDS, &hash_algo, &[1], &[]).await;
                let read_costs = vec![
                    ShardReadCost::Local,
                    ShardReadCost::Local,
                    ShardReadCost::Local,
                    ShardReadCost::Local,
                    ShardReadCost::Remote,
                    ShardReadCost::Remote,
                ];
                let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
                let mut parallel_reader = ParallelReader::new_with_metrics_path_and_read_costs(
                    readers,
                    erasure,
                    0,
                    NUM_SHARDS * BLOCK_SIZE,
                    None,
                    read_costs,
                );

                let (bufs, errs) = parallel_reader.read().await;

                assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
                assert!(matches!(errs[1], Some(Error::FileNotFound)));
                assert_eq!(bufs[4].as_deref(), Some(&[4u8; SHARD_SIZE][..]));
                assert!(bufs[5].is_none());
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_parallel_reader_local_corrupt_falls_back_to_remote() {
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            async {
            const NUM_SHARDS: usize = 1;
            const BLOCK_SIZE: usize = 64;
            const DATA_SHARDS: usize = 4;
            const PARITY_SHARDS: usize = 2;
            const SHARD_SIZE: usize = BLOCK_SIZE / DATA_SHARDS;

            let hash_algo = HashAlgorithm::HighwayHash256;
            let readers = make_test_readers(DATA_SHARDS + PARITY_SHARDS, SHARD_SIZE, NUM_SHARDS, &hash_algo, &[], &[1]).await;
            let read_costs = vec![
                ShardReadCost::Local,
                ShardReadCost::Local,
                ShardReadCost::Local,
                ShardReadCost::Local,
                ShardReadCost::Remote,
                ShardReadCost::Remote,
            ];
            let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
            let mut parallel_reader = ParallelReader::new_with_metrics_path_and_read_costs(
                readers,
                erasure,
                0,
                NUM_SHARDS * BLOCK_SIZE,
                None,
                read_costs,
            );

            let (bufs, errs) = parallel_reader.read().await;

            assert_eq!(DATA_SHARDS, bufs.iter().filter(|buf| buf.is_some()).count());
            assert!(
                matches!(&errs[1], Some(DiskError::Io(err)) if err.kind() == ErrorKind::InvalidData && err.to_string().contains("bitrot"))
            );
            assert_eq!(bufs[4].as_deref(), Some(&[4u8; SHARD_SIZE][..]));
            assert!(bufs[5].is_none());
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_erasure_decode_local_first_preserves_output_order() {
        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_SHARD_LOCALITY_SCHEDULING, Some("on")),
                (ENV_RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE, None::<&str>),
            ],
            async {
                const DATA_SHARDS: usize = 4;
                const PARITY_SHARDS: usize = 2;
                const BLOCK_SIZE: usize = 64;

                let total_data: Vec<u8> = (0..BLOCK_SIZE as u32).map(|i| i as u8).collect();
                let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
                let shard_size = erasure.shard_size();
                let hash_algo = HashAlgorithm::HighwayHash256;
                let shard_bufs = encode_test_object(&erasure, &total_data, shard_size, &hash_algo).await;
                let readers = shard_bufs
                    .iter()
                    .map(|buf| Some(BitrotReader::new(Cursor::new(buf.clone()), shard_size, hash_algo.clone(), false)))
                    .collect();
                let read_costs = vec![
                    ShardReadCost::Remote,
                    ShardReadCost::Remote,
                    ShardReadCost::Local,
                    ShardReadCost::Local,
                    ShardReadCost::SameNode,
                    ShardReadCost::SameNode,
                ];

                let mut output = Vec::new();
                let (written, err) = erasure
                    .decode_with_read_costs(&mut output, readers, 0, total_data.len(), total_data.len(), read_costs)
                    .await;

                assert!(err.is_none(), "unexpected decode error: {err:?}");
                assert_eq!(written, total_data.len());
                assert_eq!(output, total_data);
            },
        )
        .await;
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

    #[test]
    fn test_zero_read_timeout_disables_scheduled_hedging() {
        assert_eq!(shard_read_hedge_delay(Duration::ZERO), None);
        assert_eq!(shard_read_hedge_delay(Duration::from_millis(50)), Some(Duration::from_millis(50)));
        assert_eq!(shard_read_hedge_delay(Duration::from_secs(60)), Some(Duration::from_millis(100)));
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

    async fn make_test_readers(
        total_shards: usize,
        shard_size: usize,
        num_shards: usize,
        hash_algo: &HashAlgorithm,
        missing: &[usize],
        corrupt: &[usize],
    ) -> Vec<Option<BitrotReader<Cursor<Vec<u8>>>>> {
        let mut readers = Vec::with_capacity(total_shards);
        for index in 0..total_shards {
            if missing.contains(&index) {
                readers.push(None);
            } else {
                readers.push(Some(
                    create_reader(shard_size, num_shards, (index % 256) as u8, hash_algo, corrupt.contains(&index)).await,
                ));
            }
        }
        readers
    }

    async fn encode_test_object(erasure: &Erasure, data: &[u8], shard_size: usize, hash_algo: &HashAlgorithm) -> Vec<Vec<u8>> {
        let total_shards = erasure.data_shards + erasure.parity_shards;
        let mut shard_writers: Vec<BitrotWriter<Cursor<Vec<u8>>>> = (0..total_shards)
            .map(|_| BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone()))
            .collect();

        let shards = erasure.encode_data(data).unwrap();
        for (index, shard) in shards.iter().enumerate() {
            shard_writers[index].write(shard).await.unwrap();
        }

        shard_writers
            .into_iter()
            .map(|writer| writer.into_inner().into_inner())
            .collect()
    }
}
