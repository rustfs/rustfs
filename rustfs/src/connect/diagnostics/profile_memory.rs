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

//! Mimalloc allocation aggregates for the bounded profile contract.
//!
//! Only cumulative allocated octets and allocation counts are sampled. Heap
//! bytes, addresses, stack traces, paths, symbols, and allocator debug text are
//! excluded from the result.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::connect::DeviceIdentity;

use super::profile_cpu::{
    CollectorLease, MemoryProfileData, ProfileCaptureRequest, ProfileData, ProfileError, ProfileResult, ProfileTool,
    SignedProfileExport, check_cancel, encode_signed_profile_export, unix_now,
};

const MAX_ALLOCATOR_STATS_BYTES: usize = 262_144;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AllocationSnapshot {
    total_allocated_bytes: u64,
    allocation_count: u64,
}

pub(crate) trait AllocationProfileSource: Send + Sync {
    fn snapshot(&self) -> Result<AllocationSnapshot, ProfileError>;
}

struct MimallocProfileSource;

impl AllocationProfileSource for MimallocProfileSource {
    fn snapshot(&self) -> Result<AllocationSnapshot, ProfileError> {
        #[cfg(target_os = "windows")]
        {
            Err(ProfileError::SourceUnavailable)
        }
        #[cfg(not(target_os = "windows"))]
        {
            parse_allocator_stats(&rustfs_mimalloc::MiMalloc::stats_json())
        }
    }
}

pub async fn export_memory_profile(
    request: &ProfileCaptureRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, ProfileError> {
    export_memory_profile_from(request, key, cancel, &MimallocProfileSource).await
}

pub(crate) async fn export_memory_profile_from(
    request: &ProfileCaptureRequest,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
    source: &dyn AllocationProfileSource,
) -> Result<SignedProfileExport, ProfileError> {
    request.validate(ProfileTool::Memory, unix_now()?)?;
    check_cancel(cancel)?;
    let _lease = CollectorLease::acquire()?;
    let started = Instant::now();
    let result = collect_window(request.sample_period, cancel, source);
    let (before, after) = tokio::time::timeout(request.duration, result)
        .await
        .map_err(|_| ProfileError::TimedOut)??;
    check_cancel(cancel)?;

    let allocated_bytes = after
        .total_allocated_bytes
        .checked_sub(before.total_allocated_bytes)
        .ok_or(ProfileError::CounterReset)?;
    let allocation_count = after
        .allocation_count
        .checked_sub(before.allocation_count)
        .ok_or(ProfileError::CounterReset)?;
    let elapsed = started.elapsed();
    let sample_period_micros = u64::try_from(elapsed.as_micros()).map_err(|_| ProfileError::LimitExceeded)?;
    let data = MemoryProfileData::allocation_aggregates(allocated_bytes, allocation_count, sample_period_micros.max(1));
    let result = ProfileResult::succeeded(request, ProfileTool::Memory, elapsed, ProfileData::Memory(data));

    encode_signed_profile_export(request, &result, key, cancel)
}

fn collect_window<'a>(
    sample_period: Duration,
    cancel: &'a CancellationToken,
    source: &'a dyn AllocationProfileSource,
) -> AllocationWindowFuture<'a> {
    Box::pin(async move {
        let before = source.snapshot()?;
        tokio::select! {
            () = cancel.cancelled() => return Err(ProfileError::Cancelled),
            () = tokio::time::sleep(sample_period) => {}
        }
        let after = source.snapshot()?;
        Ok((before, after))
    })
}

type AllocationWindowFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(AllocationSnapshot, AllocationSnapshot), ProfileError>> + Send + 'a>>;

pub(crate) fn parse_allocator_stats(stats: &str) -> Result<AllocationSnapshot, ProfileError> {
    if stats.is_empty() || stats.len() > MAX_ALLOCATOR_STATS_BYTES {
        return Err(ProfileError::SourceUnavailable);
    }
    let value = serde_json::from_str::<Value>(stats).map_err(|_| ProfileError::SourceUnavailable)?;
    let total_allocated_bytes = sum_metrics(&value, &["malloc_normal", "malloc_huge"], "total")?;
    let allocation_count = sum_metrics(&value, &["malloc_normal_count", "malloc_huge_count"], "total")?;
    Ok(AllocationSnapshot {
        total_allocated_bytes,
        allocation_count,
    })
}

fn sum_metrics(value: &Value, metrics: &[&str], field: &str) -> Result<u64, ProfileError> {
    metrics.iter().try_fold(0_u64, |sum, metric| {
        let value = metric_field(value, metric, field).ok_or(ProfileError::SourceUnavailable)?;
        sum.checked_add(value).ok_or(ProfileError::LimitExceeded)
    })
}

fn metric_field(value: &Value, metric: &str, field: &str) -> Option<u64> {
    match value {
        Value::Object(fields) => {
            if let Some(metric_value) = fields.get(metric)
                && let Some(value) = numeric_field(metric_value, field)
            {
                return Some(value);
            }
            fields.values().find_map(|value| metric_field(value, metric, field))
        }
        Value::Array(values) => values.iter().find_map(|value| metric_field(value, metric, field)),
        _ => None,
    }
}

fn numeric_field(value: &Value, field: &str) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse().ok(),
        Value::Object(fields) => fields.get(field).and_then(|value| numeric_field(value, field)),
        _ => None,
    }
}
