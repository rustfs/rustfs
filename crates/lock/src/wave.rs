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

//! Optimistic all-or-nothing lock-probe waves over independent keys.
//!
//! Bulk operations (S3 `DeleteObjects`) are the only multi-key lock holders.
//! Acquiring their locks strictly one quorum round-trip per key serializes a
//! 1000-key batch; probing the keys concurrently is safe as long as a losing
//! wave gives every lock back before the caller falls back to its canonical
//! serial order — two overlapping batches must never hold-and-wait on each
//! other.

use std::future::Future;

use futures::StreamExt as _;

/// Count of probe waves that acquired every key. Diagnostics for
/// wave-vs-fallback assertions and benchmarks; incremented once per wave.
pub static PROBE_WAVE_SUCCESSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Count of probe waves that lost at least one key and fell back. Diagnostics
/// for wave-vs-fallback assertions and benchmarks; incremented once per wave.
pub static PROBE_WAVE_FALLBACKS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Probe `keys` in bounded chunks of `chunk` concurrent attempts.
///
/// The wave is all-or-nothing at chunk granularity: the first failing probe
/// stops further chunks from launching, every already-acquired guard is
/// handed to `release_all`, and `None` is returned so the caller runs its
/// canonical serial fallback. In-flight probes of the failing chunk are
/// drained to completion before `release_all`, so no acquire RPC is
/// cancelled mid-flight.
///
/// `release_all` **must await the unlock RPCs** (e.g.
/// [`crate::namespace::NamespaceLockGuard::release_and_wait`]) before
/// returning: distributed release is fire-and-forget on drop, and the serial
/// fallback re-acquires the same keys immediately afterwards.
pub async fn probe_lock_wave<K, G, Probe, ProbeFut, Release, ReleaseFut>(
    keys: Vec<K>,
    chunk: usize,
    probe: Probe,
    release_all: Release,
) -> Option<Vec<G>>
where
    K: Send,
    G: Send,
    Probe: Fn(K) -> ProbeFut,
    ProbeFut: Future<Output = Option<G>> + Send,
    Release: FnOnce(Vec<G>) -> ReleaseFut,
    ReleaseFut: Future<Output = ()> + Send,
{
    let chunk = chunk.max(1);
    let mut acquired: Vec<G> = Vec::with_capacity(keys.len());
    let mut keys = keys.into_iter();
    let mut failed = false;

    while !failed {
        let chunk_keys: Vec<K> = keys.by_ref().take(chunk).collect();
        if chunk_keys.is_empty() {
            break;
        }
        let results: Vec<Option<G>> = futures::stream::iter(chunk_keys.into_iter().map(&probe))
            .buffered(chunk)
            .collect()
            .await;
        for result in results {
            match result {
                Some(guard) => acquired.push(guard),
                None => failed = true,
            }
        }
    }

    if failed {
        release_all(acquired).await;
        PROBE_WAVE_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        None
    } else {
        PROBE_WAVE_SUCCESSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(acquired)
    }
}
