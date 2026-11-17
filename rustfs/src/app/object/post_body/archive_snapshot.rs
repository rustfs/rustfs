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

//! Immutable encrypted snapshots for archive preflight and replay.
//!
//! The caller must supply one process-wide budget. A snapshot holds its
//! reservation until the last replay releases its ciphertext.

use super::*;

const MEMORY_SAMPLE_ATTEMPTS: usize = 4;
const MAX_MEMORY_RESERVE_FLOOR: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy)]
struct MemorySample {
    total: u64,
    available: u64,
}

type MemoryProbe = dyn Fn() -> io::Result<MemorySample> + Send + Sync;

#[derive(Default)]
struct MemoryState {
    reserved: u64,
    pending: u64,
    generation: u64,
}

pub(super) struct ArchiveSnapshotBudget {
    limit: u64,
    state: Mutex<MemoryState>,
    sample: Box<MemoryProbe>,
    floor: fn(MemorySample) -> u64,
}

struct ArchiveSnapshotReservation {
    budget: Arc<ArchiveSnapshotBudget>,
    bytes: u64,
    pending: bool,
}

impl ArchiveSnapshotBudget {
    pub(super) fn new(limit: u64) -> Arc<Self> {
        Self::with_probe(limit, system_memory_sample, system_memory_floor)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.limit == 0
    }

    fn with_probe(
        limit: u64,
        sample: impl Fn() -> io::Result<MemorySample> + Send + Sync + 'static,
        floor: fn(MemorySample) -> u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(MemoryState::default()),
            sample: Box::new(sample),
            floor,
        })
    }

    #[cfg(test)]
    pub(super) fn test(limit: u64) -> Arc<Self> {
        Self::with_probe(
            limit,
            || {
                Ok(MemorySample {
                    total: u64::MAX,
                    available: u64::MAX,
                })
            },
            |_| 0,
        )
    }

    fn try_reserve(self: &Arc<Self>, bytes: u64) -> io::Result<ArchiveSnapshotReservation> {
        self.try_reserve_with(bytes, || (self.sample)())
    }

    fn try_reserve_with(
        self: &Arc<Self>,
        bytes: u64,
        mut sample: impl FnMut() -> io::Result<MemorySample>,
    ) -> io::Result<ArchiveSnapshotReservation> {
        let unavailable = || io::Error::from(io::ErrorKind::WouldBlock);
        for _ in 0..MEMORY_SAMPLE_ATTEMPTS {
            let generation = {
                let state = self.state.lock();
                state
                    .reserved
                    .checked_add(bytes)
                    .filter(|&total| total <= self.limit)
                    .ok_or_else(unavailable)?;
                if state.generation == u64::MAX {
                    return Err(unavailable());
                }
                state.generation
            };
            // The file lock is held by the caller; no path takes it while
            // holding this budget lock. Sampling can block, so it runs outside
            // the budget lock, and completed allocations invalidate old samples.
            let observed = sample().map_err(|_| unavailable())?;
            if observed.total == 0 || observed.available > observed.total {
                return Err(unavailable());
            }
            let mut state = self.state.lock();
            if state.generation != generation {
                continue;
            }
            let reserved = state
                .reserved
                .checked_add(bytes)
                .filter(|&total| total <= self.limit)
                .ok_or_else(unavailable)?;
            let pending = state.pending.checked_add(bytes).ok_or_else(unavailable)?;
            let required = pending.checked_add((self.floor)(observed)).ok_or_else(unavailable)?;
            if required > observed.available {
                return Err(unavailable());
            }
            state.reserved = reserved;
            state.pending = pending;
            return Ok(ArchiveSnapshotReservation {
                budget: Arc::clone(self),
                bytes,
                pending: true,
            });
        }
        Err(unavailable())
    }

    #[cfg(test)]
    pub(super) fn reserved(&self) -> u64 {
        self.state.lock().reserved
    }
}

impl ArchiveSnapshotReservation {
    fn mark_allocated(&mut self) {
        if self.pending {
            let mut state = self.budget.state.lock();
            state.pending -= self.bytes;
            state.generation = state.generation.saturating_add(1);
            self.pending = false;
        }
    }
}

impl Drop for ArchiveSnapshotReservation {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();
        state.reserved -= self.bytes;
        if self.pending {
            state.pending -= self.bytes;
        }
        state.generation = state.generation.saturating_add(1);
    }
}

fn system_memory_floor(sample: MemorySample) -> u64 {
    (sample.total / 10).clamp(1, MAX_MEMORY_RESERVE_FLOOR)
}

#[cfg(target_os = "linux")]
fn system_memory_sample() -> io::Result<MemorySample> {
    let sample = crate::cgroup_resources::live::sample()?;
    Ok(MemorySample {
        total: sample.memory_limit,
        available: sample.memory_headroom,
    })
}

#[cfg(not(target_os = "linux"))]
fn system_memory_sample() -> io::Result<MemorySample> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "archive memory admission is unavailable on this platform",
    ))
}

struct SnapshotAllocation {
    ciphertext: Vec<u8>,
    _reserved: ArchiveSnapshotReservation,
    _extra_reserved: Option<ArchiveSnapshotReservation>,
}

impl AsRef<[u8]> for SnapshotAllocation {
    fn as_ref(&self) -> &[u8] {
        &self.ciphertext
    }
}

pub(super) struct ArchiveSnapshot {
    staged: Arc<StagedFile>,
    ciphertext: Bytes,
    length: u64,
}

pub(super) struct PreflightedArchiveSnapshot(ArchiveSnapshot);

impl ArchiveSnapshot {
    pub(super) async fn capture(staged: Arc<StagedFile>, length: u64, budget: Arc<ArchiveSnapshotBudget>) -> S3Result<Self> {
        Self::capture_inner(staged, length, budget, || Ok(())).await
    }

    #[cfg(test)]
    pub(super) async fn capture_with_hook(
        staged: Arc<StagedFile>,
        length: u64,
        budget: Arc<ArchiveSnapshotBudget>,
        hook: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> S3Result<Self> {
        Self::capture_inner(staged, length, budget, hook).await
    }

    async fn capture_inner(
        staged: Arc<StagedFile>,
        length: u64,
        budget: Arc<ArchiveSnapshotBudget>,
        after_reserve: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> S3Result<Self> {
        let ciphertext = file_io(Arc::clone(&staged), move |file| {
            let size = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
            let mut reserved = budget.try_reserve(u64::try_from(size).map_err(io::Error::other)?)?;
            after_reserve()?;
            let mut ciphertext = Vec::new();
            ciphertext
                .try_reserve_exact(size)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            let extra = ciphertext
                .capacity()
                .checked_sub(size)
                .ok_or_else(|| io::Error::other("invalid archive snapshot allocation"))?;
            let mut extra_reserved = if extra == 0 {
                None
            } else {
                Some(budget.try_reserve(u64::try_from(extra).map_err(io::Error::other)?)?)
            };
            ciphertext.resize(size, 0);
            reserved.mark_allocated();
            if let Some(extra) = extra_reserved.as_mut() {
                extra.mark_allocated();
            }
            let mut allocation = SnapshotAllocation {
                ciphertext,
                _reserved: reserved,
                _extra_reserved: extra_reserved,
            };
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut allocation.ciphertext)?;
            if read_chunk(file, &mut [0; 1])? != 0 {
                return Err(io::Error::other("staged archive changed size during snapshot"));
            }
            Ok(allocation)
        })
        .await
        .map_err(snapshot_error)?;
        Ok(Self {
            staged,
            ciphertext: Bytes::from_owner(ciphertext),
            length,
        })
    }

    fn replay(&self) -> Body {
        let cursor = std::io::Cursor::new(self.ciphertext.clone());
        let replay = stream::try_unfold(
            (Arc::clone(&self.staged), cursor, 0u64, Some(self.length)),
            |(staged, mut cursor, sequence, remaining)| async move {
                let Some(remaining) = remaining else {
                    return Ok(None);
                };
                let (staged, cursor, bytes, sequence, remaining) = tokio::task::spawn_blocking(move || {
                    if remaining == 0 {
                        read_end_frame(&mut cursor, &staged.cipher, sequence)?;
                        return Ok::<_, io::Error>((staged, cursor, Zeroizing::new(Vec::new()), sequence, 0));
                    }
                    let bytes = read_frame(&mut cursor, &staged.cipher, sequence)?;
                    let count = u64::try_from(bytes.len()).map_err(io::Error::other)?;
                    if count == 0 || count > remaining {
                        return Err(io::Error::other("invalid archive snapshot frame length"));
                    }
                    let next = sequence
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("archive snapshot frame counter exhausted"))?;
                    let remaining = remaining - count;
                    if remaining == 0 {
                        read_end_frame(&mut cursor, &staged.cipher, next)?;
                    }
                    Ok::<_, io::Error>((staged, cursor, bytes, next, remaining))
                })
                .await
                .map_err(|_| io::Error::other("archive snapshot read task failed"))??;
                if bytes.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((
                        Bytes::from_owner(bytes),
                        (staged, cursor, sequence, (remaining != 0).then_some(remaining)),
                    )))
                }
            },
        );
        from_io_stream(replay)
    }

    pub(super) async fn preflight(
        self,
        archive_key: &str,
        headers: &HeaderMap,
        policy: &PostArchiveKeyPolicy,
    ) -> S3Result<PreflightedArchiveSnapshot> {
        let (chunks, _) = BodyChunks::with_trailers(self.replay(), None);
        let source = StreamReader::new(chunks.map(|chunk| chunk.map_err(s3s_body_error_to_io)));
        super::super::extract::post_archive_preflight::preflight_post_archive(source, self.length, archive_key, headers, policy)
            .await?;
        Ok(PreflightedArchiveSnapshot(self))
    }
}

impl PreflightedArchiveSnapshot {
    pub(super) fn into_replay(self) -> Body {
        self.0.replay()
    }
}

fn snapshot_error(error: io::Error) -> S3Error {
    if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::OutOfMemory) {
        staging_capacity_error()
    } else {
        staging_io_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    #[test]
    fn post_archive_memory_budget_handles_five_gib_and_overflow() {
        const FIVE_GIB: u64 = 5 * 1024 * 1024 * 1024;
        let budget = ArchiveSnapshotBudget::test(FIVE_GIB);
        let held = budget.try_reserve(FIVE_GIB).expect("5 GiB fits");
        assert_eq!(budget.reserved(), FIVE_GIB);
        assert!(budget.try_reserve(1).is_err());
        drop(held);
        assert_eq!(budget.reserved(), 0);

        let budget = ArchiveSnapshotBudget::test(u64::MAX);
        let held = budget.try_reserve(u64::MAX).expect("exact limit fits");
        assert!(budget.try_reserve(1).is_err(), "overflow must fail closed");
        drop(held);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_is_shared_across_concurrent_reservations() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let budget = ArchiveSnapshotBudget::test(5 * GIB);
        let start = Arc::new(Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let budget = Arc::clone(&budget);
            let start = Arc::clone(&start);
            workers.push(std::thread::spawn(move || {
                start.wait();
                budget.try_reserve(GIB)
            }));
        }
        start.wait();
        let mut admitted = Vec::new();
        for worker in workers {
            if let Ok(reservation) = worker.join().expect("reservation worker") {
                admitted.push(reservation);
            }
        }
        assert_eq!(admitted.len(), 5, "only five 1 GiB reservations fit");
        assert_eq!(budget.reserved(), 5 * GIB);
        drop(admitted);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_counts_pending_and_reserve_floor() {
        let sample = MemorySample {
            total: 100,
            available: 100,
        };
        let budget = ArchiveSnapshotBudget::with_probe(200, move || Ok(sample), |_| 10);
        let first = budget.try_reserve(60).expect("first allocation fits");
        assert!(budget.try_reserve(31).is_err(), "pending bytes must count against headroom");
        let second = budget.try_reserve(30).expect("exactly available headroom fits");
        assert_eq!(budget.reserved(), 90);
        drop(second);
        drop(first);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_refuses_bad_samples_and_zero_limit() {
        for sample in [
            MemorySample { total: 0, available: 0 },
            MemorySample {
                total: 100,
                available: 101,
            },
        ] {
            let budget = ArchiveSnapshotBudget::with_probe(100, move || Ok(sample), |_| 0);
            assert!(budget.try_reserve(1).is_err(), "invalid sample must fail closed");
            assert_eq!(budget.reserved(), 0);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let budget = ArchiveSnapshotBudget::with_probe(
            0,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("unavailable"))
            },
            |_| 0,
        );
        assert!(budget.try_reserve(1).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "a closed budget must not start a sample");
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_refuses_a_failed_nonzero_sample() {
        let budget = ArchiveSnapshotBudget::with_probe(100, || Err(io::Error::other("sample unavailable")), |_| 0);
        assert!(budget.try_reserve(1).is_err(), "a missing sample must not admit memory");
        assert_eq!(budget.reserved(), 0, "failed sampling must not retain capacity");
    }

    #[test]
    fn post_archive_memory_budget_rejects_floor_arithmetic_overflow() {
        let sample = MemorySample {
            total: u64::MAX,
            available: u64::MAX,
        };
        let budget = ArchiveSnapshotBudget::with_probe(u64::MAX, move || Ok(sample), |_| 10);
        assert!(budget.try_reserve(u64::MAX).is_err(), "headroom plus floor must not wrap");
        assert_eq!(budget.reserved(), 0);
        let held = budget.try_reserve(u64::MAX - 10).expect("exact available headroom fits");
        drop(held);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_refuses_exhausted_sampling_generation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let budget = ArchiveSnapshotBudget::with_probe(
            100,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(MemorySample {
                    total: 100,
                    available: 100,
                })
            },
            |_| 0,
        );
        budget.state.lock().generation = u64::MAX - 1;
        let held = budget.try_reserve(1).expect("last valid generation");
        drop(held);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(budget.try_reserve(1).is_err(), "saturated generation must fail closed");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the exhausted generation must not resample");
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_floor_is_bounded() {
        assert_eq!(
            system_memory_floor(MemorySample {
                total: 100,
                available: 100,
            }),
            10
        );
        assert_eq!(
            system_memory_floor(MemorySample {
                total: 20 * 1024 * 1024 * 1024,
                available: 0,
            }),
            MAX_MEMORY_RESERVE_FLOOR
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn post_archive_memory_budget_refuses_an_unsupported_platform_sampler() {
        let budget = ArchiveSnapshotBudget::new(1024);
        assert!(budget.try_reserve(1).is_err(), "missing system sampler must fail closed");
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn post_archive_memory_budget_retries_stale_headroom_after_allocation() {
        let budget = ArchiveSnapshotBudget::with_probe(
            200,
            || {
                Ok(MemorySample {
                    total: 100,
                    available: 100,
                })
            },
            |_| 10,
        );
        let worker_budget = Arc::clone(&budget);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_calls = Arc::clone(&calls);
        let worker = std::thread::spawn(move || {
            worker_budget.try_reserve_with(50, || {
                if worker_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_tx.send(()).expect("announce sample");
                    release_rx.recv_timeout(Duration::from_secs(5)).expect("release sample");
                    Ok(MemorySample {
                        total: 100,
                        available: 100,
                    })
                } else {
                    Ok(MemorySample {
                        total: 100,
                        available: 40,
                    })
                }
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).expect("sampling started");
        let mut other = budget.try_reserve(60).expect("competing allocation fits");
        other.mark_allocated();
        release_tx.send(()).expect("continue first sample");
        assert!(
            worker.join().expect("worker finished").is_err(),
            "stale headroom must not admit another allocation"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the stale sample must be retried");
        assert_eq!(budget.reserved(), 60);
        drop(other);
        assert_eq!(budget.reserved(), 0);
    }
}
