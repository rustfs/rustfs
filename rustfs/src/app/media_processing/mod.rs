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

//! Turns a bucket into an image or video host.
//!
//! After an object is stored a bounded worker pool picks it up, probes what it
//! actually contains, and derives the renditions the bucket's profile asks for —
//! thumbnails, resolution ladders, format conversions. The variants land in the
//! same bucket under a plain prefix, so existing policies, lifecycle rules and
//! presigned URLs apply to them unchanged.
//!
//! Deriving is best-effort. A rendition that cannot be produced is logged and
//! skipped; it never turns a successful client write into a failed one.

pub mod profile;
pub mod worker;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustfs_media_processing::Engine;
use tokio::sync::mpsc::{Receiver, Sender, error::TrySendError};

use crate::app::object::DefaultObjectUsecase;

/// One object waiting to be processed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MediaTask {
    pub bucket: String,
    pub key: String,
    pub size: u64,
}

#[derive(Debug, Default)]
struct Stats {
    completed: AtomicU64,
    failed: AtomicU64,
    /// Tasks dropped because the queue was full or already closed. Reported so
    /// operators can see the pool is undersized instead of wondering why
    /// variants went missing.
    dropped: AtomicU64,
}

/// Shared handle to the processing pool.
#[derive(Clone)]
pub(crate) struct MediaProcessingState {
    sender: Sender<MediaTask>,
    engine: Arc<Engine>,
    stats: Arc<Stats>,
    /// Tasks queued or running, so a repeated write of the same object does not
    /// queue the same derivation twice.
    in_flight: Arc<Mutex<HashSet<MediaTask>>>,
    workers: usize,
    capacity: usize,
}

static MEDIA_PROCESSING: std::sync::OnceLock<MediaProcessingState> = std::sync::OnceLock::new();

impl MediaProcessingState {
    /// Starts the pool once. Repeat calls return the running instance without
    /// spawning a second set of workers.
    fn start(engine: Engine, workers: usize, capacity: usize) -> &'static Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        let state = Self {
            sender,
            engine: Arc::new(engine),
            stats: Arc::new(Stats::default()),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            workers,
            capacity,
        };

        let shared = Arc::new(tokio::sync::Mutex::new(receiver));
        MEDIA_PROCESSING.get_or_init(|| {
            for _ in 0..workers {
                tokio::spawn(worker_loop(state.clone(), Arc::clone(&shared)));
            }
            state
        })
    }

    /// Queues `task` without blocking the request that produced it.
    ///
    /// A full queue means the pool is saturated; the write already succeeded, so
    /// the only honest response is to drop the task and make it visible.
    fn enqueue(&self, task: MediaTask) {
        {
            let mut in_flight = self.in_flight.lock().expect("in_flight lock poisoned");
            if !in_flight.insert(task.clone()) {
                tracing::debug!(
                    event = "media_processing_already_in_flight",
                    component = "rustfs",
                    subsystem = "media_processing",
                    bucket = %task.bucket,
                    key = %task.key,
                    "Skipping a duplicate derivation request"
                );
                return;
            }
        }

        match self.sender.try_send(task) {
            Ok(()) => {}
            Err(TrySendError::Full(task)) => {
                self.forget(&task);
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    event = "media_processing_queue_full",
                    component = "rustfs",
                    subsystem = "media_processing",
                    bucket = %task.bucket,
                    key = %task.key,
                    workers = self.workers,
                    capacity = self.capacity,
                    "Media processing queue is full; dropping the task"
                );
            }
            Err(TrySendError::Closed(task)) => {
                self.forget(&task);
                // Counted like a full queue: the derivation was requested and
                // will not happen, and an operator looking at the counters after
                // a shutdown should be able to see how much work was lost.
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    event = "media_processing_queue_closed",
                    component = "rustfs",
                    subsystem = "media_processing",
                    bucket = %task.bucket,
                    key = %task.key,
                    "Media processing is not running; dropping the task"
                );
            }
        }
    }

    fn forget(&self, task: &MediaTask) {
        self.in_flight.lock().expect("in_flight lock poisoned").remove(task);
    }

    /// Completed, failed and dropped task counts.
    #[cfg(test)]
    fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.completed.load(Ordering::Relaxed),
            self.stats.failed.load(Ordering::Relaxed),
            self.stats.dropped.load(Ordering::Relaxed),
        )
    }
}

async fn worker_loop(state: MediaProcessingState, receiver: Arc<tokio::sync::Mutex<Receiver<MediaTask>>>) {
    // The store is only reachable once the app context is published; before
    // that there is nothing sensible a worker could do with a task.
    let store = DefaultObjectUsecase::from_global().object_store_pub();

    loop {
        let received = {
            let mut receiver = receiver.lock().await;
            tokio::select! {
                received = receiver.recv() => received,
                // A shutdown signal reads as "no more work", which ends the loop.
                _ = tokio::signal::ctrl_c() => None,
            }
        };
        let Some(task) = received else {
            break;
        };

        let Some(store) = store.as_ref() else {
            state.stats.failed.fetch_add(1, Ordering::Relaxed);
            state.forget(&task);
            tracing::warn!(
                event = "media_processing_store_unavailable",
                component = "rustfs",
                subsystem = "media_processing",
                bucket = %task.bucket,
                key = %task.key,
                "Object store is not available; dropping the task"
            );
            continue;
        };

        match worker::process(store, &state.engine, &task).await {
            Ok(()) => {
                state.stats.completed.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                state.stats.failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    event = "media_processing_task_failed",
                    component = "rustfs",
                    subsystem = "media_processing",
                    bucket = %task.bucket,
                    key = %task.key,
                    error = %error,
                    "Media processing task failed"
                );
            }
        }
        state.forget(&task);
    }
}

/// Queues `task` when media processing is running.
///
/// The no-op when the pool was never started is deliberate: a deployment
/// without ffmpeg must behave exactly like one that never had the feature.
pub(crate) fn enqueue_media_processing(bucket: &str, key: &str, size: i64) {
    let Some(state) = MEDIA_PROCESSING.get() else {
        return;
    };
    state.enqueue(MediaTask {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        // A negative size is meaningless; treat it as unknown rather than
        // casting a sentinel into an unsigned length.
        size: u64::try_from(size).unwrap_or(0),
    });
}

/// Whether media processing is running; surfaced so the admin API can report a
/// disabled feature instead of silently doing nothing.
pub(crate) fn is_running() -> bool {
    MEDIA_PROCESSING.get().is_some()
}

/// Build-time configuration for the pool.
pub(crate) struct Options {
    ffmpeg: String,
    ffprobe: String,
    workers: usize,
    capacity: usize,
    timeout: Duration,
}

impl Options {
    /// Reads configuration from the environment, clamped to bounds that keep a
    /// typo from disabling the pool or exhausting the host.
    fn from_env() -> Self {
        let bounded = |name: &str, fallback: usize, min: usize, max: usize| -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(fallback)
                .clamp(min, max)
        };
        Self {
            ffmpeg: std::env::var("RUSTFS_MEDIA_FFMPEG").unwrap_or_else(|_| "ffmpeg".to_owned()),
            ffprobe: std::env::var("RUSTFS_MEDIA_FFPROBE").unwrap_or_else(|_| "ffprobe".to_owned()),
            workers: bounded("RUSTFS_MEDIA_WORKERS", 2, 1, 64),
            capacity: bounded("RUSTFS_MEDIA_QUEUE_CAPACITY", 1024, 1, 1_000_000),
            timeout: Duration::from_secs(bounded("RUSTFS_MEDIA_TIMEOUT_SECS", 3600, 1, 86_400) as u64),
        }
    }
}

/// Builds an engine from the same configuration the pool uses.
///
/// The admin API reports capabilities through this so what it advertises and
/// what actually runs cannot drift apart.
pub(crate) fn engine_from_env() -> Engine {
    let options = Options::from_env();
    Engine::new(options.ffmpeg, options.ffprobe).with_timeout(options.timeout)
}

/// Starts the pool when the toolchain is usable.
///
/// A missing or crippled ffmpeg is not an error: the feature stays off and
/// nothing else in the server is affected.
pub(crate) async fn init_media_processing() {
    let options = Options::from_env();
    let engine = Engine::new(options.ffmpeg.clone(), options.ffprobe.clone()).with_timeout(options.timeout);
    if !engine.is_available().await {
        tracing::warn!(
            event = "media_processing_disabled",
            component = "rustfs",
            subsystem = "media_processing",
            ffmpeg = %options.ffmpeg,
            "ffmpeg could not be started; media processing will not start"
        );
        return;
    }
    if !engine.supports_encoder("libx264").await {
        tracing::warn!(
            event = "media_processing_disabled",
            component = "rustfs",
            subsystem = "media_processing",
            ffmpeg = %options.ffmpeg,
            "libx264 is unavailable; media processing will not start"
        );
        return;
    }

    let state = MediaProcessingState::start(engine, options.workers, options.capacity);
    tracing::info!(
        event = "media_processing_started",
        component = "rustfs",
        subsystem = "media_processing",
        workers = state.workers,
        capacity = state.capacity,
        "Media processing pool is running"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn options_are_read_from_the_environment_and_clamped() {
        let options = Options::from_env();
        assert!(!options.ffmpeg.is_empty());
        assert!(!options.ffprobe.is_empty());
        assert!((1..=64).contains(&options.workers));
        assert!(options.capacity >= 1);
        assert!(options.timeout >= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn enqueueing_without_a_running_pool_is_a_no_op() {
        // A deployment that never started the pool must never panic here.
        enqueue_media_processing("bucket", "key", 1024);
        assert!(!is_running());
    }

    #[tokio::test]
    async fn a_task_is_only_queued_once_while_it_is_in_flight() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(4);
        let state = MediaProcessingState {
            sender,
            engine: Arc::new(Engine::new("ffmpeg", "ffprobe")),
            stats: Arc::new(Stats::default()),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            workers: 1,
            capacity: 4,
        };

        let task = MediaTask {
            bucket: "b".to_owned(),
            key: "k".to_owned(),
            size: 10,
        };
        // The receiver stays alive, so the first send is accepted and the task
        // holds its reservation for as long as it is in flight.
        state.enqueue(task.clone());
        state.enqueue(task);
        assert_eq!(state.in_flight.lock().expect("lock").len(), 1, "a duplicate must not queue twice");
        assert_eq!(state.stats().2, 0, "neither send was dropped");

        // Finishing the task releases the reservation so a later write of the
        // same object can be queued again.
        state.forget(&MediaTask {
            bucket: "b".to_owned(),
            key: "k".to_owned(),
            size: 10,
        });
        assert!(state.in_flight.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn an_undeliverable_task_is_counted_as_dropped_and_released() {
        // With no receiver alive the send fails. The reservation must be
        // released, or a later write of the same object could never be queued.
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        drop(receiver);
        let state = MediaProcessingState {
            sender,
            engine: Arc::new(Engine::new("ffmpeg", "ffprobe")),
            stats: Arc::new(Stats::default()),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            workers: 1,
            capacity: 4,
        };

        state.enqueue(MediaTask {
            bucket: "b".to_owned(),
            key: "other".to_owned(),
            size: 10,
        });
        assert!(state.in_flight.lock().expect("lock").is_empty());
        assert_eq!(state.stats().2, 1, "the undeliverable task must be counted as dropped");
    }
}
