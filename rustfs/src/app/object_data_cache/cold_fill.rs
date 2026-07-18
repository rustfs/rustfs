// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

use crate::app::storage_api::object_usecase::error::StorageError;
use bytes::Bytes;
use rustfs_object_data_cache::ObjectDataCacheKey;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const MAX_PRODUCER_ATTEMPTS: u8 = 2;
const MAX_ACTIVE_SESSIONS: usize = 1024;
const MAX_WAITERS_PER_SESSION: usize = 2048;
const MAX_GLOBAL_WAITERS: usize = 8192;
const SESSION_SHARDS: usize = 64;

macro_rules! cold_fill_counter_increment {
    ($name:literal, $value:expr $(, $label_key:literal => $label_value:literal)*) => {
        if rustfs_io_metrics::metrics_enabled() {
            #[cfg(not(test))]
            {
                static HANDLE: std::sync::LazyLock<metrics::Counter> =
                    std::sync::LazyLock::new(|| metrics::counter!($name $(, $label_key => $label_value)*));
                HANDLE.increment($value);
            }
            #[cfg(test)]
            metrics::counter!($name $(, $label_key => $label_value)*).increment($value);
        }
    };
}

macro_rules! cold_fill_gauge_adjust {
    ($name:literal, $method:ident, $value:expr) => {
        #[cfg(not(test))]
        {
            static HANDLE: std::sync::LazyLock<metrics::Gauge> = std::sync::LazyLock::new(|| metrics::gauge!($name));
            HANDLE.$method($value);
        }
        #[cfg(test)]
        metrics::gauge!($name).$method($value);
    };
}

macro_rules! cold_fill_histogram_record {
    ($name:literal, $value:expr) => {
        if rustfs_io_metrics::metrics_enabled() {
            #[cfg(not(test))]
            {
                static HANDLE: std::sync::LazyLock<metrics::Histogram> = std::sync::LazyLock::new(|| metrics::histogram!($name));
                HANDLE.record($value);
            }
            #[cfg(test)]
            metrics::histogram!($name).record($value);
        }
    };
}

#[derive(Clone, Copy)]
pub(crate) enum ColdFillDiskPermitOwner {
    Producer,
    Follower,
}

tokio::task_local! {
    static COLD_FILL_DISK_PERMIT_OWNER: ColdFillDiskPermitOwner;
}

pub(crate) fn current_cold_fill_disk_permit_owner() -> Option<ColdFillDiskPermitOwner> {
    COLD_FILL_DISK_PERMIT_OWNER.try_with(|owner| *owner).ok()
}

#[cfg(test)]
pub(crate) async fn scope_cold_fill_disk_permit_owner_for_test<F: Future>(
    owner: ColdFillDiskPermitOwner,
    future: F,
) -> F::Output {
    COLD_FILL_DISK_PERMIT_OWNER.scope(owner, future).await
}

#[derive(Debug)]
pub(crate) struct ColdFillCoordinator {
    sessions: [Mutex<HashMap<ObjectDataCacheKey, Arc<ColdFillSession>>>; SESSION_SHARDS],
    active_sessions: AtomicUsize,
    global_waiters: AtomicUsize,
    next_session_id: AtomicU64,
}

#[derive(Debug)]
struct ColdFillSession {
    id: u64,
    producer_deadline: Option<tokio::time::Instant>,
    state: Mutex<ColdFillSessionState>,
    changed: Notify,
    cancelled: CancellationToken,
}

#[derive(Debug)]
struct ColdFillSessionState {
    producer_active: bool,
    reader_started: bool,
    attempts: u8,
    waiters: usize,
    consumers: usize,
    result: Option<ColdFillResult>,
    completed_at: Option<Instant>,
}

#[derive(Debug, Clone)]
enum ColdFillResult {
    Ready(Result<Bytes, ColdFillError>),
    Bypass,
}

pub(crate) enum ColdFillRole {
    Produce(ColdFillProducer),
    Wait(ColdFillWaiter),
    Bypass,
    Rejected,
}

pub(crate) enum ColdFillWaitOutcome {
    Produce(ColdFillProducer),
    Ready(Result<Bytes, ColdFillError>),
    Bypass,
    DeadlineExceeded,
}

#[derive(Debug)]
pub(crate) enum ColdFillCoordinateOutcome {
    Ready(Result<Bytes, ColdFillError>),
    Bypass,
    Rejected,
}

#[derive(Debug, Clone)]
pub(crate) enum ColdFillError {
    Storage(StorageError),
    DiskAdmissionClosed,
}

pub(crate) struct ColdFillProducer {
    coordinator: Arc<ColdFillCoordinator>,
    key: ObjectDataCacheKey,
    session: Arc<ColdFillSession>,
    finished: bool,
    consumer_transferred: bool,
}

pub(crate) struct ColdFillWaiter {
    coordinator: Arc<ColdFillCoordinator>,
    key: ObjectDataCacheKey,
    session: Arc<ColdFillSession>,
    global_waiter_counted: bool,
    waiter_metric_recorded: bool,
    consumer_counted: bool,
}

impl ColdFillCoordinator {
    #[cfg(test)]
    pub(crate) fn global_waiter_count_for_test(&self) -> usize {
        self.global_waiters.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn active_session_count_for_test(&self) -> usize {
        self.active_sessions.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn join(self: &Arc<Self>, key: ObjectDataCacheKey) -> ColdFillRole {
        self.join_with_producer_deadline(key, None)
    }

    fn join_with_producer_deadline(
        self: &Arc<Self>,
        key: ObjectDataCacheKey,
        producer_deadline: Option<tokio::time::Instant>,
    ) -> ColdFillRole {
        // Global lock order: a shard's session-map mutex may be followed by a
        // session-state mutex. Code that needs the map after inspecting state
        // must release state first and then call remove_if_current.
        let mut sessions = self.sessions[self.session_shard(&key)]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(session) = sessions.get(&key) {
            let mut state = session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.consumers == 0 || session.cancelled.is_cancelled() {
                drop(state);
                drop(sessions);
                return ColdFillRole::Bypass;
            }
            let Some(consumers) = state.consumers.checked_add(1) else {
                drop(state);
                drop(sessions);
                cold_fill_counter_increment!("rustfs_object_data_cache_cold_fill_waiter_rejected_total", 1);
                return ColdFillRole::Rejected;
            };
            if state.waiters >= MAX_WAITERS_PER_SESSION || !self.reserve_global_waiter() {
                drop(state);
                drop(sessions);
                cold_fill_counter_increment!("rustfs_object_data_cache_cold_fill_waiter_rejected_total", 1);
                return ColdFillRole::Rejected;
            }
            state.consumers = consumers;
            state.waiters += 1;
            let session_waiters = state.waiters;
            drop(state);
            let session = Arc::clone(session);
            drop(sessions);
            let waiter_metric_recorded = self.record_waiter_join(session_waiters);
            return ColdFillRole::Wait(ColdFillWaiter {
                coordinator: Arc::clone(self),
                key,
                session,
                global_waiter_counted: true,
                waiter_metric_recorded,
                consumer_counted: true,
            });
        }

        if self
            .active_sessions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < MAX_ACTIVE_SESSIONS).then_some(current + 1)
            })
            .is_err()
        {
            drop(sessions);
            cold_fill_counter_increment!("rustfs_object_data_cache_cold_fill_session_rejected_total", 1);
            return ColdFillRole::Rejected;
        }

        let session_id = match self
            .next_session_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(1))
        {
            Ok(session_id) => session_id,
            Err(_) => {
                self.active_sessions.fetch_sub(1, Ordering::Relaxed);
                drop(sessions);
                cold_fill_counter_increment!("rustfs_object_data_cache_cold_fill_session_rejected_total", 1);
                return ColdFillRole::Rejected;
            }
        };
        let session = Arc::new(ColdFillSession {
            id: session_id,
            producer_deadline,
            state: Mutex::new(ColdFillSessionState {
                producer_active: true,
                reader_started: false,
                attempts: 1,
                waiters: 0,
                consumers: 1,
                result: None,
                completed_at: None,
            }),
            changed: Notify::new(),
            cancelled: CancellationToken::new(),
        });
        sessions.insert(key.clone(), Arc::clone(&session));
        drop(sessions);
        cold_fill_counter_increment!(
            "rustfs_object_data_cache_cold_fill_producer_started_total",
            1,
            "attempt" => "initial"
        );
        ColdFillRole::Produce(ColdFillProducer {
            coordinator: Arc::clone(self),
            key,
            session,
            finished: false,
            consumer_transferred: false,
        })
    }

    fn remove_if_current(&self, key: &ObjectDataCacheKey, session: &Arc<ColdFillSession>) {
        let mut sessions = self.sessions[self.session_shard(key)]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sessions.get(key).is_some_and(|current| current.id == session.id) {
            sessions.remove(key);
            self.active_sessions.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn session_shard(&self, key: &ObjectDataCacheKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        usize::try_from(hasher.finish()).unwrap_or(usize::MAX) % SESSION_SHARDS
    }

    fn reserve_global_waiter(&self) -> bool {
        let result = self
            .global_waiters
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < MAX_GLOBAL_WAITERS).then_some(current + 1)
            });
        if let Ok(previous) = result {
            let _ = previous;
            true
        } else {
            false
        }
    }

    fn release_global_waiter(&self, waiter_metric_recorded: bool) {
        self.global_waiters.fetch_sub(1, Ordering::Relaxed);
        if waiter_metric_recorded {
            cold_fill_gauge_adjust!("rustfs_object_data_cache_cold_fill_waiters", decrement, 1.0);
        }
    }

    fn record_waiter_join(&self, session_waiters: usize) -> bool {
        let waiter_metric_recorded = rustfs_io_metrics::metrics_enabled();
        if waiter_metric_recorded {
            cold_fill_gauge_adjust!("rustfs_object_data_cache_cold_fill_waiters", increment, 1.0);
        }
        cold_fill_histogram_record!("rustfs_object_data_cache_cold_fill_waiters_per_session", session_waiters as f64);
        waiter_metric_recorded
    }
}

impl Default for ColdFillCoordinator {
    fn default() -> Self {
        Self {
            sessions: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            active_sessions: AtomicUsize::new(0),
            global_waiters: AtomicUsize::new(0),
            next_session_id: AtomicU64::new(1),
        }
    }
}

pub(crate) async fn coordinate_cold_fill<F, Fut>(
    coordinator: &Arc<ColdFillCoordinator>,
    key: ObjectDataCacheKey,
    waiter_deadline: Option<tokio::time::Instant>,
    proposed_producer_deadline: Option<tokio::time::Instant>,
    mut start: F,
) -> ColdFillCoordinateOutcome
where
    F: FnMut(ColdFillProducer) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut role = coordinator.join_with_producer_deadline(key, proposed_producer_deadline);
    loop {
        match role {
            ColdFillRole::Produce(mut producer) => {
                let waiter = producer.waiter();
                tokio::spawn(COLD_FILL_DISK_PERMIT_OWNER.scope(ColdFillDiskPermitOwner::Producer, start(producer)));
                match COLD_FILL_DISK_PERMIT_OWNER
                    .scope(ColdFillDiskPermitOwner::Follower, waiter.wait_until(waiter_deadline))
                    .await
                {
                    ColdFillWaitOutcome::Ready(result) => return ColdFillCoordinateOutcome::Ready(result),
                    ColdFillWaitOutcome::Bypass => return ColdFillCoordinateOutcome::Bypass,
                    ColdFillWaitOutcome::DeadlineExceeded => {
                        return ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)));
                    }
                    ColdFillWaitOutcome::Produce(producer) => role = ColdFillRole::Produce(producer),
                }
            }
            ColdFillRole::Wait(waiter) => match COLD_FILL_DISK_PERMIT_OWNER
                .scope(ColdFillDiskPermitOwner::Follower, waiter.wait_until(waiter_deadline))
                .await
            {
                ColdFillWaitOutcome::Produce(producer) => role = ColdFillRole::Produce(producer),
                ColdFillWaitOutcome::Ready(result) => return ColdFillCoordinateOutcome::Ready(result),
                ColdFillWaitOutcome::Bypass => return ColdFillCoordinateOutcome::Bypass,
                ColdFillWaitOutcome::DeadlineExceeded => {
                    return ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)));
                }
            },
            ColdFillRole::Bypass => return ColdFillCoordinateOutcome::Bypass,
            ColdFillRole::Rejected => return ColdFillCoordinateOutcome::Rejected,
        }
    }
}

impl ColdFillProducer {
    pub(crate) fn waiter(&mut self) -> ColdFillWaiter {
        self.consumer_transferred = true;
        ColdFillWaiter {
            coordinator: Arc::clone(&self.coordinator),
            key: self.key.clone(),
            session: Arc::clone(&self.session),
            global_waiter_counted: false,
            waiter_metric_recorded: false,
            consumer_counted: true,
        }
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.session.cancelled.clone()
    }

    pub(crate) fn deadline(&self) -> Option<tokio::time::Instant> {
        self.session.producer_deadline
    }

    pub(crate) fn mark_reader_started(&self) {
        let mut state = self.session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.reader_started = true;
    }

    pub(crate) fn finish(self, result: Result<Bytes, StorageError>) {
        self.finish_shared(result.map_err(ColdFillError::Storage));
    }

    pub(crate) fn finish_shared(mut self, result: Result<Bytes, ColdFillError>) {
        let release_consumer = !self.consumer_transferred;
        self.consumer_transferred = true;
        let last_consumer;
        {
            let mut state = self.session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if release_consumer {
                state.consumers = state.consumers.saturating_sub(1);
            }
            state.result = Some(ColdFillResult::Ready(result));
            state.completed_at = rustfs_io_metrics::metrics_enabled().then(Instant::now);
            state.producer_active = false;
            last_consumer = state.consumers == 0;
        }
        self.finished = true;
        self.session.changed.notify_waiters();
        self.coordinator.remove_if_current(&self.key, &self.session);
        if last_consumer {
            self.session.cancelled.cancel();
        }
    }

    pub(crate) fn relinquish_or_finish(mut self, error: ColdFillError) {
        let retryable;
        {
            let mut state = self.session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            retryable = state.consumers > 0 && !state.reader_started && state.attempts < MAX_PRODUCER_ATTEMPTS;
            if retryable {
                state.producer_active = false;
            } else {
                state.result = Some(ColdFillResult::Ready(Err(error)));
                state.completed_at = rustfs_io_metrics::metrics_enabled().then(Instant::now);
                state.producer_active = false;
            }
        }
        self.finished = true;
        self.session.changed.notify_waiters();
        if !retryable {
            self.coordinator.remove_if_current(&self.key, &self.session);
        }
    }

    pub(crate) fn bypass(mut self) {
        let release_consumer = !self.consumer_transferred;
        self.consumer_transferred = true;
        let last_consumer;
        {
            let mut state = self.session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if release_consumer {
                state.consumers = state.consumers.saturating_sub(1);
            }
            state.result = Some(ColdFillResult::Bypass);
            state.completed_at = rustfs_io_metrics::metrics_enabled().then(Instant::now);
            state.producer_active = false;
            last_consumer = state.consumers == 0;
        }
        self.finished = true;
        self.session.changed.notify_waiters();
        self.coordinator.remove_if_current(&self.key, &self.session);
        if last_consumer {
            self.session.cancelled.cancel();
        }
    }
}

impl Drop for ColdFillProducer {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let release_consumer = !self.consumer_transferred;
        self.consumer_transferred = true;
        let (retryable, last_consumer) = {
            let mut state = self.session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if release_consumer {
                state.consumers = state.consumers.saturating_sub(1);
            }
            if !state.reader_started && state.attempts < MAX_PRODUCER_ATTEMPTS && state.consumers > 0 {
                state.producer_active = false;
                (true, state.consumers == 0)
            } else {
                state.result = Some(ColdFillResult::Ready(Err(ColdFillError::Storage(StorageError::OperationCanceled))));
                state.completed_at = rustfs_io_metrics::metrics_enabled().then(Instant::now);
                state.producer_active = false;
                (false, state.consumers == 0)
            }
        };
        if last_consumer {
            self.session.cancelled.cancel();
        }
        self.session.changed.notify_waiters();
        if !retryable || last_consumer {
            self.coordinator.remove_if_current(&self.key, &self.session);
        }
    }
}

impl ColdFillWaiter {
    async fn wait_until(self, deadline: Option<tokio::time::Instant>) -> ColdFillWaitOutcome {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, self.wait())
                .await
                .unwrap_or(ColdFillWaitOutcome::DeadlineExceeded),
            None => self.wait().await,
        }
    }

    pub(crate) async fn wait(mut self) -> ColdFillWaitOutcome {
        loop {
            let session = Arc::clone(&self.session);
            let notified = session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(result) = &state.result {
                    let outcome = match result {
                        ColdFillResult::Ready(result) => ColdFillWaitOutcome::Ready(result.clone()),
                        ColdFillResult::Bypass => ColdFillWaitOutcome::Bypass,
                    };
                    let (release_global, release_waiter_metric) = self.release_waiter_slot(&mut state, true);
                    let last_consumer = state.consumers == 0;
                    let producer_active = state.producer_active;
                    let completed_at = state.completed_at;
                    drop(state);
                    self.finish_release(release_global, release_waiter_metric, last_consumer, producer_active);
                    if let Some(completed_at) = completed_at {
                        cold_fill_histogram_record!(
                            "rustfs_object_data_cache_cold_fill_completion_handoff_seconds",
                            completed_at.elapsed().as_secs_f64()
                        );
                    }
                    return outcome;
                }
                if !state.producer_active && !state.reader_started && state.attempts < MAX_PRODUCER_ATTEMPTS {
                    state.producer_active = true;
                    state.attempts += 1;
                    let (release_global, release_waiter_metric) = self.release_waiter_slot(&mut state, false);
                    self.consumer_counted = false;
                    drop(state);
                    if release_global {
                        self.coordinator.release_global_waiter(release_waiter_metric);
                    }
                    cold_fill_counter_increment!(
                        "rustfs_object_data_cache_cold_fill_producer_started_total",
                        1,
                        "attempt" => "successor"
                    );
                    return ColdFillWaitOutcome::Produce(ColdFillProducer {
                        coordinator: Arc::clone(&self.coordinator),
                        key: self.key.clone(),
                        session: Arc::clone(&self.session),
                        finished: false,
                        consumer_transferred: false,
                    });
                }
            }
            notified.await;
        }
    }

    fn release_waiter_slot(&mut self, state: &mut ColdFillSessionState, release_consumer: bool) -> (bool, bool) {
        let release_global = self.global_waiter_counted;
        let release_waiter_metric = release_global && self.waiter_metric_recorded;
        if self.global_waiter_counted {
            state.waiters = state.waiters.saturating_sub(1);
            self.global_waiter_counted = false;
            self.waiter_metric_recorded = false;
        }
        if release_consumer && self.consumer_counted {
            state.consumers = state.consumers.saturating_sub(1);
            self.consumer_counted = false;
        }
        (release_global, release_waiter_metric)
    }

    fn finish_release(&self, release_global: bool, release_waiter_metric: bool, last_consumer: bool, producer_active: bool) {
        if release_global {
            self.coordinator.release_global_waiter(release_waiter_metric);
        }
        if last_consumer {
            self.session.cancelled.cancel();
            if !producer_active {
                self.coordinator.remove_if_current(&self.key, &self.session);
            }
        }
    }
}

impl Drop for ColdFillWaiter {
    fn drop(&mut self) {
        if !self.global_waiter_counted && !self.consumer_counted {
            return;
        }
        let session = Arc::clone(&self.session);
        let mut state = session.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (release_global, release_waiter_metric) = self.release_waiter_slot(&mut state, true);
        let last_consumer = state.consumers == 0;
        let producer_active = state.producer_active;
        drop(state);
        self.finish_release(release_global, release_waiter_metric, last_consumer, producer_active);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_object_data_cache::ObjectDataCacheBodyVariant;

    fn key() -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", None, "etag", 4, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    fn indexed_key(index: usize) -> ObjectDataCacheKey {
        ObjectDataCacheKey::new(
            "bucket",
            format!("object-{index}"),
            None,
            "etag",
            4,
            ObjectDataCacheBodyVariant::FullObjectPlainV1,
        )
    }

    #[test]
    #[serial_test::serial(cold_fill_metrics_gate)]
    fn cold_fill_metrics_follow_runtime_gate_and_release_waiter_gauge() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        async fn start_session(
            coordinator: Arc<ColdFillCoordinator>,
            key: ObjectDataCacheKey,
        ) -> (ColdFillProducer, ColdFillWaiter) {
            let ColdFillRole::Produce(producer) = coordinator.join(key.clone()) else {
                panic!("first request must own the producer role");
            };
            let ColdFillRole::Wait(waiter) = coordinator.join(key) else {
                panic!("second request must join as a waiter");
            };
            (producer, waiter)
        }

        let metrics_was_enabled = rustfs_io_metrics::metrics_enabled();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("metric test runtime must build");

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                rustfs_io_metrics::set_metrics_enabled(false);
                let (producer, waiter) = start_session(Arc::new(ColdFillCoordinator::default()), indexed_key(1)).await;
                rustfs_io_metrics::set_metrics_enabled(true);
                producer.finish(Ok(Bytes::from_static(b"body")));
                assert!(matches!(waiter.wait().await, ColdFillWaitOutcome::Ready(Ok(_))));
            });
        });
        assert!(
            snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .all(|(composite, _, _, _)| { composite.key().name() != "rustfs_object_data_cache_cold_fill_waiters" }),
            "a waiter admitted while metrics were disabled must not record an unmatched decrement"
        );

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                rustfs_io_metrics::set_metrics_enabled(true);
                let (producer, waiter) = start_session(Arc::new(ColdFillCoordinator::default()), indexed_key(2)).await;
                rustfs_io_metrics::set_metrics_enabled(false);
                producer.finish(Ok(Bytes::from_static(b"body")));
                assert!(matches!(waiter.wait().await, ColdFillWaitOutcome::Ready(Ok(_))));

                rustfs_io_metrics::set_metrics_enabled(true);
                let (producer, waiter) = start_session(Arc::new(ColdFillCoordinator::default()), indexed_key(3)).await;
                producer.finish(Ok(Bytes::from_static(b"body")));
                assert!(matches!(waiter.wait().await, ColdFillWaitOutcome::Ready(Ok(_))));
            });
        });
        let snapshot = snapshotter.snapshot().into_vec();
        let waiter_gauge = snapshot
            .iter()
            .find_map(|(composite, _unit, _description, value)| {
                (composite.key().name() == "rustfs_object_data_cache_cold_fill_waiters").then_some(value)
            })
            .expect("enabled metrics must record the waiter gauge");
        let DebugValue::Gauge(waiter_gauge) = waiter_gauge else {
            panic!("waiter metric must be a gauge");
        };
        assert_eq!(waiter_gauge.into_inner(), 0.0);
        for name in [
            "rustfs_object_data_cache_cold_fill_waiters_per_session",
            "rustfs_object_data_cache_cold_fill_completion_handoff_seconds",
        ] {
            assert!(
                snapshot.iter().any(|(composite, _, _, _)| composite.key().name() == name),
                "enabled metrics must record {name}"
            );
        }
        rustfs_io_metrics::set_metrics_enabled(metrics_was_enabled);
    }

    #[tokio::test]
    async fn cold_fill_waiter_observes_shared_result() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first join must produce");
        };
        let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
            panic!("second join must wait");
        };
        producer.finish(Ok(Bytes::from_static(b"body")));
        let ColdFillWaitOutcome::Ready(result) = waiter.wait().await else {
            panic!("completed session must be ready");
        };
        assert_eq!(result.expect("shared cold fill should succeed"), Bytes::from_static(b"body"));
    }

    #[tokio::test]
    async fn cold_fill_only_promotes_before_reader_start() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first join must produce");
        };
        let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
            panic!("second join must wait");
        };
        drop(producer);
        let promoted = match waiter.wait().await {
            ColdFillWaitOutcome::Produce(producer) => producer,
            ColdFillWaitOutcome::Ready(_) | ColdFillWaitOutcome::Bypass | ColdFillWaitOutcome::DeadlineExceeded => {
                panic!("second attempt must promote")
            }
        };
        let ColdFillRole::Wait(final_waiter) = coordinator.join(key()) else {
            panic!("third join must wait for promoted producer");
        };
        promoted.mark_reader_started();
        drop(promoted);
        let ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::OperationCanceled))) = final_waiter.wait().await
        else {
            panic!("reader-started producer cancellation must be terminal");
        };
        let ColdFillRole::Produce(_) = coordinator.join(key()) else {
            panic!("terminal failed session must be removed");
        };
    }

    #[tokio::test]
    async fn cold_fill_bypass_wakes_waiters() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first join must produce");
        };
        let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
            panic!("second join must wait");
        };
        producer.bypass();
        assert!(matches!(waiter.wait().await, ColdFillWaitOutcome::Bypass));
    }

    #[tokio::test]
    async fn one_follower_cancel_does_not_cancel_producer_or_other_followers() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(mut producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let leader = producer.waiter();
        let ColdFillRole::Wait(cancelled_follower) = coordinator.join(key()) else {
            panic!("second request must follow");
        };
        let ColdFillRole::Wait(surviving_follower) = coordinator.join(key()) else {
            panic!("third request must follow");
        };

        drop(cancelled_follower);
        assert!(!producer.cancellation_token().is_cancelled());
        producer.finish(Ok(Bytes::from_static(b"body")));

        for waiter in [leader, surviving_follower] {
            let ColdFillWaitOutcome::Ready(Ok(body)) = waiter.wait().await else {
                panic!("remaining consumers must receive the shared result");
            };
            assert_eq!(body, Bytes::from_static(b"body"));
        }
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn aborted_pre_reader_producer_promotes_only_one_successor() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let ColdFillRole::Wait(first_follower) = coordinator.join(key()) else {
            panic!("second request must follow");
        };
        let ColdFillRole::Wait(second_follower) = coordinator.join(key()) else {
            panic!("third request must follow");
        };
        let task = tokio::spawn(async move {
            let _producer = producer;
            std::future::pending::<()>().await;
        });
        task.abort();
        let _ = task.await;

        let ColdFillWaitOutcome::Produce(successor) = first_follower.wait().await else {
            panic!("exactly one follower must become the successor");
        };
        let second_task = tokio::spawn(async move { second_follower.wait().await });
        tokio::task::yield_now().await;
        assert!(!second_task.is_finished(), "the other follower must wait for the unique successor");
        successor.finish(Ok(Bytes::from_static(b"body")));
        let ColdFillWaitOutcome::Ready(Ok(body)) = second_task.await.expect("second follower task must join") else {
            panic!("the waiting follower must receive the successor result");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn sole_leader_waiter_promotes_after_pre_reader_producer_abort_or_panic() {
        for panic_producer in [false, true] {
            let coordinator = Arc::new(ColdFillCoordinator::default());
            let ColdFillRole::Produce(mut producer) = coordinator.join(key()) else {
                panic!("first request must produce");
            };
            let leader = producer.waiter();
            let producer_task = tokio::spawn(async move {
                if panic_producer {
                    panic!("simulated pre-reader producer panic");
                }
                let _producer = producer;
                std::future::pending::<()>().await;
            });
            if !panic_producer {
                producer_task.abort();
            }
            assert!(producer_task.await.is_err(), "producer task must abort or panic");

            let ColdFillWaitOutcome::Produce(successor) = leader.wait().await else {
                panic!("the sole leader waiter must become the successor");
            };
            successor.finish(Ok(Bytes::from_static(b"body")));
            assert_eq!(coordinator.active_session_count_for_test(), 0);
        }
    }

    #[tokio::test]
    async fn aborted_post_reader_producer_is_terminal() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let ColdFillRole::Wait(follower) = coordinator.join(key()) else {
            panic!("second request must follow");
        };
        let task = tokio::spawn(async move {
            producer.mark_reader_started();
            let _producer = producer;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        assert!(matches!(
            follower.wait().await,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::OperationCanceled)))
        ));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[test]
    fn cold_fill_waiter_limit_rejects_without_starting_another_producer() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(_producer) = coordinator.join(key()) else {
            panic!("first join must produce");
        };
        let mut waiters = Vec::with_capacity(MAX_WAITERS_PER_SESSION);
        for _ in 0..MAX_WAITERS_PER_SESSION {
            let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
                panic!("waiter below the cap must join");
            };
            waiters.push(waiter);
        }
        assert!(matches!(coordinator.join(key()), ColdFillRole::Rejected));
        drop(waiters.pop());
        assert!(matches!(coordinator.join(key()), ColdFillRole::Wait(_)));
    }

    #[test]
    fn canceled_unobserved_producers_release_session_capacity() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        for index in 0..MAX_ACTIVE_SESSIONS {
            let ColdFillRole::Produce(producer) = coordinator.join(indexed_key(index)) else {
                panic!("unobserved key must initially produce");
            };
            drop(producer);
        }
        assert_eq!(coordinator.active_sessions.load(Ordering::Relaxed), 0);
        assert!(matches!(coordinator.join(indexed_key(MAX_ACTIVE_SESSIONS)), ColdFillRole::Produce(_)));
    }

    #[test]
    fn last_consumer_cancels_and_late_request_bypasses_until_producer_ack() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(mut producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let leader = producer.waiter();
        drop(leader);

        assert!(producer.cancellation_token().is_cancelled());
        assert!(matches!(coordinator.join(key()), ColdFillRole::Bypass));
        drop(producer);

        assert_eq!(coordinator.active_sessions.load(Ordering::Relaxed), 0);
        assert!(matches!(coordinator.join(key()), ColdFillRole::Produce(_)));
    }

    #[test]
    fn last_consumer_then_producer_relinquish_removes_abandoned_session() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(mut producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        drop(producer.waiter());

        producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));

        assert_eq!(coordinator.active_session_count_for_test(), 0);
        assert!(matches!(coordinator.join(key()), ColdFillRole::Produce(_)));
    }

    #[test]
    fn producer_and_last_waiter_drop_remove_retryable_session() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
            panic!("second request must wait");
        };

        drop(producer);
        drop(waiter);

        assert_eq!(coordinator.active_sessions.load(Ordering::Relaxed), 0);
        assert!(matches!(coordinator.join(key()), ColdFillRole::Produce(_)));
    }

    #[test]
    fn session_ids_are_monotonic_and_overflow_fails_closed() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(first) = coordinator.join(indexed_key(1)) else {
            panic!("first session must produce");
        };
        let first_id = first.session.id;
        drop(first);
        let ColdFillRole::Produce(second) = coordinator.join(indexed_key(2)) else {
            panic!("second session must produce");
        };
        assert!(second.session.id > first_id);
        drop(second);

        coordinator.next_session_id.store(u64::MAX, Ordering::Relaxed);
        assert!(matches!(coordinator.join(indexed_key(3)), ColdFillRole::Rejected));
        assert!(matches!(coordinator.join(indexed_key(4)), ColdFillRole::Rejected));
        assert_eq!(coordinator.active_sessions.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn late_old_session_remove_cannot_delete_new_same_key_session() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let session_key = key();
        let ColdFillRole::Produce(old_producer) = coordinator.join(session_key.clone()) else {
            panic!("old session must produce");
        };
        let old_session = Arc::clone(&old_producer.session);
        old_producer.bypass();

        let ColdFillRole::Produce(new_producer) = coordinator.join(session_key.clone()) else {
            panic!("new session must produce after old completion");
        };
        assert_ne!(old_session.id, new_producer.session.id);
        coordinator.remove_if_current(&session_key, &old_session);

        assert_eq!(coordinator.active_session_count_for_test(), 1);
        assert!(matches!(coordinator.join(session_key), ColdFillRole::Wait(_)));
        drop(new_producer);
    }

    #[tokio::test]
    async fn second_pre_reader_failure_is_terminal_and_removes_session() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let attempts = Arc::new(AtomicUsize::new(0));
        let outcome = coordinate_cold_fill(&coordinator, key(), None, None, {
            let attempts = Arc::clone(&attempts);
            move |producer| {
                let attempts = Arc::clone(&attempts);
                async move {
                    let error = ColdFillError::Storage(StorageError::other("pre-reader failure"));
                    attempts.fetch_add(1, Ordering::Relaxed);
                    producer.relinquish_or_finish(error);
                }
            }
        })
        .await;

        assert!(matches!(
            outcome,
            ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Io(_))))
        ));
        assert_eq!(attempts.load(Ordering::Relaxed), usize::from(MAX_PRODUCER_ATTEMPTS));
        assert_eq!(coordinator.active_sessions.load(Ordering::Relaxed), 0);
        assert!(matches!(coordinator.join(key()), ColdFillRole::Produce(_)));
    }

    #[tokio::test]
    async fn cold_fill_accepts_two_thousand_requests_with_one_producer() {
        const REQUESTS: usize = 2000;
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let ColdFillRole::Produce(producer) = coordinator.join(key()) else {
            panic!("first request must produce");
        };
        let mut waiters = Vec::with_capacity(REQUESTS - 1);
        for _ in 1..REQUESTS {
            let ColdFillRole::Wait(waiter) = coordinator.join(key()) else {
                panic!("every follower in the acceptance matrix must wait");
            };
            waiters.push(waiter);
        }
        assert_eq!(coordinator.global_waiters.load(Ordering::Relaxed), REQUESTS - 1);

        producer.finish(Ok(Bytes::from_static(b"body")));
        for waiter in waiters {
            let ColdFillWaitOutcome::Ready(Ok(body)) = waiter.wait().await else {
                panic!("every follower must receive the shared body");
            };
            assert_eq!(body, Bytes::from_static(b"body"));
        }
        assert_eq!(coordinator.global_waiters.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn leader_waiter_deadline_does_not_cancel_longer_lived_follower() {
        let coordinator = Arc::new(ColdFillCoordinator::default());
        let producer_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let producer_starts = Arc::new(AtomicUsize::new(0));
        let now = tokio::time::Instant::now();
        let producer_deadline = now + std::time::Duration::from_secs(1);

        let leader = {
            let coordinator = Arc::clone(&coordinator);
            let producer_gate = Arc::clone(&producer_gate);
            let producer_starts = Arc::clone(&producer_starts);
            tokio::spawn(async move {
                coordinate_cold_fill(
                    &coordinator,
                    key(),
                    Some(now + std::time::Duration::from_millis(20)),
                    Some(producer_deadline),
                    move |producer| {
                        let producer_gate = Arc::clone(&producer_gate);
                        let producer_starts = Arc::clone(&producer_starts);
                        async move {
                            assert!(matches!(current_cold_fill_disk_permit_owner(), Some(ColdFillDiskPermitOwner::Producer)));
                            producer_starts.fetch_add(1, Ordering::Relaxed);
                            assert_eq!(producer.deadline(), Some(producer_deadline));
                            let permit = producer_gate.acquire().await.expect("producer barrier must remain open");
                            permit.forget();
                            producer.finish(Ok(Bytes::from_static(b"body")));
                        }
                    },
                )
                .await
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while coordinator.active_session_count_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader must publish its session");

        let follower = {
            let coordinator = Arc::clone(&coordinator);
            let producer_starts = Arc::clone(&producer_starts);
            tokio::spawn(async move {
                coordinate_cold_fill(
                    &coordinator,
                    key(),
                    Some(now + std::time::Duration::from_secs(1)),
                    Some(now + std::time::Duration::from_secs(2)),
                    move |producer| {
                        let producer_starts = Arc::clone(&producer_starts);
                        async move {
                            producer_starts.fetch_add(1, Ordering::Relaxed);
                            producer.finish(Ok(Bytes::from_static(b"unexpected")));
                        }
                    },
                )
                .await
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while coordinator.global_waiter_count_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower must join before the leader deadline");

        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        assert!(matches!(
            leader.await.expect("leader task must complete"),
            ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));

        producer_gate.add_permits(1);
        let ColdFillCoordinateOutcome::Ready(Ok(body)) = follower.await.expect("follower task must complete") else {
            panic!("follower must receive the shared producer body");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(producer_starts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cold_fill_production_seam_runs_permit_reader_and_allocation_once_per_key() {
        const REQUESTS: usize = 2000;
        for key_count in [1_usize, 4, 32] {
            let coordinator = Arc::new(ColdFillCoordinator::default());
            let producer_gate = Arc::new(tokio::sync::Semaphore::new(0));
            let permits = Arc::new(AtomicUsize::new(0));
            let readers = Arc::new(AtomicUsize::new(0));
            let allocations = Arc::new(AtomicUsize::new(0));
            let mut tasks = tokio::task::JoinSet::new();

            for request in 0..REQUESTS {
                let coordinator = Arc::clone(&coordinator);
                let producer_gate = Arc::clone(&producer_gate);
                let permits = Arc::clone(&permits);
                let readers = Arc::clone(&readers);
                let allocations = Arc::clone(&allocations);
                tasks.spawn(async move {
                    let outcome =
                        coordinate_cold_fill(&coordinator, indexed_key(request % key_count), None, None, move |producer| {
                            let producer_gate = Arc::clone(&producer_gate);
                            let permits = Arc::clone(&permits);
                            let readers = Arc::clone(&readers);
                            let allocations = Arc::clone(&allocations);
                            async move {
                                permits.fetch_add(1, Ordering::Relaxed);
                                readers.fetch_add(1, Ordering::Relaxed);
                                allocations.fetch_add(1, Ordering::Relaxed);
                                let permit = producer_gate.acquire().await.expect("test producer gate must stay open");
                                permit.forget();
                                producer.finish(Ok(Bytes::from_static(b"body")));
                            }
                        })
                        .await;
                    assert!(matches!(outcome, ColdFillCoordinateOutcome::Ready(Ok(_))));
                });
            }

            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while permits.load(Ordering::Relaxed) < key_count
                    || coordinator.global_waiters.load(Ordering::Relaxed) < REQUESTS - key_count
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all matrix requests must join before the watchdog expires");
            producer_gate.add_permits(key_count);
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while let Some(result) = tasks.join_next().await {
                    result.expect("cold-fill matrix request must complete");
                }
            })
            .await
            .expect("all matrix responses must complete before the watchdog expires");

            assert_eq!(permits.load(Ordering::Relaxed), key_count);
            assert_eq!(readers.load(Ordering::Relaxed), key_count);
            assert_eq!(allocations.load(Ordering::Relaxed), key_count);
        }
    }
}
