// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded hooks in the concrete Catalog backend. Observations carry no completion authority.

use super::super::{TableCatalogStoreError, TableCatalogStoreResult};
use super::real_catalog_backend::{FaultEvent, FaultPhase};
use sha2::{Digest, Sha256};
use std::{
    num::NonZeroU16,
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

const MAX_RULES: usize = 32;
const MAX_EVENTS: usize = 256;

type Matcher = dyn Fn(&str, &str, &[u8]) -> bool + Send + Sync;
static RULES: LazyLock<Mutex<Vec<Weak<Rule>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) enum CatalogWriteFaultPhase {
    Before,
    AfterNative,
    AfterFence,
    BeforeResponse,
}
impl CatalogWriteFaultPhase {
    fn event_name(self) -> &'static str {
        let native_phase = match self {
            Self::Before => Some(FaultPhase::Before),
            Self::AfterNative => Some(FaultPhase::After),
            Self::AfterFence | Self::BeforeResponse => None,
        };
        match native_phase {
            Some(FaultPhase::Before) => "before",
            Some(FaultPhase::After) => "after",
            None if self == Self::AfterFence => "after-fence",
            None => "before-response",
        }
    }
}
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub(crate) enum CatalogWriteFenceOutcome {
    NotProvided,
    NativeError,
    Held,
    Lost,
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum CatalogWriteFaultAction {
    Fail,
    Pause(Duration),
}
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct CatalogWriteFaultSnapshot {
    pub(crate) matched_writes: u16,
    pub(crate) hits: u16,
    pub(crate) released: bool,
    pub(crate) timed_out: bool,
    pub(crate) closed: bool,
    pub(crate) overflow: bool,
    pub(crate) native_returned: bool,
    pub(crate) response_succeeded: Option<bool>,
    pub(crate) injected_failure: bool,
    pub(crate) native_succeeded: Option<bool>,
    pub(crate) tail_drained_requested: bool,
    pub(crate) final_fence: Option<CatalogWriteFenceOutcome>,
    pub(crate) events: Vec<FaultEvent>,
}
struct Rule {
    instance: usize,
    _instance_owner: Arc<dyn Send + Sync>,
    matcher: Box<Matcher>,
    occurrence: NonZeroU16,
    phase: CatalogWriteFaultPhase,
    action: CatalogWriteFaultAction,
    started: Instant,
    state: Mutex<CatalogWriteFaultSnapshot>,
    released: AtomicBool,
    closed: AtomicBool,
    wake: Notify,
}
pub(crate) struct CatalogWriteFaultHandle {
    rule: Arc<Rule>,
}
impl CatalogWriteFaultHandle {
    pub(crate) fn snapshot(&self) -> CatalogWriteFaultSnapshot {
        self.rule.state.lock().expect("fault state").clone()
    }
    pub(crate) fn release(&self) {
        self.rule.release(false);
    }
}
impl Drop for CatalogWriteFaultHandle {
    fn drop(&mut self) {
        self.rule.release(true);
    }
}
impl Rule {
    fn release(&self, close: bool) {
        if close {
            self.closed.store(true, Ordering::Release);
        }
        self.released.store(true, Ordering::Release);
        let mut state = self.state.lock().expect("fault state");
        state.released = true;
        state.closed |= close;
        drop(state);
        self.wake.notify_waiters();
    }
    fn event(&self, bucket: &str, key: &str, length: usize, sha256: &str, phase: &str) {
        let mut state = self.state.lock().expect("fault state");
        if state.events.len() == MAX_EVENTS {
            state.overflow = true;
            return;
        }
        let sequence = state.events.len();
        state.events.push(FaultEvent {
            sequence,
            elapsed_micros: self.started.elapsed().as_micros(),
            method: "put".into(),
            bucket: bucket.into(),
            object: key.into(),
            phase: phase.into(),
            length,
            sha256: sha256.into(),
        });
    }
}

/// The Catalog owner supplies an exact typed matcher; the driver cannot register raw object keys.
pub(in crate::table_catalog) fn arm(
    instance: usize,
    instance_owner: Arc<dyn Send + Sync>,
    matcher: Box<Matcher>,
    occurrence: NonZeroU16,
    phase: CatalogWriteFaultPhase,
    action: CatalogWriteFaultAction,
) -> TableCatalogStoreResult<CatalogWriteFaultHandle> {
    if occurrence.get() > 1024
        || matches!(action,CatalogWriteFaultAction::Pause(timeout) if timeout.is_zero() || timeout > Duration::from_secs(120))
    {
        return Err(TableCatalogStoreError::Invalid(
            "invalid Catalog fault occurrence or pause timeout".into(),
        ));
    }
    let mut rules = RULES.lock().expect("fault registry");
    rules.retain(|rule| rule.upgrade().is_some_and(|rule| !rule.closed.load(Ordering::Acquire)));
    if rules.len() >= MAX_RULES {
        return Err(TableCatalogStoreError::Unavailable("CapacityExceeded: Catalog fault rules".into()));
    }
    let rule = Arc::new(Rule {
        instance,
        _instance_owner: instance_owner,
        matcher,
        occurrence,
        phase,
        action,
        started: Instant::now(),
        released: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        wake: Notify::new(),
        state: Mutex::new(CatalogWriteFaultSnapshot {
            matched_writes: 0,
            hits: 0,
            released: false,
            timed_out: false,
            closed: false,
            overflow: false,
            native_returned: false,
            response_succeeded: None,
            injected_failure: false,
            native_succeeded: None,
            tail_drained_requested: false,
            final_fence: None,
            events: Vec::new(),
        }),
    });
    rules.push(Arc::downgrade(&rule));
    Ok(CatalogWriteFaultHandle { rule })
}

pub(in crate::table_catalog) struct CatalogWriteObservation {
    selected: Vec<Arc<Rule>>,
    bucket: String,
    key: String,
    length: usize,
    sha256: String,
}
impl CatalogWriteObservation {
    pub(in crate::table_catalog) fn begin(instance: usize, bucket: &str, key: &str, data: &[u8], tail_drained: bool) -> Self {
        // Release the registry before invoking matchers or locking per-rule state.
        let candidates = {
            let registry = RULES.lock().expect("fault registry");
            let snapshot = registry
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|rule| rule.instance == instance && !rule.closed.load(Ordering::Acquire))
                .collect::<Vec<_>>();
            drop(registry);
            snapshot
        };
        let selected = candidates
            .into_iter()
            .filter(|rule| {
                if !(rule.matcher)(bucket, key, data) {
                    return false;
                }
                let mut state = rule.state.lock().expect("fault state");
                let Some(count) = state.matched_writes.checked_add(1) else {
                    state.overflow = true;
                    return false;
                };
                state.matched_writes = count;
                if count != rule.occurrence.get() {
                    return false;
                }
                state.tail_drained_requested = tail_drained;
                true
            })
            .collect::<Vec<_>>();
        let digest = if selected.is_empty() {
            String::new()
        } else {
            hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower)
        };
        Self {
            selected,
            bucket: bucket.into(),
            key: key.into(),
            length: data.len(),
            sha256: digest,
        }
    }
    pub(in crate::table_catalog) fn response_ready(&self, succeeded: bool) {
        for rule in &self.selected {
            rule.state.lock().expect("fault state").response_succeeded = Some(succeeded);
            rule.event(
                &self.bucket,
                &self.key,
                self.length,
                &self.sha256,
                if succeeded { "response-success" } else { "response-error" },
            );
        }
    }
    pub(in crate::table_catalog) fn native_started(&self) {
        for rule in &self.selected {
            rule.event(&self.bucket, &self.key, self.length, &self.sha256, "inner-started");
        }
    }
    pub(in crate::table_catalog) fn native_returned(&self, succeeded: bool) {
        for rule in &self.selected {
            let mut state = rule.state.lock().expect("fault state");
            state.native_returned = true;
            state.native_succeeded = Some(succeeded);
            drop(state);
            rule.event(
                &self.bucket,
                &self.key,
                self.length,
                &self.sha256,
                if succeeded { "inner-completed" } else { "inner-error" },
            );
        }
    }
    pub(in crate::table_catalog) fn fence_checked(&self, outcome: CatalogWriteFenceOutcome) {
        for rule in &self.selected {
            rule.state.lock().expect("fault state").final_fence = Some(outcome);
        }
    }
    pub(in crate::table_catalog) async fn checkpoint(&self, phase: CatalogWriteFaultPhase) -> TableCatalogStoreResult<()> {
        let mut failed = false;
        for rule in &self.selected {
            rule.event(&self.bucket, &self.key, self.length, &self.sha256, phase.event_name());
            if rule.phase != phase || rule.closed.load(Ordering::Acquire) {
                continue;
            }
            rule.state.lock().expect("fault state").hits += 1;
            match rule.action {
                CatalogWriteFaultAction::Fail => {
                    rule.state.lock().expect("fault state").injected_failure = true;
                    failed = true;
                }
                CatalogWriteFaultAction::Pause(timeout) => {
                    let released = async {
                        loop {
                            let notified = rule.wake.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if rule.released.load(Ordering::Acquire) {
                                break;
                            }
                            notified.await;
                        }
                    };
                    if tokio::time::timeout(timeout, released).await.is_err() {
                        let mut state = rule.state.lock().expect("fault state");
                        state.timed_out = true;
                        state.injected_failure = true;
                        failed = true;
                    }
                }
            }
        }
        if failed {
            Err(TableCatalogStoreError::Unavailable("injected Catalog write boundary failure".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concrete_catalog_fault_drop_releases_original_checkpoint() {
        let instance = Arc::new(7u8);
        let identity = Arc::as_ptr(&instance) as usize;
        let handle = arm(
            identity,
            instance.clone(),
            Box::new(|bucket, key, _| bucket == "catalog" && key == "owner-key"),
            NonZeroU16::new(1).unwrap(),
            CatalogWriteFaultPhase::Before,
            CatalogWriteFaultAction::Pause(Duration::from_secs(2)),
        )
        .unwrap();
        let state = handle.rule.clone();
        let observation = CatalogWriteObservation::begin(identity, "catalog", "owner-key", b"actual-bytes", true);
        let release = async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while handle.snapshot().hits == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(handle);
        };
        let (outcome, ()) = tokio::join!(observation.checkpoint(CatalogWriteFaultPhase::Before), release);
        outcome.unwrap();
        let snapshot = state.state.lock().unwrap();
        assert_eq!(snapshot.hits, 1);
        assert!(snapshot.closed && snapshot.released);
        assert!(!snapshot.native_returned);
        assert!(snapshot.final_fence.is_none());
    }

    #[tokio::test]
    async fn concrete_catalog_fault_preserves_instance_and_native_fence_observations() {
        let instance = Arc::new(8u8);
        let identity = Arc::as_ptr(&instance) as usize;
        let handle = arm(
            identity,
            instance.clone(),
            Box::new(|bucket, key, _| bucket == "catalog" && key == "owner-key"),
            NonZeroU16::new(1).unwrap(),
            CatalogWriteFaultPhase::AfterNative,
            CatalogWriteFaultAction::Fail,
        )
        .unwrap();
        let other = CatalogWriteObservation::begin(identity + 1, "catalog", "owner-key", b"bytes", true);
        other.checkpoint(CatalogWriteFaultPhase::AfterNative).await.unwrap();
        assert_eq!(handle.snapshot().hits, 0);
        let own = CatalogWriteObservation::begin(identity, "catalog", "owner-key", b"bytes", true);
        own.checkpoint(CatalogWriteFaultPhase::Before).await.unwrap();
        own.native_started();
        own.native_returned(true);
        assert!(own.checkpoint(CatalogWriteFaultPhase::AfterNative).await.is_err());
        own.fence_checked(CatalogWriteFenceOutcome::Lost);
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.native_succeeded, Some(true));
        assert!(matches!(snapshot.final_fence, Some(CatalogWriteFenceOutcome::Lost)));
        assert_eq!(snapshot.hits, 1);
        for _ in 0..MAX_EVENTS + 1 {
            handle.rule.event("catalog", "owner-key", 5, "digest", "bounded-event");
        }
        assert_eq!(handle.snapshot().events.len(), MAX_EVENTS);
        assert!(handle.snapshot().overflow);
        handle.release();
    }
    #[tokio::test(start_paused = true)]
    async fn concrete_catalog_fault_pause_deadline_records_unknown_native_outcome() {
        let instance = Arc::new(9u8);
        let identity = Arc::as_ptr(&instance) as usize;
        let handle = arm(
            identity,
            instance.clone(),
            Box::new(|bucket, key, _| bucket == "catalog" && key == "owner-key"),
            NonZeroU16::new(1).unwrap(),
            CatalogWriteFaultPhase::Before,
            CatalogWriteFaultAction::Pause(Duration::from_secs(2)),
        )
        .unwrap();
        let observation = CatalogWriteObservation::begin(identity, "catalog", "owner-key", b"bytes", true);
        let result = observation.checkpoint(CatalogWriteFaultPhase::Before).await;
        assert!(result.is_err());
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.hits, 1);
        assert!(snapshot.timed_out && snapshot.injected_failure);
        assert!(!snapshot.native_returned);
        assert!(snapshot.native_succeeded.is_none());
        assert!(snapshot.final_fence.is_none());
    }
}
