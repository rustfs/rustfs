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

//! Builder-style [`Target`] mock shared by this crate's unit tests and, behind the off-by-default
//! `test-support` cargo feature, by downstream test suites.
//!
//! The module is compiled only under `cfg(test)` or when a dependent explicitly opts in via the
//! `test-support` feature, so the mock can never reach a production binary. Every knob defaults
//! off: a plain [`MockTarget::new`] is an enabled, reachable, storeless target whose delivery
//! methods all succeed immediately. The mock emits no tracing events.

use crate::arn::TargetID;
use crate::plugin::PluginEvent;
use crate::store::{FailedEventStore, Key, Store};
use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
use crate::{StoreError, Target, TargetError};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

/// The queued-payload store handle a [`MockTarget`] serves from its [`Target::store`] accessor.
pub type SharedQueuedStore = Arc<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>;

/// Increments its counter when dropped, however the owning future ends, so a test that holds the
/// probe open past its caller's deadline can prove the cancelled health future was actually
/// dropped instead of left running.
struct HealthDropGuard(Arc<AtomicUsize>);

impl Drop for HealthDropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Consumes one unit of a failure budget, returning true while the budget is not exhausted.
/// A budget of `usize::MAX` behaves as "always fail" for any realistic call count.
fn consume_failure_budget(budget: &AtomicUsize) -> bool {
    budget
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
        .is_ok()
}

/// A configurable mock implementation of [`Target`].
///
/// All observable state (call counters, gates, signals) lives behind shared handles, so a clone
/// kept aside keeps observing the original after it is boxed into a runtime, and
/// [`Target::clone_dyn`] clones observe the same counters. Builder methods consume `self` and
/// each knob defaults off; accessors read the shared state from any clone.
#[derive(Clone)]
pub struct MockTarget {
    id: TargetID,
    enabled: bool,
    /// Overrides the [`Target::is_active`] result; defaults to the enabled flag.
    active: Option<bool>,
    health_delay: Duration,
    health_started: Arc<Notify>,
    health_drops: Arc<AtomicUsize>,
    init_calls: Arc<AtomicUsize>,
    init_failures_remaining: Arc<AtomicUsize>,
    /// When set, `init` notifies the handle on entry and then never returns.
    blocking_init: Option<Arc<Notify>>,
    close_calls: Arc<AtomicUsize>,
    close_started: Arc<Notify>,
    block_on_close: Arc<AtomicBool>,
    close_gate: Arc<Semaphore>,
    save_calls: Arc<AtomicUsize>,
    save_failures_remaining: Arc<AtomicUsize>,
    final_failures: Arc<AtomicU64>,
    store: Option<SharedQueuedStore>,
    failed_store: Option<Arc<dyn FailedEventStore>>,
}

impl MockTarget {
    /// Creates an enabled, reachable, storeless mock identified as `<id>:<name>`.
    pub fn new(id: &str, name: &str) -> Self {
        Self {
            id: TargetID::new(id.to_string(), name.to_string()),
            enabled: true,
            active: None,
            health_delay: Duration::ZERO,
            health_started: Arc::new(Notify::new()),
            health_drops: Arc::new(AtomicUsize::new(0)),
            init_calls: Arc::new(AtomicUsize::new(0)),
            init_failures_remaining: Arc::new(AtomicUsize::new(0)),
            blocking_init: None,
            close_calls: Arc::new(AtomicUsize::new(0)),
            close_started: Arc::new(Notify::new()),
            block_on_close: Arc::new(AtomicBool::new(false)),
            close_gate: Arc::new(Semaphore::new(0)),
            save_calls: Arc::new(AtomicUsize::new(0)),
            save_failures_remaining: Arc::new(AtomicUsize::new(0)),
            final_failures: Arc::new(AtomicU64::new(0)),
            store: None,
            failed_store: None,
        }
    }

    /// Marks the target disabled: `is_enabled` returns false and `health` short-circuits.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Overrides the `is_active` result independently of the enabled flag.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = Some(active);
        self
    }

    /// Makes `is_active` sleep for `delay` (after notifying [`Self::health_started`]) before
    /// answering, so probe timeouts and cancellation can be exercised under a paused clock.
    pub fn with_health_delay(mut self, delay: Duration) -> Self {
        self.health_delay = delay;
        self
    }

    /// Fails the first `failures` `init` calls with [`TargetError::Initialization`], then
    /// succeeds. Pass `usize::MAX` for a target whose init always fails.
    pub fn with_init_failures(mut self, failures: usize) -> Self {
        self.init_failures_remaining = Arc::new(AtomicUsize::new(failures));
        self
    }

    /// Makes `init` notify `entered` and then never return, so cancellation of an in-flight
    /// initialization can be exercised.
    pub fn with_blocking_init(mut self, entered: Arc<Notify>) -> Self {
        self.blocking_init = Some(entered);
        self
    }

    /// Fails the first `failures` `save` calls with [`TargetError::Request`], then succeeds.
    pub fn with_save_failures(mut self, failures: usize) -> Self {
        self.save_failures_remaining = Arc::new(AtomicUsize::new(failures));
        self
    }

    /// Serves `store` from the [`Target::store`] accessor. The caller owns the backing store and
    /// its directory lifecycle; the mock only hands out the reference.
    pub fn with_store(mut self, store: SharedQueuedStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Serves `failed_store` from the [`Target::failed_store`] accessor. Pass the same underlying
    /// store as [`Self::with_store`] to model a target whose live queue also parks terminal
    /// failures.
    pub fn with_failed_store(mut self, failed_store: Arc<dyn FailedEventStore>) -> Self {
        self.failed_store = Some(failed_store);
        self
    }

    /// Returns the mock's identity without needing an event-type annotation.
    pub fn target_id(&self) -> TargetID {
        self.id.clone()
    }

    /// When `block` is set, `close` waits on [`Self::close_gate`] after counting and signalling,
    /// until the gate receives a permit. Takes effect for closes that start after the call.
    pub fn set_block_on_close(&self, block: bool) {
        self.block_on_close.store(block, Ordering::SeqCst);
    }

    /// The semaphore a blocked `close` waits on; add a permit to release it.
    pub fn close_gate(&self) -> Arc<Semaphore> {
        Arc::clone(&self.close_gate)
    }

    /// Notified once every time a `close` call starts.
    pub fn close_started(&self) -> Arc<Notify> {
        Arc::clone(&self.close_started)
    }

    /// Notified once every time an `is_active` probe starts.
    pub fn health_started(&self) -> Arc<Notify> {
        Arc::clone(&self.health_started)
    }

    /// How many times `init` was called across all clones.
    pub fn init_call_count(&self) -> usize {
        self.init_calls.load(Ordering::SeqCst)
    }

    /// How many times `close` was called across all clones.
    pub fn close_call_count(&self) -> usize {
        self.close_calls.load(Ordering::SeqCst)
    }

    /// How many times `save` was called across all clones.
    pub fn save_call_count(&self) -> usize {
        self.save_calls.load(Ordering::SeqCst)
    }

    /// How many `is_active` futures finished or were dropped mid-flight across all clones.
    pub fn health_drop_count(&self) -> usize {
        self.health_drops.load(Ordering::SeqCst)
    }

    /// How many final delivery failures were recorded across all clones.
    pub fn final_failure_count(&self) -> u64 {
        self.final_failures.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl<E> Target<E> for MockTarget
where
    E: PluginEvent,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        self.health_started.notify_one();
        // Held across the delay so an aborted probe future is observable via health_drop_count.
        let _drop_guard = HealthDropGuard(Arc::clone(&self.health_drops));
        tokio::time::sleep(self.health_delay).await;
        Ok(self.active.unwrap_or(self.enabled))
    }

    async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        self.save_calls.fetch_add(1, Ordering::SeqCst);
        if consume_failure_budget(&self.save_failures_remaining) {
            return Err(TargetError::Request("forced save failure".to_string()));
        }
        Ok(())
    }

    async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        self.close_started.notify_one();
        if self.block_on_close.load(Ordering::SeqCst) {
            let _permit = self.close_gate.acquire().await.expect("close gate should remain open");
        }
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn failed_store(&self) -> Option<&dyn FailedEventStore> {
        self.failed_store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(self.clone())
    }

    async fn init(&self) -> Result<(), TargetError> {
        self.init_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = &self.blocking_init {
            entered.notify_one();
            return std::future::pending().await;
        }
        if consume_failure_budget(&self.init_failures_remaining) {
            return Err(TargetError::Initialization("forced init failure".to_string()));
        }
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn record_final_failure(&self) {
        self.final_failures.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::MockTarget;
    use crate::Target;
    use crate::target::EntityTarget;
    use rustfs_s3_types::EventName;
    use std::sync::Arc;

    fn sample_event() -> Arc<EntityTarget<String>> {
        Arc::new(EntityTarget {
            object_name: "obj.txt".to_string(),
            bucket_name: "bucket-a".to_string(),
            event_name: EventName::ObjectCreatedPut,
            data: "payload".to_string(),
        })
    }

    // Leak-guard contract, part one: the mock is compiled only under cfg(test) or the
    // off-by-default `test-support` feature (see lib.rs), so production builds cannot even name
    // it. This test documents the default surface a consumer gets when it does opt in.
    #[tokio::test]
    async fn defaults_are_inert() {
        let target = MockTarget::new("primary", "webhook");
        let handle: &dyn Target<String> = &target;

        assert_eq!(handle.id().to_string(), "primary:webhook");
        assert!(handle.is_enabled());
        assert!(handle.is_active().await.expect("the default probe succeeds"));
        handle.init().await.expect("the default init succeeds");
        handle.save(sample_event()).await.expect("the default save succeeds");
        handle.close().await.expect("the default close succeeds");
        assert!(handle.store().is_none());
        assert!(handle.failed_store().is_none());

        assert_eq!(target.init_call_count(), 1);
        assert_eq!(target.save_call_count(), 1);
        assert_eq!(target.close_call_count(), 1);
        assert_eq!(target.final_failure_count(), 0);
    }

    #[tokio::test]
    async fn clones_and_clone_dyn_share_the_same_counters() {
        let target = MockTarget::new("primary", "webhook");
        let observer = target.clone();
        let boxed: Box<dyn Target<String> + Send + Sync> = Box::new(target);
        let second = boxed.clone_dyn();

        boxed.close().await.expect("close succeeds");
        second.close().await.expect("close succeeds");
        second.record_final_failure();

        assert_eq!(observer.close_call_count(), 2);
        assert_eq!(observer.final_failure_count(), 1);
    }

    #[tokio::test]
    async fn init_failure_budget_fails_first_then_succeeds() {
        let target = MockTarget::new("primary", "webhook").with_init_failures(2);
        let handle: &dyn Target<String> = &target;

        assert!(handle.init().await.is_err());
        assert!(handle.init().await.is_err());
        handle.init().await.expect("the failure budget is spent, so init succeeds");
        assert_eq!(target.init_call_count(), 3);
    }

    #[tokio::test]
    async fn save_failure_budget_fails_first_then_succeeds() {
        let target = MockTarget::new("primary", "webhook").with_save_failures(1);
        let handle: &dyn Target<String> = &target;

        assert!(handle.save(sample_event()).await.is_err());
        handle
            .save(sample_event())
            .await
            .expect("the failure budget is spent, so save succeeds");
        assert_eq!(target.save_call_count(), 2);
    }

    #[tokio::test]
    async fn active_override_decouples_the_probe_from_enablement() {
        let target = MockTarget::new("primary", "webhook").with_active(false);
        let handle: &dyn Target<String> = &target;

        assert!(handle.is_enabled());
        assert!(!handle.is_active().await.expect("the probe itself succeeds"));
    }

    // Leak-guard contract, part two: the `test-support` feature must never ship by default and
    // must stay a pure cfg gate, so no production dependency edge can drag the mock in.
    #[test]
    fn test_support_feature_never_ships_by_default() {
        let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
            .expect("the crate manifest should be readable");
        let default_features = manifest
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("default = "))
            .expect("the crate manifest should declare a default feature list");
        assert_eq!(default_features, "default = []", "test-support must stay out of the default feature set");
        let feature = manifest
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("test-support = "))
            .expect("the crate manifest should declare the test-support feature");
        assert_eq!(
            feature, "test-support = []",
            "test-support must stay a pure cfg gate that activates no dependencies"
        );
    }
}
