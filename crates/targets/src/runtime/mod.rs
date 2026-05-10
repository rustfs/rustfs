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

use crate::Target;
use crate::arn::TargetID;
use crate::store::{Key, Store, ensure_store_entry_raw_readable};
use crate::target::QueuedPayload;
use crate::target::TargetDeliverySnapshot;
use crate::{StoreError, TargetError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::sync::{Semaphore, mpsc};

/// Shared target trait object used by the runtime manager.
pub type SharedTarget<E> = Arc<dyn Target<E> + Send + Sync>;
type ReplayHook<E> = Arc<dyn Fn(ReplayEvent<E>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Default)]
pub struct ReplayWorkerManager {
    cancellers: HashMap<String, mpsc::Sender<()>>,
}

impl ReplayWorkerManager {
    pub fn new() -> Self {
        Self {
            cancellers: HashMap::new(),
        }
    }

    pub fn insert(&mut self, target_id: String, cancel_tx: mpsc::Sender<()>) {
        self.cancellers.insert(target_id, cancel_tx);
    }

    pub fn len(&self) -> usize {
        self.cancellers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cancellers.is_empty()
    }

    pub fn snapshot(&self, target_count: usize) -> RuntimeStatusSnapshot {
        RuntimeStatusSnapshot {
            replay_worker_count: self.len(),
            target_count,
        }
    }

    pub async fn stop_all(&mut self, log_prefix: &str) {
        for (target_id, cancel_tx) in self.cancellers.drain() {
            tracing::info!(target_id = %target_id, "{log_prefix}");
            let _ = cancel_tx.send(()).await;
        }
    }
}

pub struct RuntimeActivation<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub replay_workers: ReplayWorkerManager,
    pub targets: Vec<SharedTarget<E>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeStatusSnapshot {
    pub replay_worker_count: usize,
    pub target_count: usize,
}

/// A read-only runtime snapshot for a target instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeTargetSnapshot {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTargetHealthState {
    Disabled,
    Error,
    Offline,
    Online,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTargetHealthSnapshot {
    pub enabled: bool,
    pub error_message: Option<String>,
    pub state: RuntimeTargetHealthState,
    pub target_id: String,
    pub target_type: String,
}

pub enum ReplayEvent<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    Delivered {
        key: Key,
        target: SharedTarget<E>,
    },
    RetryableError {
        error: TargetError,
        key: Key,
        retry_count: usize,
        target: SharedTarget<E>,
    },
    Dropped {
        key: Key,
        reason: String,
        target: SharedTarget<E>,
    },
    PermanentFailure {
        error: TargetError,
        key: Key,
        target: SharedTarget<E>,
    },
    RetryExhausted {
        key: Key,
        target: SharedTarget<E>,
    },
    UnreadableEntry {
        error: StoreError,
        key: Key,
        target: SharedTarget<E>,
    },
}

/// Shared runtime container for managing instantiated targets.
///
/// This intentionally focuses on low-risk shared lifecycle primitives first:
/// add/remove/close/list/snapshot. Replay workers and reload orchestration can
/// be layered on top in later phases.
pub struct TargetRuntimeManager<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    targets: HashMap<String, SharedTarget<E>>,
}

impl<E> Default for TargetRuntimeManager<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Debug for TargetRuntimeManager<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetRuntimeManager")
            .field("target_count", &self.targets.len())
            .finish()
    }
}

impl<E> TargetRuntimeManager<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        Self { targets: HashMap::new() }
    }

    pub fn add_arc(&mut self, target: SharedTarget<E>) -> Option<SharedTarget<E>> {
        let key = target.id().to_string();
        self.targets.insert(key, target)
    }

    pub fn add_boxed(&mut self, target: Box<dyn Target<E> + Send + Sync>) -> Option<SharedTarget<E>> {
        self.add_arc(Arc::from(target))
    }

    pub fn get(&self, key: &str) -> Option<SharedTarget<E>> {
        self.targets.get(key).cloned()
    }

    pub fn get_by_target_id(&self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.get(&target_id.to_string())
    }

    pub fn remove(&mut self, key: &str) -> Option<SharedTarget<E>> {
        self.targets.remove(key)
    }

    pub fn remove_by_target_id(&mut self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.remove(&target_id.to_string())
    }

    pub fn clear(&mut self) {
        self.targets.clear();
    }

    pub async fn remove_and_close(&mut self, key: &str) -> Option<SharedTarget<E>> {
        let target = self.targets.remove(key)?;
        if let Err(err) = target.close().await {
            tracing::error!(target_id = %key, error = %err, "Failed to close target during removal");
        }
        Some(target)
    }

    pub async fn remove_by_target_id_and_close(&mut self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.remove_and_close(&target_id.to_string()).await
    }

    pub async fn clear_and_close(&mut self) {
        let target_ids: Vec<String> = self.targets.keys().cloned().collect();
        for target_id in target_ids {
            let _ = self.remove_and_close(&target_id).await;
        }
        self.targets.clear();
    }

    pub fn target_ids(&self) -> Vec<TargetID> {
        self.targets.values().map(|target| target.id()).collect()
    }

    pub fn keys(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    pub fn values(&self) -> Vec<SharedTarget<E>> {
        self.targets.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn snapshots(&self) -> Vec<RuntimeTargetSnapshot> {
        let mut snapshots = Vec::with_capacity(self.targets.len());
        for target in self.targets.values() {
            let delivery = target.delivery_snapshot();
            let target_id = target.id();
            snapshots.push(snapshot_from_delivery(target_id, delivery));
        }
        snapshots.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        snapshots
    }

    pub fn status_snapshot(&self, replay_workers: &ReplayWorkerManager) -> RuntimeStatusSnapshot {
        replay_workers.snapshot(self.len())
    }

    pub async fn health_snapshots(&self) -> Vec<RuntimeTargetHealthSnapshot> {
        let mut snapshots = Vec::with_capacity(self.targets.len());
        for target in self.targets.values() {
            let enabled = target.is_enabled();
            let target_id = target.id();
            let (state, error_message) = if !enabled {
                (RuntimeTargetHealthState::Disabled, None)
            } else {
                match target.is_active().await {
                    Ok(true) => (RuntimeTargetHealthState::Online, None),
                    Ok(false) => (RuntimeTargetHealthState::Offline, None),
                    Err(err) => (RuntimeTargetHealthState::Error, Some(err.to_string())),
                }
            };

            snapshots.push(RuntimeTargetHealthSnapshot {
                enabled,
                error_message,
                state,
                target_id: target_id.to_string(),
                target_type: target_id.name,
            });
        }
        snapshots.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        snapshots
    }
}

fn snapshot_from_delivery(target_id: TargetID, delivery: TargetDeliverySnapshot) -> RuntimeTargetSnapshot {
    RuntimeTargetSnapshot {
        failed_messages: delivery.failed_messages,
        queue_length: delivery.queue_length,
        target_id: target_id.to_string(),
        target_type: target_id.name,
        total_messages: delivery.total_messages,
    }
}

pub async fn init_target_and_optionally_start_replay<E, F, G>(
    target: Box<dyn Target<E> + Send + Sync>,
    on_replay_start: F,
    start_replay: G,
) -> Option<(SharedTarget<E>, Option<mpsc::Sender<()>>)>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    F: FnOnce(&str, bool),
    G: FnOnce(Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>, SharedTarget<E>) -> mpsc::Sender<()>,
{
    let target_id = target.id().to_string();
    let has_store = target.store().is_some();

    if let Err(err) = target.init().await {
        tracing::error!(target_id = %target_id, error = %err, "Failed to initialize target");
        if !has_store {
            return None;
        }
        tracing::warn!(
            target_id = %target_id,
            "Proceeding with store-backed target despite init failure"
        );
    }

    let shared: SharedTarget<E> = Arc::from(target);
    if !shared.is_enabled() {
        on_replay_start(&target_id, false);
        return Some((shared, None));
    }

    let cancel = shared
        .store()
        .map(|store| start_replay(store.boxed_clone(), Arc::clone(&shared)));
    on_replay_start(&target_id, cancel.is_some());
    Some((shared, cancel))
}

pub async fn activate_targets_with_replay<E, F, Fut>(
    targets: Vec<Box<dyn Target<E> + Send + Sync>>,
    mut activate_one: F,
) -> RuntimeActivation<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    F: FnMut(Box<dyn Target<E> + Send + Sync>) -> Fut,
    Fut: Future<Output = Option<(SharedTarget<E>, Option<mpsc::Sender<()>>)>>,
{
    let mut replay_workers = ReplayWorkerManager::new();
    let mut shared_targets = Vec::new();

    for target in targets {
        if let Some((shared_target, cancel_tx)) = activate_one(target).await {
            let target_id = shared_target.id().to_string();
            if let Some(cancel_tx) = cancel_tx {
                replay_workers.insert(target_id, cancel_tx);
            }
            shared_targets.push(shared_target);
        }
    }

    RuntimeActivation {
        replay_workers,
        targets: shared_targets,
    }
}

pub fn start_replay_worker<E>(
    mut store: Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
    target: SharedTarget<E>,
    hook: ReplayHook<E>,
    semaphore: Option<Arc<Semaphore>>,
    batch_timeout: Duration,
    idle_sleep: Duration,
) -> mpsc::Sender<()>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    let (cancel_tx, cancel_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        stream_replay_worker(&mut *store, target, cancel_rx, hook, semaphore, batch_timeout, idle_sleep).await;
    });

    cancel_tx
}

async fn stream_replay_worker<E>(
    store: &mut (dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    target: SharedTarget<E>,
    mut cancel_rx: mpsc::Receiver<()>,
    hook: ReplayHook<E>,
    semaphore: Option<Arc<Semaphore>>,
    batch_timeout: Duration,
    idle_sleep: Duration,
) where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    const MAX_RETRIES: usize = 5;
    const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);

    let mut batch_keys = Vec::with_capacity(1);
    let mut last_flush = tokio::time::Instant::now();

    loop {
        if cancel_rx.try_recv().is_ok() {
            return;
        }

        let keys = store.list();
        if keys.is_empty() {
            if !batch_keys.is_empty() && last_flush.elapsed() >= batch_timeout {
                process_replay_batch(&mut batch_keys, target.clone(), &hook, semaphore.clone()).await;
                last_flush = tokio::time::Instant::now();
            }
            tokio::time::sleep(idle_sleep).await;
            continue;
        }

        for key in keys {
            if cancel_rx.try_recv().is_ok() {
                if !batch_keys.is_empty() {
                    process_replay_batch(&mut batch_keys, target.clone(), &hook, semaphore.clone()).await;
                }
                return;
            }

            match ensure_store_entry_raw_readable(&*store, &key) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    hook(ReplayEvent::UnreadableEntry {
                        error: err,
                        key,
                        target: target.clone(),
                    })
                    .await;
                    continue;
                }
            }

            batch_keys.push(key);
            if !batch_keys.is_empty() || last_flush.elapsed() >= batch_timeout {
                process_replay_batch(&mut batch_keys, target.clone(), &hook, semaphore.clone()).await;
                last_flush = tokio::time::Instant::now();
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn process_replay_batch<E>(
        batch_keys: &mut Vec<Key>,
        target: SharedTarget<E>,
        hook: &ReplayHook<E>,
        semaphore: Option<Arc<Semaphore>>,
    ) where
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    {
        if batch_keys.is_empty() {
            return;
        }

        let _permit = match semaphore {
            Some(ref semaphore) => match semaphore.clone().acquire_owned().await {
                Ok(permit) => Some(permit),
                Err(err) => {
                    tracing::error!(error = %err, "Failed to acquire replay semaphore permit");
                    return;
                }
            },
            None => None,
        };

        for key in batch_keys.iter() {
            let mut retry_count = 0usize;
            let mut success = false;

            while retry_count < MAX_RETRIES && !success {
                match target.send_from_store(key.clone()).await {
                    Ok(_) => {
                        hook(ReplayEvent::Delivered {
                            key: key.clone(),
                            target: target.clone(),
                        })
                        .await;
                        success = true;
                    }
                    Err(err) => match err {
                        TargetError::NotConnected | TargetError::Timeout(_) => {
                            retry_count += 1;
                            hook(ReplayEvent::RetryableError {
                                error: err,
                                key: key.clone(),
                                retry_count,
                                target: target.clone(),
                            })
                            .await;

                            let jitter = Duration::from_millis(key.to_string().len() as u64 % 500);
                            let backoff = 1u32 << retry_count as u32;
                            tokio::time::sleep(BASE_RETRY_DELAY * backoff + jitter).await;
                        }
                        TargetError::Dropped(reason) => {
                            hook(ReplayEvent::Dropped {
                                key: key.clone(),
                                reason,
                                target: target.clone(),
                            })
                            .await;
                            break;
                        }
                        other => {
                            hook(ReplayEvent::PermanentFailure {
                                error: other,
                                key: key.clone(),
                                target: target.clone(),
                            })
                            .await;
                            break;
                        }
                    },
                }
            }

            if retry_count >= MAX_RETRIES && !success {
                hook(ReplayEvent::RetryExhausted {
                    key: key.clone(),
                    target: target.clone(),
                })
                .await;
            }
        }

        batch_keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::TargetRuntimeManager;
    use crate::StoreError;
    use crate::arn::TargetID;
    use crate::store::{Key, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{Target, TargetError};
    use async_trait::async_trait;
    use serde::{Serialize, de::DeserializeOwned};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct TestTarget {
        id: TargetID,
        close_calls: Arc<AtomicUsize>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: TargetID::new(id.to_string(), name.to_string()),
                close_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn runtime_manager_removes_and_closes_target() {
        let mut manager = TargetRuntimeManager::<String>::new();
        let target = TestTarget::new("primary", "webhook");
        let close_calls = Arc::clone(&target.close_calls);

        manager.add_boxed(Box::new(target));
        assert_eq!(manager.len(), 1);

        let removed = manager.remove_and_close("primary:webhook").await;
        assert!(removed.is_some());
        assert_eq!(manager.len(), 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_manager_snapshots_targets() {
        let mut manager = TargetRuntimeManager::<String>::new();
        manager.add_boxed(Box::new(TestTarget::new("primary", "webhook")));

        let snapshots = manager.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].target_id, "primary:webhook");
        assert_eq!(snapshots[0].target_type, "webhook");
    }
}
