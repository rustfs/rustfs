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
use crate::target::TargetDeliverySnapshot;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

/// Shared target trait object used by the runtime manager.
pub type SharedTarget<E> = Arc<dyn Target<E> + Send + Sync>;

/// A read-only runtime snapshot for a target instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeTargetSnapshot {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
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
