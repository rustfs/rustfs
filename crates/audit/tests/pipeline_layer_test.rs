//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use async_trait::async_trait;
use rustfs_audit::{AuditPipeline, AuditRegistry, AuditRuntimeFacade, AuditRuntimeView};
use rustfs_targets::arn::TargetID;
use rustfs_targets::store::{Key, Store};
use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
use rustfs_targets::{StoreError, Target, TargetError};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
struct TestTarget {
    close_calls: Arc<AtomicUsize>,
    id: TargetID,
    init_calls: Arc<AtomicUsize>,
}

impl TestTarget {
    fn new(id: &str, name: &str) -> Self {
        Self {
            close_calls: Arc::new(AtomicUsize::new(0)),
            id: TargetID::new(id.to_string(), name.to_string()),
            init_calls: Arc::new(AtomicUsize::new(0)),
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

    async fn init(&self) -> Result<(), TargetError> {
        self.init_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn audit_runtime_view_lists_empty_targets() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let runtime_view = AuditRuntimeView::new(registry);

    assert!(runtime_view.list_targets().await.is_empty());
    assert!(runtime_view.get_target_values().await.is_empty());
    assert!(runtime_view.get_target("missing").await.is_none());
}

#[tokio::test]
async fn audit_pipeline_reports_empty_runtime_snapshots() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let pipeline = AuditPipeline::new(registry);

    assert!(pipeline.snapshot_target_metrics().await.is_empty());
    assert!(pipeline.snapshot_target_health().await.is_empty());
}

#[tokio::test]
async fn audit_runtime_facade_stops_empty_replay_workers() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let replay_workers = Arc::new(RwLock::new(rustfs_targets::ReplayWorkerManager::new()));
    let facade = AuditRuntimeFacade::new(registry, replay_workers);

    facade.stop_replay_workers().await;
}

#[tokio::test]
async fn audit_runtime_facade_activates_empty_target_list() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let replay_workers = Arc::new(RwLock::new(rustfs_targets::ReplayWorkerManager::new()));
    let facade = AuditRuntimeFacade::new(registry, replay_workers);

    let activation = facade.activate_targets_with_replay(Vec::new()).await;
    assert!(activation.targets.is_empty());
    assert_eq!(activation.replay_workers.len(), 0);
}

#[tokio::test]
async fn audit_runtime_view_upsert_and_remove_target() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let runtime_view = AuditRuntimeView::new(registry.clone());
    let target = TestTarget::new("primary", "webhook");
    let init_calls = Arc::clone(&target.init_calls);
    let close_calls = Arc::clone(&target.close_calls);

    runtime_view
        .upsert_target("primary:webhook".to_string(), Box::new(target))
        .await
        .expect("upsert should succeed");

    assert_eq!(runtime_view.list_targets().await, vec!["primary:webhook".to_string()]);
    assert_eq!(init_calls.load(Ordering::SeqCst), 1);

    runtime_view
        .remove_target("primary:webhook")
        .await
        .expect("remove should succeed");

    assert!(runtime_view.list_targets().await.is_empty());
    assert_eq!(close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn audit_runtime_facade_replace_targets_commits_runtime_state() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let replay_workers = Arc::new(RwLock::new(rustfs_targets::ReplayWorkerManager::new()));
    let facade = AuditRuntimeFacade::new(registry.clone(), replay_workers.clone());
    let target = TestTarget::new("primary", "webhook");
    let activation = rustfs_targets::RuntimeActivation {
        replay_workers: rustfs_targets::ReplayWorkerManager::new(),
        targets: vec![Arc::new(target) as rustfs_targets::SharedTarget<rustfs_audit::AuditEntry>],
    };

    facade
        .replace_targets(activation)
        .await
        .expect("replace_targets should succeed");

    let runtime_view = AuditRuntimeView::new(registry);
    assert_eq!(runtime_view.list_targets().await, vec!["primary:webhook".to_string()]);
    assert_eq!(replay_workers.read().await.len(), 0);
}
