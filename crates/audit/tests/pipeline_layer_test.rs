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

use rustfs_audit::{AuditPipeline, AuditRegistry, AuditRuntimeFacade, AuditRuntimeView, system::AuditSystemState};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

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
    let state = Arc::new(RwLock::new(AuditSystemState::Stopped));

    let activation = facade.activate_targets_with_replay(state, Vec::new()).await;
    assert!(activation.targets.is_empty());
    assert_eq!(activation.replay_workers.len(), 0);
}
