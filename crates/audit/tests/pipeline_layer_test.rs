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

use rustfs_audit::{AuditEntry, AuditError, AuditPipeline, AuditRegistry, AuditRuntimeFacade, AuditRuntimeView};
use rustfs_targets::SharedTarget;
use rustfs_targets::testkit::MockTarget;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Builds a target whose `save()` always fails, used to exercise the dispatch
/// failure-propagation paths.
fn failing_target(id: &str, name: &str) -> MockTarget {
    MockTarget::new(id, name).with_save_failures(usize::MAX)
}

fn pipeline_with_targets(targets: Vec<(&str, SharedTarget<AuditEntry>)>) -> AuditPipeline {
    let mut registry = AuditRegistry::new();
    for (id, target) in targets {
        registry.add_shared_target(id.to_string(), target);
    }
    AuditPipeline::new(Arc::new(Mutex::new(registry)))
}

#[tokio::test]
async fn audit_pipeline_dispatch_propagates_total_failure() {
    let failing = failing_target("primary", "webhook");
    let observer = failing.clone();
    let pipeline = pipeline_with_targets(vec![("primary:webhook", Arc::new(failing))]);

    let result = pipeline.dispatch(Arc::new(AuditEntry::default())).await;

    assert!(
        matches!(result, Err(AuditError::Target(_))),
        "dispatch must surface an error when every target fails, got {result:?}"
    );
    assert_eq!(observer.save_call_count(), 1, "the failing target should have been invoked");
}

#[tokio::test]
async fn audit_pipeline_dispatch_tolerates_partial_failure() {
    let failing = failing_target("primary", "webhook");
    let healthy = MockTarget::new("secondary", "webhook");
    let pipeline = pipeline_with_targets(vec![
        ("primary:webhook", Arc::new(failing)),
        ("secondary:webhook", Arc::new(healthy)),
    ]);

    let result = pipeline.dispatch(Arc::new(AuditEntry::default())).await;

    assert!(
        result.is_ok(),
        "dispatch should succeed when at least one target accepts the event, got {result:?}"
    );
}

#[tokio::test]
async fn audit_pipeline_dispatch_batch_propagates_total_failure() {
    let failing = failing_target("primary", "webhook");
    let pipeline = pipeline_with_targets(vec![("primary:webhook", Arc::new(failing))]);

    let entries = vec![Arc::new(AuditEntry::default()), Arc::new(AuditEntry::default())];
    let result = pipeline.dispatch_batch(entries).await;

    assert!(
        matches!(result, Err(AuditError::Target(_))),
        "dispatch_batch must surface an error when every delivery fails, got {result:?}"
    );
}

#[tokio::test]
async fn audit_pipeline_dispatch_batch_tolerates_partial_failure() {
    let failing = failing_target("primary", "webhook");
    let healthy = MockTarget::new("secondary", "webhook");
    let pipeline = pipeline_with_targets(vec![
        ("primary:webhook", Arc::new(failing)),
        ("secondary:webhook", Arc::new(healthy)),
    ]);

    let entries = vec![Arc::new(AuditEntry::default())];
    let result = pipeline.dispatch_batch(entries).await;

    assert!(
        result.is_ok(),
        "dispatch_batch should succeed when a healthy target accepts the batch, got {result:?}"
    );
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
async fn stopping_audit_replay_workers_is_a_no_op_when_there_are_none() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let replay_workers = Arc::new(RwLock::new(rustfs_targets::ReplayWorkerManager::new()));
    let facade = AuditRuntimeFacade::new(registry, Arc::clone(&replay_workers));

    facade.stop_replay_workers().await;

    // The stop path takes the manager's workers and hands them to the adapter,
    // so an empty facade must leave it empty rather than wedge it, and a second
    // call — which shutdown paths make — must stay harmless (rustfs/backlog#1836).
    assert!(replay_workers.read().await.is_empty());
    facade.stop_replay_workers().await;
    assert!(replay_workers.read().await.is_empty());
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
    let target = MockTarget::new("primary", "webhook");
    let observer = target.clone();

    runtime_view
        .upsert_target("primary:webhook".to_string(), Box::new(target))
        .await
        .expect("upsert should succeed");

    assert_eq!(runtime_view.list_targets().await, vec!["primary:webhook".to_string()]);
    assert_eq!(observer.init_call_count(), 1);

    runtime_view
        .remove_target("primary:webhook")
        .await
        .expect("remove should succeed");

    assert!(runtime_view.list_targets().await.is_empty());
    assert_eq!(observer.close_call_count(), 1);
}

#[tokio::test]
async fn audit_runtime_facade_replace_targets_commits_runtime_state() {
    let registry = Arc::new(Mutex::new(AuditRegistry::new()));
    let replay_workers = Arc::new(RwLock::new(rustfs_targets::ReplayWorkerManager::new()));
    let facade = AuditRuntimeFacade::new(registry.clone(), replay_workers.clone());
    let target = MockTarget::new("primary", "webhook");
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
