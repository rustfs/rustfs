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

use super::*;
use rustfs_madmin::SRRemoveV2Req;

#[test]
fn decommission_role_only_cleanup_removes_legacy_role() {
    let config = PersistedReplicationConfiguration {
        role: "arn:rustfs:replication::b:bucket".to_string(),
        rules: vec![build_site_replication_rule("arn:rustfs:replication::c:bucket", 1, "operator")],
    };
    let (remaining, removed) = prune_removed_site_replication_rules(config, &HashSet::from(["b".to_string()]));
    assert_eq!(removed, 1, "a role-only mutation must be persisted");
    assert!(remaining.expect("operator rule survives").role.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn decommission_durable_cleanup_receipt_survives_state_reload() {
    use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
    use std::sync::Arc;
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("site_decommission")
        .disk_count(1)
        .init_bucket_metadata(false)
        .build()
        .await;
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore)).await.expect("IAM");
    publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    )));
    let local = current_local_runtime_peer(&SiteReplicationState::default());
    let remote = PeerInfo {
        deployment_id: "retired-site".to_string(),
        name: "retired".to_string(),
        endpoint: "http://127.0.0.1:1".to_string(),
        ..Default::default()
    };
    let seed = SiteReplicationState {
        name: local.name.clone(),
        peers: BTreeMap::from([
            (local.deployment_id.clone(), local.clone()),
            (remote.deployment_id.clone(), remote),
        ]),
        ..Default::default()
    };
    save_site_replication_state(&seed).await.expect("seed durable topology");
    let loaded = load_site_replication_state()
        .await
        .expect("read canonical persisted topology");
    let plan = plan_site_removal(
        &loaded,
        &local,
        &SRRemoveV2Req {
            mode: "dry-run".to_string(),
            sites: vec!["retired".to_string()],
            ..Default::default()
        },
    )
    .expect("plan");
    let admitted = plan.clone();
    let local_id = local.deployment_id;
    update_site_replication_state(move |state| admit_site_removal(state, admitted, &local_id, true))
        .await
        .expect("admit");
    let restored = load_site_replication_state().await.expect("reload pending");
    assert!(restored.pending_remove_v2.is_some());
    removal::resume_site_removal_v2(restored.pending_remove_v2.as_ref().expect("pending"))
        .await
        .expect("resume without remote");
    let restored = load_site_replication_state().await.expect("reload committed");
    assert!(restored.pending_remove_v2.is_none());
    assert!(
        restored.remove_v2_receipts.contains_key(&plan.id),
        "disabled replication must retain its completion fence"
    );
    assert!(!restored.enabled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn decommission_durable_cleanup_failure_retains_journal_until_retry_succeeds() {
    use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
    use std::sync::Arc;
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("site_decommission_retry")
        .disk_count(1)
        .init_bucket_metadata(true)
        .build()
        .await;
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore)).await.expect("IAM");
    publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    )));
    let bucket = "decommission-cleanup-retry";
    env.ecstore
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket");
    removal::FAIL_NEXT_LOCAL_REMOVAL_CLEANUP.store(true, std::sync::atomic::Ordering::SeqCst);
    let local = current_local_runtime_peer(&SiteReplicationState::default());
    let remote = PeerInfo {
        deployment_id: "retired-site".to_string(),
        name: "retired".to_string(),
        endpoint: "http://127.0.0.1:1".to_string(),
        ..Default::default()
    };
    let seed = SiteReplicationState {
        name: local.name.clone(),
        peers: BTreeMap::from([
            (local.deployment_id.clone(), local.clone()),
            (remote.deployment_id.clone(), remote),
        ]),
        ..Default::default()
    };
    save_site_replication_state(&seed).await.expect("seed");
    let loaded = load_site_replication_state().await.expect("read canonical topology");
    let stale_runtime = SiteReplicationRuntime {
        state: loaded.clone(),
        local_peer: local.clone(),
        service_account_secret_key: "test-secret".to_string(),
    };
    let plan = plan_site_removal(
        &loaded,
        &local,
        &SRRemoveV2Req {
            mode: "dry-run".to_string(),
            sites: vec!["retired".to_string()],
            ..Default::default()
        },
    )
    .expect("plan");
    let admitted = plan.clone();
    let local_id = local.deployment_id;
    update_site_replication_state(move |state| admit_site_removal(state, admitted, &local_id, true))
        .await
        .expect("admit");
    let incarnation = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .expect("incarnation");
    assert!(
        ensure_site_replication_bucket_setup_with_runtime_for_incarnation(bucket, &stale_runtime, incarnation)
            .await
            .is_err(),
        "an old topology snapshot must not restore the retired target"
    );
    let errors = removal::drive_site_removal_v2_locked(&plan.id).await.expect("partial driver");
    assert!(!errors.is_empty(), "cleanup failure must remain partial");
    let restored = load_site_replication_state().await.expect("reload");
    assert!(
        !restored
            .pending_remove_v2
            .as_ref()
            .expect("journal must survive failure")
            .local_cleanup_complete
    );
    assert!(!restored.remove_v2_receipts.contains_key(&plan.id));
    removal::resume_site_removal_v2(restored.pending_remove_v2.as_ref().expect("pending"))
        .await
        .expect("retry");
    let restored = load_site_replication_state().await.expect("reload complete");
    assert!(restored.pending_remove_v2.is_none());
    assert!(restored.remove_v2_receipts.contains_key(&plan.id));
}
