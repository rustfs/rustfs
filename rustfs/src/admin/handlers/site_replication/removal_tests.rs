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
    assert!(
        ensure_site_replication_bucket_targets_with_runtime(
            bucket,
            &stale_runtime.state,
            &stale_runtime.local_peer,
            None,
            &stale_runtime.service_account_secret_key,
            incarnation
        )
        .await
        .is_err(),
        "target-only refresh must reject the same stale topology"
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

#[test]
fn decommission_ordinary_removal_timestamp_advances_both_retained_fences() {
    let now = OffsetDateTime::UNIX_EPOCH;
    for (topology, fence) in [
        (now + time::Duration::days(2), now + time::Duration::days(1)),
        (now + time::Duration::days(1), now + time::Duration::days(2)),
    ] {
        let state = SiteReplicationState {
            updated_at: Some(topology),
            removal_v2_fence: Some(fence),
            ..Default::default()
        };
        let allocated = next_site_removal_timestamp(&state, now).expect("allocate above clock rollback");
        assert!(allocated > topology && allocated > fence);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn decommission_durable_cleanup_preserves_disk_shared_target_and_cancels_orphan_resync() {
    use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
    use std::sync::Arc;
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("site_cleanup_snapshot")
        .disk_count(1)
        .build()
        .await;
    let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore)).await.expect("IAM");
    publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(rustfs_kms::KmsServiceManager::new()),
    )));
    let bucket = "cleanup-authoritative-target";
    env.make_bucket(bucket, true).await;
    let shared_arn = format!("arn:rustfs:replication::retired:{bucket}");
    let orphan_arn = format!("arn:rustfs:replication:us-east-1:retired:{bucket}");
    let mut shared = BucketTarget {
        arn: shared_arn.clone(),
        deployment_id: "retired".to_string(),
        target_bucket: bucket.to_string(),
        target_type: BucketTargetType::ReplicationService,
        endpoint: "old.example.test:9000".to_string(),
        ..Default::default()
    };
    let cached = BucketTargets {
        targets: vec![shared.clone()],
    };
    metadata_sys::update(bucket, BUCKET_TARGETS_FILE, serde_json::to_vec(&cached).expect("encode cached targets"))
        .await
        .expect("seed node cache");
    let config = PersistedReplicationConfiguration {
        role: String::new(),
        rules: vec![
            build_site_replication_rule(&shared_arn, 1, "site-repl-retired"),
            build_site_replication_rule(&shared_arn, 2, "operator-shared"),
        ],
    };
    metadata_sys::update(bucket, BUCKET_REPLICATION_CONFIG, serialize(&config).expect("encode rules"))
        .await
        .expect("seed rules");
    shared.endpoint = "current.example.test:9000".to_string();
    let orphan = BucketTarget {
        arn: orphan_arn.clone(),
        deployment_id: "retired".to_string(),
        target_bucket: bucket.to_string(),
        target_type: BucketTargetType::ReplicationService,
        endpoint: "orphan.example.test:9000".to_string(),
        ..Default::default()
    };
    let authoritative = BucketTargets {
        targets: vec![shared, orphan],
    };
    let mut disk = metadata_sys::get_config_from_disk(bucket).await.expect("disk snapshot");
    disk.bucket_targets_config_json = serde_json::to_vec(&authoritative).expect("encode disk targets");
    disk.save_with_store(env.ecstore.clone())
        .await
        .expect("persist without node cache reload");
    assert_eq!(
        metadata_sys::list_bucket_targets(bucket).await.expect("stale cache").targets[0].endpoint,
        "old.example.test:9000"
    );
    crate::storage::storage_api::init_background_replication(env.ecstore.clone()).await;
    let pool = current_replication_pool_handle().expect("replication pool");
    for arn in [&shared_arn, &orphan_arn] {
        pool.clone()
            .admit_bucket_resync(replication::resync_opts(bucket, arn.clone(), &format!("run-{arn}"), None))
            .await
            .expect("persist resync intent");
    }
    cleanup_removed_site_replication_bucket(bucket, &HashSet::from(["retired".to_string()]))
        .await
        .expect("cleanup");
    let disk = metadata_sys::get_config_from_disk(bucket)
        .await
        .expect("read final disk metadata");
    let remaining = disk.bucket_target_config.expect("remaining targets");
    assert_eq!(remaining.targets.len(), 1);
    assert_eq!(remaining.targets[0].arn, shared_arn);
    assert_eq!(
        remaining.targets[0].endpoint, "current.example.test:9000",
        "shared target must come from authoritative disk snapshot"
    );
    let resync = pool
        .read_durable_bucket_resync_status(bucket)
        .await
        .expect("reload resync from disk");
    assert_eq!(
        resync.targets_map[&orphan_arn].resync_status,
        replication::ResyncStatusType::ResyncCanceled
    );
    assert_eq!(
        resync.targets_map[&shared_arn].resync_status,
        replication::ResyncStatusType::ResyncPending
    );
}
