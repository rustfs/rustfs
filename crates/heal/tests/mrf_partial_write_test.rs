// Copyright 2026 RustFS Team
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

//! Real local EC12+4 members and the production MRF consumer/manager. These
//! tests never start a scanner, auto-heal, or an explicit Admin Heal request.
//! Run with nextest: channel and storage singletons require separate processes.

#![recursion_limit = "256"]

use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    mrf_queue::{self, snapshot::inspect_local_committed_snapshot},
    storage::ECStoreHealStorage,
};
use rustfs_test_utils::TestECStoreEnv;
use std::{future::Future, path::Path, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt;

mod storage_api;
use storage_api::endpoint_index::{EndpointServerPools, Endpoints, init_local_disks};
use storage_api::integration::{
    DiskAPI, DiskError, DiskStore, EcstoreStorageError, ObjectIO, ObjectOperations, ObjectOptions, PutObjReader,
    RUSTFS_META_BUCKET, ReadOptions,
};

const SNAPSHOT_LIMIT: usize = 64 * 1024 * 1024;

#[tokio::test]
async fn partial_write_persistence_failure_is_reported_and_retained_for_retry() {
    use rustfs_common::mrf_channel::{MrfDurableAdmissionError, MrfScope, persist_partial_write_intent};
    let root = tempfile::tempdir().expect("failure fixture directory");
    let env = TestECStoreEnv::builder().base_dir(root.path()).build().await;
    env.make_bucket("partial-persistence", false).await;
    let manager = manager(&env);
    mrf_queue::spawn_mrf_consumer(manager.clone());
    let scope = MrfScope {
        pool_index: 0,
        set_index: 0,
    };
    persist_partial_write_intent("partial-persistence", "old.bin", None, scope)
        .await
        .expect("initial responsibility must commit");
    assert!(snapshot_contains("old.bin").await);
    let snapshot = inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
        .await
        .expect("initial committed snapshot should validate")
        .expect("initial responsibility should have a committed snapshot");
    // Block only the successor manifest, preserving the committed anchor.
    // Removing each empty blocker restores writes in one operation, so the
    // consumer cannot recreate a directory between removal and restoration.
    let manifest_path = format!(".heal-mrf-commit.{}.bin", 1 - snapshot.slot());
    let manifest_blockers: Vec<_> = env
        .disk_paths
        .iter()
        .map(|path| path.join(RUSTFS_META_BUCKET).join(&manifest_path))
        .collect();
    for path in &manifest_blockers {
        tokio::fs::create_dir(path)
            .await
            .expect("block successor checkpoint manifest");
    }
    assert_eq!(
        persist_partial_write_intent("partial-persistence", "new.bin", None, scope).await,
        Err(MrfDurableAdmissionError::Persistence),
        "failed checkpoint publication must not be acknowledged as durable success"
    );
    for path in &manifest_blockers {
        tokio::fs::remove_dir(path).await.expect("remove checkpoint manifest blocker");
    }
    for disk in env.ecstore.pools[0].get_disks(0).disks.read().await.iter().flatten() {
        disk.reset_health_for_store_init_retry();
    }
    assert!(
        wait_until(|| async { snapshot_contains("new.bin").await }).await,
        "the failed durable submission must remain resident and retry publication"
    );
    assert!(
        snapshot_contains("old.bin").await,
        "retry must preserve the previously admitted obligation"
    );
}

#[test]
fn legacy_unbound_generation_is_parked_and_requires_explicit_risk_acceptance() {
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-unbound-lifecycle".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("unbound lifecycle runtime should build");
            runtime.block_on(legacy_unbound_generation_is_parked_and_requires_explicit_risk_acceptance_inner());
        })
        .expect("unbound lifecycle test thread should spawn")
        .join()
        .expect("unbound lifecycle test thread should finish");
}

async fn legacy_unbound_generation_is_parked_and_requires_explicit_risk_acceptance_inner() {
    use rustfs_common::mrf_channel::{MrfScope, persist_partial_write_intent};

    temp_env::async_with_vars([("RUSTFS_HEAL_MRF_ENABLE", Some("true"))], async {
        let root = tempfile::tempdir().expect("unbound lifecycle fixture directory");
        let env = TestECStoreEnv::builder().base_dir(root.path()).build().await;
        env.make_bucket("unbound-lifecycle", false).await;
        let manager = manager(&env);
        manager.start().await.expect("manager should start");
        mrf_queue::spawn_mrf_consumer(manager.clone());
        persist_partial_write_intent(
            "unbound-lifecycle",
            "legacy.bin",
            None,
            MrfScope {
                pool_index: 0,
                set_index: 0,
            },
        )
        .await
        .expect("old-format intent should still be durably admitted");

        assert!(
            wait_until(|| async {
                mrf_queue::list_legacy_responsibilities(None, 8).await.is_ok_and(|snapshot| {
                    snapshot
                        .responsibilities
                        .iter()
                        .any(|item| item.bucket == "unbound-lifecycle" && item.object == "legacy.bin")
                })
            })
            .await,
            "unbound old journal intent must become visible"
        );
        let snapshot = mrf_queue::list_legacy_responsibilities(None, 8)
            .await
            .expect("listing should be available");
        let entry = snapshot
            .responsibilities
            .iter()
            .find(|item| item.bucket == "unbound-lifecycle" && item.object == "legacy.bin")
            .expect("visible generation-unknown entry");
        assert_eq!(entry.status, "legacy_generation_unknown");
        assert!(entry.source_bucket_incarnation_id.is_none());
        assert!(
            snapshot_contains("legacy.bin").await,
            "generation uncertainty must preserve journal responsibility"
        );
        let operations = manager.operations_snapshot().await;
        assert_eq!(
            operations.queue_length, 0,
            "unbound old intent must not be sent to the current bucket generation"
        );

        assert!(matches!(
            mrf_queue::recheck_legacy_responsibility(entry.responsibility_id, entry.bucket_incarnation_id).await,
            Err(rustfs_heal::heal::mrf_queue::MrfLifecycleControlError::InvalidAction(_))
        ));
        mrf_queue::accept_unverified_legacy_risk(mrf_queue::MrfLegacyRiskAcceptanceRequest {
            responsibility_id: entry.responsibility_id,
            expected_bucket_incarnation_id: entry.bucket_incarnation_id,
            acknowledge_unknown_source_incarnation: true,
            acknowledge_incarnation_mismatch: false,
            actor: "integration-test-operator".to_string(),
            reason: "The old journal has no source bucket-generation binding".to_string(),
            reference: "TEST-ISSUE-2682-UNBOUND".to_string(),
            request_id: uuid::Uuid::new_v4(),
        })
        .await
        .expect("unbound risk disposition requires explicit acknowledgment and must be durable");
        assert!(
            snapshot_contains("legacy.bin").await,
            "risk acknowledgment must not delete the intent record"
        );
        let accepted = mrf_queue::list_legacy_responsibilities(None, 8)
            .await
            .expect("accepted status should be readable");
        let accepted = accepted
            .responsibilities
            .iter()
            .find(|item| item.responsibility_id == entry.responsibility_id)
            .expect("accepted unbound generation remains visible");
        assert_eq!(accepted.status, "operator_accepted_unverified");
        assert!(
            accepted
                .accepted
                .as_ref()
                .is_some_and(|audit| audit.acknowledged_unknown_source_incarnation)
        );
        manager.stop().await.expect("manager should stop");
    })
    .await;
}

#[test]
fn unversioned_deleted_partial_write_is_discharged_by_an_absence_proof() {
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-partial-write-absence".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("partial-write absence runtime should build");
            runtime.block_on(unversioned_deleted_partial_write_is_discharged_by_an_absence_proof_inner());
        })
        .expect("partial-write absence test thread should spawn")
        .join()
        .expect("partial-write absence test thread should finish");
}

async fn unversioned_deleted_partial_write_is_discharged_by_an_absence_proof_inner() {
    use rustfs_common::mrf_channel::{MrfScope, persist_partial_write_intent_with_incarnation};

    temp_env::async_with_vars([("RUSTFS_HEAL_MRF_ENABLE", Some("true"))], async {
        let root = tempfile::tempdir().expect("partial-write absence fixture directory");
        let env = TestECStoreEnv::builder().disk_count(16).base_dir(root.path()).build().await;
        env.make_bucket("partial-absence", false).await;
        let mut coordinator_pool = env.endpoint_pools.as_ref()[0].clone();
        let mut endpoints = coordinator_pool.endpoints.as_ref().to_vec();
        for endpoint in endpoints.iter_mut().skip(4) {
            endpoint.is_local = false;
        }
        coordinator_pool.endpoints = Endpoints::from(endpoints);
        init_local_disks(EndpointServerPools::from(vec![coordinator_pool]))
            .await
            .expect("coordinator journal disks");

        let manager = manager(&env);
        mrf_queue::spawn_mrf_consumer(manager.clone());
        let source_bucket_incarnation_id = env.ecstore.pools[0]
            .get_disks(0)
            .bucket_incarnation_id_from_disk("partial-absence")
            .await
            .expect("fixture bucket source incarnation");
        for (object, version_id) in [
            ("deleted-unversioned.bin", None),
            ("deleted-versioned.bin", Some(uuid::Uuid::new_v4())),
        ] {
            persist_partial_write_intent_with_incarnation(
                "partial-absence",
                object,
                version_id,
                MrfScope {
                    pool_index: 0,
                    set_index: 0,
                },
                Some(source_bucket_incarnation_id),
            )
            .await
            .expect("durable partial-write responsibility must commit before scheduling");
            assert!(snapshot_contains(object).await, "committed responsibility must exist before repair runs");
        }

        manager.start().await.expect("MRF scheduler should start");
        for object in ["deleted-unversioned.bin", "deleted-versioned.bin"] {
            assert!(
                wait_until(|| async { !snapshot_contains(object).await }).await,
                "complete absence proof must discharge the durable responsibility"
            );
        }
        assert!(
            wait_until(|| async {
                let snapshot = manager.operations_snapshot().await;
                snapshot.queue_length == 0 && snapshot.active_tasks == 0
            })
            .await,
            "discharged absence repair must leave no queued work"
        );
        manager.stop().await.expect("absence manager should stop");
    })
    .await;
}

#[test]
fn degraded_deleted_partial_write_is_discharged_by_an_absence_proof() {
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-partial-write-absence".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("partial-write absence runtime should build");
            runtime.block_on(degraded_deleted_partial_write_is_discharged_by_an_absence_proof_inner());
        })
        .expect("partial-write absence test thread should spawn")
        .join()
        .expect("partial-write absence test thread should finish");
}

async fn degraded_deleted_partial_write_is_discharged_by_an_absence_proof_inner() {
    temp_env::async_with_vars(
        [
            ("RUSTFS_HEAL_MRF_ENABLE", Some("true")),
            ("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", Some("false")),
            ("RUSTFS_SHARD_INTEGRITY_WRITE", Some("false")),
            ("RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED", Some("false")),
        ],
        async {
            let root = tempfile::tempdir().expect("partial-write absence fixture directory");
            let env = TestECStoreEnv::builder().disk_count(16).base_dir(root.path()).build().await;
            let set = env.ecstore.pools[0].get_disks(0);
            assert_eq!(env.ecstore.pools[0].parity_count, 4, "fixture must be EC12+4");
            env.make_bucket("partial-absence", false).await;
            env.make_bucket("partial-absence-versioned", true).await;
            let mut coordinator_pool = env.endpoint_pools.as_ref()[0].clone();
            let mut endpoints = coordinator_pool.endpoints.as_ref().to_vec();
            for endpoint in endpoints.iter_mut().skip(4) {
                endpoint.is_local = false;
            }
            coordinator_pool.endpoints = Endpoints::from(endpoints);
            init_local_disks(EndpointServerPools::from(vec![coordinator_pool]))
                .await
                .expect("coordinator journal disks");

            let manager = manager(&env);
            mrf_queue::spawn_mrf_consumer(manager.clone());
            let all: Vec<_> = set
                .disks
                .read()
                .await
                .iter()
                .map(|disk| disk.clone().expect("all sixteen members start online"))
                .collect();
            for path in &env.disk_paths[12..] {
                tokio::fs::rename(path, path.with_extension("offline"))
                    .await
                    .expect("detach the unavailable test node");
                tokio::fs::write(path, b"offline member")
                    .await
                    .expect("prevent the endpoint monitor from reopening the node");
            }
            for disk in &mut set.disks.write().await[12..] {
                *disk = None;
            }

            let deleted_object = "flink-key/.incomplete/upload-unversioned/part-1";
            put(&env, "partial-absence", deleted_object, b"object to delete", false).await;
            assert!(
                snapshot_contains(deleted_object).await,
                "a degraded PUT must durably admit its partial-write responsibility"
            );
            let versioned_bucket = "partial-absence-versioned";
            let versioned_object = "flink-key/.incomplete/upload-versioned/part-1";
            let version_id = put(&env, versioned_bucket, versioned_object, b"version to delete", true)
                .await
                .expect("degraded versioned PUT must return a version ID");
            assert!(
                snapshot_contains(versioned_object).await,
                "a degraded versioned PUT must durably admit its partial-write responsibility"
            );
            assert_eq!(replicas(&all, "partial-absence", deleted_object, None, false).await, 12);
            assert_eq!(replicas(&all, versioned_bucket, versioned_object, Some(&version_id), false).await, 12);

            for path in &env.disk_paths[8..12] {
                tokio::fs::rename(path, path.with_extension("offline"))
                    .await
                    .expect("detach a second unavailable test node");
                tokio::fs::write(path, b"offline member")
                    .await
                    .expect("prevent the endpoint monitor from reopening the second node");
            }
            for disk in &mut set.disks.write().await[8..12] {
                *disk = None;
            }
            let failed_object = "flink-key/.incomplete/upload-under-quorum/part-1";
            let mut failed_reader = PutObjReader::from_vec(b"write below quorum".to_vec());
            let failed_put = env
                .ecstore
                .put_object("partial-absence", failed_object, &mut failed_reader, &Default::default())
                .await;
            let failed_put = failed_put.expect_err("two unavailable nodes must reject this write");
            assert!(
                matches!(
                    &failed_put,
                    EcstoreStorageError::ErasureWriteQuorum | EcstoreStorageError::InsufficientWriteQuorum(_, _)
                ),
                "the rejected write must report a write-quorum failure: {failed_put:?}"
            );
            assert!(
                !snapshot_contains(failed_object).await,
                "a subquorum PUT that was never committed must not create MRF responsibility"
            );

            for (path, disk) in env.disk_paths[12..].iter().zip(&all[12..]) {
                tokio::fs::remove_file(path).await.expect("remove offline sentinel");
                tokio::fs::rename(path.with_extension("offline"), path)
                    .await
                    .expect("restore the same member data");
                disk.reset_health_for_store_init_retry();
            }
            for (path, disk) in env.disk_paths[8..12].iter().zip(&all[8..12]) {
                tokio::fs::remove_file(path)
                    .await
                    .expect("remove second-node offline sentinel");
                tokio::fs::rename(path.with_extension("offline"), path)
                    .await
                    .expect("restore the second node's original member data");
                disk.reset_health_for_store_init_retry();
            }
            *set.disks.write().await = all.iter().cloned().map(Some).collect();
            assert!(
                env.ecstore
                    .get_object_info("partial-absence", failed_object, &ObjectOptions::default())
                    .await
                    .is_err(),
                "the rejected subquorum PUT must not become a visible object after rejoin"
            );

            env.ecstore
                .delete_object("partial-absence", deleted_object, ObjectOptions::default())
                .await
                .expect("delete the exact object after its durable heal responsibility commits");
            assert!(
                env.ecstore
                    .get_object_info("partial-absence", deleted_object, &ObjectOptions::default())
                    .await
                    .is_err(),
                "the deleted key must be absent before replay"
            );
            env.ecstore
                .delete_object(
                    versioned_bucket,
                    versioned_object,
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(version_id.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("delete the exact version after its durable heal responsibility commits");
            let deleted_version_options = ObjectOptions {
                versioned: true,
                version_id: Some(version_id),
                ..Default::default()
            };
            assert!(
                env.ecstore
                    .get_object_info(versioned_bucket, versioned_object, &deleted_version_options)
                    .await
                    .is_err(),
                "the deleted version must be absent before replay"
            );
            for object in [deleted_object, versioned_object] {
                assert!(
                    snapshot_contains(object).await,
                    "deletion must not release the pending MRF responsibility"
                );
            }

            manager.start().await.expect("MRF scheduler should start");
            for object in [deleted_object, versioned_object] {
                assert!(
                    wait_until(|| async { !snapshot_contains(object).await }).await,
                    "complete absence proof must discharge the durable responsibility"
                );
            }
            assert!(
                env.ecstore
                    .get_object_info("partial-absence", deleted_object, &ObjectOptions::default())
                    .await
                    .is_err(),
                "absence-proof replay must not recreate the deleted object"
            );
            assert!(
                env.ecstore
                    .get_object_info(versioned_bucket, versioned_object, &deleted_version_options)
                    .await
                    .is_err(),
                "absence-proof replay must not recreate the deleted version"
            );
            assert!(
                wait_until(|| async {
                    let snapshot = manager.operations_snapshot().await;
                    snapshot.queue_length == 0 && snapshot.active_tasks == 0
                })
                .await,
                "discharged absence repair must leave no queued work"
            );
            manager.stop().await.expect("absence manager should stop");
        },
    )
    .await;
}

async fn wait_until<F: FnMut() -> Fut, Fut: Future<Output = bool>>(mut probe: F) -> bool {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if probe().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

fn manager(env: &TestECStoreEnv) -> Arc<HealManager> {
    Arc::new(HealManager::new(
        Arc::new(ECStoreHealStorage::new(env.ecstore.clone())),
        Some(HealConfig {
            enable_auto_heal: false,
            heal_interval: Duration::from_millis(50),
            ..Default::default()
        }),
    ))
}

async fn put(env: &TestECStoreEnv, bucket: &str, object: &str, payload: &[u8], versioned: bool) -> Option<String> {
    let mut reader = PutObjReader::from_vec(payload.to_vec());
    env.ecstore
        .put_object(
            bucket,
            object,
            &mut reader,
            &ObjectOptions {
                versioned,
                ..Default::default()
            },
        )
        .await
        .expect("degraded PUT must retain write quorum")
        .version_id
        .map(|version| version.to_string())
}

async fn replicas(disks: &[DiskStore], bucket: &str, object: &str, version: Option<&str>, deleted: bool) -> usize {
    let results = futures::future::join_all(disks.iter().map(|disk| async move {
        disk.read_version("", bucket, object, version.unwrap_or_default(), &ReadOptions::default())
            .await
            .is_ok_and(|fi| fi.deleted == deleted && fi.version_id.map(|id| id.to_string()).as_deref() == version)
    }))
    .await;
    results.into_iter().filter(|present| *present).count()
}

async fn assert_payload(env: &TestECStoreEnv, bucket: &str, object: &str, version: Option<&str>, payload: &[u8]) {
    let mut reader = env
        .ecstore
        .get_object_reader(
            bucket,
            object,
            None,
            Default::default(),
            &ObjectOptions {
                version_id: version.map(str::to_owned),
                ..Default::default()
            },
        )
        .await
        .expect("repaired version should be readable");
    let mut body = Vec::new();
    reader
        .stream
        .read_to_end(&mut body)
        .await
        .expect("entire repaired body must decode");
    assert_eq!(body, payload);
}

async fn snapshot_contains(object: &str) -> bool {
    inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
        .await
        .expect("committed snapshot must validate")
        .is_some_and(|snapshot| {
            snapshot
                .payload()
                .windows(object.len())
                .any(|bytes| bytes == object.as_bytes())
        })
}

#[test]
fn partial_write_ec12_4_ack_rejoin_repairs_versions_and_delete_marker() {
    // Keep the current-thread scheduler while matching the debug server's stack budget for real-storage futures.
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-partial-write-ec12-4".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("partial-write test runtime should build");
            runtime.block_on(partial_write_ec12_4_ack_rejoin_repairs_versions_and_delete_marker_inner());
        })
        .expect("partial-write test thread should spawn")
        .join()
        .expect("partial-write test thread should finish");
}

async fn partial_write_ec12_4_ack_rejoin_repairs_versions_and_delete_marker_inner() {
    temp_env::async_with_vars(
        [
            ("RUSTFS_HEAL_MRF_ENABLE", Some("true")),
            ("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", Some("false")),
            ("RUSTFS_SHARD_INTEGRITY_WRITE", Some("false")),
            ("RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED", Some("false")),
        ],
        async {
            let root = tempfile::tempdir().expect("test directory should be created");
            let env = TestECStoreEnv::builder().disk_count(16).base_dir(root.path()).build().await;
            let set = env.ecstore.pools[0].get_disks(0);
            assert_eq!(env.ecstore.pools[0].parity_count, 4, "fixture must be EC12+4");
            env.make_bucket("partial-new", false).await;
            env.make_bucket("partial-versions", true).await;
            // The coordinator owns four journal disks; the other twelve
            // members are local I/O stand-ins for the other three nodes.
            let mut coordinator_pool = env.endpoint_pools.as_ref()[0].clone();
            let mut endpoints = coordinator_pool.endpoints.as_ref().to_vec();
            for endpoint in endpoints.iter_mut().skip(4) {
                endpoint.is_local = false;
            }
            coordinator_pool.endpoints = Endpoints::from(endpoints);
            init_local_disks(EndpointServerPools::from(vec![coordinator_pool]))
                .await
                .expect("coordinator journal disks");
            let manager = manager(&env);
            mrf_queue::spawn_mrf_consumer(manager.clone());
            let payload1 = b"first acknowledged generation".repeat(4096);
            let payload2 = b"second acknowledged generation".repeat(4096);
            let first = put(&env, "partial-versions", "versioned.bin", &payload1, true)
                .await
                .expect("first version ID");
            assert!(
                inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
                    .await
                    .expect("healthy-write snapshot check")
                    .is_none(),
                "complete writes must not create MRF checkpoints"
            );
            let all: Vec<_> = set
                .disks
                .read()
                .await
                .iter()
                .map(|disk| disk.clone().expect("all sixteen members start online"))
                .collect();
            // Make reconnection fail throughout the outage. Clearing inventory
            // alone lets the production endpoint monitor reopen these disks.
            for path in &env.disk_paths[12..] {
                tokio::fs::rename(path, path.with_extension("offline"))
                    .await
                    .expect("detach test member");
                tokio::fs::write(path, b"offline member")
                    .await
                    .expect("block automatic reopen");
            }
            for disk in &mut set.disks.write().await[12..] {
                *disk = None;
            }
            put(&env, "partial-new", "new.bin", &payload1, false).await;
            assert!(snapshot_contains("new.bin").await, "PUT ACK must follow committed MRF admission");
            let second = put(&env, "partial-versions", "versioned.bin", &payload2, true)
                .await
                .expect("second version ID");
            let marker = env
                .ecstore
                .delete_object(
                    "partial-versions",
                    "versioned.bin",
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("delete marker must retain write quorum")
                .version_id
                .expect("marker version ID")
                .to_string();
            assert!(
                snapshot_contains("versioned.bin").await,
                "version and marker responsibility must be committed"
            );
            assert_eq!(replicas(&all, "partial-new", "new.bin", None, false).await, 12);
            assert_eq!(replicas(&all, "partial-versions", "versioned.bin", Some(&second), false).await, 12);
            assert_eq!(replicas(&all, "partial-versions", "versioned.bin", Some(&marker), true).await, 12);

            manager.start().await.expect("MRF scheduler should start");
            assert!(
                wait_until(|| async {
                    let snapshot = manager.operations_snapshot().await;
                    snapshot.queue_length == 0 && snapshot.active_tasks == 0
                })
                .await,
                "initial offline-target attempts should finish"
            );
            assert!(
                snapshot_contains("new.bin").await,
                "unsuccessful repair must retain the committed obligation"
            );
            for (path, disk) in env.disk_paths[12..].iter().zip(&all[12..]) {
                tokio::fs::remove_file(path).await.expect("remove offline sentinel");
                tokio::fs::rename(path.with_extension("offline"), path)
                    .await
                    .expect("restore the same member data");
                disk.reset_health_for_store_init_retry();
            }
            *set.disks.write().await = all.iter().cloned().map(Some).collect();
            assert!(
                wait_until(|| async {
                    replicas(&all, "partial-new", "new.bin", None, false).await == 16
                        && replicas(&all, "partial-versions", "versioned.bin", Some(&second), false).await == 16
                        && replicas(&all, "partial-versions", "versioned.bin", Some(&marker), true).await == 16
                })
                .await,
                "MRF alone must restore all sixteen physical members"
            );
            assert_eq!(replicas(&all, "partial-versions", "versioned.bin", Some(&first), false).await, 16);
            assert_payload(&env, "partial-new", "new.bin", None, &payload1).await;
            assert_payload(&env, "partial-versions", "versioned.bin", Some(&first), &payload1).await;
            assert_payload(&env, "partial-versions", "versioned.bin", Some(&second), &payload2).await;
            let latest_options = ObjectOptions {
                versioned: true,
                ..Default::default()
            };
            let latest = env
                .ecstore
                .get_object_info("partial-versions", "versioned.bin", &latest_options)
                .await
                .expect("metadata API should return the latest marker identity");
            assert!(latest.delete_marker, "the latest generation must remain the delete marker");
            assert_eq!(latest.version_id.map(|id| id.to_string()).as_deref(), Some(marker.as_str()));
            assert!(
                env.ecstore
                    .get_object_reader("partial-versions", "versioned.bin", None, Default::default(), &latest_options)
                    .await
                    .is_err(),
                "a latest body read must not expose either data version through the marker"
            );
            assert!(
                wait_until(|| async {
                    let snapshot = manager.operations_snapshot().await;
                    snapshot.queue_length == 0 && snapshot.active_tasks == 0
                })
                .await,
                "legacy repair attempts must finish without inventing verification"
            );
            assert!(snapshot_contains("new.bin").await);
            assert!(snapshot_contains("versioned.bin").await);
            let before_purge = inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
                .await
                .expect("legacy checkpoint must validate")
                .expect("unverified legacy responsibility must remain");
            // Deleting an existing marker by VersionId removes a version; it
            // must persist a purge so the returning members cannot resurrect it.
            for path in &env.disk_paths[12..] {
                tokio::fs::rename(path, path.with_extension("offline"))
                    .await
                    .expect("detach member for marker purge");
                tokio::fs::write(path, b"offline member")
                    .await
                    .expect("keep purge member offline");
            }
            for disk in &mut set.disks.write().await[12..] {
                *disk = None;
            }
            env.ecstore
                .delete_object(
                    "partial-versions",
                    "versioned.bin",
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(marker.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("explicit marker purge should retain quorum");
            assert!(
                inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
                    .await
                    .expect("purge checkpoint must validate")
                    .expect("purge responsibility must be committed")
                    .sequence()
                    > before_purge.sequence(),
                "a degraded marker purge must commit a successor checkpoint"
            );
            for (path, disk) in env.disk_paths[12..].iter().zip(&all[12..]) {
                tokio::fs::remove_file(path).await.expect("remove purge outage sentinel");
                tokio::fs::rename(path.with_extension("offline"), path)
                    .await
                    .expect("restore purged member data");
                disk.reset_health_for_store_init_retry();
            }
            *set.disks.write().await = all.iter().cloned().map(Some).collect();
            assert!(
                wait_until(|| async {
                    let results = futures::future::join_all(all.iter().map(|disk| async {
                        disk.read_version("", "partial-versions", "versioned.bin", &marker, &ReadOptions::default())
                            .await
                    }))
                    .await;
                    results
                        .iter()
                        .all(|result| matches!(result, Err(DiskError::FileVersionNotFound)))
                })
                .await,
                "MRF must complete the marker purge on the returning members"
            );
            assert_payload(&env, "partial-versions", "versioned.bin", Some(&first), &payload1).await;
            assert_payload(&env, "partial-versions", "versioned.bin", Some(&second), &payload2).await;
            manager.stop().await.expect("test manager should stop");
        },
    )
    .await;
}

#[test]
fn partial_write_crash_fixture() {
    let Ok(root) = std::env::var("RUSTFS_TEST_PARTIAL_WRITE_CRASH_ROOT") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().expect("child runtime should start");
    runtime.block_on(async {
        let env = TestECStoreEnv::builder().base_dir(&root).build().await;
        env.make_bucket("partial-crash", false).await;
        let set = env.ecstore.pools[0].get_disks(0);
        set.disks.write().await[3] = None;
        let manager = manager(&env);
        mrf_queue::spawn_mrf_consumer(manager);
        put(&env, "partial-crash", "crash.bin", b"durable partial write across SIGKILL", false).await;
        assert!(snapshot_contains("crash.bin").await);
        tokio::fs::write(Path::new(&root).join("ack-ready"), b"ready")
            .await
            .expect("signal durable ACK boundary");
        std::future::pending::<()>().await;
    });
}

#[tokio::test]
async fn partial_write_sigkill_replay_rearms_and_repairs() {
    partial_write_sigkill_replay_scenario(true).await;
}

#[tokio::test]
async fn legacy_sigkill_replay_repairs_without_releasing_unverified_responsibility() {
    partial_write_sigkill_replay_scenario(false).await;
}

async fn partial_write_sigkill_replay_scenario(protected: bool) {
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().expect("crash test directory");
    let log = std::fs::File::create(root.path().join("child.log")).expect("child log");
    let mut child = Command::new(std::env::current_exe().expect("integration test executable"))
        .args(["--exact", "partial_write_crash_fixture", "--nocapture"])
        .env("RUSTFS_TEST_PARTIAL_WRITE_CRASH_ROOT", root.path())
        .env("RUSTFS_HEAL_MRF_ENABLE", "true")
        .env("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false")
        .env("RUSTFS_SHARD_INTEGRITY_WRITE", protected.to_string())
        .env("RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED", protected.to_string())
        .stdout(Stdio::from(log.try_clone().expect("clone child log")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("crash child should start");
    let ready = wait_until(|| async { root.path().join("ack-ready").exists() }).await;
    child.kill().expect("kill child without runtime shutdown or journal flush");
    child.wait().expect("reap child");
    assert!(
        ready,
        "child must reach the durable ACK boundary: {}",
        std::fs::read_to_string(root.path().join("child.log")).expect("read child evidence")
    );
    let env = TestECStoreEnv::builder().base_dir(root.path()).build().await;
    let set = env.ecstore.pools[0].get_disks(0);
    let all: Vec<_> = set
        .disks
        .read()
        .await
        .iter()
        .map(|disk| disk.clone().expect("reopened disk"))
        .collect();
    assert_eq!(replicas(&all, "partial-crash", "crash.bin", None, false).await, 3);
    set.disks.write().await[3] = None;
    let manager = manager(&env);
    manager.start().await.expect("restarted scheduler should start");
    mrf_queue::spawn_mrf_consumer(manager.clone());
    assert!(snapshot_contains("crash.bin").await, "restart must find durable responsibility");
    *set.disks.write().await = all.iter().cloned().map(Some).collect();
    let healed = wait_until(|| async { replicas(&all, "partial-crash", "crash.bin", None, false).await == 4 }).await;
    assert!(
        healed,
        "replayed responsibility must heal the returning member; manager={:?}; responsibility={:?}",
        manager.operations_snapshot().await,
        mrf_queue::list_legacy_responsibilities(None, 16).await
    );
    assert_payload(&env, "partial-crash", "crash.bin", None, b"durable partial write across SIGKILL").await;
    if protected {
        assert!(
            wait_until(|| async {
                inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
                    .await
                    .expect("valid checkpoint")
                    .is_none()
            })
            .await,
            "replayed responsibility must be released only after verified repair"
        );
    } else {
        // A repair can finish before the next legacy check parks its responsibility.
        assert!(
            wait_until(|| async {
                let snapshot = manager.operations_snapshot().await;
                snapshot.queue_length == 0
                    && snapshot.active_tasks == 0
                    && mrf_queue::list_legacy_responsibilities(None, 32).await.is_ok_and(|state| {
                        state.responsibilities.iter().any(|entry| {
                            entry.bucket == "partial-crash"
                                && entry.object == "crash.bin"
                                && entry.status == "held_unverified_legacy"
                        })
                    })
            })
            .await,
            "legacy replay attempts must finish"
        );
        // Cover two MRF admission backoff periods so a missed hold notice
        // cannot pass merely because its task finished between polls.
        let retry_window = tokio::time::Instant::now() + Duration::from_secs(12);
        while tokio::time::Instant::now() < retry_window {
            let snapshot = manager.operations_snapshot().await;
            assert_eq!(snapshot.queue_length, 0, "an unverified legacy result must not refill the manager queue");
            assert_eq!(
                snapshot.active_tasks,
                0,
                "a held intent must not stay active; lifecycle state: {:?}",
                mrf_queue::list_legacy_responsibilities(None, 32)
                    .await
                    .expect("lifecycle state should be inspectable")
                    .responsibilities
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(snapshot_contains("crash.bin").await, "unverified legacy responsibility must remain");

        let before = mrf_queue::list_legacy_responsibilities(None, 32)
            .await
            .expect("held lifecycle listing should be available");
        let held = before
            .responsibilities
            .iter()
            .find(|entry| entry.bucket == "partial-crash" && entry.object == "crash.bin")
            .expect("legacy durable intent must be visible with its exact identity");
        assert_eq!(held.status, "held_unverified_legacy");
        assert_eq!(
            held.source_bucket_incarnation_id,
            Some(
                env.ecstore.pools[0]
                    .get_disks(0)
                    .bucket_incarnation_id_from_disk("partial-crash")
                    .await
                    .expect("source bucket incarnation should remain stable")
            ),
            "the producer-bound bucket generation must survive journal replay"
        );
        let responsibility_id = held.responsibility_id;
        let bucket_incarnation_id = held.bucket_incarnation_id;
        let request_id = uuid::Uuid::new_v4();
        mrf_queue::accept_unverified_legacy_risk(mrf_queue::MrfLegacyRiskAcceptanceRequest {
            responsibility_id,
            expected_bucket_incarnation_id: bucket_incarnation_id,
            acknowledge_unknown_source_incarnation: true,
            acknowledge_incarnation_mismatch: false,
            actor: "integration-test-operator".to_string(),
            reason: "The operator has accepted that legacy object identity cannot be proven automatically".to_string(),
            reference: "TEST-ISSUE-2682".to_string(),
            request_id,
        })
        .await
        .expect("explicit risk acceptance must persist before success");
        assert!(snapshot_contains("crash.bin").await, "risk acceptance must retain the MRF responsibility");
        let accepted = mrf_queue::list_legacy_responsibilities(None, 32)
            .await
            .expect("accepted lifecycle state should remain queryable");
        let accepted = accepted
            .responsibilities
            .iter()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .expect("accepted responsibility must remain visible");
        assert_eq!(accepted.status, "operator_accepted_unverified");
        assert_eq!(accepted.accepted.as_ref().map(|audit| audit.request_id), Some(request_id));
        assert_eq!(
            accepted.accepted.as_ref().map(|audit| audit.actor.as_str()),
            Some("integration-test-operator")
        );

        manager
            .stop()
            .await
            .expect("first manager should stop before restart verification");
        let log = std::fs::File::create(root.path().join("lifecycle-restart.log")).expect("restart child log");
        let mut child = Command::new(std::env::current_exe().expect("integration test executable"))
            .args(["--exact", "mrf_legacy_lifecycle_restore_fixture", "--nocapture"])
            .env("RUSTFS_TEST_MRF_LIFECYCLE_ROOT", root.path())
            .env("RUSTFS_TEST_MRF_LIFECYCLE_ID", responsibility_id.to_string())
            .stdout(Stdio::from(log.try_clone().expect("clone child log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("lifecycle restart fixture should start");
        let restored = wait_until(|| async { root.path().join("lifecycle-restored").exists() }).await;
        let status = child.wait().expect("lifecycle restart fixture should exit");
        assert!(
            restored && status.success(),
            "operator-accepted state should survive process restart: {}",
            std::fs::read_to_string(root.path().join("lifecycle-restart.log")).expect("read child evidence")
        );
        return;
    }
    manager.stop().await.expect("restarted manager should stop");
}

#[test]
fn mrf_legacy_lifecycle_restore_fixture() {
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-lifecycle-restore".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("lifecycle restore runtime should build");
            runtime.block_on(mrf_legacy_lifecycle_restore_fixture_inner());
        })
        .expect("lifecycle restore thread should spawn")
        .join()
        .expect("lifecycle restore thread should finish");
}

async fn mrf_legacy_lifecycle_restore_fixture_inner() {
    let Ok(root) = std::env::var("RUSTFS_TEST_MRF_LIFECYCLE_ROOT") else {
        return;
    };
    let expected_id = std::env::var("RUSTFS_TEST_MRF_LIFECYCLE_ID")
        .expect("lifecycle child responsibility ID")
        .parse::<uuid::Uuid>()
        .expect("valid lifecycle child responsibility ID");
    let root = std::path::PathBuf::from(root);
    let env = TestECStoreEnv::builder().base_dir(&root).build().await;
    let manager = manager(&env);
    manager.start().await.expect("restarted heal manager should start");
    mrf_queue::spawn_mrf_consumer(manager.clone());
    assert!(
        wait_until(|| async {
            mrf_queue::list_legacy_responsibilities(None, 32).await.is_ok_and(|snapshot| {
                snapshot
                    .responsibilities
                    .iter()
                    .any(|entry| entry.responsibility_id == expected_id)
            })
        })
        .await,
        "durable operator-accepted lifecycle state must replay"
    );
    let restored = mrf_queue::list_legacy_responsibilities(None, 32)
        .await
        .expect("restored lifecycle listing should be available");
    let entry = restored
        .responsibilities
        .iter()
        .find(|entry| entry.responsibility_id == expected_id)
        .expect("restart listing matched the stable generation");
    assert_eq!(entry.status, "operator_accepted_unverified");
    assert_eq!(
        entry.accepted.as_ref().map(|audit| audit.actor.as_str()),
        Some("integration-test-operator")
    );
    assert!(
        snapshot_contains("crash.bin").await,
        "risk-accepted responsibility remains in the durable journal"
    );
    tokio::fs::write(root.join("lifecycle-restored"), b"restored")
        .await
        .expect("signal lifecycle restore");
    manager.stop().await.expect("restarted manager should stop");
}
