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
use storage_api::integration::{DiskAPI, DiskStore, ObjectIO, ObjectOperations, ObjectOptions, PutObjReader, ReadOptions};

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
    for path in &env.disk_paths {
        tokio::fs::rename(path, path.with_extension("offline"))
            .await
            .expect("detach journal disk");
        tokio::fs::write(path, b"unwritable journal root")
            .await
            .expect("prevent journal writes");
    }
    assert_eq!(
        persist_partial_write_intent("partial-persistence", "new.bin", None, scope).await,
        Err(MrfDurableAdmissionError::Persistence),
        "failed checkpoint publication must not be acknowledged as durable success"
    );
    for path in &env.disk_paths {
        tokio::fs::remove_file(path).await.expect("remove journal fault");
        tokio::fs::rename(path.with_extension("offline"), path)
            .await
            .expect("restore journal disk");
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

#[tokio::test]
async fn partial_write_ec12_4_ack_rejoin_repairs_versions_and_delete_marker() {
    temp_env::async_with_vars(
        [
            ("RUSTFS_HEAL_MRF_ENABLE", Some("true")),
            ("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", Some("false")),
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
                    inspect_local_committed_snapshot(SNAPSHOT_LIMIT)
                        .await
                        .expect("cleanup snapshot must validate")
                        .is_none()
                })
                .await,
                "verified repair must release durable responsibility"
            );
            // Deleting an existing marker by VersionId removes a version; it
            // must not create a new partial-write repair responsibility.
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
                    .expect("purge must not create a checkpoint")
                    .is_none(),
                "a physical marker purge must not be admitted as a marker creation repair"
            );
            for (path, disk) in env.disk_paths[12..].iter().zip(&all[12..]) {
                tokio::fs::remove_file(path).await.expect("remove purge outage sentinel");
                tokio::fs::rename(path.with_extension("offline"), path)
                    .await
                    .expect("restore purged member data");
                disk.reset_health_for_store_init_retry();
            }
            *set.disks.write().await = all.iter().cloned().map(Some).collect();
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
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().expect("crash test directory");
    let log = std::fs::File::create(root.path().join("child.log")).expect("child log");
    let mut child = Command::new(std::env::current_exe().expect("integration test executable"))
        .args(["--exact", "partial_write_crash_fixture", "--nocapture"])
        .env("RUSTFS_TEST_PARTIAL_WRITE_CRASH_ROOT", root.path())
        .env("RUSTFS_HEAL_MRF_ENABLE", "true")
        .env("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false")
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
    assert!(
        wait_until(|| async { replicas(&all, "partial-crash", "crash.bin", None, false).await == 4 }).await,
        "replayed responsibility must heal the returning member"
    );
    assert_payload(&env, "partial-crash", "crash.bin", None, b"durable partial write across SIGKILL").await;
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
    manager.stop().await.expect("restarted manager should stop");
}
