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

#![recursion_limit = "256"]

//! HS-01 (rustfs/backlog#1865): MRF intent pipeline integration tests.
//!
//! Drives the real consumer loop (`spawn_mrf_consumer`) against a real
//! 4-disk `ECStore` heal storage and a `HealManager` that has not started its
//! scheduler, so submitted intents stay observable in the admission queue.
//! Under `cargo nextest` each test runs in its own process, which keeps the
//! process-global MRF channel singleton safe.

use rustfs_common::mrf_channel::{self, MrfKind};
use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    mrf_queue,
    storage::{ECStoreHealStorage, HealStorageAPI},
};
use serial_test::serial;
#[cfg(unix)]
use std::{
    fs::{File, OpenOptions},
    io::Write,
};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

mod storage_api;

use storage_api::endpoint_index::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints, init_local_disks};

const META_BUCKET: &str = ".rustfs.sys";
const JOURNAL_REL: &str = "buckets/.heal/mrf/journal.bin";
const SCOPED_JOURNAL_REL: &str = "buckets/.heal/mrf/journal-scoped.bin";

async fn heal_env() -> (Vec<std::path::PathBuf>, Arc<dyn HealStorageAPI>) {
    heal_env_at(None).await
}

async fn heal_env_at(base_dir: Option<&Path>) -> (Vec<std::path::PathBuf>, Arc<dyn HealStorageAPI>) {
    let mut builder = rustfs_test_utils::TestECStoreEnv::builder().prefix("rustfs_heal_mrf_test");
    if let Some(base_dir) = base_dir {
        builder = builder.base_dir(base_dir);
    }
    let env = builder.build().await;
    let heal_storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    (env.disk_paths, heal_storage)
}

fn make_manager(storage: Arc<dyn HealStorageAPI>) -> Arc<HealManager> {
    Arc::new(HealManager::new(
        storage,
        Some(HealConfig {
            // Keep the scheduler from draining the queue before assertions.
            heal_interval: Duration::from_secs(3600),
            enable_auto_heal: false,
            ..Default::default()
        }),
    ))
}

async fn register_local_disks(disk_paths: &[std::path::PathBuf], cmd_line: &str) {
    let mut endpoints: Vec<Endpoint> = disk_paths
        .iter()
        .map(|p| Endpoint::try_from(p.to_string_lossy().as_ref()).expect("endpoint from disk path"))
        .collect();
    for (i, endpoint) in endpoints.iter_mut().enumerate() {
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);
    }
    let pool = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: endpoints.len(),
        endpoints: Endpoints::from(endpoints),
        cmd_line: cmd_line.to_string(),
        platform: String::new(),
    };
    init_local_disks(EndpointServerPools::from(vec![pool]))
        .await
        .expect("local disks should register");
}

/// Encode one journal record independently of the implementation, so a format
/// drift between writer and this fixture fails loudly here.
fn journal_record(kind: u8, bucket: &str, object: &str, version: Option<[u8; 16]>, attempts: u8) -> Vec<u8> {
    let mut body = vec![1u8, 1, kind, attempts];
    body.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
    match version {
        Some(bytes) => {
            body.push(1);
            body.extend_from_slice(&bytes);
        }
        None => body.push(0),
    }
    body.extend_from_slice(&(bucket.len() as u32).to_le_bytes());
    body.extend_from_slice(&(object.len() as u32).to_le_bytes());
    body.extend_from_slice(bucket.as_bytes());
    body.extend_from_slice(object.as_bytes());
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&body);
    body.extend_from_slice(&(hasher.finalize() as u32).to_le_bytes());
    body
}

fn scoped_journal_record(
    kind: u8,
    bucket: &str,
    object: &str,
    version: Option<[u8; 16]>,
    attempts: u8,
    pool_index: u32,
    set_index: u32,
) -> Vec<u8> {
    let mut body = vec![1u8, 2, kind, attempts];
    body.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
    match version {
        Some(bytes) => {
            body.push(1);
            body.extend_from_slice(&bytes);
        }
        None => body.push(0),
    }
    body.extend_from_slice(&pool_index.to_le_bytes());
    body.extend_from_slice(&set_index.to_le_bytes());
    body.extend_from_slice(
        &u32::try_from(bucket.len())
            .expect("fixture bucket length must fit journal format")
            .to_le_bytes(),
    );
    body.extend_from_slice(
        &u32::try_from(object.len())
            .expect("fixture object length must fit journal format")
            .to_le_bytes(),
    );
    body.extend_from_slice(bucket.as_bytes());
    body.extend_from_slice(object.as_bytes());
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&body);
    body.extend_from_slice(
        &u32::try_from(hasher.finalize())
            .expect("CRC32 must fit the journal checksum field")
            .to_le_bytes(),
    );
    body
}

fn write_journal_path_to_disks(disk_paths: &[std::path::PathBuf], relative_path: &str, data: &[u8]) {
    for path in disk_paths {
        let journal = path.join(META_BUCKET).join(relative_path);
        std::fs::create_dir_all(journal.parent().expect("journal parent")).expect("create journal dir");
        std::fs::write(&journal, data).expect("write journal fixture");
    }
}

#[cfg(unix)]
fn write_journal_path_to_disks_synced(disk_paths: &[std::path::PathBuf], relative_path: &str, data: &[u8]) {
    for path in disk_paths {
        let journal = path.join(META_BUCKET).join(relative_path);
        let parent = journal.parent().expect("journal parent");
        std::fs::create_dir_all(parent).expect("create journal dir");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&journal)
            .expect("open synced journal fixture");
        file.write_all(data).expect("write synced journal fixture");
        file.sync_all().expect("sync journal fixture");
        File::open(parent)
            .expect("open journal parent for sync")
            .sync_all()
            .expect("sync journal parent");
    }
}

fn write_journal_to_disks(disk_paths: &[std::path::PathBuf], data: &[u8]) {
    write_journal_path_to_disks(disk_paths, JOURNAL_REL, data);
}

fn journal_exists_on_all_disks(disk_paths: &[std::path::PathBuf], relative_path: &str) -> bool {
    disk_paths
        .iter()
        .all(|path| Path::new(path).join(META_BUCKET).join(relative_path).exists())
}

fn journal_matches_on_all_disks(disk_paths: &[PathBuf], relative_path: &str, expected: &[u8]) -> bool {
    disk_paths
        .iter()
        .all(|path| std::fs::read(path.join(META_BUCKET).join(relative_path)).is_ok_and(|actual| actual == expected))
}

async fn wait_until<F, Fut>(deadline: Duration, mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if probe().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// A decode-failure intent delivered on the global channel must surface in the
/// heal manager as an Urgent request attributed to the MRF source.
#[tokio::test]
#[serial]
async fn decode_failure_intent_maps_to_urgent_mrf_heal_request() {
    let (_disk_paths, storage) = heal_env().await;
    let manager = make_manager(storage);

    mrf_queue::spawn_mrf_consumer(manager.clone());

    assert!(
        mrf_channel::try_send_mrf_intent(MrfKind::DecodeFailure, "mrf-bucket", "mrf-object", None),
        "intent should be accepted while the consumer holds the channel"
    );

    let appeared = wait_until(Duration::from_secs(10), || async {
        let snapshot = manager.operations_snapshot().await;
        snapshot.queued_by_source.mrf >= 1 && snapshot.queued_by_priority.urgent >= 1
    })
    .await;
    assert!(
        appeared,
        "MRF intent must reach the manager queue as an Urgent request (snapshot: {:?})",
        manager.operations_snapshot().await
    );

    assert!(
        mrf_channel::take_mrf_repaired_events_for("mrf-bucket").is_empty(),
        "accepted intent must wait for successful heal completion before repaired notice fan-out"
    );
}

/// A journal left behind by a previous process must be replayed into the
/// manager queue and then removed, and a torn tail must not block replay of
/// the intact records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn journal_replay_arms_intents_and_deletes_the_file() {
    let (disk_paths, storage) = heal_env().await;

    // The journal reader resolves disks through the process-local disk map;
    // register the environment's disks the same way server startup does.
    register_local_disks(&disk_paths, "mrf-test").await;

    let mut journal = journal_record(1, "replay-bucket", "replay-object", Some([9u8; 16]), 0);
    journal.extend(journal_record(3, "replay-bucket", "partial-object", None, 1));
    // Torn tail: a third record truncated mid-way must not block the two
    // intact records above.
    journal.extend_from_slice(&journal_record(2, "replay-bucket", "metadata-object", None, 0)[..8]);
    write_journal_to_disks(&disk_paths, &journal);

    let manager = make_manager(storage);
    // Replay directly (not via the process-global channel consumer, which the
    // sibling test already claimed in this process under plain `cargo test`).
    let replayed = mrf_queue::replay_journal_once(&manager).await;
    assert_eq!(replayed, 2, "the two intact records must be replayed");

    let snapshot = manager.operations_snapshot().await;
    assert_eq!(snapshot.queued_by_source.mrf, 2, "replayed intents must be attributed to the MRF source");

    assert!(
        disk_paths
            .iter()
            .all(|path| !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()),
        "the journal file must be removed after a successful replay"
    );
    assert!(
        disk_paths
            .iter()
            .all(|path| !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()),
        "the authoritative journal file must also be removed after replay"
    );

    let snapshot = manager.operations_snapshot().await;
    assert_eq!(snapshot.queued_by_priority.urgent, 1, "the decode-failure record must replay as Urgent");
    assert!(snapshot.queued_by_priority.normal >= 1, "the partial-write record must replay as Normal");
}

/// A canonical snapshot and its compatibility mirror may differ after a
/// partial flush. Replay must choose the complete canonical epoch instead of
/// combining records that never coexisted in memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn authoritative_journal_is_not_merged_with_legacy_mirror() {
    let (disk_paths, storage) = heal_env().await;
    register_local_disks(&disk_paths, "mrf-authoritative-test").await;

    let authoritative = journal_record(1, "authoritative-bucket", "authoritative-object", None, 0);
    let legacy = journal_record(1, "legacy-bucket", "legacy-object", None, 0);
    write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &authoritative);
    write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &legacy);

    let manager = make_manager(storage);
    let replayed = mrf_queue::replay_journal_once(&manager).await;
    assert_eq!(replayed, 1, "only the authoritative snapshot epoch may replay");

    let snapshot = manager.operations_snapshot().await;
    assert_eq!(snapshot.queued_by_source.mrf, 1);
    assert!(
        disk_paths.iter().all(|path| {
            !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
                && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
        }),
        "replay cleanup must remove both journal paths"
    );

    // A scoped-only snapshot is valid during a rollout where no legacy
    // compatibility mirror was written. Missing legacy files must not leave
    // the runtime in a permanent cleanup-retry state.
    let scoped_only = journal_record(1, "scoped-only-bucket", "scoped-only-object", None, 0);
    write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &scoped_only);
    assert_eq!(mrf_queue::replay_journal_once(&manager).await, 1);
    assert!(disk_paths.iter().all(|path| {
        !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
            && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
    }));

    let scoped_v2 = scoped_journal_record(1, "scoped-v2-bucket", "scoped-v2-object", None, 0, 3, 7);
    let stale_legacy = journal_record(1, "stale-legacy-bucket", "stale-legacy-object", None, 0);
    write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &scoped_v2);
    write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &stale_legacy);
    assert_eq!(
        mrf_queue::replay_journal_once(&manager).await,
        1,
        "a scoped v2 authoritative epoch must not be merged with a stale v1 legacy mirror"
    );
    assert_eq!(
        manager.operations_snapshot().await.queued_by_source.mrf,
        3,
        "only the three authoritative/scoped-only epochs should have reached the manager"
    );
    assert!(disk_paths.iter().all(|path| {
        !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
            && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
    }));
}

/// The authoritative journal carries the full replay responsibility identity.
/// A stale legacy mirror must not collapse same-object records that differ by
/// kind or erasure-set scope after restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn authoritative_journal_replay_preserves_kind_and_scope_identity() {
    let (disk_paths, storage) = heal_env().await;
    register_local_disks(&disk_paths, "mrf-authoritative-identity-test").await;

    let mut authoritative = scoped_journal_record(3, "identity-bucket", "same-object", None, 0, 3, 7);
    authoritative.extend(scoped_journal_record(3, "identity-bucket", "same-object", None, 0, 3, 8));
    authoritative.extend(journal_record(2, "identity-bucket", "same-object", None, 0));
    authoritative.extend(journal_record(1, "identity-bucket", "same-object", Some([4u8; 16]), 0));
    let stale_legacy = journal_record(3, "identity-bucket", "stale-legacy-object", None, 0);
    write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &authoritative);
    write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &stale_legacy);

    let manager = make_manager(storage);
    let replayed = mrf_queue::replay_journal_once(&manager).await;
    assert_eq!(
        replayed, 4,
        "all authoritative kind/scope identities must decode before manager admission"
    );

    let snapshot = manager.operations_snapshot().await;
    assert_eq!(
        snapshot.queued_by_source.mrf, 4,
        "same-object MRF replay must retain distinct kind and scope responsibilities"
    );
    assert_eq!(
        snapshot.queued_by_priority.normal, 2,
        "the two scoped partial-write records must remain independently queued"
    );
    assert_eq!(
        snapshot.queued_by_priority.high, 1,
        "metadata corruption must not merge with object repair responsibility"
    );
    assert_eq!(
        snapshot.queued_by_priority.urgent, 1,
        "decode-failure repair must not merge with object repair responsibility"
    );
    assert!(disk_paths.iter().all(|path| {
        !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
            && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
    }));
}

/// If replay reaches a full heal-manager queue, the old journal remains the
/// durable restart anchor until a later consumer flush publishes the pending
/// successor snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn journal_replay_retains_file_when_manager_is_full() {
    let (disk_paths, storage) = heal_env().await;
    register_local_disks(&disk_paths, "mrf-full-replay-test").await;

    let mut journal = journal_record(1, "full-bucket", "first-object", None, 0);
    journal.extend(journal_record(1, "full-bucket", "second-object", None, 0));
    write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &journal);
    write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &journal);

    let manager = Arc::new(HealManager::new(
        storage.clone(),
        Some(HealConfig {
            queue_size: 1,
            heal_interval: Duration::from_secs(3600),
            enable_auto_heal: false,
            ..Default::default()
        }),
    ));
    let replayed = mrf_queue::replay_journal_once(&manager).await;
    assert_eq!(replayed, 2, "both records must be decoded before manager admission");
    assert_eq!(
        manager.operations_snapshot().await.queued_by_source.mrf,
        1,
        "only the first record can enter a one-slot manager queue"
    );
    assert!(
        disk_paths
            .iter()
            .all(|path| Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()),
        "replay must keep the authoritative journal when a later record is pending retry"
    );

    let restarted = Arc::new(HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            heal_interval: Duration::from_secs(3600),
            enable_auto_heal: false,
            ..Default::default()
        }),
    ));
    let replayed_after_restart = mrf_queue::replay_journal_once(&restarted).await;
    assert_eq!(
        replayed_after_restart, 2,
        "retained startup journal must replay again after a process restart"
    );
    assert_eq!(
        restarted.operations_snapshot().await.queued_by_source.mrf,
        1,
        "the restart sees the same bounded admission state instead of a lost tail"
    );
    assert!(
        disk_paths
            .iter()
            .all(|path| Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()),
        "the anchor remains until a successor snapshot can safely replace it"
    );
}

#[test]
fn mrf_journal_child_process_fixture() {
    let Ok(root) = std::env::var("RUSTFS_MRF_REPLAY_CHILD_ROOT") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime should build");
    runtime.block_on(async {
        let (disk_paths, _storage) = heal_env_at(Some(Path::new(&root))).await;
        let mut journal = journal_record(1, "child-restart-bucket", "first-object", None, 0);
        journal.extend(journal_record(1, "child-restart-bucket", "second-object", None, 0));
        write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &journal);
        write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &journal);
        assert!(
            journal_exists_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL),
            "child process must publish the authoritative MRF journal before exiting"
        );
    });
    std::process::exit(77);
}

#[test]
fn mrf_successor_flush_child_process_fixture() {
    let Ok(root) = std::env::var("RUSTFS_MRF_SUCCESSOR_FLUSH_CHILD_ROOT") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime should build");
    runtime.block_on(async {
        let (disk_paths, storage) = heal_env_at(Some(Path::new(&root))).await;
        register_local_disks(&disk_paths, "mrf-successor-flush-child").await;

        let mut startup = journal_record(1, "successor-bucket", "first-object", None, 0);
        startup.extend(journal_record(1, "successor-bucket", "second-object", None, 0));
        write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &startup);
        write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &startup);

        let manager = Arc::new(HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                heal_interval: Duration::from_secs(3600),
                enable_auto_heal: false,
                ..Default::default()
            }),
        ));
        mrf_queue::spawn_mrf_consumer(manager.clone());
        let expected_successor = journal_record(1, "successor-bucket", "second-object", None, 2);
        let flushed = wait_until(Duration::from_secs(10), || async {
            manager.operations_snapshot().await.queued_by_source.mrf == 1
                && journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &expected_successor)
                && journal_matches_on_all_disks(&disk_paths, JOURNAL_REL, &expected_successor)
        })
        .await;
        assert!(
            flushed,
            "child process must publish the pending successor snapshot before the delete phase"
        );
    });
    std::process::exit(78);
}

#[test]
#[cfg(unix)]
fn mrf_successor_flush_waiting_child_process_fixture() {
    let Ok(root) = std::env::var("RUSTFS_MRF_SUCCESSOR_KILL_CHILD_ROOT") else {
        return;
    };
    let ready_path = std::env::var("RUSTFS_MRF_SUCCESSOR_KILL_READY")
        .map(PathBuf::from)
        .expect("ready marker path should be provided");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime should build");
    runtime.block_on(async {
        let (disk_paths, storage) = heal_env_at(Some(Path::new(&root))).await;
        register_local_disks(&disk_paths, "mrf-successor-kill-child").await;

        let mut startup = journal_record(1, "service-kill-bucket", "first-object", None, 0);
        startup.extend(journal_record(1, "service-kill-bucket", "second-object", None, 0));
        write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &startup);
        write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &startup);

        let manager = Arc::new(HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                heal_interval: Duration::from_secs(3600),
                enable_auto_heal: false,
                ..Default::default()
            }),
        ));
        mrf_queue::spawn_mrf_consumer(manager.clone());
        let expected_successor = journal_record(1, "service-kill-bucket", "second-object", None, 2);
        let flushed = wait_until(Duration::from_secs(10), || async {
            manager.operations_snapshot().await.queued_by_source.mrf == 1
                && journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &expected_successor)
                && journal_matches_on_all_disks(&disk_paths, JOURNAL_REL, &expected_successor)
        })
        .await;
        assert!(
            flushed,
            "child process must publish the pending successor snapshot before it can be killed"
        );
        std::fs::write(&ready_path, b"ready").expect("write ready marker");
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

#[test]
#[cfg(unix)]
fn mrf_authoritative_fsync_waiting_child_process_fixture() {
    let Ok(root) = std::env::var("RUSTFS_MRF_FSYNC_KILL_CHILD_ROOT") else {
        return;
    };
    let ready_path = std::env::var("RUSTFS_MRF_FSYNC_KILL_READY")
        .map(PathBuf::from)
        .expect("ready marker path should be provided");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime should build");
    runtime.block_on(async {
        let (disk_paths, _storage) = heal_env_at(Some(Path::new(&root))).await;
        register_local_disks(&disk_paths, "mrf-fsync-kill-child").await;

        let mut startup = journal_record(1, "fsync-kill-bucket", "first-object", None, 0);
        startup.extend(journal_record(1, "fsync-kill-bucket", "second-object", None, 0));
        write_journal_path_to_disks(&disk_paths, SCOPED_JOURNAL_REL, &startup);
        write_journal_path_to_disks(&disk_paths, JOURNAL_REL, &startup);

        let successor = journal_record(1, "fsync-kill-bucket", "second-object", None, 2);
        write_journal_path_to_disks_synced(&disk_paths, SCOPED_JOURNAL_REL, &successor);
        assert!(
            journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &successor)
                && journal_matches_on_all_disks(&disk_paths, JOURNAL_REL, &startup),
            "child process must reach the canonical-fsync/stale-legacy boundary"
        );
        std::fs::write(&ready_path, b"ready").expect("write ready marker");
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

/// A journal published by a different OS process must remain a durable anchor
/// when the restarted process can only admit a prefix of the replayed intents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn journal_replay_retains_child_process_anchor_when_manager_is_full() {
    let temp_dir = tempfile::tempdir().expect("child process MRF root");
    let status = Command::new(std::env::current_exe().expect("test binary path"))
        .arg("mrf_journal_child_process_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .env("RUSTFS_MRF_REPLAY_CHILD_ROOT", temp_dir.path())
        .status()
        .expect("child MRF fixture should start");
    assert_eq!(status.code(), Some(77), "child process did not reach the MRF journal boundary");

    let (disk_paths, storage) = heal_env_at(Some(temp_dir.path())).await;
    assert!(
        journal_exists_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL),
        "restarted process must see the authoritative MRF journal left by the child"
    );

    let restarted = Arc::new(HealManager::new(
        storage,
        Some(HealConfig {
            queue_size: 1,
            heal_interval: Duration::from_secs(3600),
            enable_auto_heal: false,
            ..Default::default()
        }),
    ));
    let replayed = mrf_queue::replay_journal_once(&restarted).await;
    assert_eq!(replayed, 2, "the restarted process must decode the complete child journal");
    assert_eq!(
        restarted.operations_snapshot().await.queued_by_source.mrf,
        1,
        "bounded admission may accept only the prefix, but must not lose the replayed tail"
    );
    assert!(
        journal_exists_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL),
        "replay must retain the child-published journal until a successor snapshot can replace it"
    );
}

/// If a process crashes after flushing a smaller successor snapshot but before
/// deleting the startup anchor, the restarted process must replay the
/// successor tail rather than losing it or merging it with stale records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn journal_replay_survives_successor_flush_before_delete() {
    let temp_dir = tempfile::tempdir().expect("successor-flush MRF root");
    let status = Command::new(std::env::current_exe().expect("test binary path"))
        .arg("mrf_successor_flush_child_process_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .env("RUSTFS_MRF_SUCCESSOR_FLUSH_CHILD_ROOT", temp_dir.path())
        .status()
        .expect("child MRF successor fixture should start");
    assert_eq!(status.code(), Some(78), "child process did not reach the successor flush boundary");

    let (disk_paths, storage) = heal_env_at(Some(temp_dir.path())).await;
    let expected_successor = journal_record(1, "successor-bucket", "second-object", None, 2);
    assert!(
        journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &expected_successor),
        "restarted process must see the pending successor snapshot"
    );

    let restarted = make_manager(storage);
    let replayed = mrf_queue::replay_journal_once(&restarted).await;
    assert_eq!(replayed, 1, "restart after successor flush must replay only the still-pending tail");
    assert_eq!(
        restarted.operations_snapshot().await.queued_by_source.mrf,
        1,
        "the successor tail must be accepted after restart"
    );
    assert!(
        disk_paths.iter().all(|path| {
            !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
                && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
        }),
        "a fully consumed successor snapshot may be deleted after restart replay"
    );
}

/// A service-style hard kill after successor flush must be equivalent to a
/// crash at the flush-before-delete boundary: restart may replay the smaller
/// successor snapshot, but must not lose or merge stale startup records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[cfg(unix)]
async fn journal_replay_survives_service_kill_after_successor_flush() {
    let temp_dir = tempfile::tempdir().expect("successor-kill MRF root");
    let ready = temp_dir.path().join("successor-flushed.ready");
    let mut child = Command::new(std::env::current_exe().expect("test binary path"))
        .arg("mrf_successor_flush_waiting_child_process_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .env("RUSTFS_MRF_SUCCESSOR_KILL_CHILD_ROOT", temp_dir.path())
        .env("RUSTFS_MRF_SUCCESSOR_KILL_READY", &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("child MRF successor fixture should start");
    let ready_seen = wait_until(Duration::from_secs(10), || {
        let ready = ready.clone();
        async move { ready.exists() }
    })
    .await;
    assert!(ready_seen, "child process did not reach the successor flush boundary");
    child.kill().expect("kill child fixture");
    let status = child.wait().expect("wait for killed child fixture");
    assert!(!status.success(), "child fixture must be terminated instead of exiting cleanly");

    let (disk_paths, storage) = heal_env_at(Some(temp_dir.path())).await;
    let expected_successor = journal_record(1, "service-kill-bucket", "second-object", None, 2);
    assert!(
        journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &expected_successor),
        "restarted process must see the successor snapshot produced before the kill"
    );

    let restarted = make_manager(storage);
    let replayed = mrf_queue::replay_journal_once(&restarted).await;
    assert_eq!(replayed, 1, "restart after service kill must replay only the still-pending tail");
    assert_eq!(
        restarted.operations_snapshot().await.queued_by_source.mrf,
        1,
        "the successor tail must be accepted after service kill restart"
    );
    assert!(
        disk_paths.iter().all(|path| {
            !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
                && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
        }),
        "a fully consumed successor snapshot may be deleted after service-kill restart replay"
    );
}

/// A hard kill between the authoritative successor fsync and the legacy mirror
/// rewrite must prefer the canonical successor tail over the stale legacy
/// startup epoch. This models the mixed-version boundary conservatively: new
/// readers must not merge epochs, while the old mirror remains crash-visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[cfg(unix)]
async fn journal_replay_survives_sigkill_after_authoritative_successor_fsync_before_legacy_mirror() {
    let temp_dir = tempfile::tempdir().expect("fsync-kill MRF root");
    let ready = temp_dir.path().join("authoritative-synced.ready");
    let mut child = Command::new(std::env::current_exe().expect("test binary path"))
        .arg("mrf_authoritative_fsync_waiting_child_process_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .env("RUSTFS_MRF_FSYNC_KILL_CHILD_ROOT", temp_dir.path())
        .env("RUSTFS_MRF_FSYNC_KILL_READY", &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("child MRF fsync fixture should start");
    let ready_seen = wait_until(Duration::from_secs(10), || {
        let ready = ready.clone();
        async move { ready.exists() }
    })
    .await;
    assert!(ready_seen, "child process did not reach the authoritative fsync boundary");
    child.kill().expect("kill child fixture");
    let status = child.wait().expect("wait for killed child fixture");
    assert!(!status.success(), "child fixture must be terminated instead of exiting cleanly");

    let (disk_paths, storage) = heal_env_at(Some(temp_dir.path())).await;
    let expected_successor = journal_record(1, "fsync-kill-bucket", "second-object", None, 2);
    let stale_startup = {
        let mut startup = journal_record(1, "fsync-kill-bucket", "first-object", None, 0);
        startup.extend(journal_record(1, "fsync-kill-bucket", "second-object", None, 0));
        startup
    };
    assert!(
        journal_matches_on_all_disks(&disk_paths, SCOPED_JOURNAL_REL, &expected_successor),
        "restarted process must see the fsynced authoritative successor"
    );
    assert!(
        journal_matches_on_all_disks(&disk_paths, JOURNAL_REL, &stale_startup),
        "legacy mirror intentionally remains at the stale startup epoch"
    );

    let restarted = make_manager(storage);
    let replayed = mrf_queue::replay_journal_once(&restarted).await;
    assert_eq!(replayed, 1, "new reader must replay only the authoritative successor tail");
    assert_eq!(
        restarted.operations_snapshot().await.queued_by_source.mrf,
        1,
        "the successor tail must be accepted after the fsync-boundary restart"
    );
    assert!(
        disk_paths.iter().all(|path| {
            !Path::new(path).join(META_BUCKET).join(JOURNAL_REL).exists()
                && !Path::new(path).join(META_BUCKET).join(SCOPED_JOURNAL_REL).exists()
        }),
        "a fully consumed authoritative successor may clean both epochs after restart replay"
    );
}
