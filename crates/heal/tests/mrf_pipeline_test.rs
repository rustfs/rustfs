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
use std::{path::Path, sync::Arc, time::Duration};

mod storage_api;

use storage_api::endpoint_index::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints, init_local_disks};

const META_BUCKET: &str = ".rustfs.sys";
const JOURNAL_REL: &str = "buckets/.heal/mrf/journal.bin";
const SCOPED_JOURNAL_REL: &str = "buckets/.heal/mrf/journal-scoped.bin";

async fn heal_env() -> (Vec<std::path::PathBuf>, Arc<dyn HealStorageAPI>) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_heal_mrf_test")
        .build()
        .await;
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

fn write_journal_path_to_disks(disk_paths: &[std::path::PathBuf], relative_path: &str, data: &[u8]) {
    for path in disk_paths {
        let journal = path.join(META_BUCKET).join(relative_path);
        std::fs::create_dir_all(journal.parent().expect("journal parent")).expect("create journal dir");
        std::fs::write(&journal, data).expect("write journal fixture");
    }
}

fn write_journal_to_disks(disk_paths: &[std::path::PathBuf], data: &[u8]) {
    write_journal_path_to_disks(disk_paths, JOURNAL_REL, data);
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
        cmd_line: "mrf-test".to_string(),
        platform: String::new(),
    };
    init_local_disks(EndpointServerPools::from(vec![pool]))
        .await
        .expect("local disks should register");

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
        cmd_line: "mrf-authoritative-test".to_string(),
        platform: String::new(),
    };
    init_local_disks(EndpointServerPools::from(vec![pool]))
        .await
        .expect("local disks should register");

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
}
