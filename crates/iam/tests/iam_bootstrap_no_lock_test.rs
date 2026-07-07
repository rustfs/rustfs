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

//! Regression test for rustfs#4304: IAM bootstrap must not depend on the
//! distributed namespace-lock quorum.
//!
//! During a sequential cluster restart the peer lock RPC endpoints are
//! unreachable, so every namespace-locked read fails with
//! `QuorumNotReached` even though the storage read quorum is already
//! satisfiable. The bulk snapshot load (`Store::load_all`) therefore has to
//! read with `no_lock = true` (startup contract rustfs#4056).
//!
//! The test builds a real 4-disk ECStore over temp dirs, seeds IAM data in
//! single-node mode, then flips the runtime into distributed-erasure mode.
//! In that mode `SetDisks::new_ns_lock` builds a distributed lock over the
//! set's lock clients — which are empty for a locally-built store — so every
//! locked read fails exactly like the sequential-restart scenario, while
//! plain storage reads keep working. The old (locked) `load_group` path must
//! fail and the lock-free `load_all` path must succeed.

use rustfs_ecstore::api::disk::endpoint::Endpoint;
use rustfs_ecstore::api::global::update_erasure_type;
use rustfs_ecstore::api::layout::{EndpointServerPools, Endpoints, PoolEndpoints, SetupType};
use rustfs_ecstore::api::storage::{ECStore, init_local_disks};
use rustfs_iam::cache::Cache;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::store::{GroupInfo, Store};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const TEST_GROUP: &str = "seq-restart-group";
const TEST_MEMBERS: [&str; 2] = ["alice", "bob"];

async fn build_local_ecstore(temp_dir: &std::path::Path) -> Arc<ECStore> {
    let disk_paths: Vec<_> = (1..=4).map(|i| temp_dir.join(format!("disk{i}"))).collect();
    for disk_path in &disk_paths {
        tokio::fs::create_dir_all(disk_path).await.unwrap();
    }

    let mut endpoints = Vec::new();
    for (i, disk_path) in disk_paths.iter().enumerate() {
        let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(i);
        endpoints.push(endpoint);
    }

    let pool_endpoints = PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 4,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "test".to_string(),
        platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
    };
    let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints]);

    init_local_disks(endpoint_pools.clone()).await.unwrap();

    // Port 0 keeps this integration binary parallel-safe alongside other
    // ECStore-backed tests.
    let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
        .await
        .unwrap()
}

/// Restores single-node erasure mode even when an assertion panics, so a
/// failing run cannot poison later `#[serial]` tests in this process.
struct ErasureModeGuard;

impl Drop for ErasureModeGuard {
    fn drop(&mut self) {
        pollster::block_on(update_erasure_type(SetupType::Erasure));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn load_all_bypasses_namespace_lock_quorum() {
    // The lock acquire timeout is latched into a OnceLock on first use, so it
    // must be shortened before the first locked operation of this process.
    temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
        let temp_dir = tempfile::TempDir::with_prefix("rustfs_iam_no_lock_test_").unwrap();
        let ecstore = build_local_ecstore(temp_dir.path()).await;
        let store = ObjectStore::new(ecstore);

        // Seed IAM data while namespace locks still work (single-node mode).
        store
            .save_group_info(TEST_GROUP, GroupInfo::new(TEST_MEMBERS.iter().map(|m| m.to_string()).collect()))
            .await
            .expect("seeding group info in single-node mode must succeed");

        let mut baseline = HashMap::new();
        store
            .load_group(TEST_GROUP, &mut baseline)
            .await
            .expect("locked load_group must succeed in single-node mode");
        assert_eq!(baseline[TEST_GROUP].members, TEST_MEMBERS, "seeded group must round-trip");

        // Flip into distributed-erasure mode: new_ns_lock now builds a
        // distributed lock over the set's (empty) lock-client list, so every
        // locked read fails — the sequential-restart failure mode of
        // rustfs#4304 (lock quorum unavailable, storage quorum healthy).
        update_erasure_type(SetupType::DistErasure).await;
        let _mode_guard = ErasureModeGuard;

        let mut locked_read = HashMap::new();
        let locked_err = store
            .load_group(TEST_GROUP, &mut locked_read)
            .await
            .expect_err("locked load_group must fail while the lock quorum is unavailable");
        assert!(locked_read.is_empty(), "failed locked read must not populate results");

        // The P0 fix: the bulk snapshot load reads with no_lock = true, so it
        // must succeed in exactly the state where the locked path fails.
        let cache = Cache::default();
        store
            .load_all(&cache)
            .await
            .unwrap_or_else(|err| panic!("load_all must bypass namespace locks (locked path failed with: {locked_err}): {err}"));

        // Back in single-node mode the locked path works again; verify the
        // data survived the whole exercise intact (fail-closed integrity).
        drop(_mode_guard);
        let mut recovered = HashMap::new();
        store
            .load_group(TEST_GROUP, &mut recovered)
            .await
            .expect("locked load_group must succeed again after restoring single-node mode");
        assert_eq!(recovered[TEST_GROUP].members, TEST_MEMBERS, "group data must be intact");
    })
    .await;
}
