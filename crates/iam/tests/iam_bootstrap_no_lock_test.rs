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

mod ecstore_test_compat;

use ecstore_test_compat::fixture::{SetupType, update_erasure_type};
use rustfs_iam::cache::Cache;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::store::{GroupInfo, Store};
use serial_test::serial;
use std::collections::HashMap;

const TEST_GROUP: &str = "seq-restart-group";
const TEST_MEMBERS: [&str; 2] = ["alice", "bob"];

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
        // Shared temp-disk ECStore env (rustfs-test-utils, backlog#1153 infra-1).
        // base_dir keeps cleanup ownership with this TempDir; the historical
        // bootstrap never initialized the bucket-metadata system, so opt out.
        let ecstore = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .build()
            .await
            .ecstore;
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

        // P3 step 1: notification-path cache refreshes use the same lock-free
        // reads, so a cross-node notification must also be able to refresh
        // this group while the lock quorum is unavailable.
        let mut notification_read = HashMap::new();
        store
            .load_group_no_lock(TEST_GROUP, &mut notification_read)
            .await
            .expect("lock-free notification-path load_group must succeed while the lock quorum is unavailable");
        assert_eq!(notification_read[TEST_GROUP].members, TEST_MEMBERS);

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
