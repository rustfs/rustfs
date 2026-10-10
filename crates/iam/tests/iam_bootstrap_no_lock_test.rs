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

use ecstore_test_compat::fixture::{
    ECStore, InstanceContext, SetupType, first_cluster_node_is_local, object_store_handle, save_config,
    set_object_store_resolver, update_erasure_type,
};
use rustfs_credentials::{Credentials, IAM_POLICY_CLAIM_NAME_SA};
use rustfs_iam::cache::Cache;
use rustfs_iam::manager::{IamCache, IamState};
use rustfs_iam::store::object::{IAM_CONFIG_USERS_PREFIX, ObjectStore};
use rustfs_iam::store::{GroupInfo, Store, UserType};
use rustfs_policy::auth::UserIdentity;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64};
use std::sync::{Arc, Mutex, OnceLock, Weak};

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
        let _route_guard = TestStoreRouteGuard::new(&ecstore);
        let store = ObjectStore::new(ecstore.clone());

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

const IDENTITY_PARENT: &str = "fixture-parent";
const IDENTITY_CHILD: &str = "fixture-service-child";

static TEST_STORE_ROUTE: Mutex<Option<Weak<ECStore>>> = Mutex::new(None);
static TEST_STORE_RESOLVER: OnceLock<()> = OnceLock::new();

struct TestStoreRouteGuard;

impl TestStoreRouteGuard {
    fn new(ecstore: &Arc<ECStore>) -> Self {
        TEST_STORE_RESOLVER.get_or_init(|| {
            assert!(
                set_object_store_resolver(Arc::new(|| {
                    TEST_STORE_ROUTE
                        .lock()
                        .expect("lock test store route")
                        .as_ref()
                        .and_then(Weak::upgrade)
                })),
                "test binary must own its resolver registration"
            );
        });
        let mut route = TEST_STORE_ROUTE.lock().expect("lock test store route");
        assert!(route.is_none(), "another test store route is active");
        *route = Some(Arc::downgrade(ecstore));
        Self
    }
}

impl Drop for TestStoreRouteGuard {
    fn drop(&mut self) {
        *TEST_STORE_ROUTE.lock().expect("lock test store route for cleanup") = None;
    }
}

struct IdentityStoreFixture {
    previous_store: Option<Arc<ECStore>>,
    ecstore: Arc<ECStore>,
    manager: IamCache<ObjectStore>,
    _route_guard: TestStoreRouteGuard,
    _temp_dir: tempfile::TempDir,
}

impl IdentityStoreFixture {
    async fn new() -> Self {
        let temp_dir = tempfile::TempDir::with_prefix("rustfs_iam_identity_fixture_").expect("create identity fixture directory");
        let ctx = Arc::new(InstanceContext::new());
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .instance_ctx(ctx.clone())
            .build()
            .await;
        ctx.set_endpoints(env.endpoint_pools.clone());
        let ecstore = env.ecstore;
        let previous_store = object_store_handle();
        let route_guard = TestStoreRouteGuard::new(&ecstore);
        let selected = object_store_handle().expect("fixture route selects its ECStore");
        assert!(Arc::ptr_eq(&selected, &ecstore), "fixture route must select the isolated ECStore");
        drop(selected);
        assert!(first_cluster_node_is_local().await, "isolated fixture has a local first endpoint");
        temp_env::async_with_vars([("RUSTFS_SKIP_BACKGROUND_TASK", Some("1"))], rustfs_iam::build_iam_sys(ecstore.clone()))
            .await
            .expect("initialize IAM metadata through production owner");
        let store = ObjectStore::new(ecstore.clone());
        let parent = UserIdentity::new(Credentials {
            access_key: IDENTITY_PARENT.to_string(),
            secret_key: "fixture-parent-secret".to_string(),
            expiration: None,
            session_token: String::new(),
            status: "on".to_string(),
            ..Default::default()
        });
        let child = UserIdentity::new(Credentials {
            access_key: IDENTITY_CHILD.to_string(),
            secret_key: "fixture-child-secret".to_string(),
            expiration: None,
            session_token: String::new(),
            status: "on".to_string(),
            parent_user: IDENTITY_PARENT.to_string(),
            claims: Some(HashMap::from([(
                IAM_POLICY_CLAIM_NAME_SA.to_string(),
                serde_json::json!("inherited-policy"),
            )])),
            ..Default::default()
        });
        assert!(child.credentials.is_service_account(), "child fixture must be a service account");
        store
            .save_user_identity(IDENTITY_PARENT, UserType::Reg, parent, None)
            .await
            .expect("persist parent identity");
        store
            .save_user_identity(IDENTITY_CHILD, UserType::Svc, child, None)
            .await
            .expect("persist service child identity");
        let manager = IamCache {
            cache: Cache::default(),
            api: store,
            state: Arc::new(AtomicU8::new(IamState::Ready as u8)),
            loading: Arc::new(AtomicBool::new(false)),
            roles: HashMap::new(),
            send_chan: tokio::sync::mpsc::channel::<i64>(1).0,
            last_timestamp: AtomicI64::new(0),
            sync_failures: AtomicU64::new(0),
            sync_successes: AtomicU64::new(0),
            last_sync_duration_millis: AtomicU64::new(0),
        };
        manager
            .load_user(IDENTITY_PARENT)
            .await
            .expect("load persisted parent into cache");
        manager
            .load_user(IDENTITY_CHILD)
            .await
            .expect("load persisted service child into cache");
        Self {
            previous_store,
            ecstore,
            manager,
            _route_guard: route_guard,
            _temp_dir: temp_dir,
        }
    }

    async fn assert_parent_and_child(&self) {
        let parent = self
            .manager
            .api
            .load_user_identity(IDENTITY_PARENT, UserType::Reg)
            .await
            .expect("read parent from real storage");
        assert_eq!(parent.credentials.access_key, IDENTITY_PARENT);
        assert_eq!(parent.credentials.parent_user, "");
        let child = self
            .manager
            .api
            .load_user_identity(IDENTITY_CHILD, UserType::Svc)
            .await
            .expect("read child from real storage");
        assert_eq!(child.credentials.access_key, IDENTITY_CHILD);
        assert_eq!(child.credentials.parent_user, IDENTITY_PARENT);
        assert!(child.credentials.is_service_account());
        let cached_parent = self.manager.get_user(IDENTITY_PARENT).await.expect("parent remains in cache");
        assert_eq!(cached_parent.credentials.access_key, IDENTITY_PARENT);
        assert_eq!(cached_parent.credentials.parent_user, "");
        let cached_child = self
            .manager
            .get_user(IDENTITY_CHILD)
            .await
            .expect("service child remains in cache");
        assert_eq!(cached_child.credentials.access_key, IDENTITY_CHILD);
        assert_eq!(cached_child.credentials.parent_user, IDENTITY_PARENT);
        assert!(cached_child.credentials.is_service_account());
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn identity_store_fixture_roundtrips_parent_and_service_child() {
    let fixture = IdentityStoreFixture::new().await;
    let before = fixture.previous_store.clone();
    let selected = object_store_handle().expect("fixture route selects its ECStore");
    assert!(Arc::ptr_eq(&selected, &fixture.ecstore));
    drop(selected);
    fixture.assert_parent_and_child().await;
    drop(fixture);
    assert!(TEST_STORE_ROUTE.lock().expect("inspect cleared test store route").is_none());
    match (before, object_store_handle()) {
        (None, None) => {}
        (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, &after), "fixture drop restores prior store identity"),
        _ => panic!("fixture drop must restore the prior store option"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn identity_notification_valid_parent_preserves_child_storage_and_cache() {
    let fixture = IdentityStoreFixture::new().await;
    fixture.assert_parent_and_child().await;
    fixture
        .manager
        .user_notification_handler(IDENTITY_PARENT, UserType::Reg)
        .await
        .expect("refresh valid parent notification");
    fixture.assert_parent_and_child().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn identity_store_fixture_raw_valid_overwrite_is_observed() {
    let fixture = IdentityStoreFixture::new().await;
    let parent = fixture
        .manager
        .api
        .load_user_identity(IDENTITY_PARENT, UserType::Reg)
        .await
        .expect("load valid parent before raw overwrite");
    let mut raw = serde_json::to_value(parent).expect("serialize valid parent fixture");
    raw["credentials"]["name"] = serde_json::json!("updated-name");
    let path = format!("{}{IDENTITY_PARENT}/identity.json", IAM_CONFIG_USERS_PREFIX.as_str());
    save_config(
        fixture.ecstore.clone(),
        &path,
        serde_json::to_vec(&raw).expect("encode raw parent fixture"),
    )
    .await
    .expect("overwrite parent through real config storage");
    fixture
        .manager
        .load_user(IDENTITY_PARENT)
        .await
        .expect("reload parent after raw overwrite");
    let stored = fixture
        .manager
        .api
        .load_user_identity(IDENTITY_PARENT, UserType::Reg)
        .await
        .expect("read overwritten parent from storage");
    assert_eq!(stored.credentials.name.as_deref(), Some("updated-name"));
    let cached = fixture
        .manager
        .get_user(IDENTITY_PARENT)
        .await
        .expect("read overwritten parent from cache");
    assert_eq!(cached.credentials.name.as_deref(), Some("updated-name"));
    fixture.assert_parent_and_child().await;
}
