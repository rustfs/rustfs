// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Regression tests: a Swift metadata POST must be persisted to the bucket
//! metadata file, not just the in-memory cache. The metadata has to survive
//! the disk-truth reloads performed by peer LoadBucketMetadata notifications
//! and the periodic refresh loop — and, transitively, a process restart.
//!
//! Durability is what makes the *content* of those writes matter, so this file
//! also covers Swift's additive account/container POST semantics: an item the
//! request does not name keeps its stored value, and removal is explicit. The
//! two are tested together because a reload is the only way to tell a real
//! merge from one that happened to look right in the cache.

#![recursion_limit = "256"]
#![cfg(feature = "swift")]

use std::collections::HashMap;

use rustfs_credentials::Credentials;
use rustfs_protocols::swift::container::{ContainerMapper, update_container_metadata};
use rustfs_protocols::swift::{MetadataUpdate, SwiftError, account, container};
use rustfs_test_utils::TestECStoreEnv;
use serde_json::json;
use sha2::{Digest, Sha256};

mod ecstore_test_compat;
use ecstore_test_compat::{get_bucket_metadata, load_bucket_metadata, object_store_handle, set_bucket_metadata};

fn keystone_credentials(project_id: &str) -> Credentials {
    let mut claims = HashMap::new();
    claims.insert("keystone_project_id".to_string(), json!(project_id));
    claims.insert("keystone_roles".to_string(), json!(["member"]));

    Credentials {
        access_key: "keystone:swift-test".to_string(),
        claims: Some(claims),
        ..Default::default()
    }
}

/// The account-metadata bucket name scheme from `swift::account`
/// (`swift-account-{sha256(account)[0..16]}`), mirrored here so the test can
/// reload that bucket's metadata from disk.
fn account_metadata_bucket_name(account: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(account.as_bytes());
    let hash = hex::encode(hasher.finalize());
    format!("swift-account-{}", &hash[0..16])
}

/// Replace the cached bucket metadata with what is actually on disk — the
/// same thing a peer LoadBucketMetadata notification or the periodic refresh
/// loop does. Before the fix this silently discarded every Swift metadata
/// POST, because those writes only ever touched the cache.
async fn reload_bucket_metadata_from_disk(bucket: &str) {
    let store = object_store_handle().expect("test store should be published");
    let bm = load_bucket_metadata(store, bucket)
        .await
        .expect("bucket metadata should load from disk");
    set_bucket_metadata(bucket.to_string(), bm)
        .await
        .expect("reloaded metadata should install");
}

/// The `swift-meta-*` tags currently persisted for a container, keyed the way
/// a Swift client sees them. `get_container_metadata` would be the natural
/// reader, but it additionally requires a data-usage snapshot for the object
/// count and byte total, which a bare test store has none of — and that is
/// orthogonal to whether the metadata itself was persisted.
async fn persisted_container_metadata(bucket: &str) -> HashMap<String, String> {
    let bm = get_bucket_metadata(bucket).await.expect("bucket metadata should be cached");
    let mut out = HashMap::new();
    if let Some(tagging) = &bm.tagging_config {
        for tag in &tagging.tag_set {
            if let (Some(key), Some(value)) = (&tag.key, &tag.value)
                && let Some(meta_key) = key.strip_prefix("swift-meta-")
            {
                out.insert(meta_key.to_string(), value.clone());
            }
        }
    }
    out
}

/// Every scenario that needs a store runs against ONE environment: the Swift
/// handlers resolve the ambient object store and bucket-metadata system, so a
/// second `TestECStoreEnv` in this process would race the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swift_metadata_writes_are_durable() {
    let env = TestECStoreEnv::builder().prefix("swift_meta_persist").build().await;

    posts_survive_disk_truth_reload(&env).await;
    tag_writers_preserve_each_others_state(&env).await;
    unrelated_account_posts_preserve_the_tempurl_key().await;
    acl_only_posts_leave_container_metadata_alone(&env).await;
    versioning_writes_reject_missing_containers().await;
}

async fn posts_survive_disk_truth_reload(env: &TestECStoreEnv) {
    // --- Container metadata POST (X-Container-Meta-*) ---
    let project_id = "swiftpersistproj";
    let swift_account = format!("AUTH_{project_id}");
    let credentials = keystone_credentials(project_id);
    let swift_container = "photos";
    let bucket = ContainerMapper::default().swift_to_s3_bucket(swift_container, project_id);
    env.make_bucket(&bucket, false).await;

    update_container_metadata(
        &swift_account,
        swift_container,
        &credentials,
        MetadataUpdate::default().set("color", "blue"),
    )
    .await
    .expect("container metadata POST should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    let container_meta = persisted_container_metadata(&bucket).await;
    assert_eq!(
        container_meta.get("color").map(String::as_str),
        Some("blue"),
        "container metadata POST must survive a disk-truth metadata reload"
    );

    // A follow-up POST names a different item. Swift POSTs are additive, so
    // the new item lands and the first one stays — and both are still there
    // after a reload, which is what proves the merge ran against disk state
    // rather than looking right in the cache.
    update_container_metadata(
        &swift_account,
        swift_container,
        &credentials,
        MetadataUpdate::default().set("season", "summer"),
    )
    .await
    .expect("second container metadata POST should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    let container_meta = persisted_container_metadata(&bucket).await;
    assert_eq!(container_meta.get("season").map(String::as_str), Some("summer"));
    assert_eq!(
        container_meta.get("color").map(String::as_str),
        Some("blue"),
        "a POST that does not name an item must not delete it"
    );

    // X-Remove-Container-Meta-Color is the deletion path, and it has to be
    // durable in the other direction: the removed item must not come back
    // from the persisted tags on reload.
    update_container_metadata(&swift_account, swift_container, &credentials, MetadataUpdate::default().remove("color"))
        .await
        .expect("container metadata removal should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    let container_meta = persisted_container_metadata(&bucket).await;
    assert!(
        !container_meta.contains_key("color"),
        "an explicitly removed item must not resurrect on reload"
    );
    assert_eq!(
        container_meta.get("season").map(String::as_str),
        Some("summer"),
        "removing one item must not disturb the others"
    );

    // --- Container versioning POST (X-Versions-Location) ---
    let archive_container = "photos-archive";
    let archive_bucket = ContainerMapper::default().swift_to_s3_bucket(archive_container, project_id);
    env.make_bucket(&archive_bucket, false).await;

    container::enable_versioning(&swift_account, swift_container, archive_container, &credentials)
        .await
        .expect("enable versioning should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    let location = container::get_versions_location(&swift_account, swift_container, &credentials)
        .await
        .expect("versions location should load");
    assert_eq!(
        location.as_deref(),
        Some(archive_container),
        "versions location must survive a disk-truth metadata reload"
    );

    // --- Account metadata POST (TempURL keys etc.) ---
    account::update_account_metadata(
        &swift_account,
        &MetadataUpdate::default().set("temp-url-key", "s3cr3t"),
        &Some(credentials.clone()),
    )
    .await
    .expect("account metadata POST should succeed");

    reload_bucket_metadata_from_disk(&account_metadata_bucket_name(&swift_account)).await;

    let loaded = account::get_account_metadata(&swift_account, &None)
        .await
        .expect("account metadata should load");
    assert_eq!(
        loaded.get("temp-url-key").map(String::as_str),
        Some("s3cr3t"),
        "account metadata POST must survive a disk-truth metadata reload"
    );
}

/// The rewrites all share one tag set, so a closure that ignored the current
/// state would still pass a single-feature test. Drive ACLs, versioning and
/// container metadata over the same container and assert each survives the
/// others — and that clearing one leaves the rest alone.
async fn tag_writers_preserve_each_others_state(env: &TestECStoreEnv) {
    let project_id = "swiftcrosstagproj";
    let swift_account = format!("AUTH_{project_id}");
    let credentials = keystone_credentials(project_id);
    let container = "shared";
    let archive = "shared-archive";
    let bucket = ContainerMapper::default().swift_to_s3_bucket(container, project_id);
    env.make_bucket(&bucket, false).await;
    env.make_bucket(&ContainerMapper::default().swift_to_s3_bucket(archive, project_id), false)
        .await;

    update_container_metadata(&swift_account, container, &credentials, MetadataUpdate::default().set("color", "blue"))
        .await
        .expect("container metadata POST should succeed");
    container::enable_versioning(&swift_account, container, archive, &credentials)
        .await
        .expect("enable versioning should succeed");
    container::set_container_acl(&swift_account, container, Some(".r:*"), Some("AUTH_other"), &credentials)
        .await
        .expect("set container ACL should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    // All three writers' state coexists after a disk-truth reload.
    let meta = persisted_container_metadata(&bucket).await;
    assert_eq!(meta.get("color").map(String::as_str), Some("blue"));
    assert_eq!(
        container::get_versions_location(&swift_account, container, &credentials)
            .await
            .expect("versions location should load")
            .as_deref(),
        Some(archive)
    );
    let acl = container::get_container_acl(&swift_account, container, &credentials)
        .await
        .expect("container ACL should load");
    assert!(!acl.read.is_empty(), "read ACL must survive the reload");
    assert!(!acl.write.is_empty(), "write ACL must survive the reload");

    // Disabling versioning drops only the versioning tag.
    container::disable_versioning(&swift_account, container, &credentials)
        .await
        .expect("disable versioning should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    assert_eq!(
        container::get_versions_location(&swift_account, container, &credentials)
            .await
            .expect("versions location should load"),
        None,
        "disable_versioning must clear the versioning tag durably"
    );
    let meta = persisted_container_metadata(&bucket).await;
    assert_eq!(
        meta.get("color").map(String::as_str),
        Some("blue"),
        "disable_versioning must not disturb container metadata"
    );
    let acl = container::get_container_acl(&swift_account, container, &credentials)
        .await
        .expect("container ACL should load");
    assert!(!acl.read.is_empty(), "disable_versioning must not disturb the ACL");
}

/// The failure additive POSTs exist to prevent. Account metadata holds the
/// TempURL signing key, so a POST that replaced the whole set — setting a
/// quota, say — would delete that key and permanently invalidate every
/// outstanding TempURL and FormPost signature for the account. Durable
/// storage made that loss permanent instead of a cache blip, so the check
/// runs against reloaded disk state.
///
/// Relies on the ambient store the caller's `TestECStoreEnv` published; the
/// account metadata bucket is created by the write path itself.
async fn unrelated_account_posts_preserve_the_tempurl_key() {
    let project_id = "swiftmergeproj";
    let swift_account = format!("AUTH_{project_id}");
    let credentials = Some(keystone_credentials(project_id));
    let account_bucket = account_metadata_bucket_name(&swift_account);

    account::update_account_metadata(&swift_account, &MetadataUpdate::default().set("temp-url-key", "s3cr3t"), &credentials)
        .await
        .expect("TempURL key POST should succeed");

    // A later POST that has nothing to do with the TempURL key.
    account::update_account_metadata(&swift_account, &MetadataUpdate::default().set("quota-bytes", "100"), &credentials)
        .await
        .expect("quota POST should succeed");

    reload_bucket_metadata_from_disk(&account_bucket).await;

    let loaded = account::get_account_metadata(&swift_account, &None)
        .await
        .expect("account metadata should load");
    assert_eq!(
        loaded.get("temp-url-key").map(String::as_str),
        Some("s3cr3t"),
        "an unrelated account POST must not delete the TempURL signing key"
    );
    assert_eq!(loaded.get("quota-bytes").map(String::as_str), Some("100"));

    // And the lookup that actually breaks when the key is lost still resolves.
    assert_eq!(
        account::get_tempurl_key(&swift_account, &None)
            .await
            .expect("TempURL key should load")
            .as_deref(),
        Some("s3cr3t"),
        "TempURL signature validation must still find the key"
    );

    // X-Remove-Account-Meta-Quota-Bytes drops that item and nothing else.
    account::update_account_metadata(&swift_account, &MetadataUpdate::default().remove("quota-bytes"), &credentials)
        .await
        .expect("account metadata removal should succeed");

    reload_bucket_metadata_from_disk(&account_bucket).await;

    let loaded = account::get_account_metadata(&swift_account, &None)
        .await
        .expect("account metadata should load");
    assert!(
        !loaded.contains_key("quota-bytes"),
        "an explicitly removed item must not resurrect on reload"
    );
    assert_eq!(
        loaded.get("temp-url-key").map(String::as_str),
        Some("s3cr3t"),
        "removing one item must not disturb the TempURL key"
    );
}

/// An ACL-only or versioning-only container POST carries no `X-Container-Meta-*`
/// header, but the handler still runs the metadata step on every POST. That
/// step must leave stored metadata alone rather than treating "named nothing"
/// as "wants everything gone".
async fn acl_only_posts_leave_container_metadata_alone(env: &TestECStoreEnv) {
    let project_id = "swiftaclonlyproj";
    let swift_account = format!("AUTH_{project_id}");
    let credentials = keystone_credentials(project_id);
    let container = "gallery";
    let bucket = ContainerMapper::default().swift_to_s3_bucket(container, project_id);
    env.make_bucket(&bucket, false).await;

    update_container_metadata(&swift_account, container, &credentials, MetadataUpdate::default().set("color", "blue"))
        .await
        .expect("container metadata POST should succeed");

    // What the handler does for `POST … -H 'X-Container-Read: .r:*'`: set the
    // ACL, then run the metadata step with an update that names no item.
    container::set_container_acl(&swift_account, container, Some(".r:*"), None, &credentials)
        .await
        .expect("ACL-only POST should succeed");
    update_container_metadata(&swift_account, container, &credentials, MetadataUpdate::default())
        .await
        .expect("the metadata step of an ACL-only POST should succeed");

    reload_bucket_metadata_from_disk(&bucket).await;

    assert_eq!(
        persisted_container_metadata(&bucket).await.get("color").map(String::as_str),
        Some("blue"),
        "an ACL-only POST must not wipe X-Container-Meta-*"
    );
    let acl = container::get_container_acl(&swift_account, container, &credentials)
        .await
        .expect("container ACL should load");
    assert!(!acl.read.is_empty(), "the ACL that POST set must be stored");
}

/// A container that does not exist must not get metadata persisted for it:
/// the metadata loader turns "nothing on disk" into a fresh default, so an
/// unguarded rewrite would create an orphan metadata file and cache a
/// fabricated default as authoritative.
async fn versioning_writes_reject_missing_containers() {
    let project_id = "swiftmissingproj";
    let swift_account = format!("AUTH_{project_id}");
    let credentials = keystone_credentials(project_id);
    let missing = "no-such-container";

    let err = container::disable_versioning(&swift_account, missing, &credentials)
        .await
        .expect_err("disabling versioning on a missing container must fail");
    assert!(
        matches!(err, SwiftError::NotFound(_)),
        "expected NotFound for a missing container, got {err:?}"
    );

    // Naming no item skips the persisted write, but must not skip the
    // existence check that turns a POST to a missing container into a 404.
    let err = update_container_metadata(&swift_account, missing, &credentials, MetadataUpdate::default())
        .await
        .expect_err("a metadata POST to a missing container must fail");
    assert!(
        matches!(err, SwiftError::NotFound(_)),
        "expected NotFound for a missing container, got {err:?}"
    );

    let bucket = ContainerMapper::default().swift_to_s3_bucket(missing, project_id);
    assert!(
        get_bucket_metadata(&bucket).await.is_err(),
        "a rejected write must not have cached metadata for a nonexistent container"
    );
}

/// Account metadata holds the account's TempURL signing key, and it is now
/// durable — so a write for someone else's account would be a persistent,
/// cluster-wide takeover of that account's pre-signed URLs, not a cache blip.
/// The write path must reject both a foreign account and a missing token,
/// while reads stay open for pre-auth TempURL signature validation.
///
/// Deliberately builds no store: both rejections must happen before the write
/// path resolves storage at all, and a second `TestECStoreEnv` in this process
/// would race the other test over the ambient store handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_metadata_write_rejects_foreign_and_anonymous_callers() {
    let victim_account = "AUTH_victimproject";
    let attacker_credentials = keystone_credentials("attackerproject");

    let poisoned = MetadataUpdate::default().set("temp-url-key", "attacker-key");

    let err = account::update_account_metadata(victim_account, &poisoned, &Some(attacker_credentials))
        .await
        .expect_err("writing another account's metadata must be rejected");
    assert!(
        matches!(err, SwiftError::Forbidden(_)),
        "cross-account metadata write must be Forbidden, got {err:?}"
    );

    let err = account::update_account_metadata(victim_account, &poisoned, &None)
        .await
        .expect_err("anonymous account metadata write must be rejected");
    assert!(
        matches!(err, SwiftError::Unauthorized(_)),
        "anonymous metadata write must be Unauthorized, got {err:?}"
    );
}
