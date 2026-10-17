use super::{build_isolated_test_store, without_storage_class_env};
use crate::{
    bucket::{
        metadata::{
            BUCKET_INCARNATION_FILE, BUCKET_METADATA_FILE, BUCKET_TAGGING_CONFIG, BUCKET_VERSIONING_CONFIG, BucketMetadata,
            load_bucket_metadata_parse_with_presence,
        },
        metadata_sys,
        versioning::VersioningApi,
    },
    disk::{BUCKET_META_PREFIX, DiskAPI as _, RUSTFS_META_BUCKET},
    runtime::instance::InstanceContext,
    storage_api_contracts::{
        bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions},
        namespace::NamespaceLocking as _,
    },
    store::ECStore,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PREFIX: &str = "outputs/";
const ENABLED: &[u8] = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
const SUSPENDED: &[u8] = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
const EXCLUDED_PREFIX: &[u8] = b"<VersioningConfiguration><Status>Enabled</Status><ExcludedPrefixes><Prefix>outputs/</Prefix></ExcludedPrefixes></VersioningConfiguration>";
const EXCLUDED_FOLDERS: &[u8] =
    b"<VersioningConfiguration><Status>Enabled</Status><ExcludeFolders>true</ExcludeFolders></VersioningConfiguration>";
const TAGGING: &[u8] = b"<Tagging><TagSet><Tag><Key>purpose</Key><Value>artifact-test</Value></Tag></TagSet></Tagging>";

async fn open_store(root: &std::path::Path) -> (Arc<InstanceContext>, Arc<ECStore>, CancellationToken) {
    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root, "artifact-bucket", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    (ctx, store, shutdown)
}

async fn drain_before_reopen(store: &ECStore, shutdown: &CancellationToken, bucket: &str) {
    shutdown.cancel();
    assert!(
        store.ctx.wait_for_detached_mutations(Duration::from_secs(30)).await,
        "ARTIFACT_PREVIOUS_STORE_MUTATIONS_DRAINED"
    );
    // Internal metadata PUTs can acknowledge quorum before their disk tails finish.
    // Acquire each key separately to wait for its actual writer to release the lock.
    for file in [BUCKET_METADATA_FILE, BUCKET_INCARNATION_FILE] {
        let key = format!("{BUCKET_META_PREFIX}/{bucket}/{file}");
        let namespace = store.pools[0].disk_set[0]
            .new_ns_lock(RUSTFS_META_BUCKET, &key)
            .await
            .expect("resolve persisted identity namespace");
        drop(
            namespace
                .get_write_lock(Duration::from_secs(30))
                .await
                .expect("identity write tail drained"),
        );
    }
}

async fn dedicated_bucket(store: &ECStore, bucket: &str) {
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                artifact_base_prefix: Some(PREFIX.to_owned()),
                // Creation must enforce versioning without relying on the request flag.
                versioning_enabled: false,
                ..Default::default()
            },
        )
        .await
        .expect("create a dedicated artifact bucket on real disks");
}

async fn persisted(store: &Arc<ECStore>, bucket: &str) -> BucketMetadata {
    let (metadata, exists) = load_bucket_metadata_parse_with_presence(store.clone(), bucket, true)
        .await
        .expect("read actual persisted bucket metadata");
    assert!(exists, "ARTIFACT_METADATA_PERSISTED");
    metadata
}

fn assert_namespace(metadata: &BucketMetadata, incarnation: Uuid) {
    assert_eq!(metadata.artifact_base_prefix.as_deref(), Some(PREFIX), "ARTIFACT_NAMESPACE_IMMUTABLE");
    assert_eq!(metadata.bucket_incarnation_id, incarnation, "ARTIFACT_INCARNATION_IMMUTABLE");
    assert!(
        metadata.versioning_config.as_ref().is_some_and(VersioningApi::enabled),
        "ARTIFACT_VERSIONING_REQUIRED"
    );
}

async fn assert_persisted_and_cached(store: &Arc<ECStore>, bucket: &str, incarnation: Uuid) {
    assert_namespace(&persisted(store, bucket).await, incarnation);
    let cached = metadata_sys::get_in(&store.ctx, bucket)
        .await
        .expect("read cached bucket identity");
    assert_namespace(&cached, incarnation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_creation_enables_versioning_and_survives_store_reopen() {
    let root = tempfile::tempdir().expect("artifact bucket store directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-created-reopen";
    dedicated_bucket(&store, bucket).await;
    let created = persisted(&store, bucket).await;
    let incarnation = created.bucket_incarnation_id;
    assert!(!incarnation.is_nil(), "ARTIFACT_INCARNATION_REQUIRED");
    assert!(created.bucket_creation_committed, "ARTIFACT_CREATION_COMMITTED");
    assert_namespace(&created, incarnation);
    drain_before_reopen(&store, &shutdown, bucket).await;
    drop(store);
    drop(ctx);

    let (_ctx, store, shutdown) = open_store(root.path()).await;
    metadata_sys::reload_bucket_metadata(store.clone(), bucket)
        .await
        .expect("load the reopened bucket from persisted metadata");
    assert_persisted_and_cached(&store, bucket, incarnation).await;
    store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                force_create: true,
                ..Default::default()
            },
        )
        .await
        .expect("ordinary force-create replay preserves the dedicated namespace");
    assert_persisted_and_cached(&store, bucket, incarnation).await;
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_ordinary_configuration_update_preserves_namespace() {
    let root = tempfile::tempdir().expect("artifact configuration store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-config-update";
    dedicated_bucket(&store, bucket).await;
    let incarnation = persisted(&store, bucket).await.bucket_incarnation_id;
    metadata_sys::update_in(&store.ctx, bucket, BUCKET_TAGGING_CONFIG, TAGGING.to_vec())
        .await
        .expect("ordinary tagging update remains supported");
    let changed = persisted(&store, bucket).await;
    assert_eq!(changed.tagging_config_xml, TAGGING, "ARTIFACT_ORDINARY_CONFIG_UPDATED");
    assert_persisted_and_cached(&store, bucket, incarnation).await;
    let sys = store.ctx.bucket_metadata_sys().expect("instance metadata system");
    let sys = sys.read().await.clone();
    let suspended = crate::bucket::utils::deserialize(SUSPENDED).expect("valid suspended versioning XML");
    for stale_typed_config in [None, Some(suspended)] {
        let mut replacement = changed.clone();
        replacement.versioning_config = stale_typed_config;
        sys.set_validated(replacement)
            .await
            .expect("validated cache setter reparses authoritative XML");
        assert_persisted_and_cached(&store, bucket, incarnation).await;
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_create_rejects_existing_bucket_type_and_prefix_changes() {
    let root = tempfile::tempdir().expect("artifact immutable creation store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let ordinary = "artifact-existing-ordinary";
    store
        .make_bucket(ordinary, &MakeBucketOptions::default())
        .await
        .expect("create ordinary bucket");
    let original = persisted(&store, ordinary).await;
    assert!(original.artifact_base_prefix.is_none());
    for force_create in [false, true] {
        let result = store
            .make_bucket(
                ordinary,
                &MakeBucketOptions {
                    artifact_base_prefix: Some(PREFIX.to_owned()),
                    force_create,
                    ..Default::default()
                },
            )
            .await;
        assert!(result.is_err(), "ARTIFACT_NAMESPACE_IMMUTABLE: ordinary bucket cannot change type");
        let unchanged = persisted(&store, ordinary).await;
        assert_eq!(unchanged.artifact_base_prefix, None);
        assert_eq!(unchanged.bucket_incarnation_id, original.bucket_incarnation_id);
        assert_eq!(unchanged.versioning_config_xml, original.versioning_config_xml);
    }
    let dedicated = "artifact-existing-dedicated";
    dedicated_bucket(&store, dedicated).await;
    let incarnation = persisted(&store, dedicated).await.bucket_incarnation_id;
    for force_create in [false, true] {
        let result = store
            .make_bucket(
                dedicated,
                &MakeBucketOptions {
                    artifact_base_prefix: Some("another/".to_owned()),
                    force_create,
                    ..Default::default()
                },
            )
            .await;
        assert!(result.is_err(), "ARTIFACT_NAMESPACE_IMMUTABLE: create cannot replace an existing prefix");
        assert_persisted_and_cached(&store, dedicated, incarnation).await;
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_full_metadata_replacement_cannot_change_namespace() {
    let root = tempfile::tempdir().expect("artifact metadata replacement store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let dedicated = "artifact-replacement-dedicated";
    let ordinary = "artifact-replacement-ordinary";
    dedicated_bucket(&store, dedicated).await;
    store
        .make_bucket(ordinary, &MakeBucketOptions::default())
        .await
        .expect("create ordinary replacement fixture");
    let original = persisted(&store, dedicated).await;
    let incarnation = original.bucket_incarnation_id;
    let sys = store.ctx.bucket_metadata_sys().expect("instance metadata system");
    let sys = sys.read().await.clone();
    for route in 0..3 {
        for prefix in [None, Some("replacement/".to_owned())] {
            let mut replacement = original.clone();
            replacement.artifact_base_prefix = prefix;
            let result = match route {
                0 => metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await,
                1 => sys.persist_and_set(replacement).await,
                _ => sys.set_validated(replacement).await,
            };
            assert!(
                result.is_err(),
                "ARTIFACT_NAMESPACE_IMMUTABLE: complete replacement cannot remove or change prefix"
            );
            assert_persisted_and_cached(&store, dedicated, incarnation).await;
        }
        let mut replacement = persisted(&store, ordinary).await;
        let ordinary_incarnation = replacement.bucket_incarnation_id;
        replacement.artifact_base_prefix = Some(PREFIX.to_owned());
        replacement.versioning_config_xml = ENABLED.to_vec();
        let result = match route {
            0 => metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await,
            1 => sys.persist_and_set(replacement).await,
            _ => sys.set_validated(replacement).await,
        };
        assert!(
            result.is_err(),
            "ARTIFACT_NAMESPACE_IMMUTABLE: complete replacement cannot promote ordinary bucket"
        );
        let unchanged = persisted(&store, ordinary).await;
        assert!(unchanged.artifact_base_prefix.is_none());
        assert_eq!(unchanged.bucket_incarnation_id, ordinary_incarnation);
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_versioning_cannot_be_suspended_or_removed() {
    let root = tempfile::tempdir().expect("artifact mandatory versioning store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-required-versioning";
    dedicated_bucket(&store, bucket).await;
    let original = persisted(&store, bucket).await;
    let incarnation = original.bucket_incarnation_id;
    let sys = store.ctx.bucket_metadata_sys().expect("instance metadata system");
    let sys = sys.read().await.clone();
    for xml in [SUSPENDED, b"".as_slice(), EXCLUDED_PREFIX, EXCLUDED_FOLDERS] {
        let result = metadata_sys::update_in(&store.ctx, bucket, BUCKET_VERSIONING_CONFIG, xml.to_vec()).await;
        assert!(
            result.is_err(),
            "ARTIFACT_VERSIONING_REQUIRED: configuration update cannot disable versioning"
        );
        assert_persisted_and_cached(&store, bucket, incarnation).await;
        for route in 0..3 {
            let mut replacement = original.clone();
            replacement.versioning_config_xml = xml.to_vec();
            replacement.versioning_config = None;
            let result = match route {
                0 => metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await,
                1 => sys.persist_and_set(replacement).await,
                _ => sys.set_validated(replacement).await,
            };
            assert!(
                result.is_err(),
                "ARTIFACT_VERSIONING_REQUIRED: complete replacement cannot disable versioning"
            );
            assert_persisted_and_cached(&store, bucket, incarnation).await;
        }
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_pending_creation_reopen_retry_preserves_namespace() {
    let root = tempfile::tempdir().expect("artifact pending creation store directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-pending-retry";
    let mut intent = BucketMetadata::new_with_default_durability(bucket);
    intent.artifact_base_prefix = Some(PREFIX.to_owned());
    intent.versioning_config_xml = ENABLED.to_vec();
    let incarnation = intent.bucket_incarnation_id;
    // Persist through the production intent writer, before any physical bucket exists.
    metadata_sys::set_new_bucket_metadata_intent_in(&ctx, intent)
        .await
        .expect("persist real creation intent");
    let pending = persisted(&store, bucket).await;
    assert!(!pending.bucket_creation_committed, "ARTIFACT_CREATION_PENDING");
    assert_namespace(&pending, incarnation);
    for disk in 0..4 {
        assert!(
            !root.path().join(format!("pool0/set0/disk{disk}/{bucket}")).exists(),
            "ARTIFACT_PHYSICAL_BUCKET_NOT_CREATED"
        );
    }
    drain_before_reopen(&store, &shutdown, bucket).await;
    drop(store);
    drop(ctx);

    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let result = store
        .make_bucket(
            bucket,
            &MakeBucketOptions {
                artifact_base_prefix: Some("replacement/".to_owned()),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err(), "ARTIFACT_NAMESPACE_IMMUTABLE: pending intent rejects a changed request");
    let pending = persisted(&store, bucket).await;
    assert_namespace(&pending, incarnation);
    assert!(!pending.bucket_creation_committed);
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("retry resumes persisted creation identity");
    let completed = persisted(&store, bucket).await;
    assert!(completed.bucket_creation_committed, "ARTIFACT_CREATION_COMMITTED");
    assert_persisted_and_cached(&store, bucket, incarnation).await;
    for disk in 0..4 {
        assert!(
            root.path().join(format!("pool0/set0/disk{disk}/{bucket}")).is_dir(),
            "ARTIFACT_PHYSICAL_BUCKET_CREATED"
        );
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_partial_volume_retry_preserves_pending_identity() {
    let root = tempfile::tempdir().expect("artifact partial volume store directory");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-partial-volume";
    let mut intent = BucketMetadata::new_with_default_durability(bucket);
    intent.artifact_base_prefix = Some(PREFIX.to_owned());
    intent.versioning_config_xml = ENABLED.to_vec();
    let incarnation = intent.bucket_incarnation_id;
    metadata_sys::set_new_bucket_metadata_intent_in(&ctx, intent)
        .await
        .expect("persist real creation intent before a partial physical creation");
    let disks = store.pools[0].disk_set[0].disk_inventory().await;
    disks[0]
        .as_ref()
        .expect("first real disk")
        .make_volume(bucket)
        .await
        .expect("create only one physical volume");
    assert!(root.path().join(format!("pool0/set0/disk0/{bucket}")).is_dir());
    for disk in 1..4 {
        assert!(!root.path().join(format!("pool0/set0/disk{disk}/{bucket}")).exists());
    }
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect_err("partial physical creation requires explicit recovery");
    let pending = persisted(&store, bucket).await;
    assert_namespace(&pending, incarnation);
    assert!(!pending.bucket_creation_committed, "ARTIFACT_PARTIAL_CREATION_STAYS_PENDING");
    for disk in 1..4 {
        assert!(
            !root.path().join(format!("pool0/set0/disk{disk}/{bucket}")).exists(),
            "ARTIFACT_PARTIAL_CREATION_NOT_SILENTLY_COMPLETED"
        );
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_metadata_replacement_cannot_spoof_incarnation() {
    let root = tempfile::tempdir().expect("artifact spoofed incarnation store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-spoofed-incarnation";
    dedicated_bucket(&store, bucket).await;
    let original = persisted(&store, bucket).await;
    let incarnation = original.bucket_incarnation_id;
    let sys = store.ctx.bucket_metadata_sys().expect("instance metadata system");
    let sys = sys.read().await.clone();
    for route in 0..4 {
        for spoofed in [Uuid::nil(), Uuid::new_v4()] {
            for (prefix, xml) in [
                (None, b"".as_slice()),
                (None, SUSPENDED),
                (Some("replacement/"), ENABLED),
                (Some(PREFIX), ENABLED),
            ] {
                let mut replacement = original.clone();
                replacement.bucket_incarnation_id = spoofed;
                replacement.artifact_base_prefix = prefix.map(str::to_owned);
                replacement.versioning_config_xml = xml.to_vec();
                replacement.versioning_config = None;
                let result = match route {
                    0 => metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await,
                    1 => sys.persist_and_set(replacement).await,
                    2 => sys.set_validated(replacement).await,
                    _ => replacement.save_with_store(store.clone()).await,
                };
                assert!(
                    result.is_err(),
                    "ARTIFACT_INCARNATION_IMMUTABLE: route={route}, spoofed={spoofed}, prefix={prefix:?}"
                );
                assert_persisted_and_cached(&store, bucket, incarnation).await;
            }
        }
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_real_delete_recreate_allows_new_namespace_and_incarnation() {
    let root = tempfile::tempdir().expect("artifact genuine recreation store directory");
    let (_ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-genuine-recreation";
    dedicated_bucket(&store, bucket).await;
    let mut incarnation = persisted(&store, bucket).await.bucket_incarnation_id;
    for prefix in [Some("replacement/"), None] {
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete the empty bucket through production storage");
        store
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    artifact_base_prefix: prefix.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .expect("genuine recreation may choose a new bucket namespace");
        let recreated = persisted(&store, bucket).await;
        assert!(!recreated.bucket_incarnation_id.is_nil(), "ARTIFACT_INCARNATION_REQUIRED");
        assert_ne!(recreated.bucket_incarnation_id, incarnation, "ARTIFACT_RECREATION_NEW_INCARNATION");
        assert_eq!(recreated.artifact_base_prefix.as_deref(), prefix, "ARTIFACT_RECREATION_NEW_NAMESPACE");
        assert_eq!(recreated.versioning_config.as_ref().is_some_and(VersioningApi::enabled), prefix.is_some());
        assert!(recreated.bucket_creation_committed, "ARTIFACT_CREATION_COMMITTED");
        let cached = metadata_sys::get_in(&store.ctx, bucket)
            .await
            .expect("recreated identity cached");
        assert_eq!(cached.bucket_incarnation_id, recreated.bucket_incarnation_id);
        assert_eq!(cached.artifact_base_prefix.as_deref(), prefix);
        incarnation = recreated.bucket_incarnation_id;
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_observes_current_generation() {
    let root = tempfile::tempdir().expect("creation observation storage");
    let (ctx, store, shutdown) = open_store(root.path()).await;
    let bucket = "artifact-create-observation";
    dedicated_bucket(&store, bucket).await;
    let initial = persisted(&store, bucket).await.bucket_incarnation_id;
    store
        .delete_bucket(bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete before observation");
    dedicated_bucket(&store, bucket).await;
    let current = persisted(&store, bucket).await.bucket_incarnation_id;
    assert_ne!(initial, current);
    // Invoke the exact observation phase consumed by make_artifact_bucket,
    // after real replacement between its creation and observation phases.
    assert_eq!(
        store
            .observe_artifact_bucket_incarnation(bucket, PREFIX)
            .await
            .expect("observe replacement"),
        current,
        "ARTIFACT_CREATE_CURRENT_GENERATION"
    );
    assert!(
        store.observe_artifact_bucket_incarnation(bucket, "other/").await.is_err(),
        "ARTIFACT_CREATE_CURRENT_GENERATION: mismatched namespace cannot produce a receipt"
    );
    let attempt = format!("{PREFIX}61/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    assert!(
        store
            .clone()
            .seal_artifact_attempt_puts(bucket, initial, &attempt)
            .await
            .is_err(),
        "ARTIFACT_CREATE_CURRENT_GENERATION: old incarnation cannot seal same prefix"
    );
    store
        .clone()
        .seal_artifact_attempt_puts(bucket, current, &attempt)
        .await
        .expect("current identity seals");
    drain_before_reopen(&store, &shutdown, bucket).await;
    drop(store);
    drop(ctx);
    let (_ctx, reopened, stop) = open_store(root.path()).await;
    assert_eq!(
        reopened
            .observe_artifact_bucket_incarnation(bucket, PREFIX)
            .await
            .expect("reopen observed identity"),
        current,
        "ARTIFACT_CREATE_CURRENT_GENERATION"
    );
    stop.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_observation_rejects_corrupt_authority() {
    use crate::{
        config::com::{read_config, save_config_with_opts},
        object_api::{ObjectOptions, WriteCompletion},
    };
    let root = tempfile::tempdir().expect("creation observation faults");
    let (mut ctx, mut store, mut shutdown) = open_store(root.path()).await;
    let bucket = "artifact-create-corruption";
    let incarnation = store
        .clone()
        .make_artifact_bucket(bucket, PREFIX)
        .await
        .expect("actual create and observation");
    let original = persisted(&store, bucket).await;
    let sys = ctx.bucket_metadata_sys().expect("actual metadata authority");
    // These faults target the authoritative cache branch actually read by the
    // observation. Public metadata updates reject them before publication.
    for fault in 0..6 {
        let mut damaged = original.clone();
        match fault {
            0 => damaged.name = "wrong-bucket".into(),
            1 => damaged.artifact_base_prefix = Some("other/".into()),
            2 => damaged.versioning_config = None,
            3 => damaged.versioning_config.as_mut().expect("versioning").exclude_folders = Some(true),
            4 => damaged.versioning_config = Some(crate::bucket::utils::deserialize(SUSPENDED).expect("suspended XML")),
            _ => {
                damaged.versioning_config = Some(crate::bucket::utils::deserialize(EXCLUDED_PREFIX).expect("excluded prefix XML"))
            }
        }
        sys.read().await.set_for_test(bucket.into(), Arc::new(damaged)).await;
        assert!(
            store.observe_artifact_bucket_incarnation(bucket, PREFIX).await.is_err(),
            "ARTIFACT_CREATE_CURRENT_GENERATION: corrupt cached authority fault={fault}"
        );
        sys.read().await.set_for_test(bucket.into(), Arc::new(original.clone())).await;
    }
    drop(sys);
    for (file, corrupt, expected_error) in [
        (BUCKET_METADATA_FILE, b"corrupt metadata".to_vec(), "read_bucket_metadata: format invalid"),
        (
            BUCKET_INCARNATION_FILE,
            Uuid::nil().as_bytes().to_vec(),
            "persisted bucket incarnation is nil",
        ),
    ] {
        let path = format!("{BUCKET_META_PREFIX}/{bucket}/{file}");
        let saved = read_config(store.clone(), &path).await.expect("real persisted record");
        let opts = ObjectOptions {
            max_parity: true,
            write_completion: WriteCompletion::TailDrained,
            ..Default::default()
        };
        save_config_with_opts(store.clone(), &path, corrupt, &opts)
            .await
            .expect("inject physical record fault");
        // Reopen with a fresh metadata authority so observation must read the
        // actual damaged record, without a cached value or a negative entry.
        drain_before_reopen(&store, &shutdown, bucket).await;
        drop(store);
        drop(ctx);
        (ctx, store, shutdown) = open_store(root.path()).await;
        let error = store
            .observe_artifact_bucket_incarnation(bucket, PREFIX)
            .await
            .expect_err("physical corruption cannot produce a receipt");
        assert!(
            error.to_string().contains(expected_error),
            "ARTIFACT_CREATE_CURRENT_GENERATION: corrupt physical authority {file}: {error}"
        );
        save_config_with_opts(store.clone(), &path, saved, &opts)
            .await
            .expect("restore physical record");
        drain_before_reopen(&store, &shutdown, bucket).await;
        drop(store);
        drop(ctx);
        (ctx, store, shutdown) = open_store(root.path()).await;
        assert_eq!(
            store
                .observe_artifact_bucket_incarnation(bucket, PREFIX)
                .await
                .expect("restored observation"),
            incarnation
        );
    }
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(storage_class_env)]
async fn artifact_bucket_create_requires_namespace_locking_before_creation() {
    temp_env::async_with_vars([("RUSTFS_LOCK_ENABLED", Some("false"))], async {
        let root = tempfile::tempdir().expect("disabled lock storage");
        let (ctx, store, shutdown) = open_store(root.path()).await;
        assert!(ctx.lock_manager().is_disabled());
        let bucket = "artifact-create-no-lock";
        let error = store
            .clone()
            .make_artifact_bucket(bucket, PREFIX)
            .await
            .expect_err("creation requires real locks");
        assert!(
            error
                .to_string()
                .contains("artifact bucket creation requires namespace locking")
        );
        assert!(
            store
                .get_bucket_info(bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                .await
                .is_err(),
            "ARTIFACT_CREATE_LOCK_REQUIRED: rejection precedes physical creation"
        );
        shutdown.cancel();
    })
    .await;
}
