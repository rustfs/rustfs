use super::{build_isolated_test_store, without_storage_class_env};
use crate::{
    bucket::{
        metadata::{BUCKET_TAGGING_CONFIG, BUCKET_VERSIONING_CONFIG, BucketMetadata, load_bucket_metadata_parse_with_presence},
        metadata_sys,
        versioning::VersioningApi,
    },
    runtime::instance::InstanceContext,
    storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions},
    store::ECStore,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PREFIX: &str = "outputs/";
const ENABLED: &[u8] = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
const SUSPENDED: &[u8] = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
const TAGGING: &[u8] = b"<Tagging><TagSet><Tag><Key>purpose</Key><Value>artifact-test</Value></Tag></TagSet></Tagging>";

async fn open_store(root: &std::path::Path) -> (Arc<InstanceContext>, Arc<ECStore>, CancellationToken) {
    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root, "artifact-bucket", &[4])).await;
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    (ctx, store, shutdown)
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
    shutdown.cancel();
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
    for direct in [false, true] {
        for prefix in [None, Some("replacement/".to_owned())] {
            let mut replacement = original.clone();
            replacement.artifact_base_prefix = prefix;
            let result = if direct {
                sys.persist_and_set(replacement).await
            } else {
                metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await
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
        let result = if direct {
            sys.persist_and_set(replacement).await
        } else {
            metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await
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
    for xml in [SUSPENDED, b"".as_slice()] {
        let result = metadata_sys::update_in(&store.ctx, bucket, BUCKET_VERSIONING_CONFIG, xml.to_vec()).await;
        assert!(
            result.is_err(),
            "ARTIFACT_VERSIONING_REQUIRED: configuration update cannot disable versioning"
        );
        assert_persisted_and_cached(&store, bucket, incarnation).await;
        for direct in [false, true] {
            let mut replacement = original.clone();
            replacement.versioning_config_xml = xml.to_vec();
            replacement.versioning_config = None;
            let result = if direct {
                sys.persist_and_set(replacement).await
            } else {
                metadata_sys::set_bucket_metadata_in(&store.ctx, replacement).await
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
    shutdown.cancel();
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
