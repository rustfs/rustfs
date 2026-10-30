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

use super::*;
use crate::bucket::lifecycle::core::IlmAction;
use crate::bucket::lifecycle::evaluator::Evaluator;
use crate::bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_VERSIONING_CONFIG};
use crate::bucket::metadata_sys;
use crate::disk::lifecycle_test_io::Observer;
use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
use crate::object_api::PutObjReader;
use crate::runtime::instance::InstanceContext;
use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
use crate::storage_api_contracts::multipart::{CompletePart, MultipartOperations};
use crate::storage_api_contracts::object::ObjectIO;
use crate::storage_api_contracts::object::ObjectOperations as _;
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use tempfile::TempDir;
use time::OffsetDateTime;

async fn fixture(pool_count: usize) -> (Vec<TempDir>, Arc<ECStore>, String, Uuid) {
    let mut dirs = Vec::new();
    let mut pools = Vec::new();
    for pool_idx in 0..pool_count {
        let mut endpoints = Vec::new();
        for disk_idx in 0..4 {
            let dir = tempfile::tempdir().expect("disk directory");
            let mut endpoint =
                crate::disk::endpoint::Endpoint::try_from(dir.path().to_str().expect("disk path")).expect("disk endpoint");
            endpoint.set_pool_index(pool_idx);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_idx);
            dirs.push(dir);
            endpoints.push(endpoint);
        }
        pools.push(PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: format!("lifecycle-exact-pool-{pool_idx}"),
            platform: "test".into(),
        });
    }
    let endpoints = EndpointServerPools(pools);
    let ctx = Arc::new(InstanceContext::new());
    crate::store::init_local_disks_with_instance_ctx(&ctx, endpoints.clone())
        .await
        .expect("initialize disks");
    let store =
        ECStore::new_with_instance_ctx("127.0.0.1:0".parse().expect("test address"), endpoints, CancellationToken::new(), ctx)
            .await
            .expect("initialize store");
    metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
    let bucket = format!("lifecycle-exact-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let incarnation = metadata_sys::get_bucket_incarnation_id_in(&store.ctx, &bucket)
        .await
        .expect("bucket incarnation");
    (dirs, store, bucket, incarnation)
}

fn version(object: &str, id: Option<Uuid>, timestamp: i64) -> FileInfo {
    let mut fi = FileInfo::new(object, 2, 2);
    fi.name = object.to_owned();
    fi.version_id = id;
    fi.versioned = id.is_some();
    fi.data_dir = Some(Uuid::from_u128(0x1234));
    fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(timestamp).expect("version timestamp"));
    fi.size = 1;
    fi.parts = vec![ObjectPartInfo {
        number: 1,
        size: 1,
        actual_size: 1,
        ..Default::default()
    }];
    fi.metadata.insert("etag".into(), timestamp.to_string());
    fi
}

async fn seed(dirs: &[TempDir], bucket: &str, object: &str, versions: &[FileInfo]) {
    for (index, dir) in dirs.iter().enumerate() {
        let mut meta = FileMeta::new();
        for version in versions {
            let mut fi = version.clone();
            fi.volume = bucket.to_owned();
            if !fi.is_canonical_delete_marker() {
                fi.erasure.index = index % 4 + 1;
            }
            fi.validate_for_metadata_read().expect("valid fixture version");
            meta.add_version(fi).expect("encode version");
        }
        let path = dir.path().join(bucket).join(path::encode_dir_object(object));
        tokio::fs::create_dir_all(&path).await.expect("object directory");
        tokio::fs::write(path.join(crate::disk::STORAGE_FORMAT_FILE), meta.marshal_msg().expect("encode metadata"))
            .await
            .expect("persist fixture metadata");
    }
}

async fn configure_versioning(store: &ECStore, bucket: &str, state: &str) {
    if state == "Unversioned" {
        return;
    }
    metadata_sys::update_in(
        &store.ctx,
        bucket,
        BUCKET_VERSIONING_CONFIG,
        format!("<VersioningConfiguration><Status>{state}</Status></VersioningConfiguration>").into_bytes(),
    )
    .await
    .expect("persist versioning configuration");
}

#[tokio::test]
async fn lifecycle_exact_matches_listing_for_complete_version_groups() {
    let (dirs, store, bucket, incarnation) = fixture(1).await;
    for state in ["Unversioned", "Enabled", "Suspended"] {
        configure_versioning(&store, &bucket, state).await;
        for object in ["abc", "nested/abc", "directory/"] {
            let mut versions = vec![version(object, None, 1_700_000_000)];
            if state != "Unversioned" {
                versions.push(version(object, Some(Uuid::from_u128(1)), 1_700_000_001));
                versions.push(FileInfo {
                    name: object.into(),
                    version_id: Some(Uuid::from_u128(2)),
                    deleted: true,
                    mod_time: Some(OffsetDateTime::from_unix_timestamp(1_700_000_002).expect("marker timestamp")),
                    ..Default::default()
                });
            }
            seed(&dirs, &bucket, object, &versions).await;
            let sibling = format!("{object}-sibling");
            seed(&dirs, &bucket, &sibling, &[version(&sibling, None, 1_700_000_003)]).await;
            let observed = Observer::new(&bucket);
            let exact = store
                .get_object_versions_for_lifecycle(&bucket, object, &ObjectOptions::default(), incarnation)
                .await
                .unwrap_or_else(|error| panic!("read {state} versions for {object}: {error}"));
            let counts = observed.snapshot();
            assert_eq!(counts.walks, 0, "exact metadata reads must never enumerate a directory");
            assert_eq!(counts.reads.len(), 4, "one exact metadata read per physical drive");
            assert!(counts.reads.iter().all(|key| key == &path::encode_dir_object(object)));
            let mut listed = store
                .clone()
                .list_object_versions_for_lifecycle(&bucket, object, None, None, None, 1000)
                .await
                .expect("reference lifecycle listing")
                .objects
                .into_iter()
                .filter(|info| info.name == object)
                .collect::<Vec<_>>();
            // The legacy list projection uses ambient bucket versioning. This
            // fixture has an isolated store; restore its known null-version
            // representation before comparing the version-group semantics.
            if state != "Unversioned" {
                for info in &mut listed {
                    if info.version_id.is_none() {
                        info.version_id = Some(Uuid::nil());
                    }
                }
                assert!(exact.iter().any(|info| info.version_id == Some(Uuid::nil())));
            }
            assert!(observed.snapshot().walks > 0, "the old prefix-listing path must exercise the observer");
            assert_eq!(exact.len(), versions.len(), "{state}: {object}");
            assert_eq!(exact.len(), listed.len());
            for (exact, listed) in exact.iter().zip(&listed) {
                assert_eq!(
                    (
                        exact.version_id,
                        exact.is_latest,
                        exact.delete_marker,
                        exact.num_versions,
                        exact.successor_mod_time
                    ),
                    (
                        listed.version_id,
                        listed.is_latest,
                        listed.delete_marker,
                        listed.num_versions,
                        listed.successor_mod_time
                    )
                );
                assert_eq!(exact.user_defined, listed.user_defined);
                assert_eq!(exact.version_purge_status, listed.version_purge_status);
            }
            let policy = crate::bucket::utils::deserialize(
                br#"<LifecycleConfiguration><Rule><ID>history</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>30</Days></Expiration><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#,
            ).expect("reference policy");
            let evaluator = Evaluator::new(Arc::new(policy));
            let to_opts = |objects: &[ObjectInfo]| {
                objects
                    .iter()
                    .map(crate::bucket::lifecycle::core::object_opts_from_object_info)
                    .collect::<Vec<_>>()
            };
            let exact_events = evaluator.eval(&to_opts(&exact)).await.expect("evaluate exact group");
            let listed_events = evaluator.eval(&to_opts(&listed)).await.expect("evaluate listed group");
            for (exact, listed) in exact_events.iter().zip(listed_events) {
                assert_eq!((exact.action, exact.due, &exact.rule_id), (listed.action, listed.due, &listed.rule_id));
            }
        }
    }
}

#[tokio::test]
async fn lifecycle_exact_merges_pools_and_more_than_one_page_of_versions() {
    let (dirs, store, bucket, incarnation) = fixture(2).await;
    configure_versioning(&store, &bucket, "Enabled").await;
    let object = "history";
    let versions = (0..1002)
        .map(|index| {
            version(
                object,
                Some(Uuid::from_u128(index + 1)),
                1_700_000_000 + i64::try_from(index).expect("timestamp index"),
            )
        })
        .collect::<Vec<_>>();
    seed(&dirs[..4], &bucket, object, &versions[..601]).await;
    seed(&dirs[4..], &bucket, object, &versions[600..]).await;
    let observed = Observer::new(&bucket);
    let exact = store
        .get_object_versions_for_lifecycle(&bucket, object, &ObjectOptions::default(), incarnation)
        .await
        .expect("merge independently authoritative pools");
    assert_eq!(exact.len(), 1002, "overlapping migration copies must be deduplicated without pagination");
    assert_eq!(exact[0].version_id, Some(Uuid::from_u128(1002)));
    assert!(exact[0].is_latest);
    assert!(exact.iter().all(|info| info.num_versions == 1002));
    for pair in exact.windows(2) {
        assert!(!pair[1].is_latest);
        assert_eq!(pair[1].successor_mod_time, pair[0].mod_time);
    }
    assert_eq!(observed.snapshot().walks, 0);
    assert_eq!(observed.snapshot().reads.len(), 8, "read count depends on drives, not version pages");
    let mut conflict = versions[600].clone();
    conflict.metadata.insert("etag".into(), "conflicting-content".into());
    seed(&dirs[4..], &bucket, object, &[conflict]).await;
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &ObjectOptions::default(), incarnation)
            .await
            .is_err(),
        "conflicting cross-pool copies cannot authorize expiry"
    );
}

#[tokio::test]
async fn lifecycle_exact_rejects_uncertain_metadata_and_stale_bucket_identity() {
    let (dirs, store, bucket, incarnation) = fixture(1).await;
    let object = "probe";
    let opts = ObjectOptions::default();
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &opts, incarnation)
            .await
            .expect("confirmed absence")
            .is_empty()
    );
    seed(&dirs, &bucket, object, &[version(object, None, 1_700_000_000)]).await;
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &opts, Uuid::new_v4())
            .await
            .is_err()
    );
    for dir in &dirs {
        tokio::fs::write(
            dir.path()
                .join(bucket.as_str())
                .join(object)
                .join(crate::disk::STORAGE_FORMAT_FILE),
            b"corrupt",
        )
        .await
        .expect("corrupt fixture metadata");
    }
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &opts, incarnation)
            .await
            .is_err(),
        "corruption is not absence"
    );
    seed(&dirs, &bucket, object, &[version(object, None, 1_700_000_000)]).await;
    let set = &store.pools[0].disk_set[0];
    let saved = set.disks.read().await.clone();
    *set.disks.write().await = vec![saved[0].clone(), saved[1].clone(), None, None];
    assert_eq!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &opts, incarnation)
            .await
            .expect("exactly the object's read quorum")
            .len(),
        1
    );
    *set.disks.write().await = vec![saved[0].clone(), None, None, None];
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &opts, incarnation)
            .await
            .is_err(),
        "a minority cannot authorize expiry"
    );
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, "missing", &opts, incarnation)
            .await
            .is_err(),
        "missing on one drive does not prove absence"
    );
    *set.disks.write().await = saved;
    let lost = ObjectOptions {
        no_lock: true,
        namespace_lock_fence: Some(crate::object_api::NamespaceLockFence::lost_for_test()),
        ..Default::default()
    };
    assert!(
        store
            .get_object_versions_for_lifecycle(&bucket, object, &lost, incarnation)
            .await
            .is_err(),
        "an inherited lost object fence must reject the snapshot"
    );
}

#[tokio::test]
async fn lifecycle_exact_write_hooks_never_walk_parent_directories() {
    let (dirs, store, bucket, _) = fixture(1).await;
    metadata_sys::update_in(&store.ctx, &bucket, BUCKET_LIFECYCLE_CONFIG,
        br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec())
        .await.expect("configure expiration");
    for index in 0..64 {
        let key = format!("probe-{index}");
        seed(&dirs, &bucket, &key, &[version(&key, None, 1_700_000_000)]).await;
    }
    for state in ["Unversioned", "Enabled", "Suspended"] {
        configure_versioning(&store, &bucket, state).await;
        let opts = ObjectOptions {
            versioned: state == "Enabled",
            version_suspended: state == "Suspended",
            ..Default::default()
        };
        let observer = Observer::new(&bucket);
        let mut source = store
            .put_object(&bucket, "probe", &mut PutObjReader::from_vec(b"payload".to_vec()), &opts)
            .await
            .expect("PUT with lifecycle");
        source.put_object_reader = Some(PutObjReader::from_vec(b"payload".to_vec()));
        let copied = store
            .copy_object(&bucket, "probe", &bucket, "copy", &mut source, &ObjectOptions::default(), &opts)
            .await
            .expect("COPY with lifecycle");
        assert_eq!(copied.size, source.size);
        let upload = store
            .new_multipart_upload(&bucket, "multipart", &opts)
            .await
            .expect("init multipart");
        let part = store
            .put_object_part(
                &bucket,
                "multipart",
                &upload.upload_id,
                1,
                &mut PutObjReader::from_vec(b"part".to_vec()),
                &opts,
            )
            .await
            .expect("upload part");
        let complete = store
            .clone()
            .complete_multipart_upload(
                &bucket,
                "multipart",
                &upload.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: part.etag,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                    checksum_crc64nvme: None,
                }],
                &opts,
            )
            .await
            .expect("complete multipart with lifecycle");
        assert_eq!(complete.size, 4);
        let counts = observer.snapshot();
        assert_eq!(counts.walks, 0, "{state}: successful writes must not run the version walker");
        for key in ["probe", "copy", "multipart"] {
            assert!(
                counts.reads.iter().filter(|read| read.as_str() == key).count() >= 4,
                "{state}: {key} must run exact expiry evaluation"
            );
        }
        assert!(
            !counts.reads.iter().any(|key| key.starts_with("probe-")),
            "prefix siblings must never enter evaluation"
        );
    }
}

#[tokio::test]
async fn lifecycle_exact_keeps_pending_purges_visible_to_expiry_guards() {
    let (dirs, store, bucket, incarnation) = fixture(1).await;
    configure_versioning(&store, &bucket, "Enabled").await;
    let object = "pending";
    let (entry, _) = super::test::test_pending_version_purge_meta_entry(&bucket, object);
    for dir in &dirs {
        let path = dir.path().join(&bucket).join(object);
        tokio::fs::create_dir_all(&path).await.expect("object directory");
        tokio::fs::write(path.join(crate::disk::STORAGE_FORMAT_FILE), &entry.metadata)
            .await
            .expect("persist pending purge");
    }
    let exact = store
        .get_object_versions_for_lifecycle(&bucket, object, &ObjectOptions::default(), incarnation)
        .await
        .expect("read pending purge");
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].num_versions, 1);
    assert_eq!(exact[0].version_purge_status, crate::bucket::replication::VersionPurgeStatusType::Pending);
    let policy = crate::bucket::utils::deserialize(
        br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#,
    ).expect("expiration policy");
    let opts = crate::bucket::lifecycle::core::object_opts_from_object_info(&exact[0]);
    let events = Evaluator::new(Arc::new(policy))
        .eval(&[opts])
        .await
        .expect("evaluate pending purge");
    assert_eq!(events[0].action, IlmAction::NoneAction);
}

#[tokio::test]
async fn lifecycle_exact_io_is_independent_of_parent_directory_width() {
    let (dirs, store, bucket, incarnation) = fixture(1).await;
    let object = "probe";
    seed(&dirs, &bucket, object, &[version(object, None, 1_700_000_000)]).await;
    let mut previous = 0;
    for width in [64, 1024, 8192] {
        let roots = dirs.iter().map(|dir| dir.path().join(&bucket)).collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            for root in roots {
                for index in previous..width {
                    std::fs::create_dir(root.join(format!("sibling-{index:05}"))).expect("parent-directory entry");
                }
            }
        })
        .await
        .expect("create wide directory");
        previous = width;
        let mut exact_times = Vec::new();
        let mut listing_times = Vec::new();
        for _ in 0..5 {
            let observer = Observer::new(&bucket);
            let started = std::time::Instant::now();
            let exact = store
                .get_object_versions_for_lifecycle(&bucket, object, &ObjectOptions::default(), incarnation)
                .await
                .expect("exact lifecycle read");
            exact_times.push(started.elapsed().as_micros());
            assert_eq!(exact.len(), 1);
            assert_eq!(observer.snapshot().walks, 0);
            assert_eq!(observer.snapshot().reads.len(), 4, "directory width {width}");
            let started = std::time::Instant::now();
            let listed = store
                .clone()
                .list_object_versions_for_lifecycle(&bucket, object, None, None, None, 1000)
                .await
                .expect("reference prefix scan");
            listing_times.push(started.elapsed().as_micros());
            assert_eq!(listed.objects.len(), 1);
            assert!(observer.snapshot().walks > 0);
        }
        exact_times.sort_unstable();
        listing_times.sort_unstable();
        eprintln!(
            "lifecycle directory width={width}: exact median={}us max={}us; prefix median={}us max={}us",
            exact_times[2], exact_times[4], listing_times[2], listing_times[4]
        );
    }
}
