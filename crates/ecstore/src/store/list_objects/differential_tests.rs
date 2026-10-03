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

use super::*;
use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
use crate::disk::STORAGE_FORMAT_FILE;
use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, DuplexStream};
use tokio::sync::Notify;
use tokio::time::timeout;

fn shared_keys() -> Vec<String> {
    serde_json::from_str(include_str!("../../../tests/fixtures/list_namespace_keys.json"))
        .expect("shared LIST namespace corpus should decode")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VersionKind {
    Object,
    Delete,
    Free,
}

#[derive(Clone, Debug)]
struct Version {
    id: Option<Uuid>,
    modified: i64,
    kind: VersionKind,
}

#[derive(Clone, Debug)]
struct Object {
    key: String,
    // Expected S3 version order is part of the input, including equal-time ties.
    versions: Vec<Version>,
}

fn namespace(keys: &[String]) -> Vec<Object> {
    keys.iter()
        .map(|key| Object {
            key: key.clone(),
            versions: vec![Version {
                id: None,
                modified: 1_705_312_300,
                kind: VersionKind::Object,
            }],
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct Page {
    objects: Vec<String>,
    prefixes: Vec<String>,
    next_key: Option<String>,
    truncated: bool,
}

// This oracle sees only the input graph. It never reads FileMeta or calls the
// production filtering, pagination, merge, or prefix helpers.
fn reference_page(objects: &[Object], prefix: &str, delimiter: Option<&str>, after: Option<&str>, max: usize) -> Page {
    let mut entries = BTreeMap::new();
    for object in objects {
        if !object.key.starts_with(prefix)
            || object
                .versions
                .iter()
                .find(|version| version.kind != VersionKind::Free)
                .is_none_or(|version| version.kind == VersionKind::Delete)
        {
            continue;
        }
        let (key, is_prefix) = match delimiter.filter(|delimiter| !delimiter.is_empty()).and_then(|delimiter| {
            object.key[prefix.len()..]
                .find(delimiter)
                .map(|offset| (object.key[..prefix.len() + offset + delimiter.len()].to_owned(), true))
        }) {
            Some(entry) => entry,
            None => (object.key.clone(), false),
        };
        if after.is_none_or(|after| key.as_str() > after) {
            entries.insert(key, is_prefix);
        }
    }
    let truncated = max > 0 && entries.len() > max;
    let selected: Vec<_> = entries.into_iter().take(max).collect();
    Page {
        next_key: truncated.then(|| selected.last().expect("positive truncated page has an entry").0.clone()),
        objects: selected
            .iter()
            .filter(|(_, prefix)| !prefix)
            .map(|(key, _)| key.clone())
            .collect(),
        prefixes: selected
            .iter()
            .filter(|(_, prefix)| *prefix)
            .map(|(key, _)| key.clone())
            .collect(),
        truncated,
    }
}

fn marker_key(marker: &str) -> &str {
    marker.split_once("[rustfs_cache:").map_or(marker, |(key, _)| key)
}

struct Fixture {
    store: Arc<ECStore>,
    bucket: String,
    objects: Vec<Object>,
    dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    async fn new(bucket: &str, objects: Vec<Object>, authoritative: bool) -> Self {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        if authoritative {
            init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("corpus bucket should be created with authoritative metadata");
        } else {
            for disk in store.pools[0].disk_set[0].disks.read().await.iter().flatten() {
                disk.make_volume(bucket)
                    .await
                    .expect("legacy corpus volume should be created");
            }
        }
        let fixture = Self {
            dirs,
            store,
            bucket: bucket.to_owned(),
            objects,
        };
        for object in &fixture.objects {
            for disk_index in 0..fixture.dirs.len() {
                fixture.write_metadata(disk_index, object).await;
            }
        }
        if !fixture.objects.is_empty() {
            observe_list_objects_mutations(&fixture.store, bucket, fixture.objects.len()).await;
        }
        fixture
    }

    fn metadata(&self, object: &Object, disk_index: usize) -> Vec<u8> {
        let mut meta = FileMeta::new();
        // Writing from oldest to newest exercises the real on-disk format's
        // canonicalization rather than giving the listing a pre-sorted model.
        for version in object.versions.iter().rev() {
            let mut info = if version.kind == VersionKind::Delete {
                FileInfo::default()
            } else {
                FileInfo::new(&object.key, 2, 2)
            };
            info.name = object.key.clone();
            info.volume = self.bucket.clone();
            info.version_id = version.id;
            info.versioned = version.id.is_some();
            info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(version.modified).expect("corpus timestamp"));
            info.deleted = version.kind == VersionKind::Delete;
            if !info.deleted {
                info.erasure.index = disk_index + 1;
                info.data_dir = Some(Uuid::from_u128(0x1234));
                info.size = 1;
                info.parts = vec![ObjectPartInfo {
                    number: 1,
                    size: 1,
                    actual_size: 1,
                    ..Default::default()
                }];
                info.metadata.insert("etag".to_owned(), "corpus-etag".to_owned());
                info.data = Some(Bytes::from_static(b"x"));
                info.set_inline_data();
            }
            info.validate_for_metadata_read()
                .expect("corpus metadata should be readable by the live index verifier");
            if info.deleted {
                assert!(info.is_canonical_delete_marker(), "corpus delete markers must be canonical");
            }
            if version.kind == VersionKind::Free {
                info.transition_status = rustfs_filemeta::TRANSITION_COMPLETE.to_owned();
                info.transition_tier = "WARM".to_owned();
                info.transitioned_objname = "corpus-remote-key".to_owned();
                info.transition_version = Some("remote-version".to_owned());
                info.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
                rustfs_utils::http::metadata_compat::insert_str(
                    &mut info.metadata,
                    rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                    "00".repeat(32),
                );
                meta.add_version(info.clone())
                    .expect("transitioned fixture version should serialize");
                let mut delete = FileInfo {
                    name: object.key.clone(),
                    version_id: info.version_id,
                    ..Default::default()
                };
                delete.set_tier_free_version_id(&Uuid::from_u128(0xf00).to_string());
                meta.delete_version(&delete)
                    .expect("fixture should leave a real free-version cleanup owner");
            } else {
                meta.add_version(info).expect("fixture version should serialize");
            }
        }
        meta.marshal_msg().expect("fixture xl.meta should encode")
    }

    async fn write_metadata(&self, disk_index: usize, object: &Object) {
        let physical_key = rustfs_utils::path::encode_dir_object(&object.key);
        let disk = self.store.pools[0].disk_set[0].disks.read().await[disk_index]
            .clone()
            .expect("fixture disk should be online");
        disk.write_all(
            &self.bucket,
            &format!("{physical_key}/{STORAGE_FORMAT_FILE}"),
            Bytes::from(self.metadata(object, disk_index)),
        )
        .await
        .expect("corpus xl.meta should reach the real local disk");
    }

    async fn page(
        &self,
        layer: usize,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<&str>,
        max: i32,
    ) -> ListObjectsInfo {
        let request = format!("corpus layer={layer}, prefix={prefix}, marker={marker:?}, delimiter={delimiter:?}, max={max}");
        let delimiter = delimiter.map(str::to_owned);
        timeout(Duration::from_secs(10), async {
            match layer {
                0 => {
                    self.store
                        .clone()
                        .list_objects_generic(&self.bucket, prefix, marker, delimiter, max, false)
                        .await
                }
                1 => {
                    self.store.pools[0]
                        .clone()
                        .list_objects_generic(&self.bucket, prefix, marker, delimiter, max, false)
                        .await
                }
                _ => {
                    self.store.pools[0].disk_set[0]
                        .clone()
                        .list_objects_generic(&self.bucket, prefix, marker, delimiter, max, false)
                        .await
                }
            }
        })
        .await
        .unwrap_or_else(|error| panic!("{request}: page must finish: {error}"))
        .unwrap_or_else(|error| panic!("corpus layer={layer}, prefix={prefix}, max={max}: {error:?}"))
    }

    async fn assert_walk(&self, layer: usize, prefix: &str, delimiter: Option<&str>, start: Option<&str>, max: usize) {
        let mut marker = start.map(str::to_owned);
        let mut seen = Vec::new();
        for page_number in 0..=self.objects.len() {
            let expected = reference_page(&self.objects, prefix, delimiter, marker.as_deref().map(marker_key), max);
            let actual = self
                .page(layer, prefix, marker.clone(), delimiter, i32::try_from(max).expect("small corpus limit"))
                .await;
            let observed = Page {
                objects: actual.objects.into_iter().map(|object| object.name).collect(),
                prefixes: actual.prefixes,
                next_key: actual.next_marker.as_deref().map(marker_key).map(str::to_owned),
                truncated: actual.is_truncated,
            };
            assert_eq!(
                observed,
                expected,
                "layer={layer}, prefix={prefix}, delimiter={delimiter:?}, max={max}, page={page_number}, marker={marker:?}, mode={:?}, provider={:?}",
                std::env::var(ENV_API_LIST_OBJECTS_INDEX_MODE).ok(),
                std::env::var(ENV_API_LIST_OBJECTS_INDEX_PROVIDER).ok(),
            );
            seen.extend(observed.objects);
            seen.extend(observed.prefixes);
            if !actual.is_truncated {
                let complete = reference_page(&self.objects, prefix, delimiter, start, if max == 0 { 0 } else { usize::MAX });
                let mut complete_keys = complete.objects;
                complete_keys.extend(complete.prefixes);
                complete_keys.sort();
                seen.sort();
                assert_eq!(seen, complete_keys, "complete walk must contain every identity exactly once");
                return;
            }
            assert!(actual.next_marker.is_some(), "truncated page must supply a cursor");
            assert_ne!(actual.next_marker, marker, "cursor must advance");
            marker = actual.next_marker;
        }
        panic!("corpus pagination did not converge");
    }

    async fn disk_walk(&self, options: WalkDirOptions) -> Vec<MetaCacheEntry> {
        let disk = self.store.pools[0].disk_set[0].disks.read().await[0]
            .clone()
            .expect("corpus walker disk should be online");
        let (reader, mut writer) = duplex(4096);
        let producer = tokio::spawn(async move { disk.walk_dir(options, &mut writer).await });
        let entries = MetacacheReader::new(reader)
            .read_all()
            .await
            .expect("real walker stream should decode");
        producer
            .await
            .expect("real walker producer should join")
            .expect("real walker should reach EOF");
        entries
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_static_namespace() {
    let keys = shared_keys();
    for count in [0, 1, 3, 4, keys.len()] {
        let fixture = Fixture::new(&format!("list-corpus-static-{count}"), namespace(&keys[..count]), true).await;
        let entries = fixture
            .disk_walk(WalkDirOptions {
                bucket: fixture.bucket.clone(),
                recursive: true,
                ..Default::default()
            })
            .await;
        let names: Vec<_> = entries
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, keys[..count], "raw walker should preserve byte order and __XLDIR__ decoding");
        for layer in 0..3 {
            for prefix in ["", "a", "a/", "中/", "missing/"] {
                for delimiter in [None, Some("/")] {
                    for max in [0, 1, 3, 4] {
                        fixture.assert_walk(layer, prefix, delimiter, None, max).await;
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_cross_subdirectory_limit() {
    let keys = ["a/first", "a/nested/second", "a0", "b/final"].map(str::to_owned);
    let fixture = Fixture::new("list-corpus-subdirectory-limit", namespace(&keys), true).await;
    for count in [1, 2, 3, 4] {
        let names: Vec<_> = fixture
            .disk_walk(WalkDirOptions {
                bucket: fixture.bucket.clone(),
                recursive: true,
                limit: i32::try_from(count).expect("small count"),
                ..Default::default()
            })
            .await
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();
        assert_eq!(
            names,
            keys[..count],
            "#7049: a recursive child that reaches the limit must stop its parent"
        );
    }
    for layer in 0..3 {
        for max in [1, 2, 3, 4] {
            fixture.assert_walk(layer, "", None, None, max).await;
        }
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_max_and_max_plus_one() {
    let max = usize::try_from(MAX_OBJECT_LIST).expect("maximum LIST size should be positive");
    for count in [max, max + 1] {
        let keys: Vec<_> = (0..count).map(|index| format!("bulk/key-{index:04}")).collect();
        let fixture = Fixture::new(&format!("list-corpus-max-{count}"), namespace(&keys), true).await;
        for layer in 0..3 {
            fixture.assert_walk(layer, "bulk/", None, None, max).await;
        }
        let index_dir = tempfile::tempdir().expect("maximum-boundary index fixture");
        let index_path = index_dir.path().join("maximum.idx");
        for mode in [ListSourceMode::IndexKeyOnly, ListSourceMode::IndexVerifiedPage] {
            temp_env::async_with_vars(
                [
                    (ENV_API_LIST_OBJECTS_INDEX_MODE, Some(mode.cursor_value())),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER, Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY)),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, index_path.to_str()),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, Some("maximum-corpus")),
                ],
                async { fixture.assert_walk(0, "bulk/", None, None, max).await },
            )
            .await;
        }
        let names: Vec<_> = fixture
            .disk_walk(WalkDirOptions {
                bucket: fixture.bucket.clone(),
                base_dir: "bulk/".to_owned(),
                recursive: true,
                limit: MAX_OBJECT_LIST,
                ..Default::default()
            })
            .await
            .into_iter()
            .filter(|entry| !entry.metadata.is_empty())
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, keys[..max], "walker must stop at max for both adjacent namespaces");
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_fixed_seed_marker_and_prefix() {
    let fixture = Fixture::new("list-corpus-seeded", namespace(&shared_keys()), true).await;
    let prefixes = ["", "a", "a/", "b", "中/"];
    let markers = [None, Some("a"), Some("a/"), Some("a/b"), Some("b"), Some("space key")];
    // Replayable query generation over one static real namespace. No cross-page
    // snapshot guarantee is assumed for concurrent writers.
    let mut seed = 0xec57_07_u64;
    for _ in 0..32 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let prefix = prefixes[usize::try_from(seed % 5).expect("small prefix index")];
        let marker = markers[usize::try_from((seed >> 8) % 6).expect("small marker index")];
        // A start-after outside the selected prefix is outside these APIs' contract.
        let marker = marker.filter(|marker| marker.starts_with(prefix));
        let delimiter = (seed & 1 == 0).then_some("/");
        let max = usize::try_from((seed >> 16) % 4 + 1).expect("small page size");
        for layer in 0..3 {
            fixture.assert_walk(layer, prefix, delimiter, marker, max).await;
        }
    }
    // A folded prefix can consume more raw candidates than the producer's
    // ordinary page budget before the next visible identity appears.
    let mut keys: Vec<_> = (0..12).map(|index| format!("data-{index:02}")).collect();
    keys.push("other-key".to_owned());
    let folded = Fixture::new("list-corpus-long-fold", namespace(&keys), true).await;
    for layer in 0..3 {
        for max in [1, 2] {
            folded.assert_walk(layer, "", Some("-"), None, max).await;
        }
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_offline_corrupt_and_minority_stale() {
    let fixture = Fixture::new("list-corpus-faults", namespace(&shared_keys()), true).await;
    let set = &fixture.store.pools[0].disk_set[0];
    // Three copies are a legal write quorum. Losing one of those three leaves
    // two readable copies among three online disks, the #7010 boundary.
    for object in &fixture.objects {
        let physical_key = rustfs_utils::path::encode_dir_object(&object.key);
        tokio::fs::remove_file(
            fixture.dirs[3]
                .path()
                .join(&fixture.bucket)
                .join(physical_key)
                .join(STORAGE_FORMAT_FILE),
        )
        .await
        .expect("fixture should retain exactly a write quorum of metadata copies");
    }
    fixture.assert_walk(0, "", None, None, 3).await;
    let offline_disk = set.disks.write().await[0]
        .take()
        .expect("a disk with a committed copy should exist before the offline fault");
    assert_eq!(
        set.disks.read().await.iter().flatten().count(),
        3,
        "offline fault must reach the real set"
    );
    for layer in 0..3 {
        fixture.assert_walk(layer, "", None, None, 3).await;
        fixture.assert_walk(layer, "", Some("/"), None, 3).await;
    }
    set.disks.write().await[0] = Some(offline_disk);
    for object in &fixture.objects {
        fixture.write_metadata(3, object).await;
    }

    let object = &fixture.objects[2];
    let metadata_path = fixture.dirs[3]
        .path()
        .join(&fixture.bucket)
        .join(&object.key)
        .join(STORAGE_FORMAT_FILE);
    tokio::fs::write(&metadata_path, b"corrupt xl.meta")
        .await
        .expect("corrupt fault should reach disk bytes");
    assert!(FileMeta::load(&tokio::fs::read(&metadata_path).await.expect("fault bytes should be readable")).is_err());
    for layer in 0..3 {
        fixture.assert_walk(layer, "a", None, None, 1).await;
    }
    fixture.write_metadata(3, object).await;

    let mut stale = object.clone();
    stale.versions[0].modified -= 10;
    fixture.write_metadata(3, &stale).await;
    let phantom = Object {
        key: "minority-only".to_owned(),
        versions: stale.versions.clone(),
    };
    fixture.write_metadata(3, &phantom).await;
    for layer in 0..3 {
        fixture.assert_walk(layer, "", None, None, 3).await;
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_versions_delete_markers_and_free_versions() {
    let mut objects = vec![
        Object {
            key: "a".to_owned(),
            versions: vec![
                Version {
                    id: Some(Uuid::from_u128(3)),
                    modified: 1_705_312_302,
                    kind: VersionKind::Object,
                },
                Version {
                    id: Some(Uuid::from_u128(2)),
                    modified: 1_705_312_301,
                    kind: VersionKind::Delete,
                },
                Version {
                    id: Some(Uuid::from_u128(1)),
                    modified: 1_705_312_300,
                    kind: VersionKind::Object,
                },
            ],
        },
        Object {
            key: "a/hidden".to_owned(),
            versions: vec![
                Version {
                    id: Some(Uuid::from_u128(5)),
                    modified: 1_705_312_301,
                    kind: VersionKind::Delete,
                },
                Version {
                    id: Some(Uuid::from_u128(4)),
                    modified: 1_705_312_300,
                    kind: VersionKind::Object,
                },
            ],
        },
        Object {
            key: "b/tie".to_owned(),
            versions: vec![
                Version {
                    id: Some(Uuid::from_u128(6)),
                    modified: 1_705_312_300,
                    kind: VersionKind::Object,
                },
                Version {
                    id: Some(Uuid::from_u128(7)),
                    modified: 1_705_312_300,
                    kind: VersionKind::Delete,
                },
            ],
        },
        Object {
            key: "folder/".to_owned(),
            versions: vec![Version {
                id: Some(Uuid::from_u128(9)),
                modified: 1_705_312_301,
                kind: VersionKind::Delete,
            }],
        },
        Object {
            key: "folder/child".to_owned(),
            versions: vec![Version {
                id: Some(Uuid::from_u128(10)),
                modified: 1_705_312_300,
                kind: VersionKind::Object,
            }],
        },
        Object {
            key: "free-only".to_owned(),
            versions: vec![Version {
                id: Some(Uuid::from_u128(8)),
                modified: 1_705_312_300,
                kind: VersionKind::Free,
            }],
        },
        Object {
            key: "null".to_owned(),
            versions: vec![Version {
                id: None,
                modified: 1_705_312_300,
                kind: VersionKind::Object,
            }],
        },
    ];
    // More cleanup-only keys than the old raw page budget can hold must not
    // hide the visible null version that follows them.
    objects.splice(
        5..5,
        (0..12).map(|index| Object {
            key: format!("free-dense-{index:02}"),
            versions: vec![Version {
                id: Some(Uuid::from_u128(0x100 + index)),
                modified: 1_705_312_300,
                kind: VersionKind::Free,
            }],
        }),
    );
    let fixture = Fixture::new("list-corpus-versions", objects, true).await;
    let physical = fixture
        .disk_walk(WalkDirOptions {
            bucket: fixture.bucket.clone(),
            recursive: true,
            incl_deleted: true,
            ..Default::default()
        })
        .await;
    let free = physical
        .iter()
        .find(|entry| entry.name == "free-only")
        .expect("free-version fault must exist on disk");
    assert_eq!(
        free.file_info_versions_with_free_versions(&fixture.bucket)
            .expect("physical free metadata should decode")
            .free_versions
            .len(),
        1
    );
    assert!(
        free.file_info_versions(&fixture.bucket)
            .expect("public free-only projection should decode")
            .versions
            .is_empty(),
        "the free-only key must occupy no public version slot"
    );
    let (cleanup_tx, mut cleanup_rx) = mpsc::channel(1);
    let cleanup_walk = fixture.store.pools[0].disk_set[0].clone().walk_internal(
        CancellationToken::new(),
        &fixture.bucket,
        "free-only",
        cleanup_tx,
        WalkOptions {
            include_free_versions: true,
            filter: Some(FileInfo::tier_free_version),
            limit: 1,
            ..Default::default()
        },
    );
    let cleanup_drain = async {
        let mut entries = Vec::new();
        while let Some(entry) = cleanup_rx.recv().await {
            entries.push(entry);
        }
        entries
    };
    let (walk_result, mut cleanup_entries) =
        timeout(Duration::from_secs(10), async { tokio::join!(cleanup_walk, cleanup_drain) })
            .await
            .expect("cleanup walk and consumer must finish together");
    walk_result.expect("the actual cleanup walk must retain free versions");
    assert_eq!(cleanup_entries.len(), 1, "cleanup walk must join and emit the owner once");
    let cleanup = cleanup_entries.pop().expect("cleanup walk must emit the physical free owner");
    assert!(cleanup.err.is_none(), "cleanup owner must not become a read error");
    let cleanup = cleanup.item.expect("cleanup walk should emit an object");
    assert_eq!(cleanup.name, "free-only");
    assert!(
        cleanup.transitioned_object.free_version,
        "cleanup must preserve the free-version identity"
    );

    let expected: Vec<_> = fixture
        .objects
        .iter()
        .flat_map(|object| {
            object
                .versions
                .iter()
                .filter(|version| version.kind != VersionKind::Free)
                .map(|version| (object.key.clone(), version.id, version.kind == VersionKind::Delete))
        })
        .collect();
    for layer in 0..3 {
        fixture.assert_walk(layer, "", None, None, 1).await;
        fixture.assert_walk(layer, "", Some("/"), None, 1).await;
        for max in [0, 1, expected.len() - 1, expected.len(), expected.len() + 1] {
            let mut marker = None;
            let mut version_marker = None;
            let mut offset = 0;
            loop {
                let result = timeout(Duration::from_secs(10), async {
                    match layer {
                    0 => {
                        fixture
                            .store
                            .clone()
                            .inner_list_object_versions(
                                &fixture.bucket,
                                "",
                                marker.clone(),
                                version_marker.clone(),
                                None,
                                i32::try_from(max).expect("small limit"),
                            )
                            .await
                    }
                    1 => {
                        fixture.store.pools[0]
                            .clone()
                            .inner_list_object_versions(
                                &fixture.bucket,
                                "",
                                marker.clone(),
                                version_marker.clone(),
                                None,
                                i32::try_from(max).expect("small limit"),
                            )
                            .await
                    }
                    _ => {
                        fixture.store.pools[0].disk_set[0]
                            .clone()
                            .inner_list_object_versions(
                                &fixture.bucket,
                                "",
                                marker.clone(),
                                version_marker.clone(),
                                None,
                                i32::try_from(max).expect("small limit"),
                            )
                            .await
                    }
                    }
                })
                .await
                .unwrap_or_else(|error| {
                    panic!("version layer={layer}, max={max}, offset={offset}, marker={marker:?}, version_marker={version_marker:?}: page must finish: {error}")
                })
                .expect("real version page should list");
                let end = (offset + max).min(expected.len());
                let actual: Vec<_> = result
                    .objects
                    .into_iter()
                    .map(|object| (object.name, object.version_id.filter(|id| !id.is_nil()), object.delete_marker))
                    .collect();
                assert_eq!(actual, expected[offset..end], "version layer={layer}, max={max}, offset={offset}");
                assert!(result.prefixes.is_empty());
                let more = max > 0 && end < expected.len();
                assert_eq!(
                    result.is_truncated, more,
                    "version layer={layer}, max={max}, offset={offset}, marker={marker:?}, version_marker={version_marker:?}, returned={actual:?}, next_key={:?}, next_version={:?}: page needs exactly one lookahead",
                    result.next_marker, result.next_version_idmarker,
                );
                if !more {
                    assert!(result.next_marker.is_none() && result.next_version_idmarker.is_none());
                    break;
                }
                let last = &expected[end - 1];
                assert_eq!(result.next_marker.as_deref().map(marker_key), Some(last.0.as_str()));
                assert_eq!(
                    result.next_version_idmarker,
                    Some(last.1.map_or_else(|| "null".to_owned(), |id| id.to_string()))
                );
                marker = result.next_marker;
                version_marker = result.next_version_idmarker;
                offset = end;
            }
        }
    }
    // A deleted directory-marker key cannot hide a different live child.
    // Keep the child's four copies and reduce only the marker to write quorum.
    tokio::fs::remove_file(
        fixture.dirs[3]
            .path()
            .join(&fixture.bucket)
            .join(rustfs_utils::path::encode_dir_object("folder/"))
            .join(STORAGE_FORMAT_FILE),
    )
    .await
    .expect("delete-marker fixture should retain three committed copies");
    for layer in 0..3 {
        fixture.assert_walk(layer, "folder/", None, None, 1).await;
        fixture.assert_walk(layer, "", Some("/"), None, 1).await;
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_orphan_cleanup_requires_authoritative_evidence() {
    for authoritative in [false, true] {
        let fixture = Fixture::new(
            &format!("list-corpus-orphan-{authoritative}"),
            namespace(&["legal/child".to_owned()]),
            authoritative,
        )
        .await;
        for dir in &fixture.dirs {
            tokio::fs::create_dir_all(dir.path().join(&fixture.bucket).join("ghost/nested/leaf"))
                .await
                .expect("orphan input must exist");
            tokio::fs::create_dir_all(dir.path().join(&fixture.bucket).join("legal/orphan/leaf"))
                .await
                .expect("orphan input must share an ancestor with a legal child");
        }
        let result = fixture.page(0, "ghost/", None, None, 1).await;
        assert!(result.objects.is_empty() && result.prefixes.is_empty() && !result.is_truncated);
        for dir in &fixture.dirs {
            assert_eq!(
                dir.path().join(&fixture.bucket).join("ghost").exists(),
                !authoritative,
                "unknown metadata must fail closed"
            );
            assert!(
                dir.path()
                    .join(&fixture.bucket)
                    .join("legal/child")
                    .join(STORAGE_FORMAT_FILE)
                    .exists(),
                "purge must preserve legal siblings"
            );
        }
        fixture.assert_walk(0, "legal/", None, None, 1).await;
        for dir in &fixture.dirs {
            assert!(
                dir.path()
                    .join(&fixture.bucket)
                    .join("legal/child")
                    .join(STORAGE_FORMAT_FILE)
                    .exists(),
                "an orphan in the same subtree must not cause the legitimate child to be purged"
            );
        }
        if authoritative {
            for dir in &fixture.dirs {
                tokio::fs::create_dir_all(dir.path().join(&fixture.bucket).join("timed-out/leaf"))
                    .await
                    .expect("timeout orphan input should exist");
            }
            let metadata_sys = fixture
                .store
                .ctx
                .bucket_metadata_sys()
                .expect("authoritative fixture has a metadata system");
            let guard = metadata_sys.write().await;
            assert!(
                timeout(Duration::from_millis(100), fixture.page(0, "timed-out/", None, None, 1))
                    .await
                    .is_err(),
                "blocked authoritative evidence must time out before cleanup can classify the prefix as empty"
            );
            drop(guard);
            for dir in &fixture.dirs {
                assert!(
                    dir.path().join(&fixture.bucket).join("timed-out").exists(),
                    "a timed-out evidence check must preserve its orphan input"
                );
            }
        }
    }
}

struct BackpressureWriter {
    inner: DuplexStream,
    blocked: Arc<Notify>,
}

impl AsyncWrite for BackpressureWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if result.is_pending() {
            self.blocked.notify_one();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn list_objects_shared_corpus_consumer_drop_joins_real_disk_producer() {
    let fixture = Fixture::new("list-corpus-cancel", namespace(&shared_keys()), true).await;
    let disk = fixture.store.pools[0].disk_set[0].disks.read().await[0]
        .clone()
        .expect("real disk should exist");
    let (reader, writer) = duplex(64);
    let blocked = Arc::new(Notify::new());
    let mut writer = BackpressureWriter {
        inner: writer,
        blocked: blocked.clone(),
    };
    let options = WalkDirOptions {
        bucket: fixture.bucket.clone(),
        recursive: true,
        ..Default::default()
    };
    let producer = tokio::spawn(async move { disk.walk_dir(options, &mut writer).await });
    let mut reader = MetacacheReader::new(reader);
    let first = timeout(Duration::from_secs(2), reader.peek())
        .await
        .expect("producer must emit before client cancellation")
        .expect("first metacache entry should decode")
        .expect("the fixture should contain a real entry");
    assert_eq!(first.name, "a", "cancellation must reach a real object stream");
    timeout(Duration::from_secs(2), blocked.notified())
        .await
        .expect("producer should encounter bounded consumer backpressure");
    drop(reader);
    let result = timeout(Duration::from_secs(2), producer)
        .await
        .expect("client drop must terminate the disk producer")
        .expect("producer must not panic");
    assert!(result.is_err(), "the disconnected stream must reach the producer as a write error");
    // A second real walk plus metadata rewrite proves disk access is reusable
    // before fixture/runtime teardown, instead of relying on process exit.
    fixture.write_metadata(0, &fixture.objects[0]).await;
    let entries = fixture
        .disk_walk(WalkDirOptions {
            bucket: fixture.bucket.clone(),
            recursive: true,
            ..Default::default()
        })
        .await;
    assert_eq!(
        entries.into_iter().filter(|entry| !entry.metadata.is_empty()).count(),
        fixture.objects.len()
    );
}

#[tokio::test]
async fn list_objects_shared_corpus_cancelled_quorum_walk_releases_senders_and_disk_lock() {
    let fixture = Fixture::new("list-corpus-quorum-cancel", namespace(&shared_keys()), true).await;
    let set = fixture.store.pools[0].disk_set[0].clone();
    let cancel = CancellationToken::new();
    let (sender, mut receiver) = mpsc::channel(1);
    let observer = sender.clone();
    let task_set = set.clone();
    let task_cancel = cancel.clone();
    let options = ListPathOptions {
        bucket: fixture.bucket.clone(),
        recursive: true,
        ask_disks: "strict".to_owned(),
        limit: 1000,
        ..Default::default()
    };
    let task = tokio::spawn(async move { task_set.list_path(task_cancel, options, sender).await });
    let first = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("quorum producer should emit before cancellation")
        .expect("real quorum walk should yield an object");
    assert_eq!(first.name, "a");
    timeout(Duration::from_secs(2), async {
        while observer.capacity() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real producer should fill the bounded channel before cancellation");
    assert!(
        timeout(Duration::from_millis(50), observer.reserve()).await.is_err(),
        "the real callback must be blocked by a full client channel before cancellation"
    );
    cancel.cancel();
    drop(receiver);
    timeout(Duration::from_secs(2), task)
        .await
        .expect("cancellation must join the real quorum walk and its disk producers")
        .expect("quorum producer should not panic")
        .expect("external cancellation is a clean listing shutdown");
    assert_eq!(
        observer.strong_count(),
        1,
        "all production callback/producer sender clones must be released"
    );
    assert!(set.disks.try_write().is_ok(), "listing cancellation must not retain the set's disk lock");
    fixture.assert_walk(0, "", None, None, 3).await;
}

#[tokio::test]
async fn list_objects_shared_corpus_enabled_index_contracts() {
    let fixture = Fixture::new("list-corpus-index", namespace(&shared_keys()), true).await;
    let index_dir = tempfile::tempdir().expect("index directory should exist");
    let path = index_dir.path().join("namespace.idx");
    let path_value = path.to_str().expect("test index path should be UTF-8");
    for provider in [
        LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY,
        LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY,
    ] {
        for mode in [ListSourceMode::IndexKeyOnly, ListSourceMode::IndexVerifiedPage] {
            temp_env::async_with_vars(
                [
                    (ENV_API_LIST_OBJECTS_INDEX_MODE, Some(mode.cursor_value())),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER, Some(provider)),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, Some(path_value)),
                    (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, Some("shared-corpus")),
                ],
                async {
                    for prefix in ["", "a", "a/", "missing/"] {
                        for delimiter in [None, Some("/")] {
                            for max in [0, 1, 3, 4] {
                                fixture.assert_walk(0, prefix, delimiter, None, max).await;
                            }
                        }
                    }
                    let result = fixture.page(0, "", None, None, 1).await;
                    let mut parsed = ListPathOptions {
                        marker: result.next_marker,
                        ..Default::default()
                    };
                    parsed.parse_marker();
                    assert_eq!(parsed.cursor_source, Some(mode), "enabled adapter must actually serve the index path");
                    assert_eq!(
                        parsed.cursor_generation.as_deref(),
                        Some(if provider == LIST_OBJECTS_INDEX_PROVIDER_WALKER_KEY_ONLY {
                            LIST_CURSOR_GENERATION_LIVE
                        } else {
                            "shared-corpus"
                        })
                    );
                },
            )
            .await;
        }
    }
    temp_env::async_with_vars(
        [
            (ENV_API_LIST_OBJECTS_INDEX_MODE, Some(ListSourceMode::IndexMetadataFast.cursor_value())),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER, Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY)),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, Some(path_value)),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, Some("shared-corpus")),
            (ENV_API_LIST_OBJECTS_METADATA_FAST_ENABLED, Some("true")),
            (ENV_API_LIST_OBJECTS_METADATA_FAST_STALENESS_MS, Some("5000")),
        ],
        async {
            fixture.assert_walk(0, "", None, None, 3).await;
            let first = fixture.page(0, "", None, None, 1).await;
            let mut cursor = ListPathOptions {
                marker: first.next_marker.clone(),
                ..Default::default()
            };
            cursor.parse_marker();
            assert_eq!(cursor.cursor_source, Some(ListSourceMode::IndexMetadataFast));
            assert_eq!(cursor.cursor_generation.as_deref(), Some("shared-corpus"));
            assert!(
                !ListSourceMode::IndexMetadataFast.can_satisfy_strong_listing(),
                "snapshot mode carries an eventual-consistency contract"
            );

            let state = ListObjectsIndexProviderState::persistent_key_only(Some(path.clone()), Some("shared-corpus".to_owned()));
            let mut expired = ListPathOptions {
                bucket: fixture.bucket.clone(),
                limit: 2,
                marker: first.next_marker,
                ..Default::default()
            };
            expired.parse_marker();
            expired.cursor_generation = Some("expired-generation".to_owned());
            assert!(
                fixture
                    .store
                    .clone()
                    .list_objects_from_metadata_fast_provider(&expired, &state, "persistent_key_only", 1, false)
                    .await
                    .expect("expired cursor must be handled")
                    .is_none(),
                "expired generation must fall back to walker"
            );
        },
    )
    .await;
    temp_env::async_with_vars(
        [
            (ENV_API_LIST_OBJECTS_INDEX_MODE, Some(ListSourceMode::IndexKeyOnly.cursor_value())),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER, Some(LIST_OBJECTS_INDEX_PROVIDER_PERSISTENT_KEY_ONLY)),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_PATH, index_dir.path().to_str()),
            (ENV_API_LIST_OBJECTS_INDEX_PROVIDER_GENERATION, Some("shared-corpus")),
        ],
        async {
            let options = ListPathOptions {
                bucket: fixture.bucket.clone(),
                limit: 2,
                ..Default::default()
            };
            assert!(
                fixture
                    .store
                    .clone()
                    .list_objects_from_opt_in_key_only_provider(&options, ListSourceMode::IndexKeyOnly, 1, false)
                    .await
                    .expect("unreadable provider must return a fallback decision")
                    .is_none(),
                "an index path that is a directory must fail the enabled provider"
            );
            fixture.assert_walk(0, "", None, None, 3).await;
            let result = fixture.page(0, "", None, None, 1).await;
            let mut cursor = ListPathOptions {
                marker: result.next_marker,
                ..Default::default()
            };
            cursor.parse_marker();
            assert_ne!(
                cursor.cursor_source,
                Some(ListSourceMode::IndexKeyOnly),
                "degraded provider must not advertise an index cursor"
            );
        },
    )
    .await;
}
