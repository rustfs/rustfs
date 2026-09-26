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

#![recursion_limit = "256"]

use rustfs_heal::{
    Error, Result,
    heal::{
        HealOptions, HealPriority, HealRequest, HealTask, HealType,
        outcome::{HealExecutionOutcome, HealObjectDisposition, HealTraversalCoverage},
        storage::{ECStoreHealStorage, HealListItem, HealObjectInfo, HealStorageAPI, HealStorageObjectResult},
        task::HealTaskStatus,
    },
};
use rustfs_heal_contracts::heal_channel::{HealOpts, HealRequestSource, HealScanMode};
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_test_utils::TestECStoreEnv;
use serial_test::serial;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt as _;
use tokio::sync::Notify;
use uuid::Uuid;

mod storage_api;
use storage_api::integration::{
    BucketInfo, BucketOperations, DeleteBucketOptions, DiskAPI, DiskError, DiskStore, ObjectIO, ObjectOperations, ObjectOptions,
    ObjectToDelete, PutObjReader, ReadOptions, ShardIntegrityWriteMode,
};

const BUCKET: &str = "concurrent-delete";
const CONTROL: &str = "a-control";
const TARGET: &str = "z-target";
const TAIL_TOKEN: &str = "enumerated-tail";

/// Split a real disk-walk page after the control object. The tail retains the
/// exact versions observed before the foreground mutation, while the root has
/// already completed a physical repair and remains the same running task.
struct PausedTraversal {
    inner: ECStoreHealStorage,
    tail: Mutex<Option<Vec<HealListItem>>>,
    captured: Notify,
    resume: Notify,
}

impl PausedTraversal {
    fn new(env: &TestECStoreEnv) -> Self {
        Self {
            inner: ECStoreHealStorage::new(env.ecstore.clone()),
            tail: Mutex::new(None),
            captured: Notify::new(),
            resume: Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl HealStorageAPI for PausedTraversal {
    async fn get_object_meta(&self, bucket: &str, object: &str) -> Result<Option<HealObjectInfo>> {
        self.inner.get_object_meta(bucket, object).await
    }

    async fn ec_decode_rebuild(&self, bucket: &str, object: &str) -> Result<Vec<u8>> {
        self.inner.ec_decode_rebuild(bucket, object).await
    }

    async fn get_bucket_info(&self, bucket: &str) -> Result<Option<BucketInfo>> {
        self.inner.get_bucket_info(bucket).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.inner.list_buckets().await
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> Result<bool> {
        self.inner.object_exists(bucket, object).await
    }

    async fn bucket_incarnation_id(&self, bucket: &str) -> Result<Option<Uuid>> {
        self.inner.bucket_incarnation_id(bucket).await
    }

    async fn admit_bucket_incarnation(&self, bucket: &str) -> Result<Uuid> {
        self.inner.admit_bucket_incarnation(bucket).await
    }

    async fn heal_object_at_incarnation(
        &self,
        bucket: &str,
        object: &str,
        version: Option<&str>,
        expected: Uuid,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        self.inner
            .heal_object_at_incarnation(bucket, object, version, expected, opts)
            .await
    }

    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version: Option<&str>,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        self.inner.heal_object(bucket, object, version, opts).await
    }

    async fn heal_object_with_receipt(
        &self,
        bucket: &str,
        object: &str,
        version: Option<&str>,
        opts: &HealOpts,
    ) -> Result<HealStorageObjectResult> {
        self.inner.heal_object_with_receipt(bucket, object, version, opts).await
    }

    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        self.inner.heal_bucket(bucket, opts).await
    }

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        self.inner.heal_format(dry_run).await
    }

    async fn get_disk_for_resume(&self, set: &str) -> Result<DiskStore> {
        self.inner.get_disk_for_resume(set).await
    }

    async fn heal_erasure_set_scopes(&self, opts: &HealOpts) -> Result<Option<Vec<(usize, usize)>>> {
        self.inner.heal_erasure_set_scopes(opts).await
    }

    async fn list_objects_for_heal_page(
        &self,
        bucket: &str,
        prefix: &str,
        token: Option<&str>,
        lifecycle: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        self.inner.list_objects_for_heal_page(bucket, prefix, token, lifecycle).await
    }

    async fn list_versions_for_heal_page_disk_walk(
        &self,
        set: &str,
        bucket: &str,
        prefix: &str,
        token: Option<&str>,
        lifecycle: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        if token == Some(TAIL_TOKEN) {
            let tail = self.tail.lock().expect("enumerated page").take().expect("one pending tail");
            self.captured.notify_one();
            self.resume.notified().await;
            return Ok((tail, None, false));
        }
        let (page, next, truncated) = self
            .inner
            .list_versions_for_heal_page_disk_walk(set, bucket, prefix, token, lifecycle)
            .await?;
        assert!(!truncated && next.is_none(), "the fixture must fit in one real disk-walk page");
        let (control, tail): (Vec<_>, Vec<_>) = page.into_iter().partition(|item| item.name == CONTROL);
        assert_eq!(control.len(), 1, "one control version must precede the mutation");
        assert!(!tail.is_empty(), "the deleted versions must have been enumerated");
        *self.tail.lock().expect("enumerated page") = Some(tail);
        Ok((control, Some(TAIL_TOKEN.to_owned()), true))
    }
}

struct Version {
    object: &'static str,
    id: Uuid,
    payload: Option<Vec<u8>>,
    data_dir: Option<Uuid>,
}

async fn put(env: &TestECStoreEnv, object: &'static str, seed: u8, versioned: bool) -> Version {
    let payload: Vec<_> = (0..(256 * 1024 + 137))
        .map(|idx| seed.wrapping_add(u8::try_from(idx % 251).expect("bounded fixture byte")))
        .collect();
    let info = env
        .ecstore
        .put_object(
            BUCKET,
            object,
            &mut PutObjReader::from_vec(payload.clone()),
            &ObjectOptions {
                versioned,
                shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
                ..Default::default()
            },
        )
        .await
        .expect("commit protected fixture version");
    let id = info.version_id.unwrap_or_default();
    let data_dir = wait_for_version_copies(env, object, id).await;
    Version {
        object,
        id,
        payload: Some(payload),
        data_dir,
    }
}

async fn wait_for_version_copies(env: &TestECStoreEnv, object: &str, id: Uuid) -> Option<Uuid> {
    let set = env.ecstore.pools[0].get_disks(0);
    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
    assert_eq!(disks.len(), 4);
    // PUT may acknowledge its write quorum before the remaining commit tail.
    // Fault injection requires the exact version to be present on all four disks.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut first = None;
            let mut complete = true;
            for disk in &disks {
                match disk
                    .read_version("", BUCKET, object, &id.to_string(), &ReadOptions::default())
                    .await
                {
                    Ok(metadata) => {
                        first.get_or_insert(metadata);
                    }
                    Err(DiskError::FileNotFound | DiskError::FileVersionNotFound) => complete = false,
                    Err(error) => panic!("read seeded version {object}/{id}: {error}"),
                }
            }
            if complete {
                break first.expect("four committed replicas").data_dir;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("seed version must commit to every disk before injecting a fault")
}

fn shard_path(env: &TestECStoreEnv, disk: usize, version: &Version) -> std::path::PathBuf {
    env.disk_paths[disk]
        .join(BUCKET)
        .join(version.object)
        .join(version.data_dir.expect("non-inline version has a data directory").to_string())
        .join("part.1")
}

async fn remove_shard(env: &TestECStoreEnv, version: &Version) {
    tokio::fs::remove_file(shard_path(env, 3, version))
        .await
        .expect("inject the missing fourth shard");
}

fn root(storage: Arc<dyn HealStorageAPI>) -> Arc<HealTask> {
    let mut request = HealRequest::new(
        HealType::Cluster,
        HealOptions {
            recursive: true,
            recreate_missing: false,
            scan_mode: HealScanMode::Deep,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    Arc::new(HealTask::from_request(request, storage))
}

async fn wait_for_tail(storage: &PausedTraversal, task: &HealTask, env: &TestECStoreEnv, control: &Version) {
    tokio::time::timeout(Duration::from_secs(30), storage.captured.notified())
        .await
        .expect("root must reach the enumerated tail");
    assert_eq!(task.get_status().await, HealTaskStatus::Running);
    let progress = task.get_progress().await;
    assert_eq!(progress.objects_scanned, 1);
    assert!(shard_path(env, 3, control).exists(), "the root must have repaired its first object");
}

async fn assert_version(env: &TestECStoreEnv, version: &Version, absent: bool) {
    let set = env.ecstore.pools[0].get_disks(0);
    let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
    assert_eq!(disks.len(), 4);
    for (index, disk) in disks.iter().enumerate() {
        let result = disk
            .read_version("", BUCKET, version.object, &version.id.to_string(), &ReadOptions::default())
            .await;
        if absent {
            assert!(
                matches!(result, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                "deleted version {} remains on disk {index}: {result:?}",
                version.id
            );
        } else {
            result.expect("retained version must exist on every disk");
            assert!(shard_path(env, index, version).exists(), "retained shard must be repaired");
        }
    }
    if !absent && let Some(expected) = &version.payload {
        let mut reader = env
            .ecstore
            .get_object_reader(
                BUCKET,
                version.object,
                None,
                http::HeaderMap::new(),
                &ObjectOptions {
                    version_id: Some(version.id.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("retained exact version must remain readable");
        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read the complete retained payload");
        assert_eq!(&actual, expected);
    }
}

#[derive(Clone, Copy)]
enum DeleteCase {
    Old,
    Current,
    Marker,
    Batch,
    Unversioned,
}

async fn delete_during_root(case: DeleteCase) {
    let directory = tempfile::tempdir().expect("concurrent delete fixture");
    let env = TestECStoreEnv::builder().base_dir(directory.path()).build().await;
    let versioned = !matches!(case, DeleteCase::Unversioned);
    env.make_bucket(BUCKET, versioned).await;
    let control = put(&env, CONTROL, 1, versioned).await;
    remove_shard(&env, &control).await;
    let mut versions = vec![put(&env, TARGET, 2, versioned).await];
    if versioned {
        versions.push(put(&env, TARGET, 3, true).await);
        versions.push(put(&env, TARGET, 4, true).await);
    }
    if matches!(case, DeleteCase::Marker | DeleteCase::Batch) {
        let marker = env
            .ecstore
            .delete_object(
                BUCKET,
                TARGET,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create the delete marker");
        assert!(marker.delete_marker);
        let marker_id = marker.version_id.expect("delete marker version");
        wait_for_version_copies(&env, TARGET, marker_id).await;
        versions.push(Version {
            object: TARGET,
            id: marker_id,
            payload: None,
            data_dir: None,
        });
        tokio::fs::remove_file(env.disk_paths[3].join(BUCKET).join(TARGET).join("xl.meta"))
            .await
            .expect("G04 minority metadata loss");
    } else {
        let fault = if matches!(case, DeleteCase::Current) {
            versions.len() - 1
        } else {
            0
        };
        remove_shard(&env, &versions[fault]).await;
    }
    if matches!(case, DeleteCase::Batch) {
        versions.push(put(&env, "z-other", 5, true).await);
    }
    let deleted_indices = match case {
        DeleteCase::Old | DeleteCase::Unversioned => vec![0],
        DeleteCase::Current => vec![2],
        DeleteCase::Marker => vec![3],
        DeleteCase::Batch => (0..versions.len()).collect(),
    };
    let storage = Arc::new(PausedTraversal::new(&env));
    let task = root(storage.clone());
    let token = task.id.clone();
    let execution = tokio::spawn({
        let task = task.clone();
        async move { task.execute().await }
    });
    wait_for_tail(&storage, &task, &env, &control).await;
    if matches!(case, DeleteCase::Marker | DeleteCase::Batch) {
        assert!(!env.disk_paths[3].join(BUCKET).join(TARGET).join("xl.meta").exists());
    } else {
        assert!(
            !shard_path(&env, 3, &versions[deleted_indices[0]]).exists(),
            "the target must still be unrepaired when DELETE starts"
        );
    }

    if matches!(case, DeleteCase::Batch) {
        let objects = deleted_indices
            .iter()
            .map(|index| ObjectToDelete {
                object_name: versions[*index].object.to_owned(),
                version_id: Some(versions[*index].id),
                ..Default::default()
            })
            .collect();
        let (deleted, errors) = env
            .ecstore
            .delete_objects(
                BUCKET,
                objects,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(deleted.len(), deleted_indices.len());
        assert_eq!(errors.len(), deleted_indices.len());
        assert!(
            errors.iter().all(Option::is_none),
            "every batch entry must have a successful ACK: {errors:?}"
        );
    } else {
        let version = &versions[deleted_indices[0]];
        env.ecstore
            .delete_object(
                BUCKET,
                version.object,
                ObjectOptions {
                    versioned,
                    version_id: versioned.then(|| version.id.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("foreground delete must be acknowledged successfully");
    }
    assert_eq!(task.get_status().await, HealTaskStatus::Running);
    // A G04 disk may already retain unreferenced bytes after the DELETE ACK.
    // Distinguish that residue from a shard created or changed by this heal.
    let mut deleted_shards = Vec::new();
    for index in &deleted_indices {
        let version = &versions[*index];
        if version.data_dir.is_none() {
            continue;
        }
        for disk in 0..4 {
            let path = shard_path(&env, disk, version);
            let before = match tokio::fs::read(&path).await {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("read post-DELETE shard {path:?}: {error}"),
            };
            deleted_shards.push((path, before));
        }
    }
    storage.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(45), execution)
        .await
        .expect("original root completion deadline")
        .expect("root task must not panic")
        .expect("the original root must discharge deleted versions");
    assert_eq!(task.id, token, "no replacement root may compensate for a failed task");
    assert_eq!(task.get_status().await, HealTaskStatus::Completed);
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.execution, HealExecutionOutcome::Completed);
    assert_eq!(outcome.coverage, HealTraversalCoverage::Complete);
    assert_eq!(outcome.counters.processed, u64::try_from(versions.len() + 1).expect("fixture size"));
    assert_eq!(outcome.counters.failed, 0, "{outcome:?}");
    assert_eq!(outcome.counters.unknown, 0, "{outcome:?}");
    assert_eq!(outcome.counters.skipped, 0, "{outcome:?}");
    for (path, before) in deleted_shards {
        match tokio::fs::read(&path).await {
            Ok(after) => assert_eq!(Some(after), before, "heal must not create or rewrite a deleted shard: {path:?}"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("read final deleted shard {path:?}: {error}"),
        }
    }
    for (index, version) in versions.iter().enumerate() {
        let absent = deleted_indices.contains(&index);
        if absent {
            let receipt = outcome
                .objects
                .iter()
                .find(|item| {
                    item.identity.object == version.object && item.identity.version_id.as_deref() == Some(&version.id.to_string())
                })
                .expect("every enumerated deleted version needs its own receipt");
            assert_eq!(receipt.disposition, HealObjectDisposition::AuthoritativelyAbsent);
            assert_eq!((receipt.identity.pool_index, receipt.identity.set_index), (Some(0), Some(0)));
        }
        assert_version(&env, version, absent).await;
    }
    assert_version(&env, &control, false).await;
}

fn run_delete_case(case: DeleteCase) {
    // Incarnation-bound repairs use a spawned owner; retain the server's stack
    // budget while exercising the real EC2+2 encode/decode futures in debug.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("concurrent delete runtime");
    runtime.block_on(delete_during_root(case));
}

#[test]
#[serial]
fn admin_root_completes_after_old_version_delete() {
    run_delete_case(DeleteCase::Old);
}

#[test]
#[serial]
fn admin_root_completes_after_current_version_delete() {
    run_delete_case(DeleteCase::Current);
}

#[test]
#[serial]
fn admin_root_completes_after_marker_delete() {
    run_delete_case(DeleteCase::Marker);
}

#[test]
#[serial]
fn admin_root_completes_after_batch_version_delete() {
    run_delete_case(DeleteCase::Batch);
}

#[test]
#[serial]
fn admin_root_completes_after_unversioned_delete() {
    run_delete_case(DeleteCase::Unversioned);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn admin_root_rejects_recreated_bucket_before_successor_repair() {
    let directory = tempfile::tempdir().expect("recreated bucket fixture");
    let env = TestECStoreEnv::builder().base_dir(directory.path()).build().await;
    env.make_bucket(BUCKET, false).await;
    let control = put(&env, CONTROL, 1, false).await;
    remove_shard(&env, &control).await;
    put(&env, TARGET, 2, false).await;
    let storage = Arc::new(PausedTraversal::new(&env));
    let original = storage
        .admit_bucket_incarnation(BUCKET)
        .await
        .expect("original bucket identity");
    let task = root(storage.clone());
    let execution = tokio::spawn({
        let task = task.clone();
        async move { task.execute().await }
    });
    wait_for_tail(&storage, &task, &env, &control).await;
    env.ecstore
        .delete_bucket(
            BUCKET,
            &DeleteBucketOptions {
                force: true,
                ..Default::default()
            },
        )
        .await
        .expect("remove the original bucket");
    env.make_bucket(BUCKET, false).await;
    let successor = put(&env, TARGET, 9, false).await;
    remove_shard(&env, &successor).await;
    assert_ne!(storage.admit_bucket_incarnation(BUCKET).await.expect("successor identity"), original);
    storage.resume.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(45), execution)
        .await
        .expect("stale root deadline")
        .expect("stale root must not panic")
        .expect_err("the old traversal must reject the recreated bucket");
    assert!(error.to_string().contains("stale_bucket_incarnation"), "{error}");
    assert!(!shard_path(&env, 3, &successor).exists(), "old root must not repair successor data");

    let fresh = root(Arc::new(ECStoreHealStorage::new(env.ecstore.clone())));
    fresh.execute().await.expect("a fresh root may repair the successor");
    assert_eq!(fresh.get_status().await, HealTaskStatus::Completed);
    assert_version(&env, &successor, false).await;
}
