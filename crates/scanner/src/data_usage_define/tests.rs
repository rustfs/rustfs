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

use super::persistence::DataUsageCacheLoadAttempt;
use super::*;
use crate::storage_api::scanner_io::{HTTPRangeSpec, ObjectIO};
use crate::{ScannerGetObjectReader, ScannerPutObjReader};
use rustfs_data_usage::{ReplicationAllStats, ReplicationTargetUsage};
use serde_json::Value;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use temp_env::{with_var, with_var_unset};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::Mutex;

const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([3; 32]);

#[derive(Debug, PartialEq, Eq)]
struct CachePutRecord {
    object: String,
    if_match: Option<String>,
    if_none_match: Option<String>,
}

#[derive(Debug)]
struct BackupFallbackStore {
    backup: Vec<u8>,
    recovered_main: Vec<u8>,
    main_reads: AtomicUsize,
    recover_main_revision: AtomicBool,
    backup_reads: Mutex<usize>,
    puts: Mutex<Vec<CachePutRecord>>,
}

impl BackupFallbackStore {
    fn new(backup: Vec<u8>, recover_main_revision: bool) -> Self {
        Self {
            backup,
            recovered_main: Vec::new(),
            main_reads: AtomicUsize::new(0),
            recover_main_revision: AtomicBool::new(recover_main_revision),
            backup_reads: Mutex::new(0),
            puts: Mutex::new(Vec::new()),
        }
    }

    fn with_recovered_main(backup: Vec<u8>, recovered_main: Vec<u8>) -> Self {
        Self {
            backup,
            recovered_main,
            main_reads: AtomicUsize::new(0),
            recover_main_revision: AtomicBool::new(true),
            backup_reads: Mutex::new(0),
            puts: Mutex::new(Vec::new()),
        }
    }

    fn reader(data: Vec<u8>, etag: &str) -> ScannerGetObjectReader {
        ScannerGetObjectReader {
            stream: Box::new(Cursor::new(data)),
            object_info: ObjectInfo {
                etag: Some(etag.to_string()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
enum CacheReadBody {
    Bytes(Vec<u8>),
    PrefixThenError(Vec<u8>),
}

#[derive(Debug)]
struct PrefixThenErrorReader {
    prefix: Cursor<Vec<u8>>,
    failed: bool,
}

impl AsyncRead for PrefixThenErrorReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.prefix.position() < u64::try_from(self.prefix.get_ref().len()).unwrap_or(u64::MAX) {
            return Pin::new(&mut self.prefix).poll_read(cx, buf);
        }
        if !self.failed {
            self.failed = true;
            return Poll::Ready(Err(std::io::Error::other("injected cache body read failure")));
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct CacheReadStore {
    main: CacheReadBody,
    backup: Option<Vec<u8>>,
    puts: AtomicUsize,
}

#[derive(Debug, Default)]
struct AmbiguousCacheCommitStore {
    data: Mutex<Option<Vec<u8>>>,
    puts: AtomicUsize,
}

impl CacheReadStore {
    fn new(main: CacheReadBody, backup: Option<Vec<u8>>) -> Self {
        Self {
            main,
            backup,
            puts: AtomicUsize::new(0),
        }
    }

    fn reader(body: CacheReadBody, etag: &str) -> ScannerGetObjectReader {
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = match body {
            CacheReadBody::Bytes(data) => Box::new(Cursor::new(data)),
            CacheReadBody::PrefixThenError(prefix) => Box::new(PrefixThenErrorReader {
                prefix: Cursor::new(prefix),
                failed: false,
            }),
        };
        ScannerGetObjectReader {
            stream,
            object_info: ObjectInfo {
                etag: Some(etag.to_string()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl ObjectIO for CacheReadStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = ScannerGetObjectReader;
    type PutObjectReader = ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::GetObjectReader> {
        if bucket != RUSTFS_META_BUCKET {
            return Err(Error::FileNotFound);
        }

        let main_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
        let backup_path = format!("{main_path}.bkp");
        if object == main_path {
            return Ok(Self::reader(self.main.clone(), "main-etag"));
        }
        if object == backup_path {
            return self
                .backup
                .clone()
                .map(|data| Self::reader(CacheReadBody::Bytes(data), "backup-etag"))
                .ok_or(Error::FileNotFound);
        }
        Err(Error::FileNotFound)
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _object: &str,
        _data: &mut Self::PutObjectReader,
        _opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::ObjectInfo> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        Ok(ObjectInfo::default())
    }
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for CacheReadStore {
    async fn delete_config_object(
        &self,
        _bucket: &str,
        _object: &str,
        _opts: crate::ScannerObjectOptions,
    ) -> crate::EcstoreResult<crate::ScannerObjectInfo> {
        Err(crate::EcstoreError::NotImplemented)
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

#[async_trait::async_trait]
impl ObjectIO for AmbiguousCacheCommitStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = ScannerGetObjectReader;
    type PutObjectReader = ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::GetObjectReader> {
        let expected_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
        if bucket != RUSTFS_META_BUCKET || object != expected_path {
            return Err(Error::FileNotFound);
        }
        let data = self.data.lock().await.clone().ok_or(Error::FileNotFound)?;
        Ok(CacheReadStore::reader(CacheReadBody::Bytes(data), "committed-etag"))
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut Self::PutObjectReader,
        _opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::ObjectInfo> {
        let expected_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
        if bucket != RUSTFS_META_BUCKET || object != expected_path {
            return Err(Error::FileNotFound);
        }
        let mut bytes = Vec::new();
        data.stream.read_to_end(&mut bytes).await?;
        *self.data.lock().await = Some(bytes);
        self.puts.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::PreconditionFailed)
    }
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for AmbiguousCacheCommitStore {
    async fn delete_config_object(
        &self,
        _bucket: &str,
        _object: &str,
        _opts: crate::ScannerObjectOptions,
    ) -> crate::EcstoreResult<crate::ScannerObjectInfo> {
        Err(crate::EcstoreError::NotImplemented)
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

#[async_trait::async_trait]
impl ObjectIO for BackupFallbackStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = ScannerGetObjectReader;
    type PutObjectReader = ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::GetObjectReader> {
        if bucket != RUSTFS_META_BUCKET {
            return Err(Error::FileNotFound);
        }

        let main_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
        let backup_path = format!("{main_path}.bkp");
        if object == main_path {
            let read = self.main_reads.fetch_add(1, Ordering::SeqCst);
            if read == 0 || !self.recover_main_revision.load(Ordering::SeqCst) {
                return Err(Error::ErasureReadQuorum);
            }
            return Ok(Self::reader(self.recovered_main.clone(), "main-etag"));
        }
        if object == backup_path {
            *self.backup_reads.lock().await += 1;
            return Ok(Self::reader(self.backup.clone(), "backup-etag"));
        }

        Err(Error::FileNotFound)
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        _data: &mut Self::PutObjectReader,
        opts: &Self::ObjectOptions,
    ) -> StorageResult<Self::ObjectInfo> {
        if bucket != RUSTFS_META_BUCKET {
            return Err(Error::FileNotFound);
        }
        let if_match = opts
            .http_preconditions
            .as_ref()
            .and_then(HTTPPreconditions::if_match_value)
            .map(str::to_owned);
        let if_none_match = opts
            .http_preconditions
            .as_ref()
            .and_then(HTTPPreconditions::if_none_match_value)
            .map(str::to_owned);
        self.puts.lock().await.push(CachePutRecord {
            object: object.to_string(),
            if_match,
            if_none_match,
        });
        Ok(ObjectInfo {
            etag: Some(format!("saved-{object}")),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for BackupFallbackStore {
    async fn delete_config_object(
        &self,
        _bucket: &str,
        _object: &str,
        _opts: crate::ScannerObjectOptions,
    ) -> crate::EcstoreResult<crate::ScannerObjectInfo> {
        Err(crate::EcstoreError::NotImplemented)
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

#[test]
fn cache_revisions_map_to_compare_and_swap_preconditions() {
    let missing = DataUsageCacheRevision::Missing.preconditions();
    let existing = DataUsageCacheRevision::Etag("etag-1".to_string()).preconditions();

    assert_eq!(missing.if_none_match_value(), Some("*"));
    assert!(missing.if_match_value().is_none());
    assert_eq!(existing.if_match_value(), Some("etag-1"));
    assert!(existing.if_none_match_value().is_none());
}

#[tokio::test]
async fn backup_cache_load_recovers_main_revision_before_cas_save() {
    let mut expected = DataUsageCache::default();
    expected.info.name = "bucket".to_string();
    let store = Arc::new(BackupFallbackStore::new(expected.marshal_msg().expect("serialize backup cache"), true));
    let mut loaded = DataUsageCache::default();

    let revisions = loaded
        .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("backup cache should load after the main revision recovers");

    assert_eq!(loaded.info.name, "bucket");
    assert_eq!(store.main_reads.load(Ordering::SeqCst), 2);
    assert!(matches!(revisions.main, DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"));
    assert!(matches!(
        revisions.backup,
        Some(DataUsageCacheRevision::Etag(ref etag)) if etag == "backup-etag"
    ));
    assert_eq!(*store.backup_reads.lock().await, 1);

    loaded
        .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
        .await
        .expect("recovered revisions should protect both cache writes");
    let puts = store.puts.lock().await;
    assert_eq!(
        *puts,
        vec![
            CachePutRecord {
                object: path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]),
                if_match: Some("main-etag".to_string()),
                if_none_match: None,
            },
            CachePutRecord {
                object: path_join_buf(&[BUCKET_META_PREFIX, &format!("{DATA_USAGE_CACHE_NAME}.bkp")]),
                if_match: Some("backup-etag".to_string()),
                if_none_match: None,
            },
        ]
    );
}

#[tokio::test]
async fn recovered_main_cache_wins_over_stale_backup() {
    let mut main = DataUsageCache::default();
    main.info.name = "current-main".to_string();
    let mut backup = DataUsageCache::default();
    backup.info.name = "stale-backup".to_string();
    let store = Arc::new(BackupFallbackStore::with_recovered_main(
        backup.marshal_msg().expect("serialize stale backup cache"),
        main.marshal_msg().expect("serialize recovered main cache"),
    ));
    let mut loaded = DataUsageCache::default();

    let revisions = loaded
        .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("a recovered valid main cache should supersede the fallback backup");

    assert_eq!(loaded.info.name, "current-main");
    assert_eq!(store.main_reads.load(Ordering::SeqCst), 2);
    assert_eq!(*store.backup_reads.lock().await, 1);
    assert!(matches!(revisions.main, DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"));
    assert!(matches!(
        revisions.backup,
        Some(DataUsageCacheRevision::Etag(ref etag)) if etag == "backup-etag"
    ));
}

#[tokio::test]
async fn cache_save_reconciles_an_ambiguous_committed_write() {
    let store = Arc::new(AmbiguousCacheCommitStore::default());
    let mut cache = DataUsageCache::default();
    cache.info.name = "bucket".to_string();
    cache.replace("bucket", "", DataUsageEntry::default());
    let revisions = DataUsageCacheRevisions {
        main: DataUsageCacheRevision::Missing,
        backup: None,
    };

    cache
        .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
        .await
        .expect("read-after-error reconciliation should recognize the committed cache");

    assert_eq!(store.puts.load(Ordering::SeqCst), 1);
    let persisted = store.data.lock().await.clone().expect("cache should be committed");
    assert_eq!(
        DataUsageCache::unmarshal(&persisted)
            .expect("committed cache should decode")
            .info
            .name,
        "bucket"
    );
}

#[tokio::test]
async fn backup_cache_load_fails_closed_without_main_revision_quorum() {
    let backup = DataUsageCache::default().marshal_msg().expect("serialize backup cache");
    let store = Arc::new(BackupFallbackStore::new(backup, false));
    let mut loaded = DataUsageCache::default();

    let error = loaded
        .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect_err("missing main revision quorum must prevent a CAS save");

    assert!(matches!(error, Error::ErasureReadQuorum));
    assert_eq!(store.main_reads.load(Ordering::SeqCst), 5);
    assert_eq!(*store.backup_reads.lock().await, 0);
}

#[tokio::test]
async fn corrupt_primary_cache_loads_valid_backup() {
    let mut expected = DataUsageCache::default();
    expected.info.name = "recovered".to_string();
    let store = Arc::new(CacheReadStore::new(
        CacheReadBody::Bytes(b"not-msgpack".to_vec()),
        Some(expected.marshal_msg().expect("serialize backup cache")),
    ));
    let mut loaded = DataUsageCache::default();

    loaded
        .load(store.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("valid backup must recover a corrupt primary cache");

    assert_eq!(loaded.info.name, "recovered");
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn corrupt_cache_without_valid_backup_rebuilds_with_cas_revisions() {
    for backup in [None, Some(b"also-not-msgpack".to_vec())] {
        let store = Arc::new(CacheReadStore::new(CacheReadBody::Bytes(b"not-msgpack".to_vec()), backup));
        let mut loaded = DataUsageCache::default();

        let revisions = loaded
            .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
            .await
            .expect("corrupt scanner caches should be discarded for a full rebuild");

        assert!(loaded.info.name.is_empty());
        assert!(loaded.cache.is_empty());
        assert!(matches!(
            revisions.main,
            DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"
        ));
        assert!(matches!(
            revisions.backup,
            Some(DataUsageCacheRevision::Missing | DataUsageCacheRevision::Etag(_))
        ));

        loaded
            .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
            .await
            .expect("rebuilt cache should replace corrupt cache objects with CAS protection");
        assert_eq!(store.puts.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn partial_cache_body_read_is_retryable_and_does_not_save() {
    let store = Arc::new(CacheReadStore::new(CacheReadBody::PrefixThenError(vec![0x81, 0xa4, b'n', b'a']), None));

    let attempt = DataUsageCache::try_load_inner(store.clone(), DATA_USAGE_CACHE_NAME, Duration::from_secs(1))
        .await
        .expect("body read failures should remain recoverable load attempts");

    assert!(matches!(attempt, DataUsageCacheLoadAttempt::Retryable(_)));
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
}

#[test]
fn test_data_usage_info_creation() {
    let mut info = DataUsageInfo::new();
    info.update_capacity(1000, 500, 500);

    assert_eq!(info.total_capacity, 1000);
    assert_eq!(info.total_used_capacity, 500);
    assert_eq!(info.total_free_capacity, 500);
    assert!(info.last_update.is_some());
}

#[test]
fn test_bucket_usage_info_merge() {
    let mut usage1 = BucketUsageInfo::new();
    usage1.size = 100;
    usage1.objects_count = 10;
    usage1.versions_count = 5;

    let mut usage2 = BucketUsageInfo::new();
    usage2.size = 200;
    usage2.objects_count = 20;
    usage2.versions_count = 10;

    usage1.merge(&usage2);

    assert_eq!(usage1.size, 300);
    assert_eq!(usage1.objects_count, 30);
    assert_eq!(usage1.versions_count, 15);
}

#[test]
fn test_size_summary_add() {
    let mut summary1 = SizeSummary::new();
    summary1.total_size = 100;
    summary1.versions = 5;

    let mut summary2 = SizeSummary::new();
    summary2.total_size = 200;
    summary2.versions = 10;

    summary1.add(&summary2);

    assert_eq!(summary1.total_size, 300);
    assert_eq!(summary1.versions, 15);
}

#[test]
fn size_summary_add_saturates_all_usage_counters() {
    let target = "arn:minio:replication::target".to_string();
    let mut summary = SizeSummary {
        total_size: usize::MAX,
        versions: usize::MAX,
        delete_markers: usize::MAX,
        replicated_size: i64::MAX,
        replicated_count: usize::MAX,
        pending_size: i64::MAX,
        failed_size: i64::MAX,
        replica_size: i64::MAX,
        replica_count: usize::MAX,
        pending_count: usize::MAX,
        failed_count: usize::MAX,
        ..Default::default()
    };
    summary.repl_target_stats.insert(
        target.clone(),
        ReplTargetSizeSummary {
            replicated_size: i64::MAX,
            replicated_count: usize::MAX,
            pending_size: i64::MAX,
            failed_size: i64::MAX,
            pending_count: usize::MAX,
            failed_count: usize::MAX,
        },
    );

    let mut increment = SizeSummary {
        total_size: 1,
        versions: 1,
        delete_markers: 1,
        replicated_size: 1,
        replicated_count: 1,
        pending_size: 1,
        failed_size: 1,
        replica_size: 1,
        replica_count: 1,
        pending_count: 1,
        failed_count: 1,
        ..Default::default()
    };
    increment.repl_target_stats.insert(
        target.clone(),
        ReplTargetSizeSummary {
            replicated_size: 1,
            replicated_count: 1,
            pending_size: 1,
            failed_size: 1,
            pending_count: 1,
            failed_count: 1,
        },
    );

    summary.add(&increment);

    assert_eq!(summary.total_size, usize::MAX);
    assert_eq!(summary.versions, usize::MAX);
    assert_eq!(summary.delete_markers, usize::MAX);
    assert_eq!(summary.replicated_size, i64::MAX);
    assert_eq!(summary.replicated_count, usize::MAX);
    assert_eq!(summary.pending_size, i64::MAX);
    assert_eq!(summary.failed_size, i64::MAX);
    assert_eq!(summary.replica_size, i64::MAX);
    assert_eq!(summary.replica_count, usize::MAX);
    assert_eq!(summary.pending_count, usize::MAX);
    assert_eq!(summary.failed_count, usize::MAX);

    let target_summary = summary
        .repl_target_stats
        .get(&target)
        .expect("replication target summary should remain present");
    assert_eq!(target_summary.replicated_size, i64::MAX);
    assert_eq!(target_summary.replicated_count, usize::MAX);
    assert_eq!(target_summary.pending_size, i64::MAX);
    assert_eq!(target_summary.failed_size, i64::MAX);
    assert_eq!(target_summary.pending_count, usize::MAX);
    assert_eq!(target_summary.failed_count, usize::MAX);
}

#[test]
fn size_summary_counts_delete_markers_separately_from_versions() {
    let mut summary = SizeSummary::new();
    let marker = ObjectInfo {
        delete_marker: true,
        version_id: Some(uuid::Uuid::new_v4()),
        ..Default::default()
    };

    summary.actions_accounting(&marker, 0, 0);

    assert_eq!(summary.delete_markers, 1);
    assert_eq!(summary.versions, 0);
    assert_eq!(summary.total_size, 0);
}

#[test]
fn size_summary_actions_accounting_accumulates_tier_stats() {
    let mut summary = SizeSummary::new();
    summary
        .tier_stats
        .insert(storageclass::STANDARD.to_string(), TierStats::default());

    let object = ObjectInfo {
        storage_class: Some(storageclass::STANDARD.to_string()),
        size: 10,
        is_latest: true,
        ..Default::default()
    };

    summary.actions_accounting(&object, 10, 10);
    summary.actions_accounting(&object, 10, 10);

    let stats = summary
        .tier_stats
        .get(storageclass::STANDARD)
        .expect("standard tier stats should remain present");
    assert_eq!(
        *stats,
        TierStats {
            total_size: 20,
            num_versions: 2,
            num_objects: 2,
        }
    );
}

#[test]
fn size_summary_unknown_accounting_keeps_physical_tier_and_version_only() {
    let mut summary = SizeSummary::default();
    summary
        .tier_stats
        .insert(storageclass::STANDARD.to_string(), TierStats::default());
    let object = ObjectInfo {
        size: 12,
        storage_class: Some(storageclass::STANDARD.to_string()),
        version_id: Some(uuid::Uuid::new_v4()),
        is_latest: true,
        ..Default::default()
    };

    summary.actions_accounting_unknown(&object);

    assert_eq!(summary.total_size, 0, "unknown logical size must not become zero or physical bytes");
    assert_eq!(summary.versions, 1);
    assert_eq!(
        summary.tier_stats.get(storageclass::STANDARD),
        Some(&TierStats {
            total_size: 12,
            num_versions: 1,
            num_objects: 1,
        })
    );
}

#[test]
fn test_data_usage_entry_merge_sums_failed_objects() {
    let mut left = DataUsageEntry {
        failed_objects: 2,
        ..Default::default()
    };

    let right = DataUsageEntry {
        failed_objects: 3,
        ..Default::default()
    };

    left.merge(&right);

    assert_eq!(left.failed_objects, 5);
}

#[test]
fn test_data_usage_entry_deserialize_defaults_failed_objects() {
    let entry = DataUsageEntry::default();
    let mut value = serde_json::to_value(&entry).expect("Failed to serialize entry");

    let Value::Object(ref mut map) = value else {
        panic!("Expected entry to serialize into an object");
    };

    map.remove("failed_objects");

    let decoded: DataUsageEntry = serde_json::from_value(value).expect("Failed to deserialize entry");
    assert_eq!(decoded.failed_objects, 0);
}

#[test]
fn test_data_usage_cache_info_deserialize_defaults_scan_resume_after() {
    let value = serde_json::json!({
        "name": "bucket",
        "next_cycle": 7,
        "last_update": null,
        "skip_healing": false,
        "lifecycle": null,
        "replication": null,
        "failed_objects": {}
    });

    let decoded: DataUsageCacheInfo = serde_json::from_value(value).expect("Failed to deserialize cache info");

    assert_eq!(decoded.name, "bucket");
    assert_eq!(decoded.next_cycle, 7);
    assert_eq!(decoded.leader_epoch, 0);
    assert!(decoded.scan_resume_after.is_none());
    assert!(decoded.scan_checkpoint.is_none());
    assert!(decoded.object_lock.is_none());
    assert!(decoded.pending_heals.is_empty());
    assert!(decoded.source.is_none());
    assert!(!decoded.snapshot_complete);
    assert!(decoded.scan_plan_digest.is_none());
    assert_eq!(decoded.cache_key_format, 0);
}

#[test]
fn test_data_usage_cache_info_unmarshal_old_msgpack_defaults_scan_resume_after() {
    #[derive(Serialize)]
    struct OldDataUsageCacheInfo {
        name: String,
        next_cycle: u64,
        last_update: Option<SystemTime>,
        skip_healing: bool,
        lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
        replication: Option<Arc<ReplicationConfig>>,
        failed_objects: HashMap<String, u64>,
    }

    let old_info = OldDataUsageCacheInfo {
        name: "bucket".to_string(),
        next_cycle: 7,
        last_update: None,
        skip_healing: true,
        lifecycle: None,
        replication: None,
        failed_objects: HashMap::from([("bad-object".to_string(), 11)]),
    };
    let mut buf = Vec::new();
    old_info
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .expect("Failed to serialize old cache info");

    let decoded: DataUsageCacheInfo = rmp_serde::from_slice(&buf).expect("Failed to deserialize old cache info");

    assert_eq!(decoded.name, "bucket");
    assert_eq!(decoded.next_cycle, 7);
    assert!(decoded.skip_healing);
    assert_eq!(decoded.failed_objects.get("bad-object"), Some(&11));
    assert!(decoded.scan_resume_after.is_none());
    assert!(decoded.scan_checkpoint.is_none());
    assert!(decoded.pending_heals.is_empty());
    assert!(decoded.source.is_none());
    assert!(!decoded.snapshot_complete);
    assert!(decoded.scan_plan_digest.is_none());
    assert_eq!(decoded.cache_key_format, 0);
}

#[test]
fn test_new_data_usage_cache_msgpack_round_trips_and_supports_old_reader() {
    #[derive(Deserialize)]
    struct OldDataUsageCacheInfo {
        name: String,
        next_cycle: u64,
        last_update: Option<SystemTime>,
        skip_healing: bool,
        lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
        replication: Option<Arc<ReplicationConfig>>,
        failed_objects: HashMap<String, u64>,
        scan_resume_after: Option<String>,
        scan_checkpoint: Option<DataUsageScanCheckpoint>,
        pending_heals: Vec<PendingScannerHeal>,
        object_lock: Option<Arc<ObjectLockConfiguration>>,
    }

    #[derive(Deserialize)]
    struct OldDataUsageCache {
        info: OldDataUsageCacheInfo,
        cache: HashMap<String, DataUsageEntry>,
    }

    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            leader_epoch: 9,
            skip_healing: true,
            failed_objects: HashMap::from([("bad-object".to_string(), 11)]),
            source: Some(DataUsageCacheSource::new(1, 2)),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );
    let buf = cache.marshal_msg().expect("Failed to serialize new cache");
    let current = DataUsageCache::unmarshal(&buf).expect("Current reader failed to deserialize new cache");
    assert_eq!(current.info.leader_epoch, 9);
    assert_eq!(current.info.source, Some(DataUsageCacheSource::new(1, 2)));
    assert!(current.info.snapshot_complete);
    assert_eq!(current.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert_eq!(current.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
    assert_eq!(current.find("bucket").map(|entry| entry.objects), Some(3));

    let decoded: OldDataUsageCache = rmp_serde::from_slice(&buf).expect("Old reader failed to deserialize new cache");

    assert_eq!(decoded.info.name, "bucket");
    assert_eq!(decoded.info.next_cycle, 7);
    assert!(decoded.info.last_update.is_none());
    assert!(decoded.info.skip_healing);
    assert!(decoded.info.lifecycle.is_none());
    assert!(decoded.info.replication.is_none());
    assert_eq!(decoded.info.failed_objects.get("bad-object"), Some(&11));
    assert!(decoded.info.scan_resume_after.is_none());
    assert!(decoded.info.scan_checkpoint.is_none());
    assert!(decoded.info.pending_heals.is_empty());
    assert!(decoded.info.object_lock.is_none());
    assert_eq!(decoded.cache.get("bucket").map(|entry| entry.objects), Some(3));
}

/// Deterministic, fully populated cache used to pin the persisted
/// `.usage-cache.bin` wire bytes. Every map/set holds at most one element
/// so the map-encoded `marshal_msg` output is byte-stable.
fn wire_fixture_cache() -> DataUsageCache {
    let mut entry = DataUsageEntry {
        size: 4096,
        objects: 3,
        versions: 5,
        delete_markers: 1,
        compacted: true,
        failed_objects: 2,
        ..Default::default()
    };
    entry.add_tier_sizes(&HashMap::from([(
        "WARM".to_string(),
        TierStats {
            total_size: 2048,
            num_versions: 2,
            num_objects: 1,
        },
    )]));
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "wire-bucket".to_string(),
            next_cycle: 7,
            leader_epoch: 9,
            last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            skip_healing: true,
            failed_objects: HashMap::from([("wire-bucket/lost".to_string(), 11)]),
            scan_resume_after: Some("wire-bucket/resume".to_string()),
            pending_heals: vec![PendingScannerHeal {
                kind: PendingScannerHealKind::Object,
                bucket: "wire-bucket".to_string(),
                object: Some("broken".to_string()),
                version_id: None,
                scan_mode: HealScanMode::Normal,
                first_seen: 100,
                last_attempt: 200,
                attempts: 3,
                last_admission_result: "deferred".to_string(),
                last_admission_reason: "budget".to_string(),
            }],
            source: Some(DataUsageCacheSource::new(1, 2)),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace("wire-bucket", "", entry);
    cache
}

/// Persisted `.usage-cache.bin` bytes produced by [`wire_fixture_cache`]
/// via `DataUsageCache::marshal_msg`: a 2-element array of the 16-field
/// map-encoded info block and the map of map-encoded entries.
///
/// The thin read-only projection in `crates/data-usage` decodes a copy of
/// this fixture (`thin_usage_cache_decodes_scanner_wire_fixture`); when
/// the encoding legitimately changes, regenerate both copies from
/// `wire_fixture_cache().marshal_msg()` and re-verify old readers.
const USAGE_CACHE_WIRE_FIXTURE: &[u8] = &[
    0x92, 0xde, 0x00, 0x10, 0xa4, 0x6e, 0x61, 0x6d, 0x65, 0xab, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
    0xaa, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x63, 0x79, 0x63, 0x6c, 0x65, 0x07, 0xac, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x5f, 0x65,
    0x70, 0x6f, 0x63, 0x68, 0x09, 0xab, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x92, 0xce, 0x65, 0x53,
    0xf1, 0x00, 0x00, 0xac, 0x73, 0x6b, 0x69, 0x70, 0x5f, 0x68, 0x65, 0x61, 0x6c, 0x69, 0x6e, 0x67, 0xc3, 0xa9, 0x6c, 0x69, 0x66,
    0x65, 0x63, 0x79, 0x63, 0x6c, 0x65, 0xc0, 0xab, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0xc0, 0xae,
    0x66, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x81, 0xb0, 0x77, 0x69, 0x72, 0x65, 0x2d,
    0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x2f, 0x6c, 0x6f, 0x73, 0x74, 0x0b, 0xb1, 0x73, 0x63, 0x61, 0x6e, 0x5f, 0x72, 0x65, 0x73,
    0x75, 0x6d, 0x65, 0x5f, 0x61, 0x66, 0x74, 0x65, 0x72, 0xb2, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
    0x2f, 0x72, 0x65, 0x73, 0x75, 0x6d, 0x65, 0xaf, 0x73, 0x63, 0x61, 0x6e, 0x5f, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69,
    0x6e, 0x74, 0xc0, 0xad, 0x70, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x5f, 0x68, 0x65, 0x61, 0x6c, 0x73, 0x91, 0x9a, 0xa6, 0x6f,
    0x62, 0x6a, 0x65, 0x63, 0x74, 0xab, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0xa6, 0x62, 0x72, 0x6f,
    0x6b, 0x65, 0x6e, 0xc0, 0x01, 0x64, 0xcc, 0xc8, 0x03, 0xa8, 0x64, 0x65, 0x66, 0x65, 0x72, 0x72, 0x65, 0x64, 0xa6, 0x62, 0x75,
    0x64, 0x67, 0x65, 0x74, 0xab, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6c, 0x6f, 0x63, 0x6b, 0xc0, 0xa6, 0x73, 0x6f, 0x75,
    0x72, 0x63, 0x65, 0x92, 0x01, 0x02, 0xb1, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x5f, 0x63, 0x6f, 0x6d, 0x70, 0x6c,
    0x65, 0x74, 0x65, 0xc3, 0xb0, 0x73, 0x63, 0x61, 0x6e, 0x5f, 0x70, 0x6c, 0x61, 0x6e, 0x5f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74,
    0xdc, 0x00, 0x20, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
    0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0xb0, 0x63, 0x61, 0x63, 0x68, 0x65, 0x5f,
    0x6b, 0x65, 0x79, 0x5f, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x01, 0x81, 0xab, 0x77, 0x69, 0x72, 0x65, 0x2d, 0x62, 0x75, 0x63,
    0x6b, 0x65, 0x74, 0x8b, 0xa8, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e, 0x90, 0xa4, 0x73, 0x69, 0x7a, 0x65, 0xcd, 0x10,
    0x00, 0xa7, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x03, 0xa8, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x05, 0xae,
    0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x5f, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x01, 0xa9, 0x6f, 0x62, 0x6a, 0x5f, 0x73,
    0x69, 0x7a, 0x65, 0x73, 0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xac, 0x6f, 0x62, 0x6a, 0x5f,
    0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x97, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb1, 0x72, 0x65, 0x70, 0x6c,
    0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0xc0, 0xa9, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63,
    0x74, 0x65, 0x64, 0xc3, 0xae, 0x66, 0x61, 0x69, 0x6c, 0x65, 0x64, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x02, 0xae,
    0x61, 0x6c, 0x6c, 0x5f, 0x74, 0x69, 0x65, 0x72, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0x91, 0x81, 0xa4, 0x57, 0x41, 0x52, 0x4d,
    0x93, 0xcd, 0x08, 0x00, 0x02, 0x01,
];

#[test]
fn usage_cache_wire_format_is_pinned() {
    // Writer: the canonical map-encoded serializer must reproduce the
    // pinned bytes. Round-trip tests cannot see format drift, so any
    // encoding change (field rename/reorder, map->array switch) fails
    // here and forces re-verifying old readers and the thin projection
    // in crates/data-usage before the fixture is regenerated.
    let encoded = wire_fixture_cache().marshal_msg().expect("marshal fixture cache");
    assert_eq!(
        encoded.as_slice(),
        USAGE_CACHE_WIRE_FIXTURE,
        "persisted .usage-cache.bin encoding drifted from the pinned fixture"
    );

    // Reader: the pinned bytes decode with every field intact.
    let decoded = DataUsageCache::unmarshal(USAGE_CACHE_WIRE_FIXTURE).expect("decode pinned fixture");
    assert_eq!(decoded.info.name, "wire-bucket");
    assert_eq!(decoded.info.next_cycle, 7);
    assert_eq!(decoded.info.leader_epoch, 9);
    assert_eq!(
        decoded.info.last_update,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
    );
    assert!(decoded.info.skip_healing);
    assert_eq!(decoded.info.failed_objects.get("wire-bucket/lost"), Some(&11));
    assert_eq!(decoded.info.scan_resume_after.as_deref(), Some("wire-bucket/resume"));
    assert_eq!(decoded.info.pending_heals.len(), 1);
    assert_eq!(decoded.info.pending_heals[0].kind, PendingScannerHealKind::Object);
    assert_eq!(decoded.info.pending_heals[0].object.as_deref(), Some("broken"));
    assert_eq!(decoded.info.source, Some(DataUsageCacheSource::new(1, 2)));
    assert!(decoded.info.snapshot_complete);
    assert_eq!(decoded.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert_eq!(decoded.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);

    let entry = decoded.cache.get("wire-bucket").expect("fixture entry decodes");
    assert_eq!(entry.size, 4096);
    assert_eq!(entry.objects, 3);
    assert_eq!(entry.versions, 5);
    assert_eq!(entry.delete_markers, 1);
    assert!(entry.compacted);
    assert_eq!(entry.failed_objects, 2);
    assert_eq!(
        entry.all_tier_stats.as_ref().and_then(|tiers| tiers.tiers.get("WARM")),
        Some(&TierStats {
            total_size: 2048,
            num_versions: 2,
            num_objects: 1,
        })
    );
}

#[test]
fn data_usage_cache_prepare_for_scan_rejects_unscoped_distributed_cache() {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            scan_resume_after: Some("bucket/prefix".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let reused = cache.prepare_for_scan("bucket", 8, 0, DataUsageCacheSource::new(1, 0), TEST_PLAN_DIGEST, true);

    assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
    assert_eq!(cache.info.name, "bucket");
    assert_eq!(cache.info.next_cycle, 8);
    assert_eq!(cache.info.source, Some(DataUsageCacheSource::new(1, 0)));
    assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert!(!cache.info.snapshot_complete);
    assert!(cache.info.scan_resume_after.is_none());
    assert!(cache.cache.is_empty());
}

#[test]
fn data_usage_cache_prepare_for_scan_preserves_matching_partial_progress() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            scan_resume_after: Some("bucket/prefix".to_string()),
            source: Some(source),
            snapshot_complete: false,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let reused = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(reused, DataUsageCachePrepareOutcome::Reused);
    assert_eq!(cache.info.scan_resume_after.as_deref(), Some("bucket/prefix"));
    assert_eq!(cache.find("bucket").map(|entry| entry.objects), Some(3));
    assert_eq!(cache.info.next_cycle, 8);
    assert_eq!(cache.info.source, Some(source));
    assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert!(!cache.info.snapshot_complete);
}

#[test]
fn data_usage_cache_prepare_for_scan_preserves_pending_heal_only_progress() {
    let source = DataUsageCacheSource::new(1, 0);
    let pending_heal = PendingScannerHeal {
        kind: PendingScannerHealKind::Object,
        bucket: "bucket".to_string(),
        object: Some("prefix/object".to_string()),
        version_id: Some("version-a".to_string()),
        scan_mode: HealScanMode::Deep,
        first_seen: 1,
        last_attempt: 2,
        attempts: 3,
        last_admission_result: "full".to_string(),
        last_admission_reason: "queue_full".to_string(),
    };
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            pending_heals: vec![pending_heal.clone()],
            size_reconciliation: HashMap::from([(
                "size-key".to_string(),
                SizeReconciliationEntry {
                    key: "size-key".to_string(),
                    bucket: "bucket".to_string(),
                    object: "prefix/object".to_string(),
                    reason: "invalid_declared_size".to_string(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        },
        ..Default::default()
    };

    let outcome = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reused);
    assert_eq!(cache.info.pending_heals, vec![pending_heal]);
    assert!(cache.info.size_reconciliation.contains_key("size-key"));
    assert!(cache.cache.is_empty());
    assert!(!cache.info.snapshot_complete);
}

#[test]
fn data_usage_cache_prepare_for_scan_preserves_namespace_pending_heals_during_key_format_rebuild() {
    let source = DataUsageCacheSource::new(1, 0);
    let pending_heal = PendingScannerHeal {
        kind: PendingScannerHealKind::Object,
        bucket: "bucket".to_string(),
        object: Some("prefix/object".to_string()),
        version_id: Some("version-a".to_string()),
        scan_mode: HealScanMode::Deep,
        first_seen: 1,
        last_attempt: 2,
        attempts: 3,
        last_admission_result: "full".to_string(),
        last_admission_reason: "queue_full".to_string(),
    };
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: DATA_USAGE_ROOT.to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            pending_heals: vec![pending_heal.clone()],
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());

    let outcome = cache.prepare_for_scan(DATA_USAGE_ROOT, 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert_eq!(cache.info.pending_heals, vec![pending_heal]);
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn data_usage_cache_prepare_for_scan_drops_pending_heals_from_a_different_scope() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "old-bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            pending_heals: vec![PendingScannerHeal {
                kind: PendingScannerHealKind::Object,
                bucket: "old-bucket".to_string(),
                object: Some("prefix/object".to_string()),
                version_id: None,
                scan_mode: HealScanMode::Normal,
                first_seen: 1,
                last_attempt: 2,
                attempts: 3,
                last_admission_result: "full".to_string(),
                last_admission_reason: "queue_full".to_string(),
            }],
            ..Default::default()
        },
        ..Default::default()
    };

    let outcome = cache.prepare_for_scan("new-bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert_eq!(cache.info.name, "new-bucket");
    assert!(cache.info.pending_heals.is_empty());
}

#[test]
fn data_usage_cache_prepare_for_scan_resets_unknown_key_format() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT + 1,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let outcome = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn data_usage_cache_prepare_for_scan_resets_persisted_windows_key_mismatch() {
    let source = DataUsageCacheSource::new(1, 0);
    let root_key = hash_path("bucket").key();
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        root_key,
        DataUsageEntry {
            children: HashSet::from(["bucket/prefix".to_string()]),
            ..Default::default()
        },
    );
    cache.cache.insert(
        "bucket\\prefix".to_string(),
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let encoded = cache.marshal_msg().expect("legacy Windows cache should serialize");
    let mut decoded = DataUsageCache::unmarshal(&encoded).expect("legacy Windows cache should deserialize");
    let outcome = decoded.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert!(decoded.cache.is_empty());
    assert_eq!(decoded.info.name, "bucket");
    assert_eq!(decoded.info.next_cycle, 8);
    assert_eq!(decoded.info.source, Some(source));
    assert!(!decoded.info.snapshot_complete);
}

#[test]
fn data_usage_cache_prepare_for_scan_resets_current_cache_with_dangling_child() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        hash_path("bucket").key(),
        DataUsageEntry {
            children: HashSet::from([hash_path("bucket/missing").key()]),
            ..Default::default()
        },
    );

    let outcome = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn data_usage_cache_prepare_for_scan_resets_complete_bucket_cache_with_detached_entry() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.cache.insert(
        hash_path("bucket").key(),
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    cache.cache.insert(
        hash_path("bucket/detached").key(),
        DataUsageEntry {
            objects: 2,
            ..Default::default()
        },
    );
    assert_eq!(cache.checked_flatten("bucket").map(|entry| entry.objects), Some(1));
    assert!(
        cache.checked_flatten_complete("bucket").is_none(),
        "complete bucket cache reuse must reject detached entries"
    );

    let outcome = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::Reset);
    assert!(cache.cache.is_empty());
    assert_eq!(cache.info.cache_key_format, DATA_USAGE_CACHE_KEY_FORMAT);
}

#[test]
fn data_usage_cache_prepare_for_scan_rejects_legacy_cache_without_a_bucket_plan() {
    let source = DataUsageCacheSource::new(0, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let reused = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, false);

    assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
    assert!(cache.find("bucket").is_none());
    assert_eq!(cache.info.source, Some(source));
    assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert!(!cache.info.snapshot_complete);
}

#[test]
fn data_usage_cache_prepare_for_scan_rejects_a_different_bucket_plan() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            source: Some(source),
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            scan_resume_after: Some("bucket/prefix".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let next_plan = DataUsageScanPlanDigest([4; 32]);
    let reused = cache.prepare_for_scan("bucket", 8, 0, source, next_plan, true);

    assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
    assert_eq!(cache.info.scan_plan_digest, Some(next_plan));
    assert!(cache.info.scan_resume_after.is_none());
    assert!(cache.cache.is_empty());
}

#[test]
fn data_usage_cache_prepare_for_scan_rejects_cycle_regression_without_mutation() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 8,
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );

    let outcome = cache.prepare_for_scan("bucket", 7, 0, source, DataUsageScanPlanDigest([4; 32]), true);

    assert_eq!(outcome, DataUsageCachePrepareOutcome::RejectedNewerCycle);
    assert_eq!(cache.info.next_cycle, 8);
    assert_eq!(cache.info.source, Some(source));
    assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
    assert!(cache.info.snapshot_complete);
    assert_eq!(cache.find("bucket").map(|entry| entry.objects), Some(3));
}

#[test]
fn data_usage_cache_prepare_for_scan_fences_leader_epochs() {
    let source = DataUsageCacheSource::new(1, 0);
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 8,
            leader_epoch: 11,
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(TEST_PLAN_DIGEST),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace("bucket", "", DataUsageEntry::default());

    let stale = cache.prepare_for_scan("bucket", 8, 10, source, TEST_PLAN_DIGEST, true);
    assert_eq!(stale, DataUsageCachePrepareOutcome::RejectedNewerLeader);
    assert_eq!(cache.info.leader_epoch, 11);
    assert!(cache.info.snapshot_complete);

    let replacement = cache.prepare_for_scan("bucket", 8, 12, source, TEST_PLAN_DIGEST, true);
    assert_eq!(replacement, DataUsageCachePrepareOutcome::Reset);
    assert_eq!(cache.info.leader_epoch, 12);
    assert!(!cache.info.snapshot_complete);
    assert!(cache.cache.is_empty());
}

#[test]
fn test_data_usage_cache_mutations_update_in_place() {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            failed_objects: HashMap::from([("bad-object".to_string(), 7)]),
            ..Default::default()
        },
        ..Default::default()
    };

    let root_hash = hash_path("bucket");
    let child_hash = hash_path("bucket/a");
    let grandchild_hash = hash_path("bucket/a/b");

    cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    cache.replace_hashed(
        &child_hash,
        &Some(root_hash.clone()),
        &DataUsageEntry {
            objects: 2,
            size: 20,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &grandchild_hash,
        &Some(child_hash.clone()),
        &DataUsageEntry {
            objects: 3,
            size: 30,
            ..Default::default()
        },
    );

    assert!(cache.find("bucket").unwrap().children.contains(&child_hash.key()));
    assert!(cache.find("bucket/a").unwrap().children.contains(&grandchild_hash.key()));
    assert_eq!(cache.search_parent(&grandchild_hash), Some(child_hash.clone()));
    assert_eq!(cache.info.failed_objects.get("bad-object"), Some(&7));

    let flat = cache.size_recursive("bucket").unwrap();
    assert_eq!(flat.objects, 5);
    assert_eq!(flat.size, 50);
    assert!(flat.children.is_empty());
}

#[test]
fn test_data_usage_cache_copy_and_delete_recursive() {
    let root_hash = hash_path("bucket");
    let child_hash = hash_path("bucket/a");
    let grandchild_hash = hash_path("bucket/a/b");

    let mut src = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    src.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    src.replace_hashed(
        &child_hash,
        &Some(root_hash.clone()),
        &DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    src.replace_hashed(
        &grandchild_hash,
        &Some(child_hash.clone()),
        &DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    let mut dst = DataUsageCache {
        info: src.info.clone(),
        ..Default::default()
    };
    dst.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    dst.copy_with_children(&src, &child_hash, &Some(root_hash.clone()));

    assert!(dst.cache.contains_key(&child_hash.key()));
    assert!(dst.cache.contains_key(&grandchild_hash.key()));
    assert!(dst.find("bucket").unwrap().children.contains(&child_hash.key()));
    assert!(dst.find("bucket/a").unwrap().children.contains(&grandchild_hash.key()));

    dst.delete_recursive(&child_hash);

    assert!(!dst.cache.contains_key(&child_hash.key()));
    assert!(!dst.cache.contains_key(&grandchild_hash.key()));
    assert!(dst.cache.contains_key(&root_hash.key()));
}

#[test]
fn test_data_usage_cache_recursive_helpers_tolerate_cycles() {
    let root_hash = hash_path("bucket");
    let child_hash = hash_path("bucket/a");

    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
    cache.replace_hashed(
        &child_hash,
        &Some(root_hash.clone()),
        &DataUsageEntry {
            objects: 2,
            size: 20,
            ..Default::default()
        },
    );
    cache.cache.entry(child_hash.key()).or_default().add_child(&root_hash);

    assert_eq!(cache.total_children_rec("bucket"), 1);

    let flat = cache.size_recursive("bucket").expect("cyclic cache should still flatten");
    assert_eq!(flat.objects, 2);
    assert_eq!(flat.size, 20);
    assert!(flat.children.is_empty());

    let mut copied = DataUsageCache {
        info: cache.info.clone(),
        ..Default::default()
    };
    copied.copy_with_children(&cache, &root_hash, &None);
    assert!(copied.cache.contains_key(&root_hash.key()));
    assert!(copied.cache.contains_key(&child_hash.key()));

    copied.delete_recursive(&root_hash);
    assert!(!copied.cache.contains_key(&root_hash.key()));
    assert!(!copied.cache.contains_key(&child_hash.key()));
}

#[test]
fn test_data_usage_cache_flatten_does_not_count_root_twice_in_cycle() {
    let root_hash = hash_path("bucket");
    let child_hash = hash_path("bucket/a");

    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "bucket".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace_hashed(
        &root_hash,
        &None,
        &DataUsageEntry {
            objects: 1,
            size: 10,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &child_hash,
        &Some(root_hash.clone()),
        &DataUsageEntry {
            objects: 2,
            size: 20,
            ..Default::default()
        },
    );
    cache.cache.entry(child_hash.key()).or_default().add_child(&root_hash);

    let flat = cache.size_recursive("bucket").expect("cyclic cache should still flatten");

    assert_eq!(flat.objects, 3);
    assert_eq!(flat.size, 30);
    assert!(flat.children.is_empty());
}

#[test]
fn size_recursive_prunes_empty_and_preserves_threshold_replication_stats() {
    let root = hash_path("bucket");
    let child = hash_path("bucket/child");
    let mut cache = DataUsageCache::default();
    cache.replace_hashed(&root, &None, &DataUsageEntry::default());
    cache.replace_hashed(
        &child,
        &Some(root.clone()),
        &DataUsageEntry {
            replication_stats: Some(ReplicationAllStats::default()),
            ..Default::default()
        },
    );

    assert!(
        cache
            .size_recursive("bucket")
            .expect("scanner bucket usage should flatten")
            .replication_stats
            .is_none()
    );

    cache.replace_hashed(
        &child,
        &Some(root.clone()),
        &DataUsageEntry {
            replication_stats: Some(ReplicationAllStats {
                targets: HashMap::from([(
                    "arn:test:threshold".to_string(),
                    ReplicationTargetUsage {
                        after_threshold_count: 1,
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }),
            ..Default::default()
        },
    );

    let flattened = cache.size_recursive("bucket").expect("scanner bucket usage should flatten");
    let replication = flattened
        .replication_stats
        .expect("threshold-only replication stats must survive pruning");

    assert_eq!(replication.targets["arn:test:threshold"].after_threshold_count, 1);
}

#[test]
fn checked_flatten_rejects_dangling_child() {
    let root_key = hash_path("bucket").key();
    let mut cache = DataUsageCache::default();
    cache.cache.insert(
        root_key,
        DataUsageEntry {
            objects: 1,
            children: HashSet::from(["missing-child".to_string()]),
            ..Default::default()
        },
    );

    assert!(
        cache.checked_flatten("bucket").is_none(),
        "a missing child must invalidate an exact usage snapshot"
    );
}

#[test]
fn checked_flatten_complete_rejects_detached_entries() {
    let mut cache = DataUsageCache::default();
    cache.cache.insert(
        hash_path("bucket").key(),
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    cache.cache.insert(
        hash_path("bucket/detached").key(),
        DataUsageEntry {
            objects: 2,
            ..Default::default()
        },
    );

    assert_eq!(
        cache.checked_flatten("bucket").map(|entry| entry.objects),
        Some(1),
        "subtree flattening may ignore entries outside the requested subtree"
    );
    assert!(
        cache.checked_flatten_complete("bucket").is_none(),
        "an authoritative cache root must reach every persisted entry"
    );
}

#[test]
fn checked_flatten_rejects_compacted_entries_with_children() {
    let root_key = hash_path("bucket").key();
    let child_key = hash_path("bucket/prefix").key();
    let mut cache = DataUsageCache::default();
    cache.cache.insert(
        root_key,
        DataUsageEntry {
            children: HashSet::from([child_key.clone()]),
            compacted: true,
            ..Default::default()
        },
    );
    cache.cache.insert(
        child_key,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    assert!(
        cache.checked_flatten_complete("bucket").is_none(),
        "a compacted entry cannot retain child links without double-counting"
    );
}

#[test]
fn checked_flatten_rejects_compacted_descendants_with_children() {
    let root_key = hash_path("bucket").key();
    let child_key = hash_path("bucket/prefix").key();
    let grandchild_key = hash_path("bucket/prefix/object").key();
    let mut cache = DataUsageCache::default();
    cache.cache.insert(
        root_key,
        DataUsageEntry {
            children: HashSet::from([child_key.clone()]),
            ..Default::default()
        },
    );
    cache.cache.insert(
        child_key,
        DataUsageEntry {
            objects: 1,
            children: HashSet::from([grandchild_key.clone()]),
            compacted: true,
            ..Default::default()
        },
    );
    cache.cache.insert(
        grandchild_key,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    assert!(
        cache.checked_flatten("bucket").is_none(),
        "a compacted descendant cannot retain child links without double-counting"
    );
}

#[test]
fn checked_flatten_accepts_depth_limit_and_rejects_deeper_tree() {
    let root_key = hash_path("bucket").key();
    let mut cache = DataUsageCache::default();
    cache.cache.insert(
        root_key.clone(),
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    let mut parent = root_key;
    for depth in 1..=MAX_DATA_USAGE_CACHE_DEPTH {
        let child = format!("depth-{depth}");
        cache
            .cache
            .get_mut(&parent)
            .expect("parent should exist")
            .children
            .insert(child.clone());
        cache.cache.insert(
            child.clone(),
            DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        parent = child;
    }

    let flattened = cache
        .checked_flatten("bucket")
        .expect("a tree ending at the configured depth limit should be valid");
    assert_eq!(flattened.objects, MAX_DATA_USAGE_CACHE_DEPTH + 1);

    let too_deep = "depth-too-deep".to_string();
    cache
        .cache
        .get_mut(&parent)
        .expect("last valid node should exist")
        .children
        .insert(too_deep.clone());
    cache.cache.insert(
        too_deep,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );

    assert!(
        cache.checked_flatten("bucket").is_none(),
        "a tree deeper than the configured limit must be rejected"
    );
}

#[test]
fn test_find_children_copy_preserves_missing_entry_behavior() {
    let mut cache = DataUsageCache::default();
    let missing_hash = hash_path("missing");

    assert!(cache.find_children_copy(missing_hash.clone()).is_empty());
    assert!(cache.cache.contains_key(&missing_hash.key()));
}

#[test]
fn test_dui_bucket_count_uses_bucket_list_after_compaction() {
    let root_hash = hash_path("root");
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "root".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace_hashed(
        &root_hash,
        &None,
        &DataUsageEntry {
            compacted: true,
            objects: 3,
            ..Default::default()
        },
    );

    let buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
    let info = cache.dui("root", &buckets);

    assert_eq!(info.buckets_count, 2);
    assert!(info.buckets_usage.is_empty());
    assert_eq!(info.objects_total_count, 3);
}

#[test]
fn test_cache_path_type_distinguishes_main_and_backup() {
    assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin"), "main");
    assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin.bkp"), "backup");
}

#[test]
fn test_cache_save_timeout_uses_default_when_env_missing() {
    with_var_unset(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(
            DataUsageCache::cache_save_timeout(),
            Duration::from_secs(rustfs_config::DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS)
        );
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_cache_save_timeout_respects_env_and_minimum_bound() {
    with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(7));
    });

    with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("0"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(1));
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[test]
fn test_cache_persistence_timeout_covers_all_save_attempts() {
    with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        assert_eq!(DataUsageCache::persistence_timeout(), Duration::from_millis(31_350));
    });
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
async fn test_retry_save_op_retries_on_error_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let result = DataUsageCache::retry_save_op("main", Duration::from_millis(200), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
        let attempts = attempts_clone.clone();
        async move {
            let current = attempts.fetch_add(1, Ordering::SeqCst);
            if current < 2 {
                return Err(StorageError::other("transient".to_string()));
            }
            Ok(())
        }
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_retry_save_op_does_not_retry_namespace_lock_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let result = DataUsageCache::retry_save_op("main", Duration::from_millis(200), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
        let attempts = attempts_clone.clone();
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "write",
                bucket: RUSTFS_META_BUCKET.to_string(),
                object: "buckets/.usage-cache.bin".to_string(),
                required: 2,
                achieved: 1,
            })
        }
    })
    .await;

    assert!(matches!(result, Err(StorageError::NamespaceLockQuorumUnavailable { .. })));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

// --- Tests for `add` function (bug #1: logic inversion) ---

/// Build a small tree: root -> child1 (leaf), child2 -> grandchild (leaf).
/// Returns (cache, root_hash).
fn build_test_tree() -> (DataUsageCache, DataUsageHash) {
    let root = hash_path("bucket");
    let c1 = hash_path("bucket/a");
    let c2 = hash_path("bucket/b");
    let gc = hash_path("bucket/b/c");

    let mut cache = DataUsageCache::default();
    cache.replace_hashed(&root, &None, &DataUsageEntry::default());
    cache.replace_hashed(
        &c1,
        &Some(root.clone()),
        &DataUsageEntry {
            objects: 1,
            size: 10,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &c2,
        &Some(root.clone()),
        &DataUsageEntry {
            objects: 2,
            size: 20,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &gc,
        &Some(c2.clone()),
        &DataUsageEntry {
            objects: 3,
            size: 30,
            ..Default::default()
        },
    );
    (cache, root)
}

fn build_underflow_test_tree() -> (DataUsageCache, DataUsageHash) {
    let root = hash_path("bucket");
    let small = hash_path("bucket/small");
    let small_a = hash_path("bucket/small/a");
    let small_b = hash_path("bucket/small/b");
    let large = hash_path("bucket/large");
    let large_a = hash_path("bucket/large/a");
    let large_b = hash_path("bucket/large/b");

    let mut cache = DataUsageCache::default();
    cache.replace_hashed(
        &root,
        &None,
        &DataUsageEntry {
            objects: 100,
            ..Default::default()
        },
    );
    cache.replace_hashed(&small, &Some(root.clone()), &DataUsageEntry::default());
    cache.replace_hashed(
        &small_a,
        &Some(small.clone()),
        &DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &small_b,
        &Some(small.clone()),
        &DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    cache.replace_hashed(&large, &Some(root.clone()), &DataUsageEntry::default());
    cache.replace_hashed(
        &large_a,
        &Some(large.clone()),
        &DataUsageEntry {
            objects: 10,
            ..Default::default()
        },
    );
    cache.replace_hashed(
        &large_b,
        &Some(large.clone()),
        &DataUsageEntry {
            objects: 10,
            ..Default::default()
        },
    );
    (cache, root)
}

#[test]
fn test_add_collects_internal_nodes_as_compaction_candidates() {
    // `add()` should collect internal nodes (with children) as compaction candidates.
    // Leaf nodes have no children to remove, so compacting them is a no-op.
    let (cache, root) = build_test_tree();
    let mut candidates = Vec::new();
    add(&cache, &root, &mut candidates);

    let mut paths: Vec<String> = candidates.iter().map(|l| l.path.key()).collect();
    paths.sort();
    // Internal nodes: "bucket" (children: [a, b]) and "bucket/b" (children: [c]).
    // Leaf nodes "bucket/a" and "bucket/b/c" are NOT collected.
    assert_eq!(paths.len(), 2, "add() should find internal nodes with children");
    assert!(paths.contains(&hash_path("bucket").key()));
    assert!(paths.contains(&hash_path("bucket/b").key()));
}

#[test]
fn test_add_returns_empty_for_missing_path() {
    let cache = DataUsageCache::default();
    let mut candidates = Vec::new();
    add(&cache, &hash_path("nonexistent"), &mut candidates);
    assert!(candidates.is_empty());
}

#[test]
fn test_add_skips_leaf_node() {
    // A leaf node (no children) is not a valid compaction candidate —
    // total_children_rec returns 0 for leaves, so compacting them has no effect.
    let mut cache = DataUsageCache::default();
    let h = hash_path("single-leaf");
    cache.replace_hashed(
        &h,
        &None,
        &DataUsageEntry {
            objects: 5,
            size: 50,
            ..Default::default()
        },
    );

    let mut candidates = Vec::new();
    add(&cache, &h, &mut candidates);
    assert!(candidates.is_empty(), "leaf node should not be a compaction candidate");
}

// --- Tests for `reduce_children_of` (bug #2: usize underflow) ---

#[test]
fn test_reduce_children_of_compacts_internal_node() {
    // Build tree: root -> c1(leaf), c2 -> gc(leaf). total=3, limit=2, remove=1.
    // Internal nodes (compaction candidates): root, c2.
    // compact_self=false skips root; c2 (objects=2, 1 child) is the only candidate.
    // Compacting c2 removes gc (removing=1), satisfying remove=1.
    let (mut cache, root) = build_test_tree();
    cache.reduce_children_of(&root, 2, false);

    // "bucket/b" should be compacted (its child gc removed).
    let entry_c2 = cache.find("bucket/b").unwrap();
    assert!(entry_c2.compacted, "internal node 'bucket/b' should be compacted");
    // "bucket/a" (leaf, not a candidate) should remain unchanged.
    let entry_c1 = cache.find("bucket/a").unwrap();
    assert!(!entry_c1.compacted, "leaf 'bucket/a' should not be compacted");
    // "bucket/b/c" was deleted when its parent was compacted.
    assert!(cache.find("bucket/b/c").is_none(), "grandchild should be removed after parent compaction");
}

#[test]
fn test_reduce_children_of_no_op_when_under_limit() {
    let (mut cache, root) = build_test_tree();
    let before = cache.cache.len();
    // limit=10 > total children => no compaction
    cache.reduce_children_of(&root, 10, false);
    assert_eq!(cache.cache.len(), before);
}

#[test]
fn test_reduce_children_of_usize_underflow_saturates() {
    let (mut cache, root) = build_underflow_test_tree();

    // total children=6, limit=5, remove=1. The smallest candidate removes
    // two descendants, so plain subtraction would underflow and compact the
    // next candidate too.
    cache.reduce_children_of(&root, 5, false);

    assert!(cache.find("bucket/small").is_some_and(|entry| entry.compacted));
    assert!(cache.find("bucket/small/a").is_none());
    assert!(cache.find("bucket/small/b").is_none());
    assert!(cache.find("bucket/large").is_some_and(|entry| !entry.compacted));
    assert!(cache.find("bucket/large/a").is_some());
    assert!(cache.find("bucket/large/b").is_some());
}

#[tokio::test]
async fn test_retry_save_op_times_out_and_returns_error_after_retries() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    let result = DataUsageCache::retry_save_op("main", Duration::from_millis(10), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
        let attempts = attempts_clone.clone();
        async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<StorageResult<()>>().await
        }
    })
    .await;

    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), (DATA_USAGE_CACHE_SAVE_RETRIES + 1) as usize);
}
