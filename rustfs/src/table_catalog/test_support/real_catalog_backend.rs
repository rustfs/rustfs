//! Fault injection at real object-backend return boundaries.

use crate::storage_api::table::WriteCommitGuard;
use crate::storage_api::table::managed::{BucketHandle, BucketIncarnationFenceGuard};
use crate::table_catalog::*;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::sync::Weak;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultPhase {
    Before,
    After,
}

#[derive(Clone)]
pub(crate) struct FaultPause {
    arrived: Arc<Notify>,
    release: Arc<Notify>,
    timeout: Duration,
}
impl FaultPause {
    pub(crate) async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.arrived.notified())
            .await
            .expect("fault boundary must arrive");
    }
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone)]
enum FaultAction {
    Fail,
    Pause(FaultPause),
}
struct FaultRule {
    operation: &'static str,
    bucket: String,
    object: String,
    attempt: usize,
    seen: usize,
    phase: FaultPhase,
    action: FaultAction,
    hits: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct FaultEvent {
    pub sequence: usize,
    pub elapsed_micros: u128,
    pub method: String,
    pub bucket: String,
    pub object: String,
    pub phase: String,
    pub length: usize,
    pub sha256: String,
}
struct FaultState {
    rules: Vec<FaultRule>,
    events: Vec<FaultEvent>,
    active_pauses: usize,
    started: Instant,
    #[cfg(unix)]
    locks: Vec<ObservedLock>,
}

#[cfg(unix)]
struct ObservedLock {
    bucket: String,
    object: String,
    signal: Option<Weak<rustfs_lock::distributed_lock::LockLostSignal>>,
    lost_observed: bool,
    first_lost_elapsed_micros: Option<u128>,
}

#[cfg(unix)]
impl ObservedLock {
    fn observe(&mut self, elapsed_micros: u128) -> Option<bool> {
        let lost = self.signal.as_ref().and_then(Weak::upgrade).map(|signal| signal.is_lost());
        if lost == Some(true) && !self.lost_observed {
            self.lost_observed = true;
            self.first_lost_elapsed_micros = Some(elapsed_micros);
        }
        lost
    }
}

impl Default for FaultState {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            events: Vec::new(),
            active_pauses: 0,
            started: Instant::now(),
            #[cfg(unix)]
            locks: Vec::new(),
        }
    }
}

struct ActivePause<'a, B> {
    backend: &'a RealCatalogBackend<B>,
    target: (&'a str, &'a str, &'a str),
    identity: (usize, &'a str),
    completed: bool,
}

impl<B> Drop for ActivePause<'_, B> {
    fn drop(&mut self) {
        self.backend.state.lock().expect("fault state").active_pauses -= 1;
        if !self.completed {
            self.backend.record(
                self.target.0,
                self.target.1,
                self.target.2,
                "pause-cancelled",
                self.identity.0,
                self.identity.1,
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct RealCatalogBackend<B> {
    inner: B,
    state: Arc<Mutex<FaultState>>,
}
impl<B> RealCatalogBackend<B> {
    pub(crate) fn new(inner: B) -> Self {
        Self {
            inner,
            state: Arc::default(),
        }
    }
    pub(crate) fn fail(&self, bucket: &str, object: &str, attempt: usize, phase: FaultPhase) {
        self.arm(("put", bucket, object), attempt, phase, FaultAction::Fail);
    }
    pub(crate) fn pause(&self, bucket: &str, object: &str, attempt: usize, phase: FaultPhase) -> FaultPause {
        self.pause_operation(("put", bucket, object), attempt, phase)
    }
    #[cfg(unix)]
    pub(crate) fn pause_with_timeout(
        &self,
        bucket: &str,
        object: &str,
        attempt: usize,
        phase: FaultPhase,
        timeout: Duration,
    ) -> FaultPause {
        let pause = FaultPause {
            arrived: Arc::default(),
            release: Arc::default(),
            timeout,
        };
        self.arm(("put", bucket, object), attempt, phase, FaultAction::Pause(pause.clone()));
        pause
    }
    pub(crate) fn pause_metadata_unlocked(&self, bucket: &str, object: &str) -> FaultPause {
        self.pause_operation(("object_metadata_unlocked", bucket, object), 1, FaultPhase::After)
    }
    fn pause_operation(&self, target: (&'static str, &str, &str), attempt: usize, phase: FaultPhase) -> FaultPause {
        let pause = FaultPause {
            arrived: Arc::default(),
            release: Arc::default(),
            timeout: Duration::from_secs(30),
        };
        self.arm(target, attempt, phase, FaultAction::Pause(pause.clone()));
        pause
    }
    fn arm(&self, target: (&'static str, &str, &str), attempt: usize, phase: FaultPhase, action: FaultAction) {
        let (operation, bucket, object) = target;
        assert!(attempt > 0);
        self.state.lock().expect("fault state").rules.push(FaultRule {
            operation,
            bucket: bucket.into(),
            object: object.into(),
            attempt,
            seen: 0,
            phase,
            action,
            hits: 0,
        });
    }
    pub(crate) fn events(&self) -> Vec<FaultEvent> {
        self.state.lock().expect("fault state").events.clone()
    }
    #[cfg(unix)]
    pub(crate) fn observations(&self) -> serde_json::Value {
        let mut state = self.state.lock().expect("fault state");
        let elapsed_micros = state.started.elapsed().as_micros();
        let locks: Vec<_> = state
            .locks
            .iter_mut()
            .enumerate()
            .map(|(id, lock)| {
                let is_lost = lock.observe(elapsed_micros);
                serde_json::json!({
                    "observation_id": id + 1, "bucket": lock.bucket, "key": lock.object,
                    "has_loss_signal": lock.signal.is_some(), "signal_alive": is_lost.is_some(),
                    "is_lost": is_lost, "lost_observed": lock.lost_observed,
                    "first_lost_elapsed_micros": lock.first_lost_elapsed_micros,
                })
            })
            .collect();
        serde_json::json!({
            "rules": state.rules.iter().map(|rule| serde_json::json!({
                "operation": rule.operation, "bucket": rule.bucket, "key": rule.object,
                "attempt": rule.attempt, "seen": rule.seen, "hits": rule.hits,
                "phase": format!("{:?}", rule.phase),
            })).collect::<Vec<_>>(),
            "active_pauses": state.active_pauses,
            "locks": locks,
        })
    }
    fn observe_lock(&self, bucket: &str, object: &str, guard: &TableCatalogLockGuard) {
        #[cfg(not(unix))]
        let _ = (bucket, object, guard);
        #[cfg(unix)]
        self.state.lock().expect("fault state").locks.push(ObservedLock {
            bucket: bucket.into(),
            object: object.into(),
            signal: guard.lock_lost_signal().as_ref().map(Arc::downgrade),
            lost_observed: false,
            first_lost_elapsed_micros: None,
        });
    }
    pub(crate) fn assert_consumed(&self) {
        let state = self.state.lock().expect("fault state");
        assert!(!state.rules.is_empty(), "the scenario must arm a fault");
        assert!(
            state.rules.iter().all(|rule| rule.hits == 1),
            "each exact fault must hit once: {:?}",
            state.events
        );
        assert_eq!(state.active_pauses, 0, "no pending fault pause");
        assert_eq!(
            state
                .events
                .iter()
                .filter(|event| matches!(event.phase.as_str(), "error-injected" | "arrived"))
                .count(),
            state.rules.len(),
            "every selected rule must reach its actual injection boundary"
        );
    }
    fn record(&self, method: &str, bucket: &str, object: &str, phase: &str, length: usize, sha256: &str) {
        let mut state = self.state.lock().expect("fault state");
        let sequence = state.events.len() + 1;
        let elapsed_micros = state.started.elapsed().as_micros();
        // Sample before returning to the caller that owns the publication guard.
        #[cfg(unix)]
        for lock in &mut state.locks {
            let _ = lock.observe(elapsed_micros);
        }
        state.events.push(FaultEvent {
            sequence,
            elapsed_micros,
            method: method.into(),
            bucket: bucket.into(),
            object: object.into(),
            phase: phase.into(),
            length,
            sha256: sha256.into(),
        });
    }
    async fn intercept<T, F>(
        &self,
        method: &str,
        bucket: &str,
        object: &str,
        identity: (usize, String),
        future: F,
    ) -> TableCatalogStoreResult<T>
    where
        F: Future<Output = TableCatalogStoreResult<T>>,
    {
        let (length, sha256) = identity;
        self.record(method, bucket, object, "attempted", length, &sha256);
        let rules = {
            let mut state = self.state.lock().expect("fault state");
            state
                .rules
                .iter_mut()
                .filter_map(|rule| {
                    let matches_operation = match rule.operation {
                        "put" => method.starts_with("put_object"),
                        operation => operation == method,
                    };
                    if !matches_operation || rule.bucket != bucket || rule.object != object || rule.hits > 0 {
                        return None;
                    }
                    rule.seen += 1;
                    if rule.seen != rule.attempt {
                        return None;
                    }
                    rule.hits += 1;
                    Some((rule.phase, rule.action.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (_, action) in rules.iter().filter(|(phase, _)| *phase == FaultPhase::Before) {
            self.inject(action, method, bucket, object, length, &sha256).await?;
        }
        self.record(method, bucket, object, "inner-started", length, &sha256);
        let result = match future.await {
            Ok(result) => result,
            Err(error) => {
                self.record(method, bucket, object, "inner-error", length, &sha256);
                return Err(error);
            }
        };
        self.record(method, bucket, object, "inner-completed", length, &sha256);
        for (_, action) in rules.iter().filter(|(phase, _)| *phase == FaultPhase::After) {
            self.inject(action, method, bucket, object, length, &sha256).await?;
        }
        Ok(result)
    }
    async fn inject(
        &self,
        action: &FaultAction,
        method: &str,
        bucket: &str,
        object: &str,
        length: usize,
        sha256: &str,
    ) -> TableCatalogStoreResult<()> {
        match action {
            FaultAction::Fail => {
                self.record(method, bucket, object, "error-injected", length, sha256);
                Err(TableCatalogStoreError::Internal(format!(
                    "I6 injected fault at {method} {bucket}/{object}"
                )))
            }
            FaultAction::Pause(pause) => {
                self.state.lock().expect("fault state").active_pauses += 1;
                let mut active = ActivePause {
                    backend: self,
                    target: (method, bucket, object),
                    identity: (length, sha256),
                    completed: false,
                };
                self.record(method, bucket, object, "arrived", length, sha256);
                pause.arrived.notify_one();
                if tokio::time::timeout(pause.timeout, pause.release.notified()).await.is_err() {
                    self.record(method, bucket, object, "pause-timeout", length, sha256);
                    active.completed = true;
                    return Err(TableCatalogStoreError::Unavailable("test fault pause deadline elapsed".into()));
                }
                active.completed = true;
                self.record(method, bucket, object, "released", length, sha256);
                Ok(())
            }
        }
    }
}

#[async_trait::async_trait]
impl<B: TableCatalogObjectBackend> TableCatalogObjectBackend for RealCatalogBackend<B> {
    fn prepared_catalog_object_reader(&self) -> TableCatalogStoreResult<&dyn CatalogObjectReader> {
        self.inner.prepared_catalog_object_reader()
    }

    async fn read_table_bucket_incarnation(&self, bucket: &str) -> TableCatalogStoreResult<Uuid> {
        self.inner.read_table_bucket_incarnation(bucket).await
    }

    async fn revalidate_bucket_handle(&self, expected: &BucketHandle) -> TableCatalogStoreResult<()> {
        self.inner.revalidate_bucket_handle(expected).await
    }

    async fn acquire_bucket_lifecycle_read(&self, expected: &BucketHandle) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let guard = self.inner.acquire_bucket_lifecycle_read(expected).await?;
        self.observe_lock(expected.storage_bucket_id().as_str(), "bucket-lifecycle", &guard);
        Ok(guard)
    }

    async fn reuse_bucket_lifecycle_read(
        &self,
        expected: &BucketHandle,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        self.inner.reuse_bucket_lifecycle_read(expected, guard).await
    }

    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &mut TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.inner.read_managed_candidate(bucket, metadata, max_size, lifecycle).await
    }

    fn registration_cache(&self) -> Option<TableCatalogRegistrationCache> {
        self.inner.registration_cache()
    }

    fn acquires_catalog_migration_read_permit(&self) -> bool {
        self.inner.acquires_catalog_migration_read_permit()
    }

    async fn acquire_catalog_migration_read_guards(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
        self.inner.acquire_catalog_migration_read_guards(table_bucket).await
    }

    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.inner.read_object(bucket, object).await
    }

    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.inner.read_object_limited(bucket, object, max_size).await
    }

    async fn read_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.inner.read_object_unlocked(bucket, object).await
    }

    async fn read_object_unlocked_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.intercept(
            "read_object_unlocked_limited",
            bucket,
            object,
            (0, String::new()),
            self.inner.read_object_unlocked_limited(bucket, object, max_size),
        )
        .await
    }

    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        self.inner.object_metadata(bucket, object).await
    }

    async fn object_metadata_unlocked(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        self.intercept(
            "object_metadata_unlocked",
            bucket,
            object,
            (0, String::new()),
            self.inner.object_metadata_unlocked(bucket, object),
        )
        .await
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        self.inner.object_exists(bucket, object).await
    }

    async fn object_exists_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        self.inner.object_exists_unlocked(bucket, object).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let identity = (data.len(), hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        self.intercept(
            "put_object",
            bucket,
            object,
            identity,
            self.inner.put_object(bucket, object, data, precondition),
        )
        .await
    }

    async fn put_object_before_publication(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let identity = (data.len(), hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        self.intercept(
            "put_object_before_publication",
            bucket,
            object,
            identity,
            self.inner.put_object_before_publication(bucket, object, data, precondition),
        )
        .await
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<()> {
        let identity = (data.len(), hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        self.intercept(
            "put_object_unlocked",
            bucket,
            object,
            identity,
            self.inner
                .put_object_unlocked(bucket, object, data, precondition, write_guards),
        )
        .await
    }

    async fn put_object_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let identity = (data.len(), hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        self.intercept(
            "put_object_fenced",
            bucket,
            object,
            identity,
            self.inner.put_object_fenced(bucket, object, data, precondition, fence),
        )
        .await
    }

    async fn put_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let identity = (data.len(), hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower));
        self.intercept(
            "put_object_unlocked_fenced",
            bucket,
            object,
            identity,
            self.inner
                .put_object_unlocked_fenced(bucket, object, data, precondition, write_guards, fence),
        )
        .await
    }

    async fn delete_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept(
            "delete_object_unlocked_fenced",
            bucket,
            object,
            identity,
            self.inner.delete_object_unlocked_fenced(bucket, object, write_guards, fence),
        )
        .await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept("delete_object", bucket, object, identity, self.inner.delete_object(bucket, object))
            .await
    }

    async fn delete_object_if_match(&self, bucket: &str, object: &str, expected_etag: &str) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept(
            "delete_object_if_match",
            bucket,
            object,
            identity,
            self.inner.delete_object_if_match(bucket, object, expected_etag),
        )
        .await
    }

    async fn delete_object_if_match_fenced(
        &self,
        bucket: &str,
        object: &str,
        expected_etag: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept(
            "delete_object_if_match_fenced",
            bucket,
            object,
            identity,
            self.inner.delete_object_if_match_fenced(bucket, object, expected_etag, fence),
        )
        .await
    }

    async fn delete_object_if_match_fenced_for_incarnation(
        &self,
        bucket: &str,
        object: &str,
        _expected_etag: &str,
        expected_incarnation_id: Uuid,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept(
            "delete_object_if_match_fenced_for_incarnation",
            bucket,
            object,
            identity,
            self.inner.delete_object_if_match_fenced_for_incarnation(
                bucket,
                object,
                _expected_etag,
                expected_incarnation_id,
                fence,
            ),
        )
        .await
    }

    async fn delete_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        let identity = (0, hex_simd::encode_to_string(Sha256::digest([]), hex_simd::AsciiCase::Lower));
        self.intercept(
            "delete_object_unlocked",
            bucket,
            object,
            identity,
            self.inner.delete_object_unlocked(bucket, object),
        )
        .await
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>> {
        self.inner.list_objects(bucket, prefix).await
    }

    async fn list_objects_for_cleanup(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogCleanupObject>> {
        self.inner.list_objects_for_cleanup(bucket, prefix).await
    }

    async fn ensure_no_multipart_uploads(&self, bucket: &str) -> TableCatalogStoreResult<()> {
        self.inner.ensure_no_multipart_uploads(bucket).await
    }

    async fn list_table_entry_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        self.inner
            .list_table_entry_objects_page(bucket, prefix, start_after, limit)
            .await
    }

    async fn list_catalog_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogCleanupPage> {
        self.inner.list_catalog_objects_page(bucket, prefix, start_after, limit).await
    }

    async fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        self.inner.list_objects_page(bucket, prefix, start_after, limit).await
    }

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let guard = self.inner.acquire_read_lock(bucket, object).await?;
        self.observe_lock(bucket, object, &guard);
        Ok(guard)
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let guard = self.inner.acquire_write_lock(bucket, object).await?;
        self.observe_lock(bucket, object, &guard);
        Ok(guard)
    }

    async fn authorize_table_warehouse_location(&self, table_bucket: &str, location: &str) -> TableCatalogStoreResult<()> {
        self.inner.authorize_table_warehouse_location(table_bucket, location).await
    }

    fn table_publication_preparation_budget(&self) -> Arc<TablePublicationPreparation> {
        self.inner.table_publication_preparation_budget()
    }

    async fn begin_table_bucket_commit_publication(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.inner.begin_table_bucket_commit_publication(table_bucket).await
    }

    fn table_bucket_commit_publication_is_held(&self, table_bucket: &str) -> bool {
        self.inner.table_bucket_commit_publication_is_held(table_bucket)
    }

    async fn prepare_table_commit_publication(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<()> {
        self.inner
            .prepare_table_commit_publication(table_bucket, namespace, table)
            .await
    }

    fn table_commit_publication_is_held(&self, table_bucket: &str, namespace: &str, table: &str) -> bool {
        self.inner.table_commit_publication_is_held(table_bucket, namespace, table)
    }

    fn catalog_migration_read_permit_status(&self) -> Option<bool> {
        self.inner.catalog_migration_read_permit_status()
    }

    fn table_commit_object_mutation_fence(&self) -> Option<TableCatalogObjectMutationFence> {
        self.inner.table_commit_object_mutation_fence()
    }

    fn complete_table_commit_publication(&self) {
        self.inner.complete_table_commit_publication()
    }
}

#[cfg(test)]
mod tests;
