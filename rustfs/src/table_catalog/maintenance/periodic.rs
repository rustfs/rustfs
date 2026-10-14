// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::super::*;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::time::Duration as StdDuration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const PERIODIC_MAINTENANCE_TABLES_PER_PASS: usize = 8;
const PERIODIC_MAINTENANCE_DISCOVERY_STEPS_PER_PASS: usize = 64;

struct BucketCursor {
    bucket: String,
    namespace_cursor: Option<String>,
    namespace: Option<String>,
    table_cursor: Option<String>,
}

impl BucketCursor {
    fn reset(&mut self) {
        self.namespace_cursor = None;
        self.namespace = None;
        self.table_cursor = None;
    }
}

/// Queue-only admission. The caller owns this future until it returns: a
/// deadline stops new operations, never cancels a catalog mutation in flight.
pub(crate) struct PeriodicMaintenanceScheduler<B: TableCatalogObjectBackend> {
    store: ConfiguredTableCatalogStore<B>,
    buckets: Vec<BucketCursor>,
    next_bucket: usize,
    interval: StdDuration,
    scheduler_id: String,
}

impl<B: TableCatalogObjectBackend> PeriodicMaintenanceScheduler<B> {
    /// Bucket names and the interval have already passed runtime configuration validation.
    pub(crate) fn new(store: ConfiguredTableCatalogStore<B>, buckets: Vec<String>, interval: StdDuration) -> Self {
        Self {
            store,
            buckets: buckets
                .into_iter()
                .map(|bucket| BucketCursor {
                    bucket,
                    namespace_cursor: None,
                    namespace: None,
                    table_cursor: None,
                })
                .collect(),
            next_bucket: 0,
            interval,
            scheduler_id: format!("periodic-{}", uuid::Uuid::new_v4()),
        }
    }

    pub(crate) async fn run_pass(
        &mut self,
        cancellation: &CancellationToken,
        admission_deadline: Instant,
    ) -> TableCatalogStoreResult<()> {
        if self.buckets.is_empty() {
            return Ok(());
        }
        let mut visited = BTreeSet::new();
        let mut unavailable_buckets = BTreeSet::new();
        let mut first_error = None;
        for _ in 0..PERIODIC_MAINTENANCE_DISCOVERY_STEPS_PER_PASS {
            if !admission_open(cancellation, admission_deadline) || visited.len() == PERIODIC_MAINTENANCE_TABLES_PER_PASS {
                break;
            }
            let bucket_index = self.next_bucket;
            self.next_bucket = (bucket_index + 1) % self.buckets.len();
            if unavailable_buckets.contains(&bucket_index) {
                continue;
            }
            let table = match self.discover_next_table(bucket_index, cancellation, admission_deadline).await {
                Ok(Some(table)) => table,
                Ok(None) => continue,
                Err(error) => {
                    if matches!(&error, TableCatalogStoreError::NotFound(_)) {
                        // A namespace removed between pages must not pin this bucket's cursor forever.
                        self.buckets[bucket_index].reset();
                    }
                    unavailable_buckets.insert(bucket_index);
                    first_error.get_or_insert(error);
                    continue;
                }
            };
            if !visited.insert((table.table_bucket.clone(), table.table_id.clone())) {
                continue;
            }
            if let Err(error) = self.queue_if_eligible(&table, cancellation, admission_deadline).await {
                // A removed/renamed table or one unhealthy bucket must not starve other buckets.
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn discover_next_table(
        &mut self,
        bucket_index: usize,
        cancellation: &CancellationToken,
        admission_deadline: Instant,
    ) -> TableCatalogStoreResult<Option<TableEntry>> {
        let cursor = &mut self.buckets[bucket_index];
        if !admission_open(cancellation, admission_deadline) {
            return Ok(None);
        }
        if self
            .store
            .get_table_bucket(&cursor.bucket)
            .await?
            .is_none_or(|bucket| bucket.state != TableCatalogEntryState::Active)
        {
            cursor.reset();
            return Ok(None);
        }
        if cursor.namespace.is_none() {
            if !admission_open(cancellation, admission_deadline) {
                return Ok(None);
            }
            let page = self
                .store
                .list_namespaces_page(&cursor.bucket, cursor.namespace_cursor.as_deref(), NonZeroUsize::MIN)
                .await?;
            cursor.namespace_cursor = page.next_cursor;
            cursor.namespace = page.entries.into_iter().next().map(|namespace| namespace.namespace);
            cursor.table_cursor = None;
        }
        let Some(namespace) = cursor.namespace.as_deref() else {
            return Ok(None);
        };
        if !admission_open(cancellation, admission_deadline) {
            return Ok(None);
        }
        let page = self
            .store
            .list_tables_page(&cursor.bucket, namespace, cursor.table_cursor.as_deref(), NonZeroUsize::MIN)
            .await?;
        cursor.table_cursor = page.next_cursor;
        if cursor.table_cursor.is_none() {
            cursor.namespace = None;
        }
        // Object-backed discovery can return an empty page with a continuation.
        Ok(page.entries.into_iter().next())
    }

    async fn queue_if_eligible(
        &self,
        table: &TableEntry,
        cancellation: &CancellationToken,
        admission_deadline: Instant,
    ) -> TableCatalogStoreResult<()> {
        if !admission_open(cancellation, admission_deadline) {
            return Ok(());
        }
        let scheduler = self
            .store
            .get_table_maintenance_scheduler_report(&table.table_bucket, &table.namespace, &table.table)
            .await?;
        if scheduler.table_id != table.table_id {
            return Err(TableCatalogStoreError::Conflict(
                "table identity changed during periodic maintenance discovery".to_string(),
            ));
        }
        if !scheduler.background_enabled || scheduler.worker_paused || scheduler.quarantine.active {
            return Ok(());
        }
        if let Some(current) = scheduler.current_job.as_ref() {
            if current.status != TableMetadataMaintenanceJobStatus::Successful {
                // Periodic admission never renews an abandoned queue or resets a failed job's retry budget.
                return Ok(());
            }
            let finished_at = current
                .finished_at
                .as_deref()
                .and_then(|timestamp| OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339).ok())
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(
                        "periodic maintenance requires a valid successful job completion timestamp".to_string(),
                    )
                })?;
            let interval = time::Duration::try_from(self.interval)
                .map_err(|_| TableCatalogStoreError::Invalid("periodic maintenance interval is out of range".to_string()))?;
            let next_admission = finished_at.checked_add(interval).ok_or_else(|| {
                TableCatalogStoreError::Invalid("periodic maintenance completion timestamp is out of range".to_string())
            })?;
            if OffsetDateTime::now_utc() < next_admission {
                return Ok(());
            }
        }
        if !admission_open(cancellation, admission_deadline) {
            return Ok(());
        }
        self.store
            .queue_periodic_table_maintenance(table, self.scheduler_id.clone(), scheduler.current_job.as_ref())
            .await?;
        Ok(())
    }
}

fn admission_open(cancellation: &CancellationToken, deadline: Instant) -> bool {
    !cancellation.is_cancelled() && Instant::now() < deadline
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;
    use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};
    use std::collections::BTreeMap;

    type TestStore = ConfiguredTableCatalogStore<TestCatalogObjectBackend>;

    fn backings() -> [TableCatalogBackingMode; 2] {
        [TableCatalogBackingMode::ObjectBacked, TableCatalogBackingMode::DurableStrong]
    }

    fn enabled_config() -> TableMaintenanceConfig {
        TableMaintenanceConfig {
            background_enabled: true,
            delete_enabled: true,
            ..Default::default()
        }
    }

    async fn seed_table(
        store: &TestStore,
        backend: &TestCatalogObjectBackend,
        bucket: &str,
        namespace: &str,
        table: &str,
        config: Option<TableMaintenanceConfig>,
    ) -> TableEntry {
        let namespace = Namespace::parse(namespace).expect("fixture namespace");
        let table = IdentifierSegment::parse(table).expect("fixture table");
        if store.get_table_bucket(bucket).await.expect("read fixture bucket").is_none() {
            store
                .put_table_bucket(test_bucket_entry(bucket))
                .await
                .expect("create fixture bucket");
        }
        if store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("read fixture namespace")
            .is_none()
        {
            store
                .create_namespace(test_namespace_entry(bucket, &namespace))
                .await
                .expect("create fixture namespace");
        }
        let metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let mut entry = test_table_entry(bucket, &namespace, &table, metadata.clone());
        entry.table_id = uuid::Uuid::new_v4().to_string();
        entry.table_uuid = entry.table_id.clone();
        entry.warehouse_location = format!("s3://{bucket}/tables/{}", entry.table_id);
        store.create_table(entry.clone()).await.expect("create fixture table");
        backend
            .seed_object(bucket, &metadata, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        if let Some(config) = config {
            store
                .put_table_maintenance_config(bucket, &namespace.public_name(), table.as_str(), config)
                .await
                .expect("configure fixture table");
        }
        entry
    }

    async fn current(store: &TestStore, table: &TableEntry) -> Option<TableMetadataMaintenanceReport> {
        store
            .get_table_metadata_maintenance_report(
                &table.table_bucket,
                &table.namespace,
                &table.table,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await
            .expect("read current fixture job")
    }

    async fn seed_report(backend: &TestCatalogObjectBackend, table: &TableEntry, report: &TableMetadataMaintenanceReport) {
        let namespace = Namespace::parse(&table.namespace).expect("fixture namespace");
        let name = IdentifierSegment::parse(&table.table).expect("fixture table");
        let paths = TableCatalogObjectPaths::default();
        for path in [
            paths.table_maintenance_job_path(&table.table_bucket, &namespace, &name, &table.table_id, &report.job.job_id),
            paths.table_maintenance_latest_job_path(&table.table_bucket, &namespace, &name, &table.table_id),
            paths.table_maintenance_current_job_path(&table.table_bucket, &namespace, &name, &table.table_id),
        ] {
            backend
                .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(report).expect("serialize fixture report"))
                .await;
        }
    }

    async fn object_versions(backend: &TestCatalogObjectBackend) -> BTreeMap<(String, String), (Vec<u8>, String)> {
        backend
            .state
            .lock()
            .await
            .objects
            .iter()
            .map(|(key, value)| (key.clone(), (value.data.clone(), value.etag.clone())))
            .collect()
    }

    async fn run_pass(scheduler: &mut PeriodicMaintenanceScheduler<TestCatalogObjectBackend>) -> TableCatalogStoreResult<()> {
        scheduler
            .run_pass(&CancellationToken::new(), Instant::now() + StdDuration::from_secs(60))
            .await
    }

    #[tokio::test]
    async fn periodic_queue_is_idempotent_and_never_runs_delete_worker() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
            let namespace = Namespace::parse(&table.namespace).expect("fixture namespace");
            let name = IdentifierSegment::parse(&table.table).expect("fixture table");
            let orphan = default_table_metadata_file_path(&namespace, &name, "00000.metadata.json");
            backend
                .seed_object(&table.table_bucket, &orphan, br#"{"metadata-log":[]}"#.to_vec())
                .await;
            let mut scheduler =
                PeriodicMaintenanceScheduler::new(store.clone(), vec![table.table_bucket.clone()], StdDuration::from_secs(300));

            run_pass(&mut scheduler).await.expect("first periodic pass queues work");
            let queued = current(&store, &table).await.expect("queued job");
            assert_eq!(queued.job.status, TableMetadataMaintenanceJobStatus::Queued);
            assert_eq!(queued.job.operation, TableMetadataMaintenanceOperation::Delete);
            assert!(queued.job.worker_id.is_none());
            assert_eq!(queued.job.attempt, 0);
            assert!(
                backend
                    .read_object(&table.table_bucket, &orphan)
                    .await
                    .expect("read orphan")
                    .is_some()
            );
            assert!(backend.state.lock().await.delete_attempts.is_empty());
            assert_eq!(
                store
                    .load_table(&table.table_bucket, &table.namespace, &table.table)
                    .await
                    .expect("read unchanged table"),
                Some(table.clone())
            );

            let versions = object_versions(&backend).await;
            run_pass(&mut scheduler).await.expect("repeated pass skips queued work");
            assert_eq!(
                object_versions(&backend).await,
                versions,
                "queue-only repetition must not rewrite history or aliases"
            );
        }
    }

    #[tokio::test]
    async fn periodic_queue_respects_disabled_paused_and_inherited_configuration() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let disabled = seed_table(&store, &backend, "disabled", "sales", "orders", None).await;
            let paused = seed_table(
                &store,
                &backend,
                "paused",
                "sales",
                "orders",
                Some(TableMaintenanceConfig {
                    worker_paused: true,
                    ..enabled_config()
                }),
            )
            .await;
            let inherited = seed_table(&store, &backend, "inherited", "sales", "orders", None).await;
            backend
                .seed_object(
                    RUSTFS_META_BUCKET,
                    &TableCatalogObjectPaths::default().table_bucket_maintenance_config_path("inherited"),
                    serde_json::to_vec(&enabled_config()).expect("serialize bucket defaults"),
                )
                .await;
            let mut scheduler = PeriodicMaintenanceScheduler::new(
                store.clone(),
                vec!["disabled".to_string(), "paused".to_string(), "inherited".to_string()],
                StdDuration::from_secs(300),
            );
            run_pass(&mut scheduler).await.expect("configuration-aware pass");
            assert!(
                current(&store, &disabled).await.is_none(),
                "disabled tables must not gain control history"
            );
            assert!(current(&store, &paused).await.is_none(), "paused tables must not gain control history");
            let queued = current(&store, &inherited)
                .await
                .expect("bucket defaults must enable admission");
            assert_eq!(queued.job.config_source, TableMaintenanceConfigSource::TableBucketDefault);
            assert_eq!(queued.job.status, TableMetadataMaintenanceJobStatus::Queued);
        }
    }

    #[tokio::test]
    async fn periodic_queue_never_recovers_failed_expired_or_quarantined_jobs() {
        for backing in backings() {
            for status in [
                TableMetadataMaintenanceJobStatus::Failed,
                TableMetadataMaintenanceJobStatus::Queued,
                TableMetadataMaintenanceJobStatus::Running,
                TableMetadataMaintenanceJobStatus::Successful,
            ] {
                let backend = TestCatalogObjectBackend::default();
                let store = TestStore::new_for_test(backend.clone(), backing);
                let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
                let mut report = store
                    .run_table_maintenance_scheduler_once("analytics", "sales", "orders", "fixture".to_string())
                    .await
                    .expect("seed real queued report")
                    .report;
                report.job.status = status.clone();
                report.job.attempt = 4;
                report.job.max_retry_attempts = 3;
                report.job.scheduled_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH));
                report.job.heartbeat_at = report.job.scheduled_at.clone();
                report.job.finished_at = report.job.scheduled_at.clone();
                report.job.next_retry_after = None;
                if status == TableMetadataMaintenanceJobStatus::Successful {
                    report.job.quarantine_enabled = true;
                    report.job.quarantined_object_count = 1;
                    report.job.quarantine_retention_seconds = 3600;
                }
                seed_report(&backend, &table, &report).await;
                let versions = object_versions(&backend).await;
                let mut scheduler =
                    PeriodicMaintenanceScheduler::new(store.clone(), vec!["analytics".to_string()], StdDuration::from_secs(300));
                run_pass(&mut scheduler)
                    .await
                    .expect("manual recovery boundary is a normal skip");
                assert_eq!(
                    object_versions(&backend).await,
                    versions,
                    "periodic passes must preserve failed budgets, expired leases and quarantine"
                );
            }
        }
    }

    #[tokio::test]
    async fn periodic_queue_success_cooldown_requires_valid_persisted_timestamp() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
            let mut report = store
                .run_table_maintenance_scheduler_once("analytics", "sales", "orders", "fixture".to_string())
                .await
                .expect("seed real report")
                .report;
            report.job.status = TableMetadataMaintenanceJobStatus::Successful;
            report.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::now_utc()));
            seed_report(&backend, &table, &report).await;
            let mut scheduler =
                PeriodicMaintenanceScheduler::new(store.clone(), vec!["analytics".to_string()], StdDuration::from_secs(300));
            let versions = object_versions(&backend).await;
            run_pass(&mut scheduler).await.expect("recent success is cooling down");
            assert_eq!(object_versions(&backend).await, versions);
            for timestamp in [None, Some("not-a-timestamp".to_string())] {
                report.job.finished_at = timestamp;
                seed_report(&backend, &table, &report).await;
                let versions = object_versions(&backend).await;
                assert!(matches!(run_pass(&mut scheduler).await, Err(TableCatalogStoreError::Invalid(_))));
                assert_eq!(object_versions(&backend).await, versions, "invalid cooldown must fail without a new job");
            }
            report.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH));
            seed_report(&backend, &table, &report).await;
            run_pass(&mut scheduler).await.expect("expired cooldown permits a new job");
            let queued = current(&store, &table).await.expect("next queue");
            assert_eq!(queued.job.status, TableMetadataMaintenanceJobStatus::Queued);
            assert_ne!(queued.job.job_id, report.job.job_id);
        }
    }

    #[tokio::test]
    async fn periodic_queue_pages_and_round_robins_buckets_across_passes() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let buckets = ["first", "second", "third"];
            let mut tables = Vec::new();
            for bucket in buckets {
                for index in 0..5 {
                    tables.push(
                        seed_table(
                            &store,
                            &backend,
                            bucket,
                            if index < 2 { "sales" } else { "support" },
                            &format!("table{index}"),
                            Some(enabled_config()),
                        )
                        .await,
                    );
                }
            }
            let mut scheduler = PeriodicMaintenanceScheduler::new(
                store.clone(),
                buckets.iter().map(|bucket| (*bucket).to_string()).collect(),
                StdDuration::from_secs(300),
            );
            run_pass(&mut scheduler).await.expect("first bounded fair pass");
            let mut per_bucket = BTreeMap::new();
            for table in &tables {
                if current(&store, table).await.is_some() {
                    *per_bucket.entry(table.table_bucket.clone()).or_insert(0_usize) += 1;
                }
            }
            assert_eq!(per_bucket.values().sum::<usize>(), PERIODIC_MAINTENANCE_TABLES_PER_PASS);
            assert_eq!(per_bucket.len(), buckets.len(), "one bucket must not consume every admission");
            assert!(per_bucket.values().all(|count| *count <= 3));
            run_pass(&mut scheduler).await.expect("resume all retained cursors");
            for table in &tables {
                assert_eq!(
                    current(&store, table)
                        .await
                        .expect("every table reached through retained cursors")
                        .job
                        .status,
                    TableMetadataMaintenanceJobStatus::Queued
                );
            }
        }
    }

    #[tokio::test]
    async fn periodic_queue_cancelled_or_expired_pass_does_not_start_discovery() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
            let mut scheduler =
                PeriodicMaintenanceScheduler::new(store, vec!["analytics".to_string()], StdDuration::from_secs(300));
            backend.reset_call_counts().await;
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            scheduler
                .run_pass(&cancelled, Instant::now() + StdDuration::from_secs(60))
                .await
                .expect("cancelled pass");
            scheduler
                .run_pass(&CancellationToken::new(), Instant::now())
                .await
                .expect("expired pass");
            assert_eq!(backend.read_call_count().await, 0);
            assert_eq!(backend.list_call_count().await, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_queue_retains_inflight_publication_after_cancellation_and_deadline() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let first = seed_table(&store, &backend, "analytics", "sales", "a", Some(enabled_config())).await;
            let second = seed_table(&store, &backend, "analytics", "sales", "b", Some(enabled_config())).await;
            let namespace = Namespace::parse(&first.namespace).expect("fixture namespace");
            let name = IdentifierSegment::parse(&first.table).expect("fixture table");
            let path = TableCatalogObjectPaths::default().table_maintenance_current_job_path(
                "analytics",
                &namespace,
                &name,
                &first.table_id,
            );
            let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &path).await;
            let mut scheduler =
                PeriodicMaintenanceScheduler::new(store.clone(), vec!["analytics".to_string()], StdDuration::from_secs(300));
            let cancellation = CancellationToken::new();
            let runner_cancellation = cancellation.clone();
            let deadline = Instant::now() + StdDuration::from_secs(30);
            let task = tokio::spawn(async move { scheduler.run_pass(&runner_cancellation, deadline).await });
            tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
                .await
                .expect("queue reaches publication pause");
            cancellation.cancel();
            tokio::time::advance(StdDuration::from_secs(31)).await;
            assert!(
                !task.is_finished(),
                "cancellation must retain the mutating future until its write returns"
            );
            pause.release();
            task.await.expect("runner task").expect("in-flight queue completes");
            assert_eq!(
                current(&store, &first).await.expect("complete first publication").job.status,
                TableMetadataMaintenanceJobStatus::Queued
            );
            assert!(
                current(&store, &second).await.is_none(),
                "no operation starts after cancellation or admission deadline"
            );
        }
    }

    #[tokio::test]
    async fn periodic_queue_atomic_guard_rejects_a_claim_changed_during_planning() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
            let pause = backend.pause_next_read(&table.table_bucket, &table.metadata_location).await;
            let claimant = store.clone();
            let observed = table.clone();
            let task = tokio::spawn(async move {
                claimant
                    .queue_periodic_table_maintenance(&observed, "first".to_string(), None)
                    .await
            });
            tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
                .await
                .expect("first claimant starts unlocked planning");
            let winner = store
                .queue_periodic_table_maintenance(&table, "second".to_string(), None)
                .await
                .expect("second claimant wins while first is planning");
            let versions = object_versions(&backend).await;
            pause.release();
            assert!(matches!(
                task.await.expect("first claimant task"),
                Err(TableCatalogStoreError::Conflict(_))
            ));
            assert_eq!(
                current(&store, &table).await.expect("winning current job").job.job_id,
                winner.report.job.job_id
            );
            assert_eq!(object_versions(&backend).await, versions, "a stale claim must not publish any sidecar");
            assert_eq!(
                store
                    .load_table(&table.table_bucket, &table.namespace, &table.table)
                    .await
                    .expect("read table"),
                Some(table)
            );
        }
    }

    #[tokio::test]
    async fn periodic_queue_atomic_guard_rejects_changed_identity_config_and_partial_aliases() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
            let mut old_identity = table.clone();
            old_identity.table_id = uuid::Uuid::new_v4().to_string();
            let versions = object_versions(&backend).await;
            assert!(matches!(
                store
                    .queue_periodic_table_maintenance(&old_identity, "periodic".to_string(), None)
                    .await,
                Err(TableCatalogStoreError::Conflict(_))
            ));
            assert_eq!(object_versions(&backend).await, versions);

            for config in [
                TableMaintenanceConfig::default(),
                TableMaintenanceConfig {
                    worker_paused: true,
                    ..enabled_config()
                },
            ] {
                store
                    .put_table_maintenance_config("analytics", "sales", "orders", config)
                    .await
                    .expect("change config after selection");
                let versions = object_versions(&backend).await;
                assert!(matches!(
                    store
                        .queue_periodic_table_maintenance(&table, "periodic".to_string(), None)
                        .await,
                    Err(TableCatalogStoreError::Conflict(_))
                ));
                assert_eq!(
                    object_versions(&backend).await,
                    versions,
                    "changed config must not cause Disabled or Paused control writes"
                );
            }
            store
                .put_table_maintenance_config("analytics", "sales", "orders", enabled_config())
                .await
                .expect("restore fixture config");
            let mut report = store
                .run_table_maintenance_scheduler_once("analytics", "sales", "orders", "fixture".to_string())
                .await
                .expect("seed full report")
                .report;
            report.job.status = TableMetadataMaintenanceJobStatus::Successful;
            report.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH));
            seed_report(&backend, &table, &report).await;
            let expected = table_maintenance_scheduler_job_summary(&report);
            report.job.status = TableMetadataMaintenanceJobStatus::Failed;
            let namespace = Namespace::parse(&table.namespace).expect("fixture namespace");
            let name = IdentifierSegment::parse(&table.table).expect("fixture table");
            let latest = TableCatalogObjectPaths::default().table_maintenance_latest_job_path(
                "analytics",
                &namespace,
                &name,
                &table.table_id,
            );
            backend
                .seed_object(
                    RUSTFS_META_BUCKET,
                    &latest,
                    serde_json::to_vec(&report).expect("serialize partial publication"),
                )
                .await;
            let versions = object_versions(&backend).await;
            assert!(matches!(
                store
                    .queue_periodic_table_maintenance(&table, "periodic".to_string(), Some(&expected))
                    .await,
                Err(TableCatalogStoreError::Conflict(_))
            ));
            assert_eq!(
                object_versions(&backend).await,
                versions,
                "partial report publication requires explicit recovery"
            );
        }
    }

    #[tokio::test]
    async fn periodic_queue_empty_discovery_is_bounded_and_inactive_buckets_are_skipped() {
        for backing in backings() {
            let backend = TestCatalogObjectBackend::default();
            let store = TestStore::new_for_test(backend.clone(), backing);
            store
                .put_table_bucket(test_bucket_entry("empty"))
                .await
                .expect("empty active bucket");
            let mut inactive = test_bucket_entry("inactive");
            inactive.state = TableCatalogEntryState::Deleted;
            store.put_table_bucket(inactive).await.expect("inactive fixture bucket");
            backend.reset_call_counts().await;
            let versions = object_versions(&backend).await;
            let mut scheduler = PeriodicMaintenanceScheduler::new(
                store,
                vec!["empty".to_string(), "inactive".to_string(), "missing".to_string()],
                StdDuration::from_secs(300),
            );
            run_pass(&mut scheduler)
                .await
                .expect("bounded discovery with no eligible tables");
            assert_eq!(object_versions(&backend).await, versions);
            assert!(
                backend.list_call_count().await <= PERIODIC_MAINTENANCE_DISCOVERY_STEPS_PER_PASS,
                "empty discovery must stop at the pass budget"
            );
            assert_eq!(scheduler.next_bucket, PERIODIC_MAINTENANCE_DISCOVERY_STEPS_PER_PASS % 3);
        }
    }

    #[tokio::test]
    async fn periodic_queue_preserves_empty_object_page_continuations() {
        let backend = TestCatalogObjectBackend::default();
        let store = TestStore::new_for_test(backend.clone(), TableCatalogBackingMode::ObjectBacked);
        let table = seed_table(&store, &backend, "analytics", "sales", "orders", Some(enabled_config())).await;
        let prefix = TableCatalogObjectPaths::default().namespace_entries_prefix("analytics");
        for index in 0..TABLE_CATALOG_LIST_MAX_KEYS {
            backend
                .seed_object(RUSTFS_META_BUCKET, &format!("{prefix}!non-entry-{index:04}"), b"fixture".to_vec())
                .await;
        }
        let first_page = store
            .list_namespaces_page("analytics", None, NonZeroUsize::MIN)
            .await
            .expect("bounded empty namespace page");
        assert!(first_page.entries.is_empty());
        assert!(first_page.next_cursor.is_some());
        let mut scheduler =
            PeriodicMaintenanceScheduler::new(store.clone(), vec!["analytics".to_string()], StdDuration::from_secs(300));
        run_pass(&mut scheduler).await.expect("continue past an empty page");
        assert_eq!(
            current(&store, &table)
                .await
                .expect("table after the empty page must be reached")
                .job
                .status,
            TableMetadataMaintenanceJobStatus::Queued
        );
    }
}
