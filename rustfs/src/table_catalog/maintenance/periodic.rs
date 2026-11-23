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

    fn backings() -> [TableCatalogBackingMode; 1] {
        [TableCatalogBackingMode::ObjectBacked]
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

use super::config::{
    VectorMaintenanceBudgetError, VectorMaintenanceBudgets, VectorMaintenanceDiscoveryBudget, VectorMaintenanceLane,
    VectorMaintenanceMemoryReservation, VectorMaintenanceRuntimeConfig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMaintenancePreparationKind {
    Current,
    Deleted,
}

pub(crate) enum VectorMaintenanceTarget {
    InspectRetirementRecovery {
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
        expected_revision: Uuid,
    },
    ExistingJob {
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
    },
    NewJob(Box<VectorMaintenanceSemanticSpec>),
    PrepareRecovery {
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        expected_source: VectorMaintenanceSourceV1,
    },
    PrepareGc {
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        expected_head: Box<VectorMaintenanceHeadV1>,
    },
    Preparation {
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenancePreparationKind,
        expected: CatalogMetadataRefRecordV1,
    },
}

#[derive(Default, Clone)]
pub(crate) struct VectorMaintenanceDiscoveryCursor {
    pub(crate) bucket_cursor: Option<VectorMaintenanceBucketCursor>,
    pub(crate) active_buckets: Vec<VectorMaintenanceActiveBucketCursor>,
    pub(crate) next_bucket: usize,
    pub(crate) memory_owner: Option<Arc<VectorMaintenanceMemoryReservation>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum VectorMaintenanceResourceStage {
    #[default]
    ExistingJobs,
    Preparation,
    Recovery,
    Retirements,
    Ann,
    Compaction,
    Gc,
    OrphanGc,
}

#[derive(Clone)]
pub(crate) struct VectorMaintenanceActiveBucketCursor {
    pub(crate) bucket: BucketHandle,
    pub(crate) resources: Option<VectorMaintenanceResourceCursor>,
    pub(crate) jobs: Option<VectorMaintenanceJobCursor>,
    pub(crate) retirements: Option<VectorRetirementDiscoveryCursor>,
    pub(crate) current_resource: Option<VectorMaintenanceResourceSnapshot>,
    pub(crate) resource_stage: VectorMaintenanceResourceStage,
}

pub(crate) struct VectorMaintenanceDiscoveryFailure {
    pub(crate) bucket: BucketHandle,
    pub(crate) category: VectorMaintenanceFailureCode,
}

pub(crate) struct VectorMaintenanceTargetPage {
    pub(crate) targets: Vec<VectorMaintenanceTarget>,
    pub(crate) failures: Vec<VectorMaintenanceDiscoveryFailure>,
    pub(crate) next_cursor: Option<VectorMaintenanceDiscoveryCursor>,
}

#[async_trait::async_trait]
pub(crate) trait VectorMaintenanceSource: Send + Sync {
    /// Recheck the fixed instance capability and current IAM identity, without rotating it.
    async fn check_admission(&self, identity: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()>;
    async fn authorize_preparation(
        &self,
        identity: &VectorMaintenanceExecutionIdentityV1,
        resource: &VectorMaintenanceResourceV1,
    ) -> TableCatalogStoreResult<()>;
    /// Read-only discovery. Each backend call consumes the shared pass budget.
    async fn next_targets(
        &self,
        cursor: Option<&VectorMaintenanceDiscoveryCursor>,
        limit: NonZeroUsize,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage>;
}

struct VectorMaintenanceQueuedTarget {
    resource: VectorMaintenanceResourceV1,
    action: VectorMaintenanceQueuedAction,
}

enum VectorMaintenanceQueuedAction {
    InspectRetirementRecovery {
        job_id: Uuid,
        expected_revision: Uuid,
    },
    PrepareRecovery {
        kind: VectorMaintenanceKind,
        expected_source: VectorMaintenanceSourceV1,
    },
    PrepareGc {
        kind: VectorMaintenanceKind,
        expected_head: Box<VectorMaintenanceHeadV1>,
    },
    Job {
        job_id: Uuid,
        kind: VectorMaintenanceKind,
        not_before: Option<OffsetDateTime>,
    },
    Preparation {
        kind: VectorMaintenancePreparationKind,
        expected: CatalogMetadataRefRecordV1,
    },
}

impl VectorMaintenanceQueuedTarget {
    fn kind(&self) -> Option<VectorMaintenanceKind> {
        match &self.action {
            VectorMaintenanceQueuedAction::InspectRetirementRecovery { .. } => Some(VectorMaintenanceKind::DeleteRecovery),
            VectorMaintenanceQueuedAction::Job { kind, .. }
            | VectorMaintenanceQueuedAction::PrepareRecovery { kind, .. }
            | VectorMaintenanceQueuedAction::PrepareGc { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    fn lane(&self) -> VectorMaintenanceLane {
        match self.kind() {
            Some(VectorMaintenanceKind::AnnBuild | VectorMaintenanceKind::Compaction) => VectorMaintenanceLane::Compute,
            _ => VectorMaintenanceLane::Control,
        }
    }

    fn priority(&self) -> u8 {
        match self.kind() {
            Some(
                VectorMaintenanceKind::CommitRecovery
                | VectorMaintenanceKind::CreateRecovery
                | VectorMaintenanceKind::DeleteRecovery,
            ) => 0,
            None => 1,
            _ => 2,
        }
    }

    fn ready(&self, now: OffsetDateTime) -> bool {
        match self.action {
            VectorMaintenanceQueuedAction::Job {
                not_before: Some(due), ..
            } => due <= now,
            _ => true,
        }
    }

    fn same_work(&self, other: &Self) -> bool {
        self.resource == other.resource
            && match (&self.action, &other.action) {
                (
                    VectorMaintenanceQueuedAction::InspectRetirementRecovery {
                        job_id: a,
                        expected_revision: ar,
                    },
                    VectorMaintenanceQueuedAction::InspectRetirementRecovery {
                        job_id: b,
                        expected_revision: br,
                    },
                ) => a == b && ar == br,
                (VectorMaintenanceQueuedAction::Job { job_id: a, .. }, VectorMaintenanceQueuedAction::Job { job_id: b, .. }) => {
                    a == b
                }
                (
                    VectorMaintenanceQueuedAction::Preparation { kind: a, expected: ar },
                    VectorMaintenanceQueuedAction::Preparation { kind: b, expected: br },
                ) => a == b && ar == br,
                _ => false,
            }
    }

    fn heap_bytes(&self) -> Option<usize> {
        let bucket = self.resource.bucket();
        let mut sizes = vec![
            bucket.storage_bucket_id.capacity(),
            bucket.account_namespace_id.capacity(),
            bucket.region.capacity(),
            bucket.public_name.capacity(),
            self.resource.namespace().capacity(),
            self.resource.name().capacity(),
            self.resource.table_id().capacity(),
        ];
        if let VectorMaintenanceQueuedAction::Preparation { expected, .. } = &self.action {
            sizes.extend([
                expected.object_key.capacity(),
                expected.content_sha256.capacity(),
                expected.etag.as_ref().map_or(0, String::capacity),
            ]);
        }
        match &self.action {
            VectorMaintenanceQueuedAction::PrepareRecovery { expected_source, .. } => {
                sizes.push(vector_reference_heap_bytes(expected_source.reference())?)
            }
            VectorMaintenanceQueuedAction::PrepareGc { expected_head, .. } => {
                sizes.push(std::mem::size_of::<VectorMaintenanceHeadV1>());
                sizes.push(vector_head_heap_bytes(expected_head)?);
            }
            _ => {}
        }
        sizes.into_iter().try_fold(0usize, usize::checked_add)
    }
}

struct VectorMaintenanceQueueSlot {
    target: VectorMaintenanceQueuedTarget,
    bytes: usize,
    _memory: Arc<VectorMaintenanceMemoryReservation>,
}

struct VectorMaintenanceQueue {
    compute: Vec<VectorMaintenanceQueueSlot>,
    control: Vec<VectorMaintenanceQueueSlot>,
    compute_bytes: usize,
    control_bytes: usize,
    lane_capacity: usize,
    lane_bytes: usize,
    next_compute: VectorMaintenanceKind,
    budgets: Arc<VectorMaintenanceBudgets>,
    _allocation: Arc<VectorMaintenanceMemoryReservation>,
}

impl VectorMaintenanceQueue {
    fn new(config: &VectorMaintenanceRuntimeConfig, budgets: Arc<VectorMaintenanceBudgets>) -> TableCatalogStoreResult<Self> {
        config.validate()?;
        let lane_capacity = config.queue_capacity.get() / 2;
        let lane_bytes = config.queue_bytes.get() / 2;
        let base_bytes = lane_capacity
            .checked_mul(std::mem::size_of::<VectorMaintenanceQueueSlot>())
            .filter(|bytes| *bytes <= lane_bytes)
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance queue storage exceeds its byte limit".into()))?;
        let allocation = budgets
            .try_reserve_memory(
                u64::try_from(base_bytes * 2)
                    .map_err(|_| TableCatalogStoreError::Invalid("maintenance queue size overflow".into()))?,
            )
            .map_err(|_| TableCatalogStoreError::Unavailable("maintenance queue memory is unavailable".into()))?;
        Ok(Self {
            compute: Vec::with_capacity(lane_capacity),
            control: Vec::with_capacity(lane_capacity),
            compute_bytes: base_bytes,
            control_bytes: base_bytes,
            lane_capacity,
            lane_bytes,
            next_compute: VectorMaintenanceKind::AnnBuild,
            budgets,
            _allocation: allocation,
        })
    }

    fn len(&self) -> usize {
        self.compute.len() + self.control.len()
    }

    fn push(&mut self, target: VectorMaintenanceQueuedTarget) -> Result<bool, VectorMaintenanceBudgetError> {
        if self
            .compute
            .iter()
            .chain(&self.control)
            .any(|slot| slot.target.same_work(&target))
        {
            return Ok(false);
        }
        let bytes = target.heap_bytes().ok_or(VectorMaintenanceBudgetError::Exceeded {
            requested_bytes: u64::MAX,
            limit_bytes: self.lane_bytes as u64,
        })?;
        let (lane, used) = match target.lane() {
            VectorMaintenanceLane::Compute => (&mut self.compute, &mut self.compute_bytes),
            VectorMaintenanceLane::Control => (&mut self.control, &mut self.control_bytes),
        };
        if lane.len() == self.lane_capacity {
            return Err(VectorMaintenanceBudgetError::Unavailable);
        }
        let next =
            used.checked_add(bytes)
                .filter(|total| *total <= self.lane_bytes)
                .ok_or(VectorMaintenanceBudgetError::Exceeded {
                    requested_bytes: bytes as u64,
                    limit_bytes: self.lane_bytes as u64,
                })?;
        let memory = self.budgets.try_reserve_memory(bytes as u64)?;
        lane.push(VectorMaintenanceQueueSlot {
            target,
            bytes,
            _memory: memory,
        });
        *used = next;
        Ok(true)
    }

    #[cfg(test)]
    fn pop_ready(&mut self, lane: VectorMaintenanceLane, now: OffsetDateTime) -> Option<VectorMaintenanceQueueSlot> {
        self.pop_ready_excluding(lane, now, &[])
    }

    fn pop_ready_excluding(
        &mut self,
        lane: VectorMaintenanceLane,
        now: OffsetDateTime,
        unavailable: &[VectorMaintenanceResourceV1],
    ) -> Option<VectorMaintenanceQueueSlot> {
        let (items, used) = match lane {
            VectorMaintenanceLane::Compute => (&mut self.compute, &mut self.compute_bytes),
            VectorMaintenanceLane::Control => (&mut self.control, &mut self.control_bytes),
        };
        let index = items
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.target.ready(now) && !unavailable.contains(&slot.target.resource))
            .min_by_key(|(_, slot)| match lane {
                VectorMaintenanceLane::Compute => u8::from(slot.target.kind() != Some(self.next_compute)),
                VectorMaintenanceLane::Control => slot.target.priority(),
            })
            .map(|(index, _)| index)?;
        let slot = items.remove(index);
        *used -= slot.bytes;
        if lane == VectorMaintenanceLane::Compute {
            self.next_compute = if slot.target.kind() == Some(VectorMaintenanceKind::AnnBuild) {
                VectorMaintenanceKind::Compaction
            } else {
                VectorMaintenanceKind::AnnBuild
            };
        }
        Some(slot)
    }

    fn restore(&mut self, slot: VectorMaintenanceQueueSlot) {
        let (lane, used) = match slot.target.lane() {
            VectorMaintenanceLane::Compute => (&mut self.compute, &mut self.compute_bytes),
            VectorMaintenanceLane::Control => (&mut self.control, &mut self.control_bytes),
        };
        *used += slot.bytes;
        lane.push(slot);
    }

    fn earliest_due(&self) -> Option<OffsetDateTime> {
        self.compute
            .iter()
            .chain(&self.control)
            .filter_map(|slot| match slot.target.action {
                VectorMaintenanceQueuedAction::Job { not_before, .. } => not_before,
                _ => None,
            })
            .filter(|due| *due > OffsetDateTime::now_utc())
            .min()
    }
}

fn vector_reference_heap_bytes(reference: &CatalogMetadataRefRecordV1) -> Option<usize> {
    reference
        .object_key
        .capacity()
        .checked_add(reference.content_sha256.capacity())?
        .checked_add(reference.etag.as_ref().map_or(0, String::capacity))
}

fn vector_head_heap_bytes(head: &VectorMaintenanceHeadV1) -> Option<usize> {
    let binding = head.binding();
    let bucket = &binding.bucket;
    let mut bytes = [
        head.version_token().capacity(),
        binding.table_id.capacity(),
        bucket.storage_bucket_id.capacity(),
        bucket.account_namespace_id.capacity(),
        bucket.region.capacity(),
        bucket.public_name.capacity(),
        binding.descriptor.format.capacity(),
        binding.descriptor.payload_base64.capacity(),
        vector_reference_heap_bytes(&binding.metadata)?,
        vector_reference_heap_bytes(&binding.descriptor.metadata)?,
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)?;
    if let Some(reference) = head
        .maintenance()
        .as_ref()
        .and_then(|binding| binding.retirement_tip().as_ref())
    {
        bytes = bytes.checked_add(vector_reference_heap_bytes(reference)?)?;
    }
    Some(bytes)
}

fn vector_retirement_cursor_reference_bytes(reference: &CatalogMetadataRefRecordV1) -> TableCatalogStoreResult<usize> {
    if reference.object_key.len() > 4096
        || reference.content_sha256.len() > 4096
        || reference.etag.as_ref().is_some_and(|etag| etag.len() > 4096)
    {
        return Err(TableCatalogStoreError::Invalid("retirement cursor reference exceeds 4096 bytes".into()));
    }
    vector_reference_heap_bytes(reference)
        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance reference size overflow".into()))
}

fn vector_discovery_cursor_bytes(cursor: &VectorMaintenanceDiscoveryCursor) -> TableCatalogStoreResult<u64> {
    // Cursor continuation strings are each limited to 4096 bytes by the Catalog reader.
    // Reserve their bounded identity storage as well as every retained descriptor capacity.
    let mut bytes = 8192usize
        .checked_add(
            cursor
                .active_buckets
                .capacity()
                .checked_mul(std::mem::size_of::<VectorMaintenanceActiveBucketCursor>())
                .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?,
        )
        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
    let reference_bytes = |reference: &CatalogMetadataRefRecordV1| -> Option<usize> {
        reference
            .object_key
            .capacity()
            .checked_add(reference.content_sha256.capacity())?
            .checked_add(reference.etag.as_ref().map_or(0, String::capacity))
    };
    for active in &cursor.active_buckets {
        let mut retained = 32_768usize;
        if let Some(retirements) = &active.retirements {
            // The cursor retains another validated resource identity and two independent exact references.
            retained = retained
                .checked_add(32_768)
                .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
            for reference in [
                retirements.binding().and_then(|binding| binding.retirement_tip().as_ref()),
                retirements.next(),
            ]
            .into_iter()
            .flatten()
            {
                retained = retained
                    .checked_add(vector_retirement_cursor_reference_bytes(reference)?)
                    .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
            }
        }
        if let Some(resource) = &active.current_resource {
            // The non-serialized exact references have individually bounded keys/ETags.
            retained = retained
                .checked_add(32_768)
                .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
            if let Some(head) = resource.head() {
                let binding = head.binding();
                let bucket = &binding.bucket;
                for capacity in [
                    head.version_token().capacity(),
                    binding.table_id.capacity(),
                    bucket.storage_bucket_id.capacity(),
                    bucket.account_namespace_id.capacity(),
                    bucket.region.capacity(),
                    bucket.public_name.capacity(),
                    binding.descriptor.format.capacity(),
                    binding.descriptor.payload_base64.capacity(),
                    reference_bytes(&binding.metadata)
                        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance reference size overflow".into()))?,
                    reference_bytes(&binding.descriptor.metadata)
                        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance reference size overflow".into()))?,
                ] {
                    retained = retained
                        .checked_add(capacity)
                        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
                }
            }
            for maintenance in [
                resource.maintenance(),
                resource.head().and_then(|head| head.maintenance().as_ref()),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(reference) = maintenance.retirement_tip() {
                    retained = retained
                        .checked_add(
                            reference_bytes(reference)
                                .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance reference size overflow".into()))?,
                        )
                        .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
                }
            }
        }
        bytes = bytes
            .checked_add(retained)
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))?;
    }
    u64::try_from(bytes).map_err(|_| TableCatalogStoreError::Invalid("maintenance cursor size overflow".into()))
}

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use tokio::task::JoinHandle;

#[derive(Default, Debug)]
pub(crate) struct VectorMaintenancePassReport {
    pub(crate) discovered: u64,
    pub(crate) queued: u64,
    pub(crate) skipped: u64,
    pub(crate) started: u64,
    pub(crate) completed: u64,
    pub(crate) partial_recovered: u64,
    pub(crate) retry_pending: u64,
    pub(crate) manual_review: u64,
    pub(crate) failed: u64,
    pub(crate) first_error: Option<VectorMaintenanceFailureCode>,
}

#[cfg(test)]
use super::fault_observation::{
    self, VectorMaintenanceCompletionObserver, VectorMaintenanceObservedOperation, VectorMaintenanceObservedTask,
};

pub(crate) struct VectorMaintenanceScheduler<B: TableCatalogObjectBackend> {
    config: VectorMaintenanceRuntimeConfig,
    backend: B,
    source: Arc<dyn VectorMaintenanceSource>,
    executor: Arc<dyn VectorMaintenanceExecutor<B>>,
    identity: VectorMaintenanceExecutionIdentityV1,
    worker: VectorMaintenanceWorker,
    policy: VectorMaintenancePolicyV1,
    budgets: Arc<VectorMaintenanceBudgets>,
    queue: VectorMaintenanceQueue,
    cursor: Option<VectorMaintenanceDiscoveryCursor>,
    #[cfg(not(test))]
    in_flight: FuturesUnordered<JoinHandle<Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError>>>,
    #[cfg(test)]
    in_flight: FuturesUnordered<VectorMaintenanceObservedTask>,
    #[cfg(test)]
    completion_observer: Option<Arc<VectorMaintenanceCompletionObserver>>,
    #[cfg(test)]
    observation_started: bool,
    first_failure: Option<VectorMaintenanceExecutionError>,
    fatal_failure: Option<VectorMaintenanceExecutionError>,
    next_lane: VectorMaintenanceLane,
}

impl<B: TableCatalogObjectBackend> VectorMaintenanceScheduler<B> {
    pub(crate) fn new(
        config: VectorMaintenanceRuntimeConfig,
        backend: B,
        source: Arc<dyn VectorMaintenanceSource>,
        executor: Arc<dyn VectorMaintenanceExecutor<B>>,
        identity: VectorMaintenanceExecutionIdentityV1,
        node_admission: Option<Arc<rustfs_concurrency::adaptive::NodeAdmission>>,
    ) -> TableCatalogStoreResult<Self> {
        config.validate()?;
        if !config.enabled || identity.account() != &config.execution_account {
            return Err(TableCatalogStoreError::Invalid(
                "Vector maintenance requires its configured execution identity".into(),
            ));
        }
        let budgets = VectorMaintenanceBudgets::new(&config, node_admission)?;
        let queue = VectorMaintenanceQueue::new(&config, budgets.clone())?;
        let policy = VectorMaintenancePolicyV1::try_new(
            config.lease_duration.as_secs(),
            config.heartbeat_interval.as_secs(),
            config.max_attempts.get(),
            config.retry_initial_backoff.as_secs(),
            config.retry_max_backoff.as_secs(),
        )?;
        let worker =
            VectorMaintenanceWorker::try_new(format!("vector-maintenance-{}", Uuid::new_v4()), Uuid::new_v4(), identity.clone())?;
        Ok(Self {
            config,
            backend,
            source,
            executor,
            identity,
            worker,
            policy,
            budgets,
            queue,
            cursor: None,
            in_flight: FuturesUnordered::new(),
            #[cfg(test)]
            completion_observer: None,
            #[cfg(test)]
            observation_started: false,
            first_failure: None,
            fatal_failure: None,
            next_lane: VectorMaintenanceLane::Control,
        })
    }

    #[cfg(test)]
    pub(crate) fn install_completion_observer(
        &mut self,
        observer: Arc<VectorMaintenanceCompletionObserver>,
    ) -> TableCatalogStoreResult<()> {
        if self.observation_started || self.completion_observer.is_some() || self.has_in_flight() || self.queue.len() != 0 {
            return Err(TableCatalogStoreError::Invalid("completion observer requires a new scheduler".into()));
        }
        self.completion_observer = Some(observer);
        Ok(())
    }

    pub(crate) async fn run_pass(
        &mut self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> TableCatalogStoreResult<VectorMaintenancePassReport> {
        #[cfg(test)]
        {
            self.observation_started = true;
        }
        let mut report = VectorMaintenancePassReport::default();
        self.collect_ready(&mut report);
        let budget = VectorMaintenanceDiscoveryBudget::new(cancellation.clone(), deadline, self.config.discovery_calls_per_pass);
        if budget.stopped() {
            return Ok(report);
        }
        self.source.check_admission(&self.identity).await?;
        if budget.stopped() {
            return Ok(report);
        }
        let page = self
            .source
            .next_targets(self.cursor.as_ref(), self.config.resources_per_pass, &budget)
            .await?;
        if page
            .targets
            .len()
            .checked_add(page.failures.len())
            .is_none_or(|count| count > self.config.resources_per_pass.get())
            || page
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.active_buckets.len() > 32 || cursor.next_bucket > cursor.active_buckets.len())
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector maintenance discovery exceeded its page bounds".into(),
            ));
        }
        for failure in page.failures {
            if failure.bucket.kind() != BucketKind::Vector {
                return Err(TableCatalogStoreError::Invalid(
                    "maintenance discovery failure belongs to another bucket scope".into(),
                ));
            }
            report.failed = report.failed.saturating_add(1);
            report.first_error.get_or_insert(failure.category);
        }
        let mut complete_page = true;
        let mut visited = Vec::with_capacity(self.config.resources_per_pass.get());
        for target in page.targets {
            if budget.stopped() {
                complete_page = false;
                break;
            }
            let resource = match &target {
                VectorMaintenanceTarget::ExistingJob { resource, .. }
                | VectorMaintenanceTarget::InspectRetirementRecovery { resource, .. }
                | VectorMaintenanceTarget::Preparation { resource, .. }
                | VectorMaintenanceTarget::PrepareRecovery { resource, .. }
                | VectorMaintenanceTarget::PrepareGc { resource, .. } => resource,
                VectorMaintenanceTarget::NewJob(spec) => &spec.resource,
            };
            let identity = resource.identity()?;
            if visited.contains(&identity) {
                return Err(TableCatalogStoreError::Invalid("duplicate resource in maintenance discovery page".into()));
            }
            visited.push(identity);
            report.discovered = report.discovered.saturating_add(1);
            match self.prepare_target(target, &budget).await {
                Ok(Some(target)) => match self.queue.push(target) {
                    Ok(true) => report.queued = report.queued.saturating_add(1),
                    Ok(false) => report.skipped = report.skipped.saturating_add(1),
                    Err(_) => {
                        complete_page = false;
                        report.skipped = report.skipped.saturating_add(1);
                    }
                },
                Ok(None) => {
                    report.skipped = report.skipped.saturating_add(1);
                    if budget.stopped() {
                        complete_page = false;
                    }
                }
                Err(_) => {
                    report.failed = report.failed.saturating_add(1);
                    report
                        .first_error
                        .get_or_insert(VectorMaintenanceFailureCode::StorageUnavailable);
                }
            }
        }
        if complete_page {
            let mut next_cursor = page.next_cursor;
            if let Some(cursor) = &mut next_cursor {
                let bytes = vector_discovery_cursor_bytes(cursor)?;
                cursor.memory_owner = Some(self.budgets.try_reserve_memory(bytes).map_err(|_| {
                    TableCatalogStoreError::Unavailable("Vector maintenance cursor memory is unavailable".into())
                })?);
            }
            self.cursor = next_cursor;
        }
        if admission_open(cancellation, deadline) {
            report.started = self.start_ready(cancellation).await?;
        }
        Ok(report)
    }

    async fn prepare_target(
        &self,
        target: VectorMaintenanceTarget,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<Option<VectorMaintenanceQueuedTarget>> {
        let (resource, job_id, spec) = match target {
            VectorMaintenanceTarget::InspectRetirementRecovery {
                resource,
                job_id,
                expected_revision,
            } => {
                resource.validate()?;
                if job_id.is_nil() || expected_revision.is_nil() {
                    return Err(TableCatalogStoreError::Invalid("invalid retirement inspection identity".into()));
                }
                return Ok(Some(VectorMaintenanceQueuedTarget {
                    resource,
                    action: VectorMaintenanceQueuedAction::InspectRetirementRecovery {
                        job_id,
                        expected_revision,
                    },
                }));
            }
            VectorMaintenanceTarget::PrepareRecovery {
                resource,
                kind,
                expected_source,
            } => {
                resource.validate()?;
                expected_source.validate()?;
                if !matches!(
                    kind,
                    VectorMaintenanceKind::CommitRecovery
                        | VectorMaintenanceKind::CreateRecovery
                        | VectorMaintenanceKind::DeleteRecovery
                ) {
                    return Err(TableCatalogStoreError::Invalid("invalid maintenance recovery preparation kind".into()));
                }
                return Ok(Some(VectorMaintenanceQueuedTarget {
                    resource,
                    action: VectorMaintenanceQueuedAction::PrepareRecovery { kind, expected_source },
                }));
            }
            VectorMaintenanceTarget::PrepareGc {
                resource,
                kind,
                expected_head,
            } => {
                resource.validate()?;
                expected_head.validate()?;
                if !matches!(kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc) {
                    return Err(TableCatalogStoreError::Invalid("invalid maintenance GC preparation kind".into()));
                }
                return Ok(Some(VectorMaintenanceQueuedTarget {
                    resource,
                    action: VectorMaintenanceQueuedAction::PrepareGc { kind, expected_head },
                }));
            }
            VectorMaintenanceTarget::Preparation {
                resource,
                kind,
                expected,
            } => {
                resource.validate()?;
                let _: CatalogMetadataRef = (&expected).try_into()?;
                return Ok(Some(VectorMaintenanceQueuedTarget {
                    resource,
                    action: VectorMaintenanceQueuedAction::Preparation { kind, expected },
                }));
            }
            VectorMaintenanceTarget::ExistingJob { resource, job_id } => (resource, job_id, None),
            VectorMaintenanceTarget::NewJob(spec) => {
                spec.validate()?;
                if spec.execution_identity != self.identity || spec.policy != self.policy {
                    return Err(TableCatalogStoreError::Conflict(
                        "maintenance plan differs from the installed identity or policy".into(),
                    ));
                }
                let spec = VectorMaintenanceJobSpec::from_semantic(*spec)?;
                (spec.resource().clone(), spec.job_id(), Some(spec))
            }
        };
        if !budget.try_start_call() {
            return Ok(None);
        }
        let store = ObjectTableCatalogStore::for_vector_scope(self.backend.clone(), resource.bucket().to_handle()?).await?;
        let snapshot = if let Some(spec) = spec {
            self.source.check_admission(&self.identity).await?;
            if !budget.try_start_call() {
                return Ok(None);
            }
            store.create_vector_maintenance_job(spec).await?
        } else {
            if !budget.try_start_call() {
                return Ok(None);
            }
            let Some(snapshot) = store.read_vector_maintenance_job(&resource.identity()?, job_id).await? else {
                return Ok(None);
            };
            snapshot
        };
        if snapshot.job().resource() != &resource || snapshot.job().execution_identity() != &self.identity {
            return Ok(None);
        }
        if matches!(
            snapshot.job().state(),
            VectorMaintenanceJobState::Succeeded
                | VectorMaintenanceJobState::Failed
                | VectorMaintenanceJobState::ManualReview
                | VectorMaintenanceJobState::PartialRecovered
                | VectorMaintenanceJobState::RecoveryRequired
        ) {
            return Ok(None);
        }
        let not_before = snapshot
            .job()
            .next_retry_at()
            .as_ref()
            .map(|value| {
                OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid maintenance retry time".into()))
            })
            .transpose()?;
        Ok(Some(VectorMaintenanceQueuedTarget {
            resource,
            action: VectorMaintenanceQueuedAction::Job {
                job_id,
                kind: *snapshot.job().kind(),
                not_before,
            },
        }))
    }

    pub(crate) async fn start_ready(&mut self, cancellation: &CancellationToken) -> TableCatalogStoreResult<u64> {
        #[cfg(test)]
        {
            self.observation_started = true;
        }
        if cancellation.is_cancelled() || self.has_fatal_failure() {
            return Ok(0);
        }
        self.source.check_admission(&self.identity).await?;
        let mut started = 0u64;
        let mut unavailable = Vec::new();
        let attempts = self.queue.len().saturating_mul(2);
        for _ in 0..attempts {
            if cancellation.is_cancelled() {
                break;
            }
            let lane = self.next_lane;
            self.next_lane = match lane {
                VectorMaintenanceLane::Compute => VectorMaintenanceLane::Control,
                VectorMaintenanceLane::Control => VectorMaintenanceLane::Compute,
            };
            let Some(slot) = self.queue.pop_ready_excluding(lane, OffsetDateTime::now_utc(), &unavailable) else {
                continue;
            };
            let resource = slot.target.resource.identity()?;
            let permit = match self.budgets.try_admit(&resource, lane) {
                Ok(permit) => permit,
                Err(_) => {
                    unavailable.push(slot.target.resource.clone());
                    self.queue.restore(slot);
                    continue;
                }
            };
            let backend = self.backend.clone();
            let context = VectorMaintenanceExecutionContext {
                identity: self.identity.clone(),
                source: self.source.clone(),
                executor: self.executor.clone(),
            };
            let worker = self.worker.clone();
            let control = VectorMaintenanceControl::new(
                cancellation.child_token(),
                Instant::now() + self.config.execution_admission_budget,
                self.budgets.clone(),
            );
            #[cfg(test)]
            let observation = self.completion_observer.as_ref().map(|observer| {
                let (job_id, operation) = match &slot.target.action {
                    VectorMaintenanceQueuedAction::Job { job_id, .. } => (Some(*job_id), VectorMaintenanceObservedOperation::Job),
                    VectorMaintenanceQueuedAction::InspectRetirementRecovery { job_id, .. } => {
                        (Some(*job_id), VectorMaintenanceObservedOperation::InspectRetirementRecovery)
                    }
                    VectorMaintenanceQueuedAction::PrepareRecovery { .. } => {
                        (None, VectorMaintenanceObservedOperation::PrepareRecovery)
                    }
                    VectorMaintenanceQueuedAction::PrepareGc { .. } => (None, VectorMaintenanceObservedOperation::PrepareGc),
                    VectorMaintenanceQueuedAction::Preparation {
                        kind: VectorMaintenancePreparationKind::Current,
                        ..
                    } => (None, VectorMaintenanceObservedOperation::EnrollCurrent),
                    VectorMaintenanceQueuedAction::Preparation {
                        kind: VectorMaintenancePreparationKind::Deleted,
                        ..
                    } => (None, VectorMaintenanceObservedOperation::EnrollDeleted),
                };
                observer.begin(&slot.target.resource, job_id, slot.target.kind(), operation)
            });
            #[cfg(test)]
            let task_observation = observation.clone();
            let execution = async move {
                let VectorMaintenanceQueueSlot { target, _memory, .. } = slot;
                let result = async {
                    control.check_admission()?;
                    let store = ObjectTableCatalogStore::for_vector_scope(backend, target.resource.bucket().to_handle()?).await?;
                    match target.action {
                        VectorMaintenanceQueuedAction::InspectRetirementRecovery {
                            job_id,
                            expected_revision,
                        } => {
                            execute_vector_maintenance_action(
                                store,
                                VectorMaintenancePreparationAction::InspectRetirementRecovery {
                                    resource: target.resource,
                                    job_id,
                                    expected_revision,
                                },
                                context,
                                control,
                            )
                            .await
                        }
                        VectorMaintenanceQueuedAction::PrepareRecovery { kind, expected_source } => {
                            execute_vector_maintenance_action(
                                store,
                                VectorMaintenancePreparationAction::PrepareRecovery {
                                    resource: target.resource,
                                    kind,
                                    expected_source,
                                },
                                context,
                                control,
                            )
                            .await
                        }
                        VectorMaintenanceQueuedAction::PrepareGc { kind, expected_head } => {
                            execute_vector_maintenance_action(
                                store,
                                VectorMaintenancePreparationAction::PrepareGc {
                                    resource: target.resource,
                                    kind,
                                    expected_head,
                                },
                                context,
                                control,
                            )
                            .await
                        }
                        VectorMaintenanceQueuedAction::Job { job_id, .. } => {
                            execute_vector_maintenance_job(store, target.resource, job_id, worker, context, control).await
                        }
                        VectorMaintenanceQueuedAction::Preparation { kind, expected } => {
                            execute_vector_maintenance_preparation(store, target.resource, kind, expected, context, control).await
                        }
                    }
                }
                .await;
                if matches!(
                    &result,
                    Ok(VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded)
                        | VectorMaintenanceWorkerResult::Prepared)
                ) {
                    permit.complete();
                }
                // The descriptor allocation also remains owned while dispatch uses its strings.
                drop(_memory);
                result
            };
            #[cfg(test)]
            let execution = fault_observation::scope(task_observation, execution);
            let task = tokio::spawn(execution);
            #[cfg(test)]
            let task = VectorMaintenanceObservedTask::new(task, observation);
            self.in_flight.push(task);
            started = started.saturating_add(1);
        }
        Ok(started)
    }

    pub(crate) fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    pub(crate) fn next_retry_delay(&self) -> Option<StdDuration> {
        self.queue
            .earliest_due()
            .and_then(|due| StdDuration::try_from(due - OffsetDateTime::now_utc()).ok())
    }

    pub(crate) fn has_failure(&self) -> bool {
        self.first_failure.is_some() || self.fatal_failure.is_some()
    }

    pub(crate) fn has_fatal_failure(&self) -> bool {
        self.fatal_failure.is_some()
    }

    pub(crate) fn take_failure(&mut self) -> Option<VectorMaintenanceRuntimeFailure> {
        self.has_failure().then(|| VectorMaintenanceRuntimeFailure {
            task_failure: self.first_failure.take(),
            fatal_failure: self.fatal_failure.take(),
        })
    }

    pub(crate) async fn join_next(&mut self) -> Option<VectorMaintenancePassReport> {
        let result = self.in_flight.next().await?;
        let mut report = VectorMaintenancePassReport::default();
        self.record_completion(
            &mut report,
            result
                .map_err(VectorMaintenanceExecutionError::Join)
                .and_then(std::convert::identity),
        );
        Some(report)
    }

    pub(crate) async fn drain(&mut self) -> VectorMaintenancePassReport {
        let mut report = VectorMaintenancePassReport::default();
        while let Some(result) = self.in_flight.next().await {
            self.record_completion(
                &mut report,
                result
                    .map_err(VectorMaintenanceExecutionError::Join)
                    .and_then(std::convert::identity),
            );
        }
        report
    }

    fn collect_ready(&mut self, report: &mut VectorMaintenancePassReport) {
        while let Some(Some(result)) = self.in_flight.next().now_or_never() {
            self.record_completion(
                report,
                result
                    .map_err(VectorMaintenanceExecutionError::Join)
                    .and_then(std::convert::identity),
            );
        }
    }

    fn record_completion(
        &mut self,
        report: &mut VectorMaintenancePassReport,
        result: Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError>,
    ) {
        if let Some(error) = record_vector_completion(report, result) {
            let retained = if error.requires_runtime_stop() {
                &mut self.fatal_failure
            } else {
                &mut self.first_failure
            };
            if retained.is_none() {
                *retained = Some(error);
            }
        }
    }
}

fn record_vector_completion(
    report: &mut VectorMaintenancePassReport,
    result: Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError>,
) -> Option<VectorMaintenanceExecutionError> {
    match result {
        Ok(
            VectorMaintenanceWorkerResult::Prepared
            | VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded),
        ) => report.completed = report.completed.saturating_add(1),
        Ok(VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::PartialRecovered)) => {
            report.partial_recovered = report.partial_recovered.saturating_add(1)
        }
        Ok(VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::ManualReview)) => {
            report.manual_review = report.manual_review.saturating_add(1)
        }
        Ok(VectorMaintenanceWorkerResult::Finished(
            VectorMaintenanceJobState::RetryScheduled | VectorMaintenanceJobState::RecoveryRequired,
        )) => report.retry_pending = report.retry_pending.saturating_add(1),
        Ok(VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Failed)) => {
            report.failed = report.failed.saturating_add(1);
            report.first_error.get_or_insert(VectorMaintenanceFailureCode::UnknownOutcome);
            return Some(VectorMaintenanceExecutionError::FailedJob);
        }
        Ok(VectorMaintenanceWorkerResult::Skipped(
            VectorMaintenanceNotEligible::NotDue | VectorMaintenanceNotEligible::RecoveryRequired,
        )) => report.retry_pending = report.retry_pending.saturating_add(1),
        Ok(VectorMaintenanceWorkerResult::Replay(state)) => {
            if matches!(
                state,
                VectorMaintenanceJobState::RecoveryRequired | VectorMaintenanceJobState::RetryScheduled
            ) {
                report.retry_pending = report.retry_pending.saturating_add(1);
            } else {
                report.skipped = report.skipped.saturating_add(1);
            }
        }
        Ok(_) => report.skipped = report.skipped.saturating_add(1),
        Err(error) => {
            report.failed = report.failed.saturating_add(1);
            report.first_error.get_or_insert(error.category());
            return Some(error);
        }
    }
    None
}

#[cfg(test)]
mod vector_queue_tests {
    use super::*;
    use crate::storage_api::table::managed::ManagedTableId;
    use crate::table_catalog::vector_maintenance_model::maintenance_now;

    struct ControlledSource;
    #[async_trait::async_trait]
    impl VectorMaintenanceSource for ControlledSource {
        async fn check_admission(&self, _: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()> {
            Ok(())
        }
        async fn authorize_preparation(
            &self,
            _: &VectorMaintenanceExecutionIdentityV1,
            _: &VectorMaintenanceResourceV1,
        ) -> TableCatalogStoreResult<()> {
            Ok(())
        }
        async fn next_targets(
            &self,
            _: Option<&VectorMaintenanceDiscoveryCursor>,
            _: NonZeroUsize,
            _: &VectorMaintenanceDiscoveryBudget,
        ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage> {
            Ok(VectorMaintenanceTargetPage {
                targets: vec![],
                failures: vec![],
                next_cursor: None,
            })
        }
    }
    struct NoExecution;
    #[async_trait::async_trait]
    impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for NoExecution {
        async fn recover_native(
            &self,
            _: Arc<ObjectTableCatalogStore<B>>,
            _: VectorMaintenanceWork,
            _: VectorMaintenanceControl,
        ) -> Result<(), VectorMaintenanceExecutionError> {
            panic!("this test does not admit native recovery");
        }

        async fn execute(
            &self,
            _: Arc<ObjectTableCatalogStore<B>>,
            _: VectorMaintenanceWork,
            _: VectorMaintenanceControl,
        ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
            panic!("this test does not admit an executor");
        }
        async fn prepare(
            &self,
            store: Arc<ObjectTableCatalogStore<B>>,
            action: VectorMaintenancePreparationAction,
            _: VectorMaintenanceControl,
        ) -> Result<(), VectorMaintenanceExecutionError> {
            apply_preparation(store, action).await
        }
    }
    async fn apply_preparation<B: TableCatalogObjectBackend>(
        store: Arc<ObjectTableCatalogStore<B>>,
        action: VectorMaintenancePreparationAction,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        match action {
            VectorMaintenancePreparationAction::EnrollCurrent { expected, commit_id } => {
                store.enroll_vector_maintenance(&expected, commit_id).await?;
            }
            VectorMaintenancePreparationAction::EnrollDeleted {
                resource,
                expected_intent,
                update_id,
            } => {
                store
                    .enroll_deleted_vector_maintenance(&resource, &expected_intent, update_id)
                    .await?;
            }
            VectorMaintenancePreparationAction::PrepareRecovery { .. }
            | VectorMaintenancePreparationAction::PrepareGc { .. }
            | VectorMaintenancePreparationAction::InspectRetirementRecovery { .. } => {
                panic!("this fixture only enrolls resources")
            }
        }
        Ok(())
    }
    fn enabled_config() -> VectorMaintenanceRuntimeConfig {
        VectorMaintenanceRuntimeConfig {
            enabled: true,
            execution_account: "maintenance-test".into(),
            ..Default::default()
        }
    }
    fn execution_identity() -> VectorMaintenanceExecutionIdentityV1 {
        VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [7; 16]).unwrap()
    }

    #[tokio::test]
    async fn vector_runtime_continues_other_resources_and_recovery_after_task_error() {
        struct Executor {
            calls: std::sync::atomic::AtomicUsize,
            entered: tokio::sync::mpsc::Sender<CatalogResourceIdentity>,
        }
        #[async_trait::async_trait]
        impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for Executor {
            async fn recover_native(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                panic!("control preparation has no claim")
            }
            async fn execute(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
                panic!("control preparation has no execution claim")
            }
            async fn prepare(
                &self,
                store: Arc<ObjectTableCatalogStore<B>>,
                action: VectorMaintenancePreparationAction,
                control: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                let VectorMaintenancePreparationAction::PrepareGc {
                    resource, expected_head, ..
                } = action
                else {
                    panic!("expected GC preparation")
                };
                let current = store
                    .read_vector_maintenance_resource(
                        &resource.identity()?,
                        &Namespace::parse(resource.namespace()).unwrap(),
                        &CatalogName::parse(resource.name().clone()).unwrap(),
                    )
                    .await?;
                assert_eq!(current.head(), Some(expected_head.as_ref()));
                control.check_admission()?;
                let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.entered.send(resource.identity()?).await.unwrap();
                if call == 0 {
                    return Err(VectorMaintenanceExecutionError::Executor {
                        category: VectorMaintenanceFailureCode::StorageUnavailable,
                        source: Box::new(std::io::Error::from_raw_os_error(5)),
                    });
                }
                Ok(())
            }
        }
        let mut fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
        fixture.entry.version = 4;
        fixture.entry.vector_maintenance_binding = Some(VectorMaintenanceBindingV1::empty());
        let namespace = Namespace::parse("indexes").unwrap();
        let first = CatalogHead::from_vector_entry(&fixture.entry)
            .unwrap()
            .resource_identity()
            .clone();
        let second = CatalogResourceIdentity::try_new(
            fixture.bucket.clone(),
            ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
            CatalogFormat::Lance,
        )
        .unwrap();
        let key = format!("{}_versions/{}/1.manifest", second.dataset_prefix(), Uuid::new_v4());
        fixture
            .backend
            .seed_object(fixture.bucket.storage_bucket_id().as_str(), &key, b"second-fixture-content".to_vec())
            .await;
        let object = fixture
            .backend
            .read_object(fixture.bucket.storage_bucket_id().as_str(), &key)
            .await
            .unwrap()
            .unwrap();
        let metadata = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone()).unwrap(), &object).unwrap();
        let descriptor =
            CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, metadata.clone(), b"opaque-fixture-descriptor".to_vec())
                .unwrap();
        let mut second_entry = fixture.entry.clone();
        second_entry.table = "second.index".into();
        second_entry.table_id = second.table_id().as_str().into();
        second_entry.table_uuid = second_entry.table_id.clone();
        second_entry.metadata_location = key;
        second_entry.warehouse_location =
            format!("s3://{}/{}", second.storage_bucket_id(), second.dataset_prefix().trim_end_matches('/'));
        second_entry.vector_head_binding = Some(VectorHeadBindingV1::try_new(&second, &metadata, &descriptor).unwrap());
        for entry in [&fixture.entry, &second_entry] {
            let path = fixture.store.paths.table_entry_path(
                fixture.bucket.storage_bucket_id().as_str(),
                &namespace,
                &CatalogName::parse(entry.table.clone()).unwrap(),
            );
            fixture
                .backend
                .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(entry).unwrap())
                .await;
        }
        let (entered, mut received) = tokio::sync::mpsc::channel(3);
        let executor = Arc::new(Executor {
            calls: std::sync::atomic::AtomicUsize::new(0),
            entered,
        });
        let mut scheduler = VectorMaintenanceScheduler::new(
            enabled_config(),
            fixture.backend,
            Arc::new(ControlledSource),
            executor,
            execution_identity(),
            None,
        )
        .unwrap();
        let observer = VectorMaintenanceCompletionObserver::new(|_| None);
        scheduler.install_completion_observer(observer.clone()).unwrap();
        for (entry, kind) in [
            (&fixture.entry, VectorMaintenanceKind::HistoryGc),
            (&second_entry, VectorMaintenanceKind::HistoryGc),
            (&fixture.entry, VectorMaintenanceKind::OrphanGc),
        ] {
            let resource = CatalogHead::from_vector_entry(entry).unwrap().resource_identity().clone();
            let target = scheduler
                .prepare_target(
                    VectorMaintenanceTarget::PrepareGc {
                        resource: VectorMaintenanceResourceV1::try_new(
                            &resource,
                            &namespace,
                            &CatalogName::parse(entry.table.clone()).unwrap(),
                        )
                        .unwrap(),
                        kind,
                        expected_head: Box::new(VectorMaintenanceHeadV1::from_catalog_entry(entry).unwrap()),
                    },
                    &VectorMaintenanceDiscoveryBudget::new(
                        CancellationToken::new(),
                        Instant::now() + StdDuration::from_secs(30),
                        NonZeroUsize::new(64).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            scheduler.queue.push(target).unwrap();
        }
        let runtime = CatalogBackgroundTask::start_fallible(&CancellationToken::new(), move |cancellation| async move {
            assert_eq!(scheduler.start_ready(&cancellation).await.unwrap(), 1);
            super::super::periodic_runtime::run_vector(
                scheduler,
                crate::storage_api::startup::services::ServerContextSlot::new(),
                StdDuration::from_secs(300),
                StdDuration::from_secs(30),
                cancellation,
            )
            .await
        });
        for expected in [&first, &second, &first] {
            assert_eq!(
                tokio::time::timeout(StdDuration::from_secs(5), received.recv())
                    .await
                    .unwrap()
                    .as_ref(),
                Some(expected)
            );
        }
        assert!(
            !runtime.task.as_ref().unwrap().is_finished(),
            "ordinary task error must retain the runtime and executor"
        );
        let error = crate::startup_shutdown::stop_catalog_background_task(Some(runtime))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        let mut source = error.get_ref().map(|source| source as &(dyn std::error::Error + 'static));
        let mut retained = false;
        while let Some(current) = source {
            if let Some(failure) = current.downcast_ref::<VectorMaintenanceRuntimeFailure>() {
                assert!(failure.fatal_failure.is_none());
                let Some(VectorMaintenanceExecutionError::Executor { source, .. }) = &failure.task_failure else {
                    panic!("original I/O error must be retained")
                };
                assert_eq!(source.downcast_ref::<std::io::Error>().unwrap().raw_os_error(), Some(5));
                retained = true;
            }
            source = current.source();
        }
        assert!(retained);
        let observation = observer.snapshot();
        assert!(!observation.overflow);
        assert_eq!(observation.next_sequence, 4);
        assert_eq!(
            observation.completions.iter().map(|item| item.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(observation.completions[0].resource.identity().unwrap(), first);
        assert!(
            observation
                .completions
                .iter()
                .all(|item| item.job_id.is_none() && item.attempt.is_none())
        );
        let failure = observation.completions[0].error.as_ref().unwrap();
        assert!(matches!(failure.phase, fault_observation::VectorMaintenanceObservedPhase::Preparation));
        assert!(matches!(
            failure.variant,
            fault_observation::VectorMaintenanceObservedErrorVariant::Executor
        ));
        assert_eq!(failure.category, VectorMaintenanceFailureCode::StorageUnavailable);
        assert!(failure.sources.iter().any(|source| source.io_raw_os_error == Some(5)));
        assert!(observation.completions[1..].iter().all(|item| item.error.is_none()));
    }

    #[tokio::test]
    async fn vector_scheduler_retains_worker_failure_when_discovery_fails() {
        struct DeniedSource;
        #[async_trait::async_trait]
        impl VectorMaintenanceSource for DeniedSource {
            async fn check_admission(&self, _: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()> {
                Err(TableCatalogStoreError::Unavailable("admission unavailable".into()))
            }
            async fn authorize_preparation(
                &self,
                _: &VectorMaintenanceExecutionIdentityV1,
                _: &VectorMaintenanceResourceV1,
            ) -> TableCatalogStoreResult<()> {
                panic!("admission rejected preparation")
            }
            async fn next_targets(
                &self,
                _: Option<&VectorMaintenanceDiscoveryCursor>,
                _: NonZeroUsize,
                _: &VectorMaintenanceDiscoveryBudget,
            ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage> {
                panic!("admission rejected discovery")
            }
        }
        let mut scheduler = VectorMaintenanceScheduler::new(
            enabled_config(),
            crate::table_catalog::test_support::TestCatalogObjectBackend::default(),
            Arc::new(DeniedSource),
            Arc::new(NoExecution),
            execution_identity(),
            None,
        )
        .unwrap();
        let worker = tokio::spawn(async {
            Err(VectorMaintenanceExecutionError::Executor {
                category: VectorMaintenanceFailureCode::StorageUnavailable,
                source: Box::new(std::io::Error::from_raw_os_error(5)),
            })
        });
        let completion = worker.abort_handle();
        scheduler.in_flight.push(worker.into());
        while !completion.is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(
            scheduler
                .run_pass(&CancellationToken::new(), Instant::now() + StdDuration::from_secs(30))
                .await
                .is_err()
        );
        let failure = scheduler
            .take_failure()
            .expect("completed worker error must survive the pass error");
        assert!(failure.fatal_failure.is_none());
        let VectorMaintenanceExecutionError::Executor { source, .. } = failure.task_failure.unwrap() else {
            panic!("original worker error type")
        };
        assert_eq!(source.downcast_ref::<std::io::Error>().unwrap().raw_os_error(), Some(5));
        assert!(scheduler.take_failure().is_none());
    }

    #[tokio::test]
    async fn vector_scheduler_retains_task_error_and_later_fatal_join() {
        let mut scheduler = VectorMaintenanceScheduler::new(
            enabled_config(),
            crate::table_catalog::test_support::TestCatalogObjectBackend::default(),
            Arc::new(ControlledSource),
            Arc::new(NoExecution),
            execution_identity(),
            None,
        )
        .unwrap();
        scheduler.in_flight.push(
            tokio::spawn(async {
                Err(VectorMaintenanceExecutionError::Executor {
                    category: VectorMaintenanceFailureCode::StorageUnavailable,
                    source: Box::new(std::io::Error::from_raw_os_error(5)),
                })
            })
            .into(),
        );
        assert_eq!(scheduler.join_next().await.unwrap().failed, 1);
        assert!(!scheduler.has_fatal_failure());
        let panicking: JoinHandle<Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError>> =
            tokio::spawn(async { panic!("controlled fatal worker failure") });
        let original_id = panicking.id();
        scheduler.in_flight.push(panicking.into());
        assert_eq!(scheduler.join_next().await.unwrap().failed, 1);
        assert!(scheduler.has_fatal_failure());
        assert_eq!(scheduler.start_ready(&CancellationToken::new()).await.unwrap(), 0);
        let failure = scheduler.take_failure().unwrap();
        assert!(matches!(
            failure.task_failure.as_ref(),
            Some(VectorMaintenanceExecutionError::Executor { .. })
        ));
        let Some(VectorMaintenanceExecutionError::Join(join)) = &failure.fatal_failure else {
            panic!("original JoinError must be retained")
        };
        assert!(join.is_panic());
        assert_eq!(join.id(), original_id);
        assert!(matches!(
            std::error::Error::source(&failure)
                .unwrap()
                .downcast_ref::<VectorMaintenanceExecutionError>(),
            Some(VectorMaintenanceExecutionError::Join(_))
        ));
    }

    #[tokio::test]
    async fn vector_scheduler_drain_retains_failed_job_state() {
        let mut scheduler = VectorMaintenanceScheduler::new(
            enabled_config(),
            crate::table_catalog::test_support::TestCatalogObjectBackend::default(),
            Arc::new(ControlledSource),
            Arc::new(NoExecution),
            execution_identity(),
            None,
        )
        .unwrap();
        let failed = tokio::spawn(async { Ok(VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Failed)) });
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
        scheduler.in_flight.push(failed.into());
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        scheduler.in_flight.push(
            tokio::spawn(async move {
                entered.send(()).unwrap();
                resume.await.unwrap();
                Ok(VectorMaintenanceWorkerResult::Prepared)
            })
            .into(),
        );
        started.await.unwrap();
        let mut draining = Box::pin(scheduler.drain());
        tokio::select! { biased; result = draining.as_mut() => panic!("failure abandoned an accepted owner: {result:?}"), _ = tokio::task::yield_now() => {} }
        release.send(()).unwrap();
        let report = draining.await;
        assert_eq!(report.failed, 1);
        assert_eq!(report.completed, 1);
        assert!(!scheduler.has_fatal_failure());
        assert!(matches!(
            scheduler.take_failure().unwrap().task_failure,
            Some(VectorMaintenanceExecutionError::FailedJob)
        ));
        assert!(!scheduler.has_in_flight());
    }

    #[tokio::test]
    async fn vector_scheduler_inspects_retirement_recovery_without_claiming() {
        struct Inspection {
            entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
            expected_state: VectorMaintenanceJobState,
        }
        #[async_trait::async_trait]
        impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for Inspection {
            async fn recover_native(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                panic!("inspection must not claim a job");
            }
            async fn execute(
                &self,
                _: Arc<ObjectTableCatalogStore<B>>,
                _: VectorMaintenanceWork,
                _: VectorMaintenanceControl,
            ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
                panic!("inspection must use preparation");
            }
            async fn prepare(
                &self,
                store: Arc<ObjectTableCatalogStore<B>>,
                action: VectorMaintenancePreparationAction,
                control: VectorMaintenanceControl,
            ) -> Result<(), VectorMaintenanceExecutionError> {
                assert!(control.dispatch_guard().is_err());
                let VectorMaintenancePreparationAction::InspectRetirementRecovery {
                    resource,
                    job_id,
                    expected_revision,
                } = action
                else {
                    panic!("expected an exact retirement inspection");
                };
                let snapshot = store
                    .read_vector_maintenance_job(&resource.identity()?, job_id)
                    .await?
                    .unwrap();
                assert_eq!(snapshot.job().revision(), &expected_revision);
                assert_eq!(snapshot.job().state(), &self.expected_state);
                assert_eq!(snapshot.job().kind(), &VectorMaintenanceKind::DeleteRecovery);
                assert_eq!(
                    snapshot.job().input().source().as_ref().unwrap().kind(),
                    &VectorMaintenanceSourceKind::RetirementAuthorization
                );
                self.entered.lock().unwrap().take().unwrap().send(()).unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.unwrap();
                Ok(())
            }
        }
        let mut fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
        fixture.entry.version = 4;
        fixture.entry.vector_maintenance_binding = Some(VectorMaintenanceBindingV1::empty());
        let namespace = Namespace::parse("indexes").unwrap();
        let name = CatalogName::parse(fixture.entry.table.clone()).unwrap();
        let path = fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&fixture.entry).unwrap())
            .await;
        let identity = CatalogHead::from_vector_entry(&fixture.entry)
            .unwrap()
            .resource_identity()
            .clone();
        let resource = VectorMaintenanceResourceV1::try_new(&identity, &namespace, &name).unwrap();
        let reference = CatalogMetadataRefRecordV1::from(&fixture.initial);
        let authorization = VectorRetirementAuthorizationV1 {
            version: 1,
            retirement_id: Uuid::new_v4(),
            retirement_revision: Uuid::new_v4(),
            created_by_job_id: Uuid::new_v4(),
            created_by_attempt: 1,
            resource: resource.clone(),
            execution_identity: execution_identity(),
            created_at: maintenance_now(),
            previous_ref: None,
            base_head: VectorMaintenanceHeadV1::from_catalog_entry(&fixture.entry).unwrap(),
            delete_intent_ref: None,
            plan_ref: reference.clone(),
            reference_evidence_ref: reference.clone(),
            targets: vec![VectorRetirementTargetV1 {
                object_key: format!("{}unused", identity.dataset_prefix()),
                body_revision: Uuid::new_v4(),
                mod_time: None,
                content_length: 1,
                etag: None,
                version_id: None,
                delete_marker: false,
            }],
        };
        let authority_path = format!(
            "{}authorizations/{}.json",
            fixture.store.paths.vector_maintenance_prefix(&identity).unwrap(),
            table_catalog_path_hash(&authorization.retirement_id().to_string())
        );
        fixture
            .backend
            .seed_object(
                RUSTFS_META_BUCKET,
                &authority_path,
                encode_vector_maintenance_record(&authorization).unwrap(),
            )
            .await;
        let object = fixture
            .backend
            .read_object(RUSTFS_META_BUCKET, &authority_path)
            .await
            .unwrap()
            .unwrap();
        let authority = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(authority_path).unwrap(), &object).unwrap();
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                resource.clone(),
                VectorMaintenanceKind::DeleteRecovery,
                VectorMaintenanceOrigin::Recovery,
                VectorMaintenanceInputV1::try_new(
                    None,
                    Some(
                        VectorMaintenanceSourceV1::try_new(
                            VectorMaintenanceSourceKind::RetirementAuthorization,
                            (&authority).into(),
                        )
                        .unwrap(),
                    ),
                    None,
                )
                .unwrap(),
                execution_identity(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let job_id = spec.job_id();
        let queued = fixture.store.create_vector_maintenance_job(spec).await.unwrap();
        let observed = fixture
            .store
            .read_vector_maintenance_resource(&identity, &namespace, &name)
            .await
            .unwrap();
        let worker = VectorMaintenanceWorker::try_new("inspection-test".into(), Uuid::new_v4(), execution_identity()).unwrap();
        let VectorMaintenanceClaimOutcome::Acquired(work) = fixture
            .store
            .claim_vector_maintenance_job(
                &identity,
                job_id,
                VectorMaintenanceClaimCondition::from_snapshot(&queued, observed),
                worker.clone(),
                Uuid::new_v4(),
            )
            .await
            .unwrap()
        else {
            panic!("fixture claim failed")
        };
        let job_path = fixture.store.paths.vector_maintenance_job_path(&identity, job_id).unwrap();
        for state in [
            VectorMaintenanceJobState::RecoveryRequired,
            VectorMaintenanceJobState::ManualReview,
        ] {
            let mut job = work.snapshot().job().clone();
            job.state = state;
            job.failure = Some(VectorMaintenanceFailureCode::UnknownOutcome);
            job.finished_at = (state == VectorMaintenanceJobState::ManualReview).then(maintenance_now);
            fixture
                .backend
                .seed_object(RUSTFS_META_BUCKET, &job_path, encode_vector_maintenance_record(&job).unwrap())
                .await;
            let snapshot = fixture
                .store
                .read_vector_maintenance_job(&identity, job_id)
                .await
                .unwrap()
                .unwrap();
            let observed = fixture
                .store
                .read_vector_maintenance_resource(&identity, &namespace, &name)
                .await
                .unwrap();
            let result = fixture
                .store
                .claim_vector_maintenance_job(
                    &identity,
                    job_id,
                    VectorMaintenanceClaimCondition::from_snapshot(&snapshot, observed),
                    worker.clone(),
                    Uuid::new_v4(),
                )
                .await
                .unwrap();
            assert!(matches!(
                (state, result),
                (
                    VectorMaintenanceJobState::RecoveryRequired,
                    VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::RecoveryRequired)
                ) | (
                    VectorMaintenanceJobState::ManualReview,
                    VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::Terminal)
                )
            ));
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let config = VectorMaintenanceRuntimeConfig {
                queue_capacity: NonZeroUsize::new(2).unwrap(),
                ..enabled_config()
            };
            let mut scheduler = VectorMaintenanceScheduler::new(
                config,
                fixture.backend.clone(),
                Arc::new(ControlledSource),
                Arc::new(Inspection {
                    entered: std::sync::Mutex::new(Some(entered)),
                    release: std::sync::Mutex::new(Some(resume)),
                    expected_state: state,
                }),
                execution_identity(),
                None,
            )
            .unwrap();
            let budget = VectorMaintenanceDiscoveryBudget::new(
                CancellationToken::new(),
                Instant::now() + StdDuration::from_secs(30),
                NonZeroUsize::new(64).unwrap(),
            );
            assert!(
                scheduler
                    .prepare_target(
                        VectorMaintenanceTarget::ExistingJob {
                            resource: resource.clone(),
                            job_id
                        },
                        &budget
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            let make_target = || VectorMaintenanceTarget::InspectRetirementRecovery {
                resource: resource.clone(),
                job_id,
                expected_revision: *snapshot.job().revision(),
            };
            for (invalid_job, invalid_revision) in [(Uuid::nil(), *snapshot.job().revision()), (job_id, Uuid::nil())] {
                assert!(matches!(
                    scheduler
                        .prepare_target(
                            VectorMaintenanceTarget::InspectRetirementRecovery {
                                resource: resource.clone(),
                                job_id: invalid_job,
                                expected_revision: invalid_revision,
                            },
                            &budget
                        )
                        .await,
                    Err(TableCatalogStoreError::Invalid(_))
                ));
            }
            let target = scheduler.prepare_target(make_target(), &budget).await.unwrap().unwrap();
            assert_eq!(target.lane(), VectorMaintenanceLane::Control);
            assert_eq!(target.priority(), 0);
            let before_bytes = scheduler.queue.control_bytes;
            assert!(scheduler.queue.push(target).unwrap());
            assert!(scheduler.queue.control_bytes > before_bytes);
            let duplicate = scheduler.prepare_target(make_target(), &budget).await.unwrap().unwrap();
            assert!(!scheduler.queue.push(duplicate).unwrap());
            let next_revision = scheduler
                .prepare_target(
                    VectorMaintenanceTarget::InspectRetirementRecovery {
                        resource: resource.clone(),
                        job_id,
                        expected_revision: Uuid::new_v4(),
                    },
                    &budget,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(scheduler.queue.push(next_revision).is_err());
            assert_eq!(scheduler.start_ready(&CancellationToken::new()).await.unwrap(), 1);
            tokio::time::timeout(StdDuration::from_secs(5), started)
                .await
                .unwrap()
                .unwrap();
            assert!(
                scheduler
                    .budgets
                    .try_admit(&identity, VectorMaintenanceLane::Compute)
                    .is_err()
            );
            release.send(()).unwrap();
            let result = tokio::time::timeout(StdDuration::from_secs(5), scheduler.drain())
                .await
                .unwrap();
            assert_eq!(result.completed, 1);
            assert_eq!(
                fixture
                    .store
                    .read_vector_maintenance_job(&identity, job_id)
                    .await
                    .unwrap()
                    .unwrap(),
                snapshot
            );
        }
    }

    #[test]
    fn vector_retirement_cursor_charges_capacity_and_rejects_oversized_references() {
        let mut key = String::with_capacity(16_384);
        key.push_str("retirement.json");
        let mut etag = String::with_capacity(8192);
        etag.push_str("etag");
        let mut reference = CatalogMetadataRefRecordV1 {
            object_key: key,
            content_length: 1,
            content_sha256: "a".repeat(64),
            body_revision: Some(Uuid::new_v4()),
            etag: Some(etag),
        };
        assert_eq!(
            vector_retirement_cursor_reference_bytes(&reference).unwrap(),
            reference.object_key.capacity() + reference.content_sha256.capacity() + reference.etag.as_ref().unwrap().capacity()
        );
        reference.object_key = "k".repeat(4096);
        reference.etag = Some("e".repeat(4096));
        assert!(vector_retirement_cursor_reference_bytes(&reference).is_ok());
        reference.object_key.push('k');
        assert!(matches!(
            vector_retirement_cursor_reference_bytes(&reference),
            Err(TableCatalogStoreError::Invalid(_))
        ));
        reference.object_key.pop();
        reference.etag.as_mut().unwrap().push('e');
        assert!(matches!(
            vector_retirement_cursor_reference_bytes(&reference),
            Err(TableCatalogStoreError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn vector_discovery_cursor_reserves_retirement_continuation_memory() {
        let mut fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
        fixture.entry.version = 4;
        let tip = CatalogMetadataRefRecordV1::from(&fixture.initial);
        fixture.entry.vector_maintenance_binding = Some(
            serde_json::from_value(serde_json::json!({
                "version": 1, "retirement_count": 1, "retirement_tip": tip,
            }))
            .unwrap(),
        );
        let namespace = Namespace::parse("indexes").unwrap();
        let name = CatalogName::parse(fixture.entry.table.clone()).unwrap();
        let path = fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&fixture.entry).unwrap())
            .await;
        let resource = CatalogHead::from_vector_entry(&fixture.entry)
            .unwrap()
            .resource_identity()
            .clone();
        let budget = VectorMaintenanceDiscoveryBudget::new(
            CancellationToken::new(),
            Instant::now() + StdDuration::from_secs(30),
            NonZeroUsize::new(3).unwrap(),
        );
        let (items, continuation) = fixture
            .store
            .list_vector_retirement_authorities(
                &resource,
                &namespace,
                &name,
                None,
                std::num::NonZeroU16::new(1).unwrap(),
                &budget,
            )
            .await
            .unwrap()
            .into_parts();
        assert!(items.is_empty());
        assert_eq!(budget.remaining_calls(), 0);
        let continuation = continuation.expect("budget exhaustion must preserve the exact next authority");
        assert!(continuation.next().is_some());
        let mut cursor = VectorMaintenanceDiscoveryCursor {
            active_buckets: vec![VectorMaintenanceActiveBucketCursor {
                bucket: fixture.bucket.clone(),
                resources: None,
                jobs: None,
                retirements: None,
                current_resource: None,
                resource_stage: VectorMaintenanceResourceStage::Retirements,
            }],
            ..Default::default()
        };
        let before = vector_discovery_cursor_bytes(&cursor).unwrap();
        cursor.active_buckets[0].retirements = Some(continuation);
        let after = vector_discovery_cursor_bytes(&cursor).unwrap();
        assert!(before < VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES);
        assert!(after > VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES);
        let config = VectorMaintenanceRuntimeConfig::default();
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let _other_work = budgets
            .try_reserve_memory(config.memory_bytes.get() - VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES)
            .unwrap();
        assert!(budgets.try_reserve_memory(before).is_ok());
        assert!(budgets.try_reserve_memory(after).is_err());
    }

    #[tokio::test]
    async fn vector_scheduler_uses_stable_catalog_job_ids_and_preserves_terminal_state() {
        let mut fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
        fixture.entry.version = 4;
        fixture.entry.vector_maintenance_binding = Some(VectorMaintenanceBindingV1::empty());
        let namespace = Namespace::parse("indexes").unwrap();
        let name = CatalogName::parse(fixture.entry.table.clone()).unwrap();
        let path = fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&fixture.entry).unwrap())
            .await;
        let identity = CatalogHead::from_vector_entry(&fixture.entry)
            .unwrap()
            .resource_identity()
            .clone();
        let resource = VectorMaintenanceResourceV1::try_new(&identity, &namespace, &name).unwrap();
        let observed = fixture
            .store
            .read_vector_maintenance_resource(&identity, &namespace, &name)
            .await
            .unwrap();
        let spec = VectorMaintenanceSemanticSpec::try_new(
            resource.clone(),
            VectorMaintenanceKind::AnnBuild,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(observed.head().cloned(), None, None).unwrap(),
            execution_identity(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .unwrap();
        let scheduler = VectorMaintenanceScheduler::new(
            enabled_config(),
            fixture.backend.clone(),
            Arc::new(ControlledSource),
            Arc::new(NoExecution),
            execution_identity(),
            None,
        )
        .unwrap();
        let budget = VectorMaintenanceDiscoveryBudget::new(
            CancellationToken::new(),
            Instant::now() + StdDuration::from_secs(30),
            NonZeroUsize::new(64).unwrap(),
        );
        let (job_id, _) = vector_maintenance_job_ids(&spec).unwrap();
        assert!(
            scheduler
                .prepare_target(VectorMaintenanceTarget::NewJob(Box::new(spec.clone())), &budget)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            scheduler
                .prepare_target(VectorMaintenanceTarget::NewJob(Box::new(spec.clone())), &budget)
                .await
                .unwrap()
                .is_some()
        );
        let job_path = fixture.store.paths.vector_maintenance_job_path(&identity, job_id).unwrap();
        assert_eq!(fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &job_path).await, 1);
        let original = fixture
            .store
            .read_vector_maintenance_job(&identity, job_id)
            .await
            .unwrap()
            .unwrap();
        let observed = fixture
            .store
            .read_vector_maintenance_resource(&identity, &namespace, &name)
            .await
            .unwrap();
        let claimed = fixture
            .store
            .claim_vector_maintenance_job(
                &identity,
                job_id,
                VectorMaintenanceClaimCondition::from_snapshot(&original, observed),
                VectorMaintenanceWorker::try_new("terminal-fixture".into(), Uuid::new_v4(), execution_identity()).unwrap(),
                Uuid::new_v4(),
            )
            .await
            .unwrap();
        let VectorMaintenanceClaimOutcome::Acquired(work) = claimed else {
            panic!("fixture claim was not acquired");
        };
        let mut terminal = work.snapshot().job().clone();
        terminal.state = VectorMaintenanceJobState::ManualReview;
        terminal.failure = Some(VectorMaintenanceFailureCode::UnknownOutcome);
        terminal.finished_at = Some(maintenance_now());
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &job_path, encode_vector_maintenance_record(&terminal).unwrap())
            .await;
        assert!(
            scheduler
                .prepare_target(VectorMaintenanceTarget::NewJob(Box::new(spec)), &budget)
                .await
                .unwrap()
                .is_none()
        );
        let after = fixture
            .store
            .read_vector_maintenance_job(&identity, job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.job().state(), &VectorMaintenanceJobState::ManualReview);
        assert_eq!(fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &job_path).await, 2);
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial]
    fn vector_scheduler_enrolls_then_drains_real_dispatch_owner() {
        crate::table_catalog::tests::prepared_commit::run_native_vector_delete_test(
            "vector-maintenance-owned-dispatch",
            || async {
                use std::sync::Mutex;
                use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
                struct AdmissionSource(Arc<AtomicBool>);
                #[async_trait::async_trait]
                impl VectorMaintenanceSource for AdmissionSource {
                    async fn check_admission(&self, _: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()> {
                        if !self.0.load(Ordering::SeqCst) {
                            return Err(TableCatalogStoreError::Unavailable("selected identity was revoked".into()));
                        }
                        Ok(())
                    }
                    async fn authorize_preparation(
                        &self,
                        identity: &VectorMaintenanceExecutionIdentityV1,
                        resource: &VectorMaintenanceResourceV1,
                    ) -> TableCatalogStoreResult<()> {
                        ControlledSource.authorize_preparation(identity, resource).await
                    }
                    async fn next_targets(
                        &self,
                        cursor: Option<&VectorMaintenanceDiscoveryCursor>,
                        limit: NonZeroUsize,
                        budget: &VectorMaintenanceDiscoveryBudget,
                    ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage> {
                        ControlledSource.next_targets(cursor, limit, budget).await
                    }
                }
                struct HeldExecutor {
                    observed: VectorMaintenanceResourceSnapshot,
                    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
                    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
                    calls: AtomicUsize,
                    recoveries: AtomicUsize,
                    recovery_preparations: AtomicUsize,
                    recovery_fails: AtomicBool,
                    revoke_identity: AtomicBool,
                    admission_valid: Arc<AtomicBool>,
                }
                #[async_trait::async_trait]
                impl<B: TableCatalogObjectBackend> VectorMaintenanceExecutor<B> for HeldExecutor {
                    async fn recover_native(
                        &self,
                        store: Arc<ObjectTableCatalogStore<B>>,
                        work: VectorMaintenanceWork,
                        control: VectorMaintenanceControl,
                    ) -> Result<(), VectorMaintenanceExecutionError> {
                        assert!(control.dispatch_guard().is_err());
                        self.recoveries.fetch_add(1, Ordering::SeqCst);
                        let guard = Arc::new(store.begin_vector_maintenance_dispatch(work.lease()).await?);
                        let lifecycle = Arc::downgrade(&guard.lifecycle_owner());
                        let dispatch = Arc::downgrade(&guard.dispatch_owner());
                        let scoped_control = control.with_dispatch(guard.clone());
                        let record = work.snapshot().job().resource();
                        let bucket = record.bucket().to_handle()?;
                        let namespace = Namespace::parse(record.namespace()).unwrap();
                        let name = CatalogName::parse(record.name().clone()).unwrap();
                        let proof = store
                            .begin_vector_operation_with_lifecycle(&bucket, &namespace, &name, guard.lifecycle_owner())
                            .await?;
                        store.read_vector_maintenance_resource_with_guard(&proof).await?;
                        drop(proof);
                        let current = store.read_vector_maintenance_job_with_guard(&guard).await?;
                        store
                            .heartbeat_vector_maintenance_job(&guard, *current.job().revision())
                            .await?;
                        drop(scoped_control);
                        drop(guard);
                        assert!(lifecycle.upgrade().is_none());
                        assert!(dispatch.upgrade().is_none());

                        // These actual writers also exclude independently allocated outer owners.
                        let lifecycle_writer = tokio::time::timeout(
                            StdDuration::from_secs(5),
                            store.backend.acquire_write_lock(
                                bucket.storage_bucket_id().as_str(),
                                crate::storage_api::table::test_support::BUCKET_LIFECYCLE_LOCK_OBJECT,
                            ),
                        )
                        .await
                        .expect("native recovery must have no outer lifecycle reader")?;
                        let dispatch_path = store
                            .paths
                            .vector_maintenance_dispatch_path(work.lease().resource(), work.lease().job_id())?;
                        let dispatch_writer = tokio::time::timeout(
                            StdDuration::from_secs(5),
                            store.backend.acquire_write_lock(store.catalog_bucket(), &dispatch_path),
                        )
                        .await
                        .expect("native recovery must have no outer dispatch reader")?;
                        let gc_path = store.paths.vector_gc_lock_path(
                            bucket.storage_bucket_id().as_str(),
                            work.lease().resource().table_id().as_str(),
                        );
                        let gc_writer = tokio::time::timeout(
                            StdDuration::from_secs(5),
                            store.backend.acquire_write_lock(store.catalog_bucket(), &gc_path),
                        )
                        .await
                        .expect("native recovery must have no outer GC reader")?;
                        assert!(!lifecycle_writer.is_lock_lost());
                        assert!(!dispatch_writer.is_lock_lost());
                        assert!(!gc_writer.is_lock_lost());
                        drop(gc_writer);
                        drop(dispatch_writer);
                        drop(lifecycle_writer);
                        if self.recovery_fails.load(Ordering::SeqCst) {
                            return Err(TableCatalogStoreError::Unavailable("native recovery remains unresolved".into()).into());
                        }
                        if self.revoke_identity.load(Ordering::SeqCst) {
                            self.admission_valid.store(false, Ordering::SeqCst);
                        }
                        Ok(())
                    }

                    async fn execute(
                        &self,
                        _: Arc<ObjectTableCatalogStore<B>>,
                        _: VectorMaintenanceWork,
                        control: VectorMaintenanceControl,
                    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
                        control.dispatch_guard()?.check_held()?;
                        self.calls.fetch_add(1, Ordering::SeqCst);
                        let entered = self.entered.lock().unwrap().take().unwrap();
                        let release = self.release.lock().unwrap().take().unwrap();
                        entered.send(()).unwrap();
                        release.await.unwrap();
                        control.dispatch_guard()?.check_held()?;
                        Ok(VectorMaintenanceExecutionOutcome::NoWork {
                            observed: Box::new(self.observed.clone()),
                            reason: VectorMaintenanceNoWorkReason::NoCandidates,
                        })
                    }
                    async fn prepare(
                        &self,
                        store: Arc<ObjectTableCatalogStore<B>>,
                        action: VectorMaintenancePreparationAction,
                        _: VectorMaintenanceControl,
                    ) -> Result<(), VectorMaintenanceExecutionError> {
                        if let VectorMaintenancePreparationAction::PrepareRecovery {
                            resource,
                            kind,
                            expected_source,
                        } = &action
                        {
                            assert_eq!(kind, &VectorMaintenanceKind::CommitRecovery);
                            assert_eq!(expected_source.kind(), &VectorMaintenanceSourceKind::MaintenanceJob);
                            let object = store
                                .backend
                                .read_object(RUSTFS_META_BUCKET, &expected_source.reference().object_key)
                                .await?
                                .unwrap();
                            let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
                            assert_eq!(job.resource(), resource);
                            assert_eq!(job.state(), &VectorMaintenanceJobState::RecoveryRequired);
                            self.recovery_preparations.fetch_add(1, Ordering::SeqCst);
                            return Ok(());
                        }
                        apply_preparation(store, action).await
                    }
                }
                let (store, entry, _fixture_owner) =
                    crate::table_catalog::tests::prepared_commit::native_vector_index_fixture().await;
                let identity = CatalogHead::from_vector_entry(&entry).unwrap().resource_identity().clone();
                let namespace = Namespace::parse("indexes").unwrap();
                let name = CatalogName::parse(entry.table.clone()).unwrap();
                let resource = VectorMaintenanceResourceV1::try_new(&identity, &namespace, &name).unwrap();
                let observed = store
                    .read_vector_maintenance_resource(&identity, &namespace, &name)
                    .await
                    .unwrap();
                assert_eq!(observed.head().unwrap().entry_version(), &3);
                let config = enabled_config();
                let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
                execute_vector_maintenance_preparation(
                    store.clone(),
                    resource.clone(),
                    VectorMaintenancePreparationKind::Current,
                    CatalogMetadataRefRecordV1::from(observed.observed_object()),
                    VectorMaintenanceExecutionContext {
                        identity: execution_identity(),
                        source: Arc::new(ControlledSource),
                        executor: Arc::new(NoExecution),
                    },
                    VectorMaintenanceControl::new(
                        CancellationToken::new(),
                        Instant::now() + StdDuration::from_secs(900),
                        budgets,
                    ),
                )
                .await
                .unwrap();
                let observed = store
                    .read_vector_maintenance_resource(&identity, &namespace, &name)
                    .await
                    .unwrap();
                assert_eq!(observed.head().unwrap().entry_version(), &4);
                let spec = VectorMaintenanceSemanticSpec::try_new(
                    resource.clone(),
                    VectorMaintenanceKind::AnnBuild,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(observed.head().cloned(), None, None).unwrap(),
                    execution_identity(),
                    None,
                    VectorMaintenancePolicyV1::default(),
                )
                .unwrap();
                let (job_id, _) = vector_maintenance_job_ids(&spec).unwrap();
                let (entered, started) = tokio::sync::oneshot::channel();
                let (release, resume) = tokio::sync::oneshot::channel();
                let admission_valid = Arc::new(AtomicBool::new(true));
                let source = Arc::new(AdmissionSource(admission_valid.clone()));
                let executor = Arc::new(HeldExecutor {
                    observed,
                    entered: Mutex::new(Some(entered)),
                    release: Mutex::new(Some(resume)),
                    calls: AtomicUsize::new(0),
                    recoveries: AtomicUsize::new(0),
                    recovery_preparations: AtomicUsize::new(0),
                    recovery_fails: AtomicBool::new(false),
                    revoke_identity: AtomicBool::new(false),
                    admission_valid,
                });
                let mut scheduler = VectorMaintenanceScheduler::new(
                    config,
                    store.backend.clone(),
                    source.clone(),
                    executor.clone(),
                    execution_identity(),
                    None,
                )
                .unwrap();
                let observer = VectorMaintenanceCompletionObserver::new(|_| None);
                scheduler.install_completion_observer(observer.clone()).unwrap();
                let token = CancellationToken::new();
                let budget = VectorMaintenanceDiscoveryBudget::new(
                    token.clone(),
                    Instant::now() + StdDuration::from_secs(30),
                    NonZeroUsize::new(64).unwrap(),
                );
                let target = scheduler
                    .prepare_target(VectorMaintenanceTarget::NewJob(Box::new(spec)), &budget)
                    .await
                    .unwrap()
                    .unwrap();
                scheduler.queue.push(target).unwrap();
                assert_eq!(scheduler.start_ready(&token).await.unwrap(), 1);
                tokio::time::timeout(StdDuration::from_secs(10), started)
                    .await
                    .unwrap()
                    .unwrap();
                token.cancel();
                assert!(
                    scheduler
                        .budgets
                        .try_admit(&identity, VectorMaintenanceLane::Control)
                        .is_err()
                );
                assert_eq!(scheduler.start_ready(&token).await.unwrap(), 0);
                executor.admission_valid.store(false, Ordering::SeqCst);
                assert!(scheduler.start_ready(&CancellationToken::new()).await.is_err());
                release.send(()).unwrap();
                let report = tokio::time::timeout(StdDuration::from_secs(30), scheduler.drain())
                    .await
                    .unwrap();
                assert_eq!(report.completed, 1);
                assert_eq!(report.failed, 0);
                assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
                assert_eq!(executor.recoveries.load(Ordering::SeqCst), 1);
                assert!(scheduler.budgets.try_admit(&identity, VectorMaintenanceLane::Control).is_ok());
                let finished = store.read_vector_maintenance_job(&identity, job_id).await.unwrap().unwrap();
                assert_eq!(finished.job().state(), &VectorMaintenanceJobState::Succeeded);
                executor.admission_valid.store(true, Ordering::SeqCst);
                assert!(
                    scheduler
                        .prepare_target(
                            VectorMaintenanceTarget::ExistingJob {
                                resource: resource.clone(),
                                job_id
                            },
                            &VectorMaintenanceDiscoveryBudget::new(
                                CancellationToken::new(),
                                Instant::now() + StdDuration::from_secs(30),
                                NonZeroUsize::new(64).unwrap()
                            )
                        )
                        .await
                        .unwrap()
                        .is_none()
                );

                executor.recovery_fails.store(true, Ordering::SeqCst);
                let spec = VectorMaintenanceSemanticSpec::try_new(
                    resource.clone(),
                    VectorMaintenanceKind::Compaction,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(executor.observed.head().cloned(), None, None).unwrap(),
                    execution_identity(),
                    None,
                    VectorMaintenancePolicyV1::default(),
                )
                .unwrap();
                let (failed_job_id, _) = vector_maintenance_job_ids(&spec).unwrap();
                let token = CancellationToken::new();
                let target = scheduler
                    .prepare_target(
                        VectorMaintenanceTarget::NewJob(Box::new(spec)),
                        &VectorMaintenanceDiscoveryBudget::new(
                            token.clone(),
                            Instant::now() + StdDuration::from_secs(30),
                            NonZeroUsize::new(64).unwrap(),
                        ),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                scheduler.queue.push(target).unwrap();
                assert_eq!(scheduler.start_ready(&token).await.unwrap(), 1);
                let report = tokio::time::timeout(StdDuration::from_secs(30), scheduler.drain())
                    .await
                    .unwrap();
                assert_eq!(report.failed, 1);
                assert_eq!(report.completed, 0);
                assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
                assert_eq!(executor.recoveries.load(Ordering::SeqCst), 2);
                let unresolved = store
                    .read_vector_maintenance_job(&identity, failed_job_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(unresolved.job().state(), &VectorMaintenanceJobState::RecoveryRequired);
                let completion_history = observer.snapshot();
                assert!(!completion_history.overflow);
                assert_eq!(completion_history.next_sequence, 3);
                assert_eq!(completion_history.completions.len(), 2);
                let completion = &completion_history.completions[1];
                assert_eq!(completion.resource, resource);
                assert_eq!(completion.job_id, Some(failed_job_id));
                assert_eq!(completion.attempt, Some(*unresolved.job().attempt()));
                let failure = completion.error.as_ref().unwrap();
                assert!(matches!(failure.phase, fault_observation::VectorMaintenanceObservedPhase::NativeRecovery));
                assert!(matches!(
                    failure.variant,
                    fault_observation::VectorMaintenanceObservedErrorVariant::Catalog
                ));
                let durable = completion.durable_job.as_ref().unwrap();
                assert_eq!(durable.job_id, failed_job_id);
                assert_eq!(durable.attempt, *unresolved.job().attempt());
                assert_eq!(durable.revision, *unresolved.job().revision());
                assert_eq!(durable.state, VectorMaintenanceJobState::RecoveryRequired);
                assert_eq!(durable.failure, unresolved.job().failure);
                let prefix = serde_json::to_value(&completion_history.completions).unwrap();

                assert!(!scheduler.has_fatal_failure());
                let failed_path = store.paths.vector_maintenance_job_path(&identity, failed_job_id).unwrap();
                let failed_object = store
                    .backend
                    .read_object(RUSTFS_META_BUCKET, &failed_path)
                    .await
                    .unwrap()
                    .unwrap();
                let failed_reference =
                    CatalogMetadataRef::from_read(CatalogObjectKey::try_new(failed_path).unwrap(), &failed_object).unwrap();
                let recovery = scheduler
                    .prepare_target(
                        VectorMaintenanceTarget::PrepareRecovery {
                            resource: resource.clone(),
                            kind: VectorMaintenanceKind::CommitRecovery,
                            expected_source: VectorMaintenanceSourceV1::try_new(
                                VectorMaintenanceSourceKind::MaintenanceJob,
                                (&failed_reference).into(),
                            )
                            .unwrap(),
                        },
                        &VectorMaintenanceDiscoveryBudget::new(
                            CancellationToken::new(),
                            Instant::now() + StdDuration::from_secs(30),
                            NonZeroUsize::new(64).unwrap(),
                        ),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                scheduler.queue.push(recovery).unwrap();
                assert_eq!(scheduler.start_ready(&CancellationToken::new()).await.unwrap(), 1);
                assert_eq!(scheduler.drain().await.completed, 1);
                assert_eq!(executor.recovery_preparations.load(Ordering::SeqCst), 1);
                let later = observer.snapshot();
                assert!(!later.overflow);
                assert_eq!(later.next_sequence, 4);
                assert_eq!(serde_json::to_value(&later.completions[..2]).unwrap(), prefix);
                assert!(scheduler.has_failure());

                assert!(scheduler.has_failure());

                executor.recovery_fails.store(false, Ordering::SeqCst);
                executor.revoke_identity.store(true, Ordering::SeqCst);
                let spec = VectorMaintenanceSemanticSpec::try_new(
                    resource.clone(),
                    VectorMaintenanceKind::AnnBuild,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(executor.observed.head().cloned(), None, None).unwrap(),
                    execution_identity(),
                    None,
                    VectorMaintenancePolicyV1::try_new(900, 60, 2, 5, 300).unwrap(),
                )
                .unwrap();
                let (revoked_job_id, _) = vector_maintenance_job_ids(&spec).unwrap();
                store
                    .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(spec).unwrap())
                    .await
                    .unwrap();
                let result = execute_vector_maintenance_job(
                    store.clone(),
                    resource,
                    revoked_job_id,
                    VectorMaintenanceWorker::try_new("identity-recheck".into(), Uuid::new_v4(), execution_identity()).unwrap(),
                    VectorMaintenanceExecutionContext {
                        identity: execution_identity(),
                        source,
                        executor: executor.clone(),
                    },
                    VectorMaintenanceControl::new(
                        CancellationToken::new(),
                        Instant::now() + StdDuration::from_secs(900),
                        scheduler.budgets.clone(),
                    ),
                )
                .await;
                assert!(matches!(
                    result,
                    Err(VectorMaintenanceExecutionError::Catalog(TableCatalogStoreError::Unavailable(_)))
                ));
                assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
                assert_eq!(executor.recoveries.load(Ordering::SeqCst), 3);
                let revoked = store
                    .read_vector_maintenance_job(&identity, revoked_job_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(revoked.job().state(), &VectorMaintenanceJobState::Running);
                executor.admission_valid.store(true, Ordering::SeqCst);
                let shutdown = CancellationToken::new();
                shutdown.cancel();
                let mut runtime = CatalogBackgroundTask::start_fallible(&shutdown, move |cancellation| {
                    super::super::periodic_runtime::run_vector(
                        scheduler,
                        crate::storage_api::startup::services::ServerContextSlot::new(),
                        StdDuration::from_secs(300),
                        StdDuration::from_secs(30),
                        cancellation,
                    )
                });
                let CatalogBackgroundTaskError::Worker(error) = runtime.join_result().await.unwrap().unwrap_err() else {
                    panic!("expected retained task failure")
                };
                let retained = error.downcast_ref::<VectorMaintenanceRuntimeFailure>().unwrap();
                assert!(retained.fatal_failure.is_none());
                assert!(matches!(
                    retained.task_failure.as_ref(),
                    Some(VectorMaintenanceExecutionError::Catalog(TableCatalogStoreError::Unavailable(_)))
                ));
            },
        );
    }

    fn resource(id: uuid::Uuid) -> VectorMaintenanceResourceV1 {
        let bucket = VectorBucketBindingV1 {
            version: 1,
            storage_bucket_id: "vectors-storage".into(),
            bucket_incarnation: uuid::Uuid::from_u128(1),
            account_namespace_id: String::new(),
            region: "us-east-1".into(),
            public_name: "vectors-public".into(),
        }
        .to_handle()
        .unwrap();
        let identity =
            CatalogResourceIdentity::try_new(bucket, ManagedTableId::try_new(id.to_string()).unwrap(), CatalogFormat::Lance)
                .unwrap();
        VectorMaintenanceResourceV1::try_new(
            &identity,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index-name").unwrap(),
        )
        .unwrap()
    }

    fn job(id: u128, kind: VectorMaintenanceKind) -> VectorMaintenanceQueuedTarget {
        VectorMaintenanceQueuedTarget {
            resource: resource(uuid::Uuid::from_u128(id)),
            action: VectorMaintenanceQueuedAction::Job {
                job_id: uuid::Uuid::from_u128(id),
                kind,
                not_before: None,
            },
        }
    }

    #[test]
    fn vector_queue_enforces_count_and_deduplicates_complete_identity() {
        let config = VectorMaintenanceRuntimeConfig {
            queue_capacity: NonZeroUsize::new(4).unwrap(),
            ..Default::default()
        };
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let mut queue = VectorMaintenanceQueue::new(&config, budgets).unwrap();
        assert!(queue.push(job(1, VectorMaintenanceKind::AnnBuild)).unwrap());
        assert!(!queue.push(job(1, VectorMaintenanceKind::AnnBuild)).unwrap());
        assert!(queue.push(job(2, VectorMaintenanceKind::Compaction)).unwrap());
        assert!(queue.push(job(3, VectorMaintenanceKind::AnnBuild)).is_err());
        assert!(queue.push(job(4, VectorMaintenanceKind::CommitRecovery)).unwrap());
        assert!(queue.push(job(5, VectorMaintenanceKind::HistoryGc)).unwrap());
        assert_eq!(queue.len(), 4);
    }

    #[test]
    fn vector_queue_counts_string_capacity_and_releases_reservation() {
        let config = VectorMaintenanceRuntimeConfig {
            queue_bytes: NonZeroUsize::new(65_536).unwrap(),
            ..Default::default()
        };
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let mut queue = VectorMaintenanceQueue::new(&config, budgets).unwrap();
        let mut target = job(1, VectorMaintenanceKind::CommitRecovery);
        if let VectorMaintenanceQueuedAction::Job { .. } = target.action {
            let expected = CatalogMetadataRefRecordV1 {
                object_key: String::with_capacity(40_000),
                content_length: 1,
                content_sha256: "00".repeat(32),
                body_revision: Some(uuid::Uuid::from_u128(3)),
                etag: None,
            };
            target.action = VectorMaintenanceQueuedAction::Preparation {
                kind: VectorMaintenancePreparationKind::Current,
                expected,
            };
        }
        assert!(matches!(queue.push(target), Err(VectorMaintenanceBudgetError::Exceeded { .. })));
        assert_eq!(queue.len(), 0);
        assert!(queue.push(job(2, VectorMaintenanceKind::CommitRecovery)).unwrap());
        let slot = queue
            .pop_ready(VectorMaintenanceLane::Control, OffsetDateTime::now_utc())
            .unwrap();
        assert_eq!(queue.len(), 0);
        drop(slot);
        assert!(queue.push(job(2, VectorMaintenanceKind::CommitRecovery)).unwrap());
    }

    #[test]
    fn vector_queue_prioritizes_recovery_and_alternates_compute() {
        let config = VectorMaintenanceRuntimeConfig::default();
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let mut queue = VectorMaintenanceQueue::new(&config, budgets).unwrap();
        for (id, kind) in [
            (1, VectorMaintenanceKind::AnnBuild),
            (2, VectorMaintenanceKind::AnnBuild),
            (3, VectorMaintenanceKind::Compaction),
            (4, VectorMaintenanceKind::HistoryGc),
            (5, VectorMaintenanceKind::DeleteRecovery),
        ] {
            queue.push(job(id, kind)).unwrap();
        }
        let now = OffsetDateTime::now_utc();
        assert_eq!(
            queue.pop_ready(VectorMaintenanceLane::Compute, now).unwrap().target.kind(),
            Some(VectorMaintenanceKind::AnnBuild)
        );
        assert_eq!(
            queue.pop_ready(VectorMaintenanceLane::Compute, now).unwrap().target.kind(),
            Some(VectorMaintenanceKind::Compaction)
        );
        assert_eq!(
            queue.pop_ready(VectorMaintenanceLane::Control, now).unwrap().target.kind(),
            Some(VectorMaintenanceKind::DeleteRecovery)
        );
        assert_eq!(
            queue.pop_ready(VectorMaintenanceLane::Control, now).unwrap().target.kind(),
            Some(VectorMaintenanceKind::HistoryGc)
        );
    }

    #[test]
    fn vector_queue_waits_for_retry_due_without_losing_other_work() {
        let config = VectorMaintenanceRuntimeConfig::default();
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let mut queue = VectorMaintenanceQueue::new(&config, budgets).unwrap();
        let now = OffsetDateTime::now_utc();
        let mut delayed = job(1, VectorMaintenanceKind::AnnBuild);
        if let VectorMaintenanceQueuedAction::Job { not_before, .. } = &mut delayed.action {
            *not_before = Some(now + time::Duration::seconds(5));
        }
        queue.push(delayed).unwrap();
        queue.push(job(2, VectorMaintenanceKind::Compaction)).unwrap();
        assert_eq!(
            queue.pop_ready(VectorMaintenanceLane::Compute, now).unwrap().target.kind(),
            Some(VectorMaintenanceKind::Compaction)
        );
        assert!(queue.pop_ready(VectorMaintenanceLane::Compute, now).is_none());
        assert!(
            queue
                .pop_ready(VectorMaintenanceLane::Compute, now + time::Duration::seconds(5))
                .is_some()
        );
    }
}
