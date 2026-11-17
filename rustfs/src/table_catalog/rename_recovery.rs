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

use super::{
    CatalogBackgroundTask, EcStoreTableCatalogObjectBackend, ObjectTableCatalogStore, TableCatalogObjectBackend,
    TableCatalogStoreError,
};
use crate::storage_api::startup::services::ServerContextSlot;
use futures::{StreamExt, stream};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const SCAN_BATCH_SIZE: usize = 32;
const RECOVERY_CONCURRENCY: usize = 4;
const RETRY_CACHE_SIZE: usize = 1024;
const SCAN_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(10);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
const LOG_COMPONENT: &str = "table_catalog";
const LOG_SUBSYSTEM: &str = "rename";
const EVENT_RENAME_RECOVERY: &str = "table_catalog_rename_recovery";

/// Recovery is always enabled, independently of the optional maintenance scheduler.
pub(crate) fn spawn_table_rename_recovery(server: Arc<ServerContextSlot>, shutdown: &CancellationToken) -> CatalogBackgroundTask {
    CatalogBackgroundTask::start(shutdown, move |cancellation| async move {
        loop {
            if cancellation.is_cancelled() {
                return;
            }
            if let Some(context) = server.installed_app_context() {
                let store = ObjectTableCatalogStore::new(EcStoreTableCatalogObjectBackend::new(context.object_store()));
                run(store, cancellation).await;
                return;
            }
            if !wait(&cancellation, Duration::from_secs(1)).await {
                return;
            }
        }
    })
}

async fn wait(cancellation: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

// Decode failures can contain persisted values; log only their error category.
fn recovery_error_kind(error: &TableCatalogStoreError) -> &'static str {
    match error {
        TableCatalogStoreError::NotFound(_)
        | TableCatalogStoreError::NamespaceNotFound(_)
        | TableCatalogStoreError::TableNotFound(_) => "not_found",
        TableCatalogStoreError::AlreadyExists(_) => "already_exists",
        TableCatalogStoreError::Conflict(_) => "conflict",
        TableCatalogStoreError::Invalid(_) => "invalid",
        TableCatalogStoreError::Unsupported(_) => "unsupported",
        TableCatalogStoreError::Unavailable(_) => "unavailable",
        TableCatalogStoreError::LockUnavailable { .. } => "lock_unavailable",
        TableCatalogStoreError::Internal(_) => "internal",
    }
}

struct Retry {
    after: Instant,
    delay: Duration,
}

#[derive(Default)]
struct RecoveryScan {
    cursor: Option<String>,
    retries: BTreeMap<String, Retry>,
    discovery_degraded: bool,
}

impl RecoveryScan {
    async fn pass<B: TableCatalogObjectBackend>(&mut self, store: &ObjectTableCatalogStore<B>, cancellation: &CancellationToken) {
        let mut buckets = BTreeSet::new();
        let mut discovery_failed = false;
        for _ in 0..SCAN_BATCH_SIZE {
            if cancellation.is_cancelled() {
                break;
            }
            let previous = self.cursor.clone();
            match store.next_table_bucket_for_rename_recovery(&mut self.cursor).await {
                Ok(Some(entry)) => {
                    if entry.active_rename_id.is_none() {
                        self.retries.remove(&entry.table_bucket);
                    } else if self
                        .retries
                        .get(&entry.table_bucket)
                        .is_none_or(|retry| Instant::now() >= retry.after)
                    {
                        buckets.insert(entry.table_bucket);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    discovery_failed = true;
                    if !self.discovery_degraded {
                        warn!(
                            event = EVENT_RENAME_RECOVERY,
                            component = LOG_COMPONENT,
                            subsystem = LOG_SUBSYSTEM,
                            state = "deferred",
                            reason = "discovery_failed",
                            error_type = recovery_error_kind(&error),
                            "Table rename recovery discovery failed"
                        );
                        self.discovery_degraded = true;
                    }
                    // A listing failure cannot advance; retry on the next tick.
                    // Invalid entries already advanced past their bucket directory.
                    if previous == self.cursor {
                        break;
                    }
                }
            }
        }
        if !discovery_failed {
            self.discovery_degraded = false;
        }
        let recoveries = stream::iter(buckets)
            .map(|bucket| async move {
                if cancellation.is_cancelled() {
                    return (bucket, None);
                }
                let result = store.recover_pending_table_rename(&bucket).await;
                (bucket, Some(result))
            })
            .buffer_unordered(RECOVERY_CONCURRENCY);
        tokio::pin!(recoveries);
        // Never cancel a started recovery: its publication lease must cover every
        // in-flight storage write, including shutdown and ambiguous write results.
        while let Some((bucket, result)) = recoveries.next().await {
            match result {
                Some(Ok(())) => {
                    self.retries.remove(&bucket);
                    info!(event = EVENT_RENAME_RECOVERY, component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM,
                        bucket = %bucket, state = "completed", "Table rename recovery completed");
                }
                Some(Err(error)) => {
                    let delay = self
                        .retries
                        .get(&bucket)
                        .map_or(INITIAL_RETRY_DELAY, |retry| (retry.delay * 2).min(MAX_RETRY_DELAY));
                    if !self.retries.contains_key(&bucket)
                        && self.retries.len() >= RETRY_CACHE_SIZE
                        && let Some(oldest) = self
                            .retries
                            .iter()
                            .min_by_key(|(_, retry)| retry.after)
                            .map(|(key, _)| key.clone())
                    {
                        self.retries.remove(&oldest);
                    }
                    self.retries.insert(
                        bucket.clone(),
                        Retry {
                            after: Instant::now() + delay,
                            delay,
                        },
                    );
                    warn!(event = EVENT_RENAME_RECOVERY, component = LOG_COMPONENT, subsystem = LOG_SUBSYSTEM,
                        bucket = %bucket, state = "deferred", reason = "recovery_failed", retry_delay_secs = delay.as_secs(), error_type = recovery_error_kind(&error),
                        "Table rename recovery will retry");
                }
                None => {}
            }
        }
    }
}

pub(super) async fn run<B: TableCatalogObjectBackend>(store: ObjectTableCatalogStore<B>, cancellation: CancellationToken) {
    let mut scan = RecoveryScan::default();
    loop {
        scan.pass(&store, &cancellation).await;
        let delay = SCAN_INTERVAL + Duration::from_millis(rand::random_range(0..=1000));
        if !wait(&cancellation, delay).await {
            return;
        }
    }
}
