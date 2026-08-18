use super::worker::{is_transient_rebalance_error, rebalance_migration_retry_delay, sleep_rebalance_migration_retry};
use crate::bucket::replication::replication_state_from_filemeta;
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::error::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::{object::ObjectIO, range::HTTPRangeSpec};
use crate::store::ECStore;
use http::HeaderMap;
use rustfs_filemeta::FileInfo;
use rustfs_utils::path::encode_dir_object;
use std::future::Future;
use tokio::time::Duration;

#[derive(Debug, Default, Clone)]
pub(crate) struct MigrationVersionResult {
    pub moved: bool,
    pub ignored: bool,
    pub cleanup_ignored: bool,
    pub failed: bool,
    pub stage: Option<&'static str>,
    pub error: Option<Error>,
}

pub(super) fn rebalance_delete_marker_opts(
    version: &FileInfo,
    version_id: Option<String>,
    src_pool_idx: usize,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
) -> ObjectOptions {
    let version_suspended = version.version_id.is_none() && version_id.is_none();
    ObjectOptions {
        versioned: !version_suspended,
        version_suspended,
        version_id: version_id.or_else(|| version_suspended.then(|| uuid::Uuid::nil().to_string())),
        mod_time: version.mod_time,
        src_pool_idx,
        data_movement: true,
        delete_marker: true,
        skip_decommissioned: true,
        expected_bucket_incarnation_id,
        delete_replication: version
            .replication_state_internal
            .as_ref()
            .map(replication_state_from_filemeta),
        ..Default::default()
    }
}

fn rebalance_remote_tiered_opts(
    version: &FileInfo,
    version_id: Option<String>,
    src_pool_idx: usize,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
) -> ObjectOptions {
    ObjectOptions {
        versioned: version_id.is_some(),
        version_id,
        mod_time: version.mod_time,
        user_defined: version.metadata.clone(),
        src_pool_idx,
        data_movement: true,
        include_part_checksums: true,
        http_preconditions: Some(crate::data_movement::data_movement_target_precondition()),
        expected_bucket_incarnation_id,
        ..Default::default()
    }
}

pub(super) fn rebalance_object_migration_read_opts(version_id: Option<String>) -> ObjectOptions {
    ObjectOptions {
        version_id,
        no_lock: true,
        data_movement: true,
        raw_data_movement_read: true,
        skip_decommissioned: true,
        skip_rebalancing: true,
        ..Default::default()
    }
}

#[async_trait::async_trait]
pub(crate) trait MigrationBackend: Send + Sync {
    async fn get_object_reader_for_migration(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()>;
}

pub(crate) struct RebalanceMigrationBackend<'a> {
    source: &'a SetDisks,
    store: &'a ECStore,
}

impl<'a> RebalanceMigrationBackend<'a> {
    pub(crate) fn new(source: &'a SetDisks, store: &'a ECStore) -> Self {
        Self { source, store }
    }
}

#[async_trait::async_trait]
impl MigrationBackend for RebalanceMigrationBackend<'_> {
    async fn get_object_reader_for_migration(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.source.get_object_reader(bucket, object, range, h, opts).await
    }

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        self.store.decommission_tiered_object(bucket, object, fi, opts).await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn migrate_entry_version<Backend, F, Fut, D, DFut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    transfer: F,
    delete_marker: D,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    D: FnMut(String, String, ObjectOptions) -> DFut + Send,
    DFut: Future<Output = Result<ObjectInfo>> + Send,
{
    migrate_entry_version_with_retry_wait_and_incarnation(
        set,
        bucket,
        pool_index,
        version,
        version_id,
        expected_bucket_incarnation_id,
        max_attempts,
        ignore_data_usage_cache,
        transfer,
        delete_marker,
        sleep_rebalance_migration_retry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub(super) async fn migrate_entry_version_with_retry_wait<Backend, F, Fut, D, DFut, W, WFut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    transfer: F,
    delete_marker: D,
    wait_retry: W,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    D: FnMut(String, String, ObjectOptions) -> DFut + Send,
    DFut: Future<Output = Result<ObjectInfo>> + Send,
    W: FnMut(Duration) -> WFut + Send,
    WFut: Future<Output = ()> + Send,
{
    migrate_entry_version_with_retry_wait_and_incarnation(
        set,
        bucket,
        pool_index,
        version,
        version_id,
        None,
        max_attempts,
        ignore_data_usage_cache,
        transfer,
        delete_marker,
        wait_retry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn migrate_entry_version_with_retry_wait_and_incarnation<Backend, F, Fut, D, DFut, W, WFut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    expected_bucket_incarnation_id: Option<uuid::Uuid>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    mut transfer: F,
    mut delete_marker: D,
    mut wait_retry: W,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
    D: FnMut(String, String, ObjectOptions) -> DFut + Send,
    DFut: Future<Output = Result<ObjectInfo>> + Send,
    W: FnMut(Duration) -> WFut + Send,
    WFut: Future<Output = ()> + Send,
{
    let max_attempts = max_attempts.max(1);

    if ignore_data_usage_cache && bucket == crate::disk::RUSTFS_META_BUCKET && version.name.contains(DATA_USAGE_CACHE_NAME) {
        return MigrationVersionResult {
            moved: false,
            ignored: true,
            cleanup_ignored: false,
            failed: false,
            stage: None,
            error: None,
        };
    }

    if version.is_remote() {
        if let Err(err) = set
            .move_remote_version_for_migration(
                &bucket,
                &version.name,
                version,
                &rebalance_remote_tiered_opts(version, version_id, pool_index, expected_bucket_incarnation_id),
            )
            .await
        {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    stage: Some("move_remote_version"),
                    error: None,
                };
            }

            return MigrationVersionResult {
                moved: false,
                ignored: false,
                cleanup_ignored: false,
                failed: true,
                stage: Some("move_remote_version"),
                error: Some(err),
            };
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            stage: None,
            error: None,
        };
    }

    if version.deleted {
        // Delete markers must be routed through the store layer (ECStore::delete_object /
        // handle_delete_object), which honours data_movement/src_pool_idx/delete_marker and
        // writes the marker to the cross-pool target. Writing via the source SetDisks would
        // silently rewrite the marker back onto the source set and lose it during cleanup.
        if let Err(err) = delete_marker(
            bucket.clone(),
            version.name.clone(),
            rebalance_delete_marker_opts(version, version_id, pool_index, expected_bucket_incarnation_id),
        )
        .await
        {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    stage: Some("delete_marker"),
                    error: None,
                };
            }

            return MigrationVersionResult {
                moved: false,
                ignored: false,
                cleanup_ignored: false,
                failed: true,
                stage: Some("delete_marker"),
                error: Some(err),
            };
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            stage: None,
            error: None,
        };
    }

    let mut last_error: Option<Error> = None;
    for attempt in 0..max_attempts {
        let rd = match set
            .get_object_reader_for_migration(
                &bucket,
                &encode_dir_object(&version.name),
                None,
                HeaderMap::new(),
                &rebalance_object_migration_read_opts(version_id.clone()),
            )
            .await
        {
            Ok(rd) => rd,
            Err(err) => {
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    return MigrationVersionResult {
                        moved: false,
                        ignored: true,
                        cleanup_ignored: true,
                        failed: false,
                        stage: Some("read_source"),
                        error: None,
                    };
                }

                last_error = Some(err);
                let Some(err) = last_error.as_ref() else {
                    continue;
                };
                if attempt + 1 >= max_attempts || !is_transient_rebalance_error(err) {
                    return MigrationVersionResult {
                        moved: false,
                        ignored: false,
                        cleanup_ignored: false,
                        failed: true,
                        stage: Some("read_source"),
                        error: last_error,
                    };
                }

                wait_retry(rebalance_migration_retry_delay(attempt, err)).await;
                continue;
            }
        };

        if let Err(err) = transfer(pool_index, bucket.clone(), rd).await {
            if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: true,
                    cleanup_ignored: true,
                    failed: false,
                    stage: Some("write_target"),
                    error: None,
                };
            }

            last_error = Some(err);
            let Some(err) = last_error.as_ref() else {
                continue;
            };
            if attempt + 1 >= max_attempts || !is_transient_rebalance_error(err) {
                return MigrationVersionResult {
                    moved: false,
                    ignored: false,
                    cleanup_ignored: false,
                    failed: true,
                    stage: Some("write_target"),
                    error: last_error,
                };
            }

            wait_retry(rebalance_migration_retry_delay(attempt, err)).await;
            continue;
        }

        return MigrationVersionResult {
            moved: true,
            ignored: false,
            cleanup_ignored: false,
            failed: false,
            stage: None,
            error: None,
        };
    }

    MigrationVersionResult {
        moved: false,
        ignored: false,
        cleanup_ignored: false,
        failed: true,
        stage: Some("migrate"),
        error: last_error,
    }
}
