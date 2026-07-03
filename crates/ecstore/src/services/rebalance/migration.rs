use super::worker::{is_transient_rebalance_error, rebalance_migration_retry_delay, sleep_rebalance_migration_retry};
use crate::bucket::replication::replication_state_from_filemeta;
use crate::data_usage::DATA_USAGE_CACHE_NAME;
use crate::error::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::{
    object::{ObjectIO, ObjectOperations as _},
    range::HTTPRangeSpec,
};
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

pub(super) fn rebalance_delete_marker_opts(version: &FileInfo, version_id: Option<String>, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: true,
        version_id,
        mod_time: version.mod_time,
        src_pool_idx,
        data_movement: true,
        delete_marker: true,
        skip_decommissioned: true,
        delete_replication: version
            .replication_state_internal
            .as_ref()
            .map(replication_state_from_filemeta),
        ..Default::default()
    }
}

fn rebalance_remote_tiered_opts(version: &FileInfo, version_id: Option<String>, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: version_id.is_some(),
        version_id,
        mod_time: version.mod_time,
        user_defined: version.metadata.clone(),
        src_pool_idx,
        data_movement: true,
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

    async fn delete_object_for_migration(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo>;

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl MigrationBackend for SetDisks {
    async fn get_object_reader_for_migration(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.get_object_reader(bucket, object, range, h, opts).await
    }

    async fn delete_object_for_migration(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        self.delete_object(bucket, object, opts).await
    }

    async fn move_remote_version_for_migration(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        self.decommission_tiered_object(bucket, object, fi, opts).await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn migrate_entry_version<Backend, F, Fut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    transfer: F,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
{
    migrate_entry_version_with_retry_wait(
        set,
        bucket,
        pool_index,
        version,
        version_id,
        max_attempts,
        ignore_data_usage_cache,
        transfer,
        sleep_rebalance_migration_retry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn migrate_entry_version_with_retry_wait<Backend, F, Fut, W, WFut>(
    set: &Backend,
    bucket: String,
    pool_index: usize,
    version: &FileInfo,
    version_id: Option<String>,
    max_attempts: usize,
    ignore_data_usage_cache: bool,
    mut transfer: F,
    mut wait_retry: W,
) -> MigrationVersionResult
where
    Backend: MigrationBackend + ?Sized,
    F: FnMut(usize, String, GetObjectReader) -> Fut + Send,
    Fut: Future<Output = Result<()>> + Send,
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
                &rebalance_remote_tiered_opts(version, version_id, pool_index),
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
        if let Err(err) = set
            .delete_object_for_migration(&bucket, &version.name, rebalance_delete_marker_opts(version, version_id, pool_index))
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
                &ObjectOptions {
                    version_id: version_id.clone(),
                    no_lock: true,
                    ..Default::default()
                },
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
