use super::migration::MigrationVersionResult;
use super::{
    DEFAULT_REBALANCE_MAX_ATTEMPTS, EVENT_REBALANCE_LISTING, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, REBAL_META_NAME,
    REBALANCE_LISTING_RETRY_BASE_DELAY, REBALANCE_MAX_ATTEMPTS_ENV, REBALANCE_MIGRATION_LOCK_RETRY_CAP,
    REBALANCE_MIGRATION_RETRY_BASE_DELAY, RebalanceBucketConfigs, RebalanceBucketOutcome, RebalanceEntryOutcome, Result,
};
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::disk::error::DiskError;
use crate::error::{
    Error, is_err_object_not_found, is_err_operation_canceled, is_err_version_not_found, is_network_or_host_down,
};
use crate::pools::ListCallback;
use crate::runtime::sources as runtime_sources;
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use rand::RngExt as _;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use std::sync::Arc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub(super) fn resolve_rebalance_worker_result<T>(
    set_idx: usize,
    worker_result: std::result::Result<Result<T>, tokio::task::JoinError>,
) -> Result<T> {
    match worker_result {
        Ok(result) => result,
        Err(err) => Err(Error::other(format!("rebalance worker {set_idx} task join error: {err}"))),
    }
}

pub(super) type RebalanceEntryTask = tokio::task::JoinHandle<Result<RebalanceEntryOutcome>>;

pub(super) async fn wait_rebalance_entry_tasks(
    set_idx: usize,
    tasks: Arc<tokio::sync::Mutex<Vec<RebalanceEntryTask>>>,
) -> Result<Option<String>> {
    let tasks = {
        let mut tasks = tasks.lock().await;
        std::mem::take(&mut *tasks)
    };

    let mut first_error = None;
    let mut first_deferred = None;
    for task in tasks {
        match task.await {
            Ok(Ok(RebalanceEntryOutcome::Completed)) => {}
            Ok(Ok(RebalanceEntryOutcome::Deferred { last_error })) => {
                if first_deferred.is_none() {
                    first_deferred = Some(last_error);
                }
            }
            Ok(Err(err)) => {
                error!("rebalance entry task failed for set {}: {}", set_idx, err);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(err) => {
                let err = Error::other(format!("rebalance entry task join error for set {set_idx}: {err}"));
                error!("{}", err);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if let Some(err) = first_error {
        Err(err)
    } else {
        Ok(first_deferred)
    }
}

pub(super) fn resolve_rebalance_save_task_result(
    pool_idx: usize,
    save_task_result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match save_task_result {
        Ok(result) => result.map_err(|err| Error::other(format!("rebalance save_task failed for pool {pool_idx}: {err}"))),
        Err(err) => Err(Error::other(format!("rebalance save_task for pool {pool_idx} join error: {err}"))),
    }
}

pub(super) fn resolve_rebalance_meta_save_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("rebalance meta save failed during {stage}: {err}")))
}

pub(super) fn rebalance_meta_lock_error(err: rustfs_lock::LockError) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: crate::disk::RUSTFS_META_BUCKET.to_string(),
            object: REBAL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => Error::other(format!(
            "failed to acquire rebalance metadata write lock on {}/{}: {other}",
            crate::disk::RUSTFS_META_BUCKET,
            REBAL_META_NAME
        )),
    }
}

pub(super) fn resolve_rebalance_meta_load_result(result: Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(Error::ConfigNotFound) => Ok(false),
        Err(err) => {
            error!("rebalanceMeta: load rebalance meta err {:?}", &err);
            Err(Error::other(format!("rebalance metadata load failed during load_rebalance_meta: {err}")))
        }
    }
}

pub(super) fn resolve_rebalance_stats_update_result(
    result: Result<()>,
    pool_idx: usize,
    bucket: &str,
    object_name: &str,
) -> Result<()> {
    result.map_err(|err| {
        Error::other(format!(
            "rebalance stats update failed for pool {pool_idx} bucket {bucket} object {object_name}: {err}"
        ))
    })
}

pub(super) fn resolve_rebalance_file_info_versions_result<T, E>(
    result: std::result::Result<T, E>,
    bucket: &str,
    object_name: &str,
) -> Result<T>
where
    E: std::fmt::Display,
{
    result.map_err(|err| Error::other(format!("rebalance file_info_versions failed for {bucket}/{object_name}: {err}")))
}

pub(super) fn resolve_rebalance_entry_cleanup_delete_result(
    result: Result<crate::object_api::ObjectInfo>,
    bucket: &str,
    object_name: &str,
) -> Result<Option<String>> {
    match result {
        Ok(_) => Ok(None),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(None),
        Err(err) => Ok(Some(format!("rebalance cleanup delete failed for {bucket}/{object_name}: {err}"))),
    }
}

pub(super) fn resolve_rebalance_migrate_result_error(
    err: Option<Error>,
    pool_idx: usize,
    bucket: &str,
    object_name: &str,
    version_id: Option<&str>,
) -> Error {
    err.unwrap_or_else(|| {
        Error::other(format!(
            "rebalance migration reported failure without error for pool {pool_idx} entry {bucket}/{object_name} version {}",
            version_id.unwrap_or("none")
        ))
    })
}

pub(super) fn should_defer_rebalance_entry_failure(err: &Error) -> bool {
    is_transient_rebalance_error(err)
}

pub(super) fn resolve_load_rebalance_stats_update_result(result: Result<()>) -> Result<()> {
    result.map_err(|err| Error::other(format!("rebalance metadata stats refresh failed after load: {err}")))
}

pub(super) async fn send_rebalance_done_signal(
    done_tx: &tokio::sync::mpsc::Sender<Result<()>>,
    signal: Result<()>,
    pool_idx: usize,
) -> Result<()> {
    done_tx
        .send(signal)
        .await
        .map_err(|err| Error::other(format!("rebalance done signal send failed for pool {pool_idx}: {err}")))
}

pub(super) fn resolve_rebalance_terminal_error(primary_err: Error, signal_result: Result<()>) -> Error {
    match signal_result {
        Ok(()) => primary_err,
        Err(signal_err) => Error::other(format!("rebalance terminal signal failed after error {primary_err}: {signal_err}")),
    }
}

pub(super) fn resolve_rebalance_bucket_error(entry_error: Option<Error>, worker_error: Option<Error>) -> Result<()> {
    if let Some(err) = entry_error {
        return Err(err);
    }

    if let Some(err) = worker_error {
        return Err(err);
    }

    Ok(())
}

pub(super) fn resolve_rebalance_bucket_result(
    result: Result<RebalanceBucketOutcome>,
    pool_idx: usize,
    bucket: &str,
) -> Result<RebalanceBucketOutcome> {
    match result {
        Ok(outcome) => Ok(outcome),
        Err(err) if is_err_operation_canceled(&err) => Err(err),
        Err(err) => Err(Error::other(format!("rebalance bucket {bucket} failed for pool {pool_idx}: {err}"))),
    }
}

pub(super) fn is_transient_rebalance_error(err: &Error) -> bool {
    match err {
        Error::SlowDown
        | Error::ErasureReadQuorum
        | Error::ErasureWriteQuorum
        | Error::InsufficientReadQuorum(_, _)
        | Error::InsufficientWriteQuorum(_, _) => true,
        Error::Lock(lock_err) => is_rebalance_transient_lock_error(lock_err),
        Error::Io(io_err) => is_rebalance_transient_io_error(io_err) || is_rebalance_transient_message(&io_err.to_string()),
        _ => is_rebalance_transient_message(&err.to_string()) || is_network_or_host_down(&err.to_string(), true),
    }
}

fn is_rebalance_transient_lock_error(err: &rustfs_lock::LockError) -> bool {
    match err {
        rustfs_lock::LockError::Timeout { .. } | rustfs_lock::LockError::Network { .. } => true,
        rustfs_lock::LockError::Internal { message } => is_rebalance_transient_message(message),
        _ => false,
    }
}

fn is_rebalance_transient_io_error(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::TimedOut {
        return true;
    }

    if let Some(disk_err) = err.get_ref().and_then(|err| err.downcast_ref::<DiskError>())
        && *disk_err == DiskError::Timeout
    {
        return true;
    }

    let message = err.to_string();
    message.eq_ignore_ascii_case("timeout") || is_rebalance_transient_message(&message)
}

fn is_rebalance_transient_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("lock acquisition timed out")
        || message.contains("remote lock rpc timed out")
        || message.contains("keepalivetimedout")
        || message.contains("i/o timeout")
        || message.contains("operation timed out")
}

pub(super) fn should_retry_rebalance_listing(err: &Error, attempt: usize, max_attempts: usize) -> bool {
    attempt + 1 < max_attempts && is_transient_rebalance_error(err)
}

pub(super) fn parse_rebalance_max_attempts(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(DEFAULT_REBALANCE_MAX_ATTEMPTS)
}

pub(super) fn rebalance_max_attempts() -> usize {
    parse_rebalance_max_attempts(std::env::var(REBALANCE_MAX_ATTEMPTS_ENV).ok().as_deref())
}

pub(super) fn rebalance_listing_retry_delay(attempt: usize) -> Duration {
    let multiplier = u32::try_from(attempt.saturating_add(1)).unwrap_or(u32::MAX);
    REBALANCE_LISTING_RETRY_BASE_DELAY.saturating_mul(multiplier)
}

fn is_rebalance_lock_or_rpc_timeout(err: &Error) -> bool {
    match err {
        Error::Lock(rustfs_lock::LockError::Timeout { .. }) | Error::Lock(rustfs_lock::LockError::Network { .. }) => true,
        Error::Io(io_err) => is_rebalance_lock_or_rpc_timeout_message(&io_err.to_string()),
        _ => is_rebalance_lock_or_rpc_timeout_message(&err.to_string()),
    }
}

fn is_rebalance_lock_or_rpc_timeout_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("lock acquisition timed out")
        || message.contains("remote lock rpc timed out")
        || message.contains("keepalivetimedout")
}

pub(super) fn rebalance_migration_retry_delay(attempt: usize, err: &Error) -> Duration {
    if is_rebalance_lock_or_rpc_timeout(err) {
        return rebalance_lock_retry_delay(attempt);
    }

    let multiplier = u32::try_from(attempt.saturating_add(1)).unwrap_or(u32::MAX);
    REBALANCE_MIGRATION_RETRY_BASE_DELAY.saturating_mul(multiplier)
}

fn rebalance_lock_retry_delay(attempt: usize) -> Duration {
    let lock_timeout = get_lock_acquire_timeout();
    let attempt_shift = u32::try_from(attempt.min(4)).unwrap_or(4);
    let multiplier = 1_u32.checked_shl(attempt_shift).unwrap_or(u32::MAX);
    let cap = lock_timeout
        .saturating_mul(multiplier)
        .min(REBALANCE_MIGRATION_LOCK_RETRY_CAP)
        .max(REBALANCE_MIGRATION_RETRY_BASE_DELAY);
    let max_millis = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX).max(1);
    let jitter_millis = rand::rng().random_range(1..=max_millis);
    Duration::from_millis(jitter_millis)
}

pub(super) async fn sleep_rebalance_migration_retry(delay: Duration) {
    tokio::time::sleep(delay).await;
}

pub(super) async fn wait_rebalance_listing_retry(rx: &CancellationToken, delay: Duration) -> Result<()> {
    tokio::select! {
        _ = rx.cancelled() => Err(Error::OperationCanceled),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}

pub(super) fn ensure_rebalance_listing_disks_available(has_disks: bool, bucket: &str) -> Result<()> {
    if !has_disks {
        return Err(Error::other(format!(
            "failed to list objects to rebalance for bucket {bucket}: no disks available"
        )));
    }

    Ok(())
}

pub(super) fn with_rebalance_entry_context(stage: &str, bucket: &str, object_name: &str, err: Error) -> Error {
    Error::other(format!("rebalance entry {stage} failed for {bucket}/{object_name}: {err}"))
}

pub(super) fn should_count_rebalance_version_complete(result: &MigrationVersionResult) -> bool {
    result.cleanup_ignored || (result.moved && !result.failed)
}

pub(super) fn should_cleanup_rebalance_source_entry(rebalanced: usize, total_versions: usize) -> bool {
    rebalanced == total_versions
}

pub(super) fn should_skip_rebalance_delete_marker(
    version: &rustfs_filemeta::FileInfo,
    remaining_versions: usize,
    replication_configured: bool,
) -> bool {
    version.deleted && remaining_versions == 1 && !replication_configured
}

pub(super) fn resolve_rebalance_optional_bucket_config_result<T>(
    bucket: &str,
    stage: &str,
    result: Result<T>,
) -> Result<Option<T>> {
    match result {
        Ok(config) => Ok(Some(config)),
        Err(Error::ConfigNotFound) => Ok(None),
        Err(err) => Err(Error::other(format!("rebalance {stage} config load failed for bucket {bucket}: {err}"))),
    }
}

pub(super) async fn load_rebalance_bucket_configs(bucket: &str) -> Result<RebalanceBucketConfigs> {
    if bucket == crate::disk::RUSTFS_META_BUCKET {
        return Ok(RebalanceBucketConfigs::default());
    }

    let _ = resolve_rebalance_optional_bucket_config_result(
        bucket,
        "versioning",
        crate::bucket::versioning_sys::BucketVersioningSys::get(bucket).await,
    )?;

    Ok(RebalanceBucketConfigs {
        lifecycle_config: runtime_sources::bucket_lifecycle_config(bucket).await,
        lock_retention: crate::bucket::object_lock::objectlock_sys::BucketObjectLockSys::get(bucket).await,
        replication_config: resolve_rebalance_optional_bucket_config_result(
            bucket,
            "replication",
            crate::bucket::metadata_sys::get_replication_config(bucket).await,
        )?,
    })
}

pub(super) async fn run_rebalance_listing_with_retry(
    set: Arc<SetDisks>,
    rx: CancellationToken,
    bucket: String,
    cb: ListCallback,
    set_idx: usize,
    max_attempts: usize,
) -> Result<()> {
    let max_attempts = max_attempts.max(1);
    let mut last_error = None;

    for attempt in 0..max_attempts {
        match set.list_objects_to_rebalance(rx.clone(), bucket.clone(), cb.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) if should_retry_rebalance_listing(&err, attempt, max_attempts) => {
                let next_attempt = attempt + 2;
                let delay = rebalance_listing_retry_delay(attempt);
                error!(
                    "rebalance listing failed for bucket {} set {} attempt {}/{}: {}; retrying in {:?}",
                    bucket,
                    set_idx,
                    attempt + 1,
                    max_attempts,
                    err,
                    delay
                );
                last_error = Some(err);
                wait_rebalance_listing_retry(&rx, delay).await?;
                info!(
                    "rebalance listing retrying bucket {} set {} attempt {}/{}",
                    bucket, set_idx, next_attempt, max_attempts
                );
            }
            Err(err) => {
                return Err(Error::other(format!(
                    "rebalance listing failed for bucket {bucket} set {set_idx} attempt {}/{}: {err}",
                    attempt + 1,
                    max_attempts
                )));
            }
        }
    }

    Err(Error::other(format!(
        "rebalance listing failed for bucket {bucket} set {set_idx} after {max_attempts} attempts: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown listing failure".to_string())
    )))
}

impl SetDisks {
    #[tracing::instrument(skip(self, rx, cb))]
    pub async fn list_objects_to_rebalance(
        self: &Arc<Self>,
        rx: CancellationToken,
        bucket: String,
        cb: ListCallback,
    ) -> Result<()> {
        debug!(
            event = EVENT_REBALANCE_LISTING,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            bucket = %bucket,
            state = "started",
            "Rebalance listing started"
        );
        let (disks, _) = self.get_online_disks_with_healing(false).await;
        ensure_rebalance_listing_disks_available(!disks.is_empty(), &bucket)?;

        debug!(
            event = EVENT_REBALANCE_LISTING,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            bucket = %bucket,
            disk_count = disks.len(),
            state = "disks_resolved",
            "Rebalance listing disks resolved"
        );
        let listing_quorum = self.set_drive_count.div_ceil(2);

        let resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: bucket.clone(),
            ..Default::default()
        };

        let cb1 = cb.clone();
        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                bucket: bucket.clone(),
                recursive: true,
                min_disks: listing_quorum,
                skip_walkdir_total_timeout: true,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    debug!(
                        event = EVENT_REBALANCE_LISTING,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REBALANCE,
                        entry = %entry.name,
                        state = "agreed_entry",
                        "Rebalance listing agreed entry"
                    );
                    Box::pin(cb1(entry))
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    let resolver = resolver.clone();
                    let cb = cb.clone();

                    match entries.resolve(resolver) {
                        Some(entry) => {
                            debug!(
                                event = EVENT_REBALANCE_LISTING,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                entry = %entry.name,
                                state = "resolved_partial_entry",
                                "Rebalance listing resolved partial entry"
                            );
                            Box::pin(async move { cb(entry).await })
                        }
                        None => {
                            debug!(
                                event = EVENT_REBALANCE_LISTING,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REBALANCE,
                                state = "partial_entry_missing",
                                "Rebalance listing partial entry missing"
                            );
                            Box::pin(async {})
                        }
                    }
                })),
                ..Default::default()
            },
        )
        .await?;

        debug!(
            event = EVENT_REBALANCE_LISTING,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            bucket = %bucket,
            state = "completed",
            "Rebalance listing completed"
        );
        Ok(())
    }
}
