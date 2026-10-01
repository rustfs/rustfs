use super::migration::MigrationVersionResult;
use super::{
    DEFAULT_REBALANCE_MAX_ATTEMPTS, EVENT_REBALANCE_LISTING, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, REBAL_META_NAME,
    REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX, REBALANCE_LISTING_RETRY_BASE_DELAY, REBALANCE_MAX_ATTEMPTS_ENV,
    REBALANCE_MIGRATION_LOCK_RETRY_CAP, REBALANCE_MIGRATION_RETRY_BASE_DELAY, REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX,
    RebalanceBucketConfigs, RebalanceBucketOutcome, RebalanceDeferKind, RebalanceEntryOutcome, Result,
};
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::core::pools::ListCallback;
use crate::data_movement::SourceCleanupError;
use crate::disk::error::DiskError;
use crate::error::{
    Error, is_err_object_not_found, is_err_operation_canceled, is_err_version_not_found, is_network_or_host_down,
};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::store::ECStore;
use rand::RngExt as _;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use std::sync::Arc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[cfg(test)]
tokio::task_local! {
    pub(super) static REBALANCE_METADATA_RETRY_PROBE: Arc<tokio::sync::Notify>;
}

/// Background walks skip the total timeout, so the per-read stall budget is what
/// catches a drive that stops answering. Keep it generous: rebalance is not
/// latency-sensitive, and one slow read is not a dead drive.
const BACKGROUND_WALKDIR_STALL_TIMEOUT: Duration = Duration::from_secs(60);

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

/// Preserve the first real failure even when another task observes cancellation
/// first. Cancellation is an outcome only when no entry or worker failed.
pub(super) fn record_rebalance_error(first_error: &mut Option<Error>, err: Error) {
    if first_error
        .as_ref()
        .is_none_or(|first| is_err_operation_canceled(first) && !is_err_operation_canceled(&err))
    {
        *first_error = Some(err);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RebalanceEntryCleanupResult {
    Completed { warning: Option<String> },
    Deferred { last_error: String },
}

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
                record_rebalance_error(&mut first_error, err);
            }
            Err(err) => {
                let err = Error::other(format!("rebalance entry task join error for set {set_idx}: {err}"));
                error!("{}", err);
                record_rebalance_error(&mut first_error, err);
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
    // Keep the source error reachable: the metadata retry policy classifies
    // transient lock timeouts by inspecting the source chain, so collapsing the
    // failure into a plain string here would make that retry a no-op.
    result.map_err(|err| {
        let rendered = format!("rebalance meta save failed during {stage}: {err}");
        crate::data_movement::data_movement_context_error(rendered, err)
    })
}

pub(super) fn rebalance_meta_lock_error(err: rustfs_lock::LockError, mode: &'static str) -> Error {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => Error::NamespaceLockQuorumUnavailable {
            mode,
            bucket: crate::disk::RUSTFS_META_BUCKET.to_string(),
            object: REBAL_META_NAME.to_string(),
            required,
            achieved,
        },
        other => crate::data_movement::data_movement_context_error(
            format!(
                "failed to acquire rebalance metadata {mode} lock on {}/{}: {other}",
                crate::disk::RUSTFS_META_BUCKET,
                REBAL_META_NAME
            ),
            Error::Lock(other),
        ),
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
        if is_err_operation_canceled(&err) {
            return err;
        }
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
    result: std::result::Result<crate::object_api::ObjectInfo, SourceCleanupError>,
    bucket: &str,
    object_name: &str,
) -> RebalanceEntryCleanupResult {
    match result {
        Ok(_) => RebalanceEntryCleanupResult::Completed { warning: None },
        Err(SourceCleanupError::Storage(err)) if is_source_cleanup_not_found(&err) => {
            RebalanceEntryCleanupResult::Completed { warning: None }
        }
        Err(SourceCleanupError::SourceChanged) => RebalanceEntryCleanupResult::Deferred {
            last_error: format!(
                "{REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX} source changed during cleanup preflight for {bucket}/{object_name}"
            ),
        },
        // A transient cleanup failure is not evidence that the source replica is gone, so the
        // entry stays incomplete and the bucket is retried instead of recording a permanent
        // cleanup warning that would block pool completion.
        Err(SourceCleanupError::Storage(err)) if is_transient_rebalance_error(&err) => RebalanceEntryCleanupResult::Deferred {
            last_error: format!(
                "{REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX} transient source cleanup failure for {bucket}/{object_name} will be retried: {err}"
            ),
        },
        Err(SourceCleanupError::Storage(err)) => RebalanceEntryCleanupResult::Completed {
            warning: Some(format!("rebalance cleanup delete failed for {bucket}/{object_name}: {err}")),
        },
    }
}

fn is_source_cleanup_not_found(err: &Error) -> bool {
    let err = rebalance_error_source(err);
    is_err_object_not_found(err) || is_err_version_not_found(err)
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

pub(super) fn resolve_rebalance_deferred_last_error(
    kind: RebalanceDeferKind,
    pending_entry_defer: Option<&str>,
    last_error: &str,
) -> Option<String> {
    match kind {
        RebalanceDeferKind::Entry => Some(last_error.to_string()),
        // A retryable cleanup conflict is progress, not a pool failure, so it must not surface as
        // `lastError`. It also must not erase an unresolved migration deferral: that marker is the
        // only signal keeping the pool from completing at the free-space goal while an entry is
        // still retried, and the two deferrals can be reported by different buckets of one pool.
        RebalanceDeferKind::SourceCleanup => pending_entry_defer
            .filter(|pending| pending.starts_with(REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX))
            .map(str::to_string),
    }
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

pub(super) fn resolve_rebalance_bucket_error(mut entry_error: Option<Error>, worker_error: Option<Error>) -> Result<()> {
    if let Some(err) = worker_error {
        record_rebalance_error(&mut entry_error, err);
    }
    entry_error.map_or(Ok(()), Err)
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
    let err = rebalance_error_source(err);
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

fn rebalance_error_source(mut err: &Error) -> &Error {
    // Stage context contains object names, so classify the preserved source,
    // not timeout-like text supplied by an object name. Iterate nested stages.
    while let Some(source) = crate::data_movement::data_movement_stage_source(err) {
        err = source;
    }
    err
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
    // `LockError::Timeout` renders "Lock acquisition timeout for resource ...", while the
    // namespace-lock layer renders "lock acquisition timed out on ..."; both are retryable.
    message.contains("lock acquisition timeout")
        || message.contains("lock acquisition timed out")
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

/// Retry lock admission or read-only metadata access, never a data mutation.
/// Each failed attempt must release its guards before the backoff so queued
/// writers and stop/replacement activation can make progress.
pub(super) async fn retry_rebalance_metadata_access<T, Access, AccessFuture>(
    cancel: Option<&CancellationToken>,
    max_attempts: usize,
    mut access: Access,
) -> Result<T>
where
    Access: FnMut() -> AccessFuture,
    AccessFuture: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0usize;
    loop {
        let result = match cancel {
            Some(cancel) => tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::OperationCanceled),
                result = access() => result,
            },
            None => access().await,
        };
        match result {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt.saturating_add(1) >= max_attempts.max(1)
                    || !matches!(rebalance_error_source(&err), Error::Lock(lock_err) if is_rebalance_transient_lock_error(lock_err))
                {
                    return Err(err);
                }
                #[cfg(test)]
                let _ = REBALANCE_METADATA_RETRY_PROBE.try_with(|probe| probe.notify_one());
                let delay = rebalance_migration_retry_delay(attempt, &err);
                match cancel {
                    Some(cancel) => wait_rebalance_listing_retry(cancel, delay).await?,
                    None => tokio::time::sleep(delay).await,
                }
                attempt += 1;
            }
        }
    }
}

pub(super) fn rebalance_listing_retry_delay(attempt: usize) -> Duration {
    let multiplier = u32::try_from(attempt.saturating_add(1)).unwrap_or(u32::MAX);
    REBALANCE_LISTING_RETRY_BASE_DELAY.saturating_mul(multiplier)
}

fn is_rebalance_lock_or_rpc_timeout(err: &Error) -> bool {
    let err = rebalance_error_source(err);
    match err {
        Error::Lock(rustfs_lock::LockError::Timeout { .. }) | Error::Lock(rustfs_lock::LockError::Network { .. }) => true,
        Error::Io(io_err) => is_rebalance_lock_or_rpc_timeout_message(&io_err.to_string()),
        _ => is_rebalance_lock_or_rpc_timeout_message(&err.to_string()),
    }
}

fn is_rebalance_lock_or_rpc_timeout_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("lock acquisition timeout")
        || message.contains("lock acquisition timed out")
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
    if is_err_operation_canceled(&err) {
        return err;
    }
    Error::other(format!("rebalance entry {stage} failed for {bucket}/{object_name}: {err}"))
}

pub(super) fn should_count_rebalance_version_complete(result: &MigrationVersionResult) -> bool {
    result.cleanup_ignored || (result.moved && !result.failed)
}

pub(super) fn should_cleanup_rebalance_source_entry(rebalanced: usize, total_versions: usize, expired: usize) -> bool {
    rebalanced.saturating_add(expired) == total_versions
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

pub(super) async fn load_rebalance_bucket_configs(api: &ECStore, bucket: &str) -> Result<RebalanceBucketConfigs> {
    if bucket == crate::disk::RUSTFS_META_BUCKET {
        return Ok(RebalanceBucketConfigs::default());
    }

    let _ = resolve_rebalance_optional_bucket_config_result(
        bucket,
        "versioning",
        crate::bucket::versioning_sys::BucketVersioningSys::get(bucket).await,
    )?;

    let expiry_configs = crate::bucket::lifecycle::get_expiry_configs(api, bucket).await?;
    Ok(RebalanceBucketConfigs {
        bucket_incarnation_id: Some(api.bucket_incarnation_id_from_disk(bucket).await?),
        lifecycle_config: expiry_configs.lifecycle.map(|config| (*config).clone()),
        object_lock_config: expiry_configs.object_lock.map(|config| (*config).clone()),
        replication_config: resolve_rebalance_optional_bucket_config_result(
            bucket,
            "replication",
            crate::bucket::metadata_sys::get_replication_config(bucket).await,
        )?,
    })
}

pub(super) async fn run_rebalance_listing_with_retry<List, ListFuture>(
    rx: CancellationToken,
    bucket: String,
    cb: ListCallback,
    set_idx: usize,
    max_attempts: usize,
    entry_tasks: Arc<tokio::sync::Mutex<Vec<RebalanceEntryTask>>>,
    mut list: List,
) -> Result<()>
where
    List: FnMut(ListCallback) -> ListFuture,
    ListFuture: std::future::Future<Output = Result<()>>,
{
    let max_attempts = max_attempts.max(1);
    let mut last_error = None;

    for attempt in 0..max_attempts {
        match list(cb.clone()).await {
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
                // The full retry re-evaluates deferred entries; only task failures block the next attempt.
                let _ = wait_rebalance_entry_tasks(set_idx, entry_tasks.clone()).await?;
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
                walkdir_stall_timeout: Some(BACKGROUND_WALKDIR_STALL_TIMEOUT),
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

#[cfg(test)]
mod error_source_tests {
    use super::*;

    #[tokio::test]
    async fn rebalance_metadata_retry_is_bounded_and_retains_the_timeout() {
        for max_attempts in [0, 1, 3] {
            let mut attempts = 0;
            let result = retry_rebalance_metadata_access(None, max_attempts, || {
                attempts += 1;
                std::future::ready(Err::<(), _>(rebalance_meta_lock_error(
                    rustfs_lock::LockError::timeout(".rustfs.sys/rebalance.bin@latest", Duration::from_secs(5)),
                    "read",
                )))
            })
            .await;
            let err = result.expect_err("persistent lock contention must not become success");
            assert_eq!(attempts, max_attempts.max(1));
            assert!(matches!(
                rebalance_error_source(&err),
                Error::Lock(rustfs_lock::LockError::Timeout { .. })
            ));
        }
    }

    #[tokio::test]
    async fn rebalance_metadata_retry_does_not_retry_permanent_or_untyped_errors() {
        for err in [
            Error::FileAccessDenied,
            Error::DiskFull,
            Error::NamespaceLockQuorumUnavailable {
                mode: "read",
                bucket: crate::disk::RUSTFS_META_BUCKET.to_string(),
                object: REBAL_META_NAME.to_string(),
                required: 3,
                achieved: 2,
            },
            Error::other("stale rebalance run rejected: lock acquisition timed out"),
            Error::other("rebalance distributed run fence lost"),
            Error::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        ] {
            let expected = err.to_string();
            let mut error = Some(crate::data_movement::data_movement_context_error(expected.clone(), err));
            let mut attempts = 0;
            let result = retry_rebalance_metadata_access(None, 3, || {
                attempts += 1;
                std::future::ready(Err::<(), _>(error.take().expect("permanent metadata failures must not be retried")))
            })
            .await;
            assert_eq!(attempts, 1);
            assert_eq!(
                rebalance_error_source(&result.expect_err("failure must remain visible")).to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn rebalance_metadata_retry_engages_for_wrapped_meta_save_lock_timeout() {
        let mut attempts = 0;
        let result = retry_rebalance_metadata_access(None, 3, || {
            attempts += 1;
            std::future::ready(resolve_rebalance_meta_save_result(
                Err(rebalance_meta_lock_error(
                    rustfs_lock::LockError::timeout(".rustfs.sys/rebalance.bin@latest", Duration::from_secs(5)),
                    "write",
                )),
                "save_rebalance_stats for pool 0 opt Stats",
            ))
        })
        .await;

        assert_eq!(attempts, 3, "a wrapped meta save lock timeout must stay retryable");
        let err = result.expect_err("persistent lock contention must not become success");
        assert!(matches!(
            rebalance_error_source(&err),
            Error::Lock(rustfs_lock::LockError::Timeout { .. })
        ));
    }

    #[tokio::test]
    async fn rebalance_metadata_retry_cancels_a_pending_attempt() {
        struct DropProbe(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let cancel = CancellationToken::new();
        let started = tokio::sync::Notify::new();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let access = retry_rebalance_metadata_access(Some(&cancel), 3, || async {
            let _probe = DropProbe(Arc::clone(&dropped));
            started.notify_one();
            std::future::pending::<Result<()>>().await
        });
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(access, async {
                started.notified().await;
                cancel.cancel();
            })
        })
        .await
        .expect("cancellation must interrupt a pending lock attempt");
        assert!(matches!(result, Err(Error::OperationCanceled)));
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rebalance_metadata_retry_cancels_before_another_attempt() {
        let cancel = CancellationToken::new();
        let mut attempts = 0;
        let result = retry_rebalance_metadata_access(Some(&cancel), 3, || {
            attempts += 1;
            let cancel = &cancel;
            async move {
                cancel.cancel();
                Err::<(), _>(Error::Lock(rustfs_lock::LockError::timeout(REBAL_META_NAME, Duration::from_secs(5))))
            }
        })
        .await;
        assert!(matches!(result, Err(Error::OperationCanceled)));
        assert_eq!(attempts, 1);
    }

    #[test]
    fn rebalance_metadata_lock_timeout_preserves_retryable_source() {
        let resource = ".rustfs.sys/rebalance.bin@latest";
        for mode in ["read", "write"] {
            let error = rebalance_meta_lock_error(rustfs_lock::LockError::timeout(resource, Duration::from_secs(5)), mode);
            assert!(
                is_transient_rebalance_error(&error),
                "metadata lock contention must remain retryable: {error}"
            );
            assert!(is_rebalance_lock_or_rpc_timeout(&error));
            assert!(matches!(
                rebalance_error_source(&error),
                Error::Lock(rustfs_lock::LockError::Timeout { resource: actual, .. }) if actual == resource
            ));
            assert!(error.to_string().contains(&format!("rebalance metadata {mode} lock")));
        }
    }

    #[test]
    fn stage_wrapped_errors_select_the_source_backoff_policy() {
        let cases = [
            (
                Error::Lock(rustfs_lock::LockError::timeout(".rustfs.sys/pool.bin@latest", Duration::from_secs(5))),
                true,
            ),
            (
                Error::Lock(rustfs_lock::LockError::network(
                    "peer unavailable",
                    std::io::Error::from(std::io::ErrorKind::ConnectionReset),
                )),
                true,
            ),
            (Error::other("remote lock rpc timed out"), true),
            (Error::SlowDown, false),
            (Error::Io(std::io::Error::other(DiskError::Timeout)), false),
            (Error::FileAccessDenied, false),
        ];
        for (mut error, lock_backoff) in cases {
            for depth in 0..=3 {
                assert_eq!(
                    is_rebalance_lock_or_rpc_timeout(&error),
                    lock_backoff,
                    "wrong backoff at depth {depth}: {error:?}"
                );
                if !lock_backoff {
                    assert_eq!(rebalance_migration_retry_delay(1, &error), REBALANCE_MIGRATION_RETRY_BASE_DELAY * 2);
                }
                error = crate::data_movement::data_movement_stage_error_for_test(
                    "rebalance_object",
                    "put_object",
                    "bucket",
                    "remote lock rpc timed out",
                    error,
                );
            }
        }
    }
    #[test]
    fn rendered_lock_timeout_text_selects_the_lock_backoff() {
        // The lock backend renders a timeout as "Lock acquisition timeout for resource ...",
        // so the message matcher must recognize that text when the error arrives re-rendered
        // instead of as a typed `Error::Lock`.
        let rendered = rustfs_lock::LockError::timeout("bucket/object@latest", Duration::from_secs(5)).to_string();
        assert!(
            rendered.contains("Lock acquisition timeout for resource"),
            "unexpected lock timeout text: {rendered}"
        );

        // The lock policy jitters the delay inside its own cap, so a far-out attempt identifies
        // the selected policy: the linear fallback would return `base * (attempt + 1)`.
        let far_attempt = 100;
        assert!(REBALANCE_MIGRATION_RETRY_BASE_DELAY * 101 > REBALANCE_MIGRATION_LOCK_RETRY_CAP);

        let mut error = Error::other(format!("Lock error: {rendered}"));
        for depth in 0..=3 {
            assert!(
                is_transient_rebalance_error(&error),
                "rendered lock timeout lost retryability at depth {depth}: {error:?}"
            );
            assert!(
                is_rebalance_lock_or_rpc_timeout(&error),
                "rendered lock timeout lost the lock backoff at depth {depth}: {error:?}"
            );
            let delay = rebalance_migration_retry_delay(far_attempt, &error);
            assert!(
                delay <= REBALANCE_MIGRATION_LOCK_RETRY_CAP && delay >= Duration::from_millis(1),
                "rendered lock timeout must stay inside the lock backoff cap at depth {depth}: {delay:?}"
            );
            error = crate::data_movement::data_movement_stage_error_for_test(
                "rebalance_object",
                "put_object",
                "bucket",
                "baseline/00042.bin",
                error,
            );
        }
    }
}
