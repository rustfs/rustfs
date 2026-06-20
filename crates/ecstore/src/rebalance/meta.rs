use super::{
    EVENT_REBALANCE_BUCKET, Error, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX,
    RebalSaveOpt, RebalStatus, RebalanceCleanupWarnings, RebalanceMeta, RebalanceStats, Result,
};
use crate::error::is_err_operation_canceled;
use std::collections::HashSet;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info};

pub(super) fn is_rebalance_pool_started(pool_stat: &RebalanceStats) -> bool {
    pool_stat.participating && pool_stat.info.status == RebalStatus::Started
}

pub(super) fn is_rebalance_in_progress(meta: &RebalanceMeta) -> bool {
    if meta.stopped_at.is_some() {
        return false;
    }

    meta.pool_stats.iter().any(is_rebalance_pool_started)
}

pub(super) fn is_rebalance_conflicting_with_decommission(meta: &RebalanceMeta) -> bool {
    is_rebalance_in_progress(meta)
}

pub(super) fn first_rebalance_bucket(pool_stat: &RebalanceStats) -> Option<String> {
    pool_stat.buckets.first().cloned()
}

pub(super) fn rebalance_meta_load_no_data_error() -> Error {
    Error::other("rebalance metadata load failed: metadata payload is too short")
}

pub(super) fn rebalance_meta_load_unknown_format_error(fmt: u16) -> Error {
    Error::other(format!("rebalance metadata load failed: unknown format {fmt}"))
}

pub(super) fn rebalance_meta_load_unknown_version_error(ver: u16) -> Error {
    Error::other(format!("rebalance metadata load failed: unknown version {ver}"))
}
pub(super) fn rebalance_goal_reached(init_free_space: u64, init_capacity: u64, bytes: u64, percent_free_goal: f64) -> bool {
    if init_capacity == 0 {
        return false;
    }

    let pfi = (init_free_space + bytes) as f64 / init_capacity as f64;
    pfi + f64::EPSILON >= percent_free_goal
}

pub(super) fn percent_free_ratio(total_free: u64, total_cap: u64) -> f64 {
    if total_cap == 0 {
        return 0.0;
    }
    total_free as f64 / total_cap as f64
}

pub(super) fn next_rebal_bucket_from_stat(pool_stat: &RebalanceStats) -> Option<String> {
    if pool_stat.buckets.is_empty() {
        return None;
    }

    first_rebalance_bucket(pool_stat)
}

pub(super) fn rebalance_metadata_not_initialized_error(operation: &str) -> Error {
    Error::other(format!("failed to {operation}: rebalance metadata not initialized"))
}

pub(super) fn invalid_rebalance_pool_index_error(pool_index: usize, pool_count: usize) -> Error {
    Error::other(format!("invalid rebalance pool index {pool_index} for {pool_count} pools"))
}

pub(super) fn clone_rebalance_pool_stats(meta: Option<&RebalanceMeta>) -> Result<Vec<RebalanceStats>> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("clone rebalance pool stats"));
    };
    Ok(meta.pool_stats.clone())
}

pub(super) fn should_accept_rebalance_stats_update(meta: &RebalanceMeta, pool_index: usize) -> bool {
    if meta.stopped_at.is_some() {
        return false;
    }

    meta.pool_stats
        .get(pool_index)
        .is_some_and(|pool_stat| pool_stat.info.status == RebalStatus::Started)
}

pub(super) fn resolve_next_rebalance_bucket(meta: Option<&RebalanceMeta>, pool_index: usize) -> Result<Option<String>> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("resolve next rebalance bucket"));
    };

    ensure_valid_rebalance_pool_index(meta.pool_stats.len(), pool_index)?;
    let Some(pool_stat) = meta.pool_stats.get(pool_index) else {
        return Err(invalid_rebalance_pool_index_error(pool_index, meta.pool_stats.len()));
    };

    if pool_stat.info.status == RebalStatus::Completed || !pool_stat.participating {
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            state = "unavailable",
            reason = "completed_or_not_participating",
            "No rebalance bucket available"
        );
        return Ok(None);
    }

    if pool_stat.buckets.is_empty() {
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            state = "unavailable",
            reason = "bucket_queue_empty",
            "No rebalance bucket available"
        );
        return Ok(None);
    }

    if let Some(bucket) = next_rebal_bucket_from_stat(pool_stat) {
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            bucket = %bucket,
            state = "selected",
            "Selected rebalance bucket"
        );
        return Ok(Some(bucket));
    }

    debug!(
        event = EVENT_REBALANCE_BUCKET,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REBALANCE,
        pool_index,
        state = "unavailable",
        reason = "selection_returned_none",
        "No rebalance bucket available"
    );
    Ok(None)
}

pub(super) fn mark_rebalance_bucket_done(meta: Option<&mut RebalanceMeta>, pool_index: usize, bucket: &str) -> Result<()> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("mark rebalance bucket done"));
    };

    ensure_valid_rebalance_pool_index(meta.pool_stats.len(), pool_index)?;
    let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) else {
        return Err(invalid_rebalance_pool_index_error(pool_index, meta.pool_stats.len()));
    };

    if take_bucket_from_rebalance_queue(pool_stat, bucket) {
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            bucket = %bucket,
            state = "queue_removed",
            remaining_bucket_count = pool_stat.buckets.len(),
            "Removed bucket from rebalance queue"
        );
        if has_deferred_rebalance_error(pool_stat) {
            pool_stat.info.last_error = None;
        }
        Ok(())
    } else {
        Err(Error::other(format!(
            "failed to mark rebalance bucket done: bucket {bucket} was not queued for pool {pool_index}"
        )))
    }
}

pub(super) fn record_rebalance_cleanup_warning_in_meta(
    meta: Option<&mut RebalanceMeta>,
    pool_index: usize,
    bucket: &str,
    object: &str,
    message: String,
    now: OffsetDateTime,
) -> Result<()> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error("record rebalance cleanup warning"));
    };

    ensure_valid_rebalance_pool_index(meta.pool_stats.len(), pool_index)?;
    let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) else {
        return Err(invalid_rebalance_pool_index_error(pool_index, meta.pool_stats.len()));
    };

    pool_stat.cleanup_warnings.count = pool_stat.cleanup_warnings.count.saturating_add(1);
    pool_stat.cleanup_warnings.last_message = Some(message);
    pool_stat.cleanup_warnings.last_bucket = Some(bucket.to_string());
    pool_stat.cleanup_warnings.last_object = Some(object.to_string());
    pool_stat.cleanup_warnings.last_at = Some(now);
    meta.last_refreshed_at = Some(now);
    Ok(())
}

pub(super) fn take_bucket_from_rebalance_queue(pool_stat: &mut RebalanceStats, bucket: &str) -> bool {
    let mut found = false;
    pool_stat.buckets.retain(|name| {
        if name == bucket {
            found = true;
            pool_stat.rebalanced_buckets.push(name.clone());
            false
        } else {
            true
        }
    });

    found
}

pub(super) fn defer_bucket_in_rebalance_queue(pool_stat: &mut RebalanceStats, bucket: &str) -> Result<()> {
    let Some(pos) = pool_stat.buckets.iter().position(|name| name == bucket) else {
        return Err(Error::other(format!("failed to defer rebalance bucket {bucket}: bucket was not queued")));
    };

    let bucket = pool_stat.buckets.remove(pos);
    pool_stat.buckets.push(bucket);
    Ok(())
}

pub(super) fn should_pool_participate(init_free_space: u64, init_capacity: u64, percent_free_goal: f64) -> bool {
    init_capacity > 0 && percent_free_ratio(init_free_space, init_capacity) < percent_free_goal
}

pub(super) fn complete_rebalance_pools_at_goal(meta: &mut RebalanceMeta, now: OffsetDateTime) -> bool {
    let mut changed = false;

    for pool_stat in meta.pool_stats.iter_mut() {
        if !is_rebalance_pool_started(pool_stat) || has_deferred_rebalance_error(pool_stat) {
            continue;
        }

        if rebalance_goal_reached(
            pool_stat.init_free_space,
            pool_stat.init_capacity,
            pool_stat.bytes,
            meta.percent_free_goal,
        ) {
            pool_stat.info.status = RebalStatus::Completed;
            pool_stat.info.end_time = Some(now);
            pool_stat.info.last_error = None;
            changed = true;
        }
    }

    changed
}

pub(super) fn complete_rebalance_pools_with_empty_queue(meta: &mut RebalanceMeta, now: OffsetDateTime) -> bool {
    let mut changed = false;

    for pool_stat in meta.pool_stats.iter_mut() {
        if !is_rebalance_pool_started(pool_stat) || !pool_stat.buckets.is_empty() {
            continue;
        }

        pool_stat.info.status = RebalStatus::Completed;
        pool_stat.info.end_time = Some(now);
        pool_stat.info.last_error = None;
        changed = true;
    }

    changed
}

pub(super) fn has_deferred_rebalance_error(pool_stat: &RebalanceStats) -> bool {
    pool_stat
        .info
        .last_error
        .as_deref()
        .is_some_and(|last_error| last_error.starts_with(REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX))
}
pub(super) fn clone_first_arc<T>(values: &[Arc<T>], err_msg: &str) -> Result<Arc<T>> {
    values.first().cloned().ok_or_else(|| Error::other(err_msg))
}

pub(super) fn clone_arc_by_index<T>(values: &[Arc<T>], idx: usize, err_prefix: &str) -> Result<Arc<T>> {
    values
        .get(idx)
        .cloned()
        .ok_or_else(|| Error::other(format!("{err_prefix}: {idx}")))
}

pub(super) fn ensure_valid_rebalance_pool_index(pool_count: usize, idx: usize) -> Result<()> {
    if idx >= pool_count {
        return Err(invalid_rebalance_pool_index_error(idx, pool_count));
    }

    Ok(())
}

pub(super) enum RebalanceTerminalEvent {
    Completed { msg: String },
    Stopped { msg: String },
    Failed { msg: String, last_error: String },
    ChannelClosed { msg: String, last_error: String },
}

impl RebalanceTerminalEvent {
    pub(super) fn message(&self) -> &str {
        match self {
            RebalanceTerminalEvent::Completed { msg }
            | RebalanceTerminalEvent::Stopped { msg }
            | RebalanceTerminalEvent::Failed { msg, .. }
            | RebalanceTerminalEvent::ChannelClosed { msg, .. } => msg,
        }
    }
}

pub(super) fn apply_rebalance_terminal_event(
    status: &mut RebalStatus,
    end_time: &mut Option<OffsetDateTime>,
    last_error: &mut Option<String>,
    terminal_event: RebalanceTerminalEvent,
    now: OffsetDateTime,
) {
    match terminal_event {
        RebalanceTerminalEvent::Completed { .. } => {
            *status = RebalStatus::Completed;
            *end_time = Some(now);
            *last_error = None;
        }
        RebalanceTerminalEvent::Stopped { .. } => {
            *status = RebalStatus::Stopped;
            *end_time = Some(now);
            *last_error = None;
        }
        RebalanceTerminalEvent::Failed { last_error: err, .. }
        | RebalanceTerminalEvent::ChannelClosed { last_error: err, .. } => {
            *status = RebalStatus::Failed;
            *end_time = Some(now);
            *last_error = Some(err);
        }
    }
}

pub(super) fn classify_rebalance_terminal_event(signal: Option<Result<()>>, now: OffsetDateTime) -> RebalanceTerminalEvent {
    match signal {
        Some(Ok(())) => RebalanceTerminalEvent::Completed {
            msg: format!("Rebalance completed at {now:?}"),
        },
        Some(Err(err)) => {
            if is_err_operation_canceled(&err) {
                RebalanceTerminalEvent::Stopped {
                    msg: format!("Rebalance stopped at {now:?}"),
                }
            } else {
                RebalanceTerminalEvent::Failed {
                    msg: format!("Rebalance failed at {now:?} with err {err:?}"),
                    last_error: err.to_string(),
                }
            }
        }
        None => RebalanceTerminalEvent::ChannelClosed {
            msg: format!("Rebalance save task channel closed unexpectedly at {now:?}"),
            last_error: format!("rebalance save channel closed before terminal event at {now:?}"),
        },
    }
}

pub(super) fn ensure_rebalance_not_decommissioning(decommission_running: bool) -> bool {
    !decommission_running
}

pub(super) fn validate_start_rebalance_state(decommission_running: bool, meta_loaded: bool) -> Result<()> {
    if !ensure_rebalance_not_decommissioning(decommission_running) {
        return Err(Error::DecommissionAlreadyRunning);
    }
    if !meta_loaded {
        return Err(Error::ConfigNotFound);
    }

    Ok(())
}

pub(super) fn should_skip_start_rebalance(cancel_attached: bool, in_progress: bool) -> bool {
    cancel_attached && in_progress
}

pub(super) fn is_rebalance_stopped_terminal_event(terminal_event: &RebalanceTerminalEvent) -> bool {
    matches!(terminal_event, RebalanceTerminalEvent::Stopped { .. })
}

pub(super) fn should_preserve_rebalance_stopped_state(
    meta_stopped: bool,
    status: RebalStatus,
    terminal_event: &RebalanceTerminalEvent,
) -> bool {
    (meta_stopped || status == RebalStatus::Stopped) && !is_rebalance_stopped_terminal_event(terminal_event)
}

pub(super) fn resolve_rebalance_participants(pool_stats: &[RebalanceStats], pool_count: usize) -> Vec<bool> {
    let mut participants = vec![false; pool_count];

    for (idx, pool_stat) in pool_stats.iter().enumerate() {
        if idx >= participants.len() {
            break;
        }

        if pool_stat.info.status == RebalStatus::Started {
            participants[idx] = pool_stat.participating;
        }
    }

    participants
}

pub(super) fn is_rebalance_actively_running(meta: &RebalanceMeta) -> bool {
    meta.cancel.is_some() && is_rebalance_in_progress(meta)
}

pub(super) fn should_ignore_rebalance_data_usage_cache(bucket: &str) -> bool {
    bucket == crate::disk::RUSTFS_META_BUCKET
}

pub(super) fn apply_rebalance_save_option(meta: &mut RebalanceMeta, pool_idx: usize, opt: RebalSaveOpt, now: OffsetDateTime) {
    match opt {
        RebalSaveOpt::Stats => {
            if pool_idx >= meta.pool_stats.len() {
                info!("save_rebalance_stats: pool_idx {pool_idx} out of range for pool_stats");
            }
        }
        RebalSaveOpt::StoppedAt => {
            apply_stopped_at(meta, now);
        }
    }

    meta.last_refreshed_at = Some(now);
}

pub(super) fn is_rebalance_terminal_status(status: RebalStatus) -> bool {
    matches!(status, RebalStatus::Completed | RebalStatus::Stopped | RebalStatus::Failed)
}

pub(super) fn merge_rebalance_bucket_lists(remote: &mut Vec<String>, local: &[String]) {
    let mut existing: HashSet<String> = remote.iter().cloned().collect();
    for bucket in local {
        if existing.insert(bucket.clone()) {
            remote.push(bucket.clone());
        }
    }
}

pub(super) fn remove_rebalanced_buckets_from_queue(pool_stat: &mut RebalanceStats) {
    let rebalanced_buckets: HashSet<String> = pool_stat.rebalanced_buckets.iter().cloned().collect();
    pool_stat.buckets.retain(|bucket| !rebalanced_buckets.contains(bucket));
}

pub(super) fn merge_rebalance_cleanup_warnings(remote: &mut RebalanceCleanupWarnings, local: &RebalanceCleanupWarnings) {
    remote.count = remote.count.max(local.count);

    if should_replace_rebalance_cleanup_warning(remote.last_at, local.last_at) {
        remote.last_message = local.last_message.clone();
        remote.last_bucket = local.last_bucket.clone();
        remote.last_object = local.last_object.clone();
        remote.last_at = local.last_at;
    }
}

pub(super) fn should_replace_rebalance_cleanup_warning(
    remote_at: Option<OffsetDateTime>,
    local_at: Option<OffsetDateTime>,
) -> bool {
    match (remote_at, local_at) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(remote), Some(local)) => local >= remote,
    }
}

pub(super) fn merge_rebalance_pool_stats(remote: &mut RebalanceStats, local: &RebalanceStats) {
    remote.init_free_space = remote.init_free_space.max(local.init_free_space);
    remote.init_capacity = remote.init_capacity.max(local.init_capacity);
    remote.participating |= local.participating;

    merge_rebalance_bucket_lists(&mut remote.buckets, &local.buckets);
    merge_rebalance_bucket_lists(&mut remote.rebalanced_buckets, &local.rebalanced_buckets);
    remove_rebalanced_buckets_from_queue(remote);

    let local_is_newer = local.num_versions >= remote.num_versions;
    remote.num_objects = remote.num_objects.max(local.num_objects);
    remote.num_versions = remote.num_versions.max(local.num_versions);
    remote.bytes = remote.bytes.max(local.bytes);
    merge_rebalance_cleanup_warnings(&mut remote.cleanup_warnings, &local.cleanup_warnings);

    if local_is_newer {
        remote.bucket = local.bucket.clone();
        remote.object = local.object.clone();
    }

    if remote.info.start_time.is_none() {
        remote.info.start_time = local.info.start_time;
    }

    if is_rebalance_terminal_status(remote.info.status) && local.info.status == RebalStatus::Started {
        return;
    }

    if remote.info.status == RebalStatus::Stopped && matches!(local.info.status, RebalStatus::Started | RebalStatus::Completed) {
        return;
    }

    match local.info.status {
        RebalStatus::Failed => {
            remote.info.status = RebalStatus::Failed;
            remote.info.end_time = local.info.end_time.or(remote.info.end_time);
            remote.info.last_error = local.info.last_error.clone().or_else(|| remote.info.last_error.clone());
        }
        RebalStatus::Stopped => {
            if remote.info.status != RebalStatus::Failed {
                remote.info.status = RebalStatus::Stopped;
                remote.info.end_time = local.info.end_time.or(remote.info.end_time);
                remote.info.last_error = None;
            }
        }
        RebalStatus::Completed => {
            if !matches!(remote.info.status, RebalStatus::Failed | RebalStatus::Stopped) {
                remote.info.status = RebalStatus::Completed;
                remote.info.end_time = local.info.end_time.or(remote.info.end_time);
                remote.info.last_error = None;
            }
        }
        RebalStatus::Started => {
            if !is_rebalance_terminal_status(remote.info.status) {
                remote.info.status = RebalStatus::Started;
                remote.info.last_error = local.info.last_error.clone();
            }
        }
        RebalStatus::None => {}
    }
}

pub(super) fn merge_rebalance_meta(remote: &mut RebalanceMeta, local: &RebalanceMeta) {
    if remote.id.is_empty() {
        *remote = local.clone();
        return;
    }

    if !local.id.is_empty() && remote.id != local.id {
        *remote = local.clone();
        return;
    }

    remote.percent_free_goal = local.percent_free_goal;
    remote.last_refreshed_at = Some(OffsetDateTime::now_utc());
    if remote.stopped_at.is_none() {
        remote.stopped_at = local.stopped_at;
    }

    if remote.pool_stats.len() < local.pool_stats.len() {
        remote.pool_stats.resize_with(local.pool_stats.len(), RebalanceStats::default);
    }

    for (idx, local_pool_stat) in local.pool_stats.iter().enumerate() {
        if let Some(remote_pool_stat) = remote.pool_stats.get_mut(idx) {
            merge_rebalance_pool_stats(remote_pool_stat, local_pool_stat);
        }
    }
}

pub(super) fn mark_started_rebalance_pools_stopped(meta: &mut RebalanceMeta, stop_time: OffsetDateTime) {
    for pool_stat in meta.pool_stats.iter_mut() {
        if pool_stat.info.status == RebalStatus::Started {
            pool_stat.info.status = RebalStatus::Stopped;
            pool_stat.info.end_time.get_or_insert(stop_time);
            pool_stat.info.last_error = None;
        }
    }
}

pub(super) fn apply_stopped_at(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    meta.stopped_at = Some(now);
    mark_started_rebalance_pools_stopped(meta, now);
}

pub(super) fn stop_rebalance_state(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    if let Some(cancel_tx) = meta.cancel.take() {
        cancel_tx.cancel();
    }

    let stop_time = meta.stopped_at.unwrap_or(now);
    if meta.stopped_at.is_none() && is_rebalance_in_progress(meta) {
        meta.stopped_at = Some(stop_time);
    }

    if meta.stopped_at.is_some() {
        mark_started_rebalance_pools_stopped(meta, stop_time);
    }
}

pub(super) fn stop_rebalance_meta_snapshot(meta: Option<&mut RebalanceMeta>, now: OffsetDateTime) -> Option<RebalanceMeta> {
    meta.map(|meta| {
        stop_rebalance_state(meta, now);
        meta.last_refreshed_at = Some(now);
        meta.clone()
    })
}
