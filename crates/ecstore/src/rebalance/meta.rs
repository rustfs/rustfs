use super::{
    EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_STATE, Error, GetObjectReader, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE,
    ObjectInfo, ObjectOptions, PutObjReader, REBAL_META_FMT, REBAL_META_NAME, REBAL_META_VER,
    REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT, REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX, REBALANCE_STOP_PROPAGATION_ERROR_PREFIX,
    RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceMeta, RebalanceStats,
    RebalanceStopPropagationRecord, Result,
};
use crate::config::com::{read_config_with_metadata, save_config_with_opts};
use crate::error::is_err_operation_canceled;
use crate::storage_api_contracts::{HTTPRangeSpec, ObjectIO};
use http::HeaderMap;
use rustfs_filemeta::FileInfo;
use std::{collections::HashSet, fmt, io::Cursor, sync::Arc};
use time::OffsetDateTime;
use tracing::{debug, info};

impl RebalanceStats {
    pub fn update(&mut self, bucket: String, fi: &FileInfo) {
        if fi.is_latest {
            self.num_objects += 1;
        }

        self.num_versions += 1;
        let on_disk_size = if fi.deleted || fi.erasure.data_blocks == 0 || fi.size <= 0 {
            0
        } else {
            let data_blocks = fi.erasure.data_blocks as i64;
            let total_blocks = fi.erasure.data_blocks.saturating_add(fi.erasure.parity_blocks) as i64;
            fi.size
                .saturating_mul(total_blocks)
                .checked_div(data_blocks)
                .unwrap_or(0)
                .max(0) as u64
        };
        self.bytes = self.bytes.saturating_add(on_disk_size);
        self.bucket = bucket;
        self.object = fi.name.clone();
    }

    pub fn update_batch(&mut self, bucket: String, versions: &[&FileInfo]) {
        for version in versions {
            self.update(bucket.clone(), version);
        }
    }
}

impl fmt::Display for RebalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self {
            RebalStatus::None => "None",
            RebalStatus::Started => "Started",
            RebalStatus::Completed => "Completed",
            RebalStatus::Stopped => "Stopped",
            RebalStatus::Failed => "Failed",
        };
        write!(f, "{status}")
    }
}

impl From<u8> for RebalStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => RebalStatus::Started,
            2 => RebalStatus::Completed,
            3 => RebalStatus::Stopped,
            4 => RebalStatus::Failed,
            _ => RebalStatus::None,
        }
    }
}

impl RebalanceMeta {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn load<S>(&mut self, store: Arc<S>) -> Result<()>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        self.load_with_opts(store, ObjectOptions::default()).await
    }

    pub async fn load_with_opts<S>(&mut self, store: Arc<S>, opts: ObjectOptions) -> Result<()>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        let (data, _) = read_config_with_metadata(store, REBAL_META_NAME, &opts).await?;
        if data.is_empty() {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_empty",
                "Rebalance metadata is empty"
            );
            return Ok(());
        }
        if data.len() <= 4 {
            return Err(rebalance_meta_load_no_data_error());
        }

        // Read header
        match u16::from_le_bytes([data[0], data[1]]) {
            REBAL_META_FMT => {}
            fmt => return Err(rebalance_meta_load_unknown_format_error(fmt)),
        }
        match u16::from_le_bytes([data[2], data[3]]) {
            REBAL_META_VER => {}
            ver => return Err(rebalance_meta_load_unknown_version_error(ver)),
        }

        let meta: Self = rmp_serde::from_read(Cursor::new(&data[4..]))?;
        *self = meta;

        self.last_refreshed_at = Some(OffsetDateTime::now_utc());

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "metadata_loaded",
            "Loaded rebalance metadata"
        );
        Ok(())
    }

    pub async fn save<S>(&self, store: Arc<S>) -> Result<()>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        self.save_with_opts(store, ObjectOptions::default()).await
    }

    pub async fn save_with_opts<S>(&self, store: Arc<S>, opts: ObjectOptions) -> Result<()>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        if self.pool_stats.is_empty() {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_save_skipped",
                reason = "no_pool_stats",
                "Skipped rebalance metadata save"
            );
            return Ok(());
        }

        let mut data = Vec::new();

        // Initialize the header
        data.extend(&REBAL_META_FMT.to_le_bytes());
        data.extend(&REBAL_META_VER.to_le_bytes());

        let msg = rmp_serde::to_vec(self)?;
        data.extend(msg);

        save_config_with_opts(store, REBAL_META_NAME, data, &opts).await?;

        Ok(())
    }
}

pub(super) fn is_rebalance_pool_started(pool_stat: &RebalanceStats) -> bool {
    pool_stat.participating && pool_stat.info.status == RebalStatus::Started
}

pub(super) fn is_rebalance_pool_active(pool_stat: &RebalanceStats) -> bool {
    is_rebalance_pool_started(pool_stat) || pool_stat.info.stopping
}

pub(super) fn is_rebalance_in_progress(meta: &RebalanceMeta) -> bool {
    meta.pool_stats.iter().any(is_rebalance_pool_active)
}

pub(crate) fn is_rebalance_conflicting_with_decommission(meta: &RebalanceMeta) -> bool {
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

pub fn encode_rebalance_stop_propagation_record(record: &RebalanceStopPropagationRecord) -> String {
    match serde_json::to_string(record) {
        Ok(payload) => format!("{REBALANCE_STOP_PROPAGATION_ERROR_PREFIX}{payload}"),
        Err(err) => {
            let payload = serde_json::json!({
                "encodeError": err.to_string(),
                "stopFailures": [],
                "terminalReloadFailures": [],
            });
            format!("{REBALANCE_STOP_PROPAGATION_ERROR_PREFIX}{payload}")
        }
    }
}

pub fn decode_rebalance_stop_propagation_record(message: &str) -> Option<RebalanceStopPropagationRecord> {
    let payload = message.strip_prefix(REBALANCE_STOP_PROPAGATION_ERROR_PREFIX)?;
    serde_json::from_str(payload).ok()
}

fn is_rebalance_stop_propagation_error(message: Option<&str>) -> bool {
    message.is_some_and(|message| message.starts_with(REBALANCE_STOP_PROPAGATION_ERROR_PREFIX))
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
    pool_stat.cleanup_warnings.last_message = Some(message.clone());
    pool_stat.cleanup_warnings.last_bucket = Some(bucket.to_string());
    pool_stat.cleanup_warnings.last_object = Some(object.to_string());
    pool_stat.cleanup_warnings.last_at = Some(now);
    pool_stat.cleanup_warnings.entries.push(RebalanceCleanupWarningEntry {
        bucket: bucket.to_string(),
        object: object.to_string(),
        message,
        timestamp: Some(now),
    });
    truncate_rebalance_cleanup_warning_entries(&mut pool_stat.cleanup_warnings.entries);
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
        if !is_rebalance_pool_started(pool_stat)
            || has_deferred_rebalance_error(pool_stat)
            || has_rebalance_cleanup_warnings(pool_stat)
        {
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
        if !is_rebalance_pool_started(pool_stat) || !pool_stat.buckets.is_empty() || has_rebalance_cleanup_warnings(pool_stat) {
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

pub(super) fn has_rebalance_cleanup_warnings(pool_stat: &RebalanceStats) -> bool {
    pool_stat.cleanup_warnings.count > 0
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

pub(super) fn validate_init_rebalance_state(decommission_running: bool, current_meta: Option<&RebalanceMeta>) -> Result<()> {
    if !ensure_rebalance_not_decommissioning(decommission_running) {
        return Err(Error::DecommissionAlreadyRunning);
    }
    if current_meta.is_some_and(|meta| !is_rebalance_meta_replaceable_for_new_id(meta)) {
        return Err(Error::RebalanceAlreadyRunning);
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

    merge_rebalance_cleanup_warning_entries(&mut remote.entries, &local.entries);
    let retained_entries = u64::try_from(remote.entries.len()).unwrap_or(u64::MAX);
    remote.count = remote.count.max(retained_entries);
}

pub(super) fn merge_rebalance_cleanup_warning_entries(
    remote: &mut Vec<RebalanceCleanupWarningEntry>,
    local: &[RebalanceCleanupWarningEntry],
) {
    for entry in local {
        if !remote.iter().any(|existing| existing == entry) {
            remote.push(entry.clone());
        }
    }

    remote.sort_by_key(|entry| entry.timestamp);
    truncate_rebalance_cleanup_warning_entries(remote);
}

fn truncate_rebalance_cleanup_warning_entries(entries: &mut Vec<RebalanceCleanupWarningEntry>) {
    if entries.len() > REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT {
        let remove_count = entries.len() - REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT;
        entries.drain(0..remove_count);
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

pub(super) fn merge_rebalance_stop_propagation_error(remote: Option<String>, local: Option<String>) -> Option<String> {
    if is_rebalance_stop_propagation_error(local.as_deref()) {
        local
    } else if is_rebalance_stop_propagation_error(remote.as_deref()) {
        remote
    } else {
        None
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
            remote.info.stopping = false;
            remote.info.end_time = local.info.end_time.or(remote.info.end_time);
            remote.info.last_error = local.info.last_error.clone().or_else(|| remote.info.last_error.clone());
        }
        RebalStatus::Stopped => {
            if remote.info.status != RebalStatus::Failed {
                remote.info.status = RebalStatus::Stopped;
                remote.info.stopping = false;
                remote.info.end_time = local.info.end_time.or(remote.info.end_time);
                remote.info.last_error =
                    merge_rebalance_stop_propagation_error(remote.info.last_error.clone(), local.info.last_error.clone());
            }
        }
        RebalStatus::Completed => {
            if !matches!(remote.info.status, RebalStatus::Failed | RebalStatus::Stopped) {
                remote.info.status = RebalStatus::Completed;
                remote.info.stopping = false;
                remote.info.end_time = local.info.end_time.or(remote.info.end_time);
                remote.info.last_error = None;
            }
        }
        RebalStatus::Started => {
            if !is_rebalance_terminal_status(remote.info.status) {
                remote.info.status = RebalStatus::Started;
                remote.info.stopping |= local.info.stopping;
                remote.info.last_error = local.info.last_error.clone();
            }
        }
        RebalStatus::None => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RebalanceMetaMergeOutcome {
    Merged,
    Replaced,
    RejectedActiveConflict,
}

pub(super) fn is_rebalance_meta_replaceable_for_new_id(meta: &RebalanceMeta) -> bool {
    meta.stopped_at.is_some() || !is_rebalance_in_progress(meta)
}

pub(super) fn merge_rebalance_meta(remote: &mut RebalanceMeta, local: &RebalanceMeta) -> RebalanceMetaMergeOutcome {
    if remote.id.is_empty() {
        *remote = local.clone();
        return RebalanceMetaMergeOutcome::Replaced;
    }

    if !local.id.is_empty() && remote.id != local.id {
        if is_rebalance_meta_replaceable_for_new_id(remote) {
            *remote = local.clone();
            return RebalanceMetaMergeOutcome::Replaced;
        }
        return RebalanceMetaMergeOutcome::RejectedActiveConflict;
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

    RebalanceMetaMergeOutcome::Merged
}

pub(super) fn mark_started_rebalance_pools_stopped(meta: &mut RebalanceMeta, stop_time: OffsetDateTime) {
    for pool_stat in meta.pool_stats.iter_mut() {
        if pool_stat.info.status == RebalStatus::Started {
            pool_stat.info.status = RebalStatus::Stopped;
            pool_stat.info.stopping = false;
            pool_stat.info.end_time.get_or_insert(stop_time);
            if !is_rebalance_stop_propagation_error(pool_stat.info.last_error.as_deref()) {
                pool_stat.info.last_error = None;
            }
        }
    }
}

pub(super) fn mark_started_rebalance_pools_stopping(meta: &mut RebalanceMeta) {
    for pool_stat in meta.pool_stats.iter_mut() {
        if pool_stat.info.status == RebalStatus::Started {
            pool_stat.info.stopping = true;
            pool_stat.info.last_error = None;
        }
    }
}

pub(super) fn apply_stopped_at(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    meta.stopped_at.get_or_insert(now);
    mark_started_rebalance_pools_stopping(meta);
}

pub(super) fn clear_rebalance_cancel_token(meta: Option<&mut RebalanceMeta>) -> bool {
    let Some(meta) = meta else {
        return false;
    };
    if let Some(cancel_tx) = meta.cancel.take() {
        cancel_tx.cancel();
        return true;
    }
    false
}

pub(super) fn stop_rebalance_state(meta: &mut RebalanceMeta, now: OffsetDateTime) {
    clear_rebalance_cancel_token(Some(meta));
    if meta.stopped_at.is_none() && is_rebalance_in_progress(meta) {
        apply_stopped_at(meta, now);
    } else if meta.stopped_at.is_some() {
        mark_started_rebalance_pools_stopping(meta);
    }
}

pub(super) fn stop_rebalance_meta_snapshot_for_id(
    meta: Option<&mut RebalanceMeta>,
    now: OffsetDateTime,
    expected_id: Option<&str>,
) -> Result<Option<RebalanceMeta>> {
    let Some(meta) = meta else {
        return Ok(None);
    };

    if let Some(expected_id) = expected_id
        && !expected_id.is_empty()
        && meta.id != expected_id
    {
        return Err(Error::other(format!(
            "rebalance stop id mismatch: expected {expected_id}, found {}",
            meta.id
        )));
    }

    stop_rebalance_state(meta, now);
    meta.last_refreshed_at = Some(now);
    Ok(Some(meta.clone()))
}

pub(super) fn rollback_rebalance_start_meta_snapshot_for_id(
    meta: Option<&mut RebalanceMeta>,
    now: OffsetDateTime,
    expected_id: Option<&str>,
    start_error: String,
) -> Option<RebalanceMeta> {
    meta.and_then(|meta| {
        if let Some(expected_id) = expected_id
            && !expected_id.is_empty()
            && meta.id != expected_id
        {
            return None;
        }

        clear_rebalance_cancel_token(Some(meta));
        meta.stopped_at.get_or_insert(now);
        meta.last_refreshed_at = Some(now);
        for pool_stat in meta.pool_stats.iter_mut() {
            if pool_stat.info.status == RebalStatus::Started {
                pool_stat.info.status = RebalStatus::Failed;
                pool_stat.info.stopping = false;
                pool_stat.info.end_time.get_or_insert(now);
                pool_stat.info.last_error = Some(start_error.clone());
            }
        }
        Some(meta.clone())
    })
}

pub(super) fn stop_rebalance_meta_snapshot(meta: Option<&mut RebalanceMeta>, now: OffsetDateTime) -> Option<RebalanceMeta> {
    let meta = meta?;
    stop_rebalance_state(meta, now);
    meta.last_refreshed_at = Some(now);
    Some(meta.clone())
}

pub(super) fn record_rebalance_stop_propagation_snapshot(
    meta: Option<&mut RebalanceMeta>,
    encoded_error: String,
    now: OffsetDateTime,
) -> Option<RebalanceMeta> {
    meta.map(|meta| {
        for pool_stat in meta.pool_stats.iter_mut() {
            if pool_stat.participating || pool_stat.info.stopping || pool_stat.info.status == RebalStatus::Stopped {
                pool_stat.info.last_error = Some(encoded_error.clone());
            }
        }
        meta.last_refreshed_at = Some(now);
        meta.clone()
    })
}
