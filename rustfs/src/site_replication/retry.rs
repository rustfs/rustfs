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

use super::*;

pub(crate) const SITE_REPLICATION_RETRY_QUEUE_LIMIT: usize = 256;

pub(crate) const SITE_REPLICATION_RETRY_FAILED_AFTER: u32 = 3;

pub(crate) const SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH: &str = "internal:endpoint-target-refresh";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SiteReplicationRetryEvent {
    pub(crate) id: String,
    pub(crate) peer_deployment_id: String,
    pub(crate) peer_endpoint: String,
    pub(crate) path: String,
    pub(crate) retry_count: u32,
    pub(crate) failed: bool,
    pub(crate) last_error: String,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<OffsetDateTime>,
    /// Peer-edit generation whose delivery failed, when the failing send
    /// carried one. Settling a *later* success for the same (peer, path) must
    /// not erase a failure recorded for a NEWER generation — see
    /// [`settle_site_replication_retry_events`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) edit_generation: Option<u64>,
}

pub(crate) fn retry_event_matches(event: &SiteReplicationRetryEvent, peer: &PeerInfo, path: &str) -> bool {
    (event.peer_deployment_id == peer.deployment_id || event.peer_endpoint == peer.endpoint) && event.path == path
}

pub(crate) const SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH: &str = "internal:retry-snapshot:iam";

pub(crate) const SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH: &str = "internal:retry-snapshot:bucket-metadata";

pub(crate) fn collapsed_retry_queue_path(path: &str) -> Option<&'static str> {
    let base_path = path.split_once('?').map(|(base, _)| base).unwrap_or(path);
    match base_path {
        "/rustfs/admin/v3/site-replication/peer/iam-item" | SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH => {
            Some(SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH)
        }
        "/rustfs/admin/v3/site-replication/peer/bucket-meta" | SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH => {
            Some(SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH)
        }
        _ => None,
    }
}

pub(crate) fn normalize_collapsed_retry_queue_paths(queue: &mut Vec<SiteReplicationRetryEvent>) -> bool {
    let mut changed = false;
    let mut normalized: Vec<SiteReplicationRetryEvent> = Vec::with_capacity(queue.len());
    for mut event in queue.drain(..) {
        if let Some(path) = collapsed_retry_queue_path(&event.path)
            && event.path != path
        {
            event.path = path.to_string();
            changed = true;
        }

        let duplicate = normalized.iter().position(|existing| {
            existing.path == event.path
                && (existing.peer_deployment_id == event.peer_deployment_id || existing.peer_endpoint == event.peer_endpoint)
        });
        let Some(index) = duplicate else {
            normalized.push(event);
            continue;
        };

        changed = true;
        let existing = &mut normalized[index];
        let event_is_newer = match (event.updated_at, existing.updated_at) {
            (Some(event), Some(existing)) => event >= existing,
            (Some(_), None) => true,
            _ => false,
        };
        if event_is_newer {
            let retry_count = existing.retry_count.max(event.retry_count);
            *existing = event;
            existing.retry_count = retry_count;
        } else {
            existing.retry_count = existing.retry_count.max(event.retry_count);
        }
        existing.failed = existing.retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER;
    }
    *queue = normalized;
    changed
}

pub(crate) async fn migrate_collapsed_retry_queue_paths() -> S3Result<()> {
    update_site_replication_state_when_changed(|state| {
        Ok(if normalize_collapsed_retry_queue_paths(&mut state.retry_queue) {
            StateCommit::Changed(())
        } else {
            StateCommit::Unchanged(())
        })
    })
    .await
}

#[cfg(test)]
pub(crate) fn dequeue_site_replication_retry_events(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
) -> usize {
    settle_site_replication_retry_events(queue, peer, path, None)
}

/// Repair-path settlement: also clears snapshot-escalated entries. Running a
/// repair is the operator's explicit accountability transfer for the
/// possibly-unreplayed deletion the marker records; ordinary delivery
/// successes must not clear it (see [`settle_site_replication_retry_events`]).
pub(crate) fn dequeue_site_replication_retry_events_including_escalated(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
) -> usize {
    let before = queue.len();
    let collapsed_path = collapsed_retry_queue_path(path);
    queue.retain(|event| {
        !retry_event_matches(event, peer, path)
            && !collapsed_path.is_some_and(|collapsed_path| retry_event_matches(event, peer, collapsed_path))
    });
    before.saturating_sub(queue.len())
}

/// Remove the retry events for (peer, path) that `generation` is entitled to
/// settle. A successful delivery only proves the peer reached the state the
/// delivery carried: while it was in flight another edit can commit, fail its
/// own delivery, and enqueue for the same (peer, path). Erasing that event
/// would leave the peer on the older edit with no retry left, so an event
/// stamped with a NEWER generation survives. `None` settles unconditionally —
/// the broadcast paths that carry no generation, whose retry events live under
/// their own paths and never collide with peer-edit deliveries.
pub(crate) fn settle_site_replication_retry_events(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
    generation: Option<u64>,
) -> usize {
    let before = queue.len();
    let collapsed_path = collapsed_retry_queue_path(path);
    queue.retain(|event| {
        if !retry_event_matches(event, peer, path) {
            return true;
        }
        // A wire-path success identifies no IAM or bucket-metadata entity.
        // This also protects legacy rows until the startup migration moves
        // them under their internal snapshot path.
        if collapsed_path.is_some() {
            return true;
        }
        // A snapshot-escalated entry records a possibly-unreplayed deletion.
        // Collapsed paths are shared by every entity, so a later successful
        // delivery of a DIFFERENT item proves nothing about the deleted one —
        // only a repair settles it (dequeue_..._including_escalated).
        if event.last_error == SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER {
            return true;
        }
        match (generation, event.edit_generation) {
            (Some(settled), Some(failed)) => failed > settled,
            _ => false,
        }
    });
    before.saturating_sub(queue.len())
}

pub(crate) fn upsert_site_replication_retry_event(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
    error: &str,
    generation: Option<u64>,
) {
    let path = collapsed_retry_queue_path(path).unwrap_or(path);
    let now = OffsetDateTime::now_utc();
    let detail = summarize_peer_error_detail(error);
    if let Some(event) = queue.iter_mut().find(|event| retry_event_matches(event, peer, path)) {
        event.retry_count = event.retry_count.saturating_add(1);
        event.failed = event.retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER;
        event.last_error = detail;
        event.updated_at = Some(now);
        // Keep the newest generation: an older delivery that fails afterwards
        // must not lower the fence and let its own success settle the event.
        event.edit_generation = event.edit_generation.max(generation);
        return;
    }

    queue.push(SiteReplicationRetryEvent {
        id: Uuid::new_v4().to_string(),
        peer_deployment_id: peer.deployment_id.clone(),
        peer_endpoint: peer.endpoint.clone(),
        path: path.to_string(),
        retry_count: 1,
        failed: false,
        last_error: detail,
        updated_at: Some(now),
        edit_generation: generation,
    });
    if queue.len() > SITE_REPLICATION_RETRY_QUEUE_LIMIT {
        let overflow = queue.len() - SITE_REPLICATION_RETRY_QUEUE_LIMIT;
        queue.drain(0..overflow);
    }
}

pub(crate) fn retry_stats_for_state(state: &SiteReplicationState) -> Option<SRRetryStats> {
    if state.retry_queue.is_empty() {
        return None;
    }

    Some(SRRetryStats {
        pending: state.retry_queue.iter().filter(|event| !event.failed).count(),
        failed: state.retry_queue.iter().filter(|event| event.failed).count(),
        last_error: state
            .retry_queue
            .iter()
            .rev()
            .find_map(|event| (!event.last_error.is_empty()).then(|| event.last_error.clone()))
            .unwrap_or_default(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    })
}

pub(crate) async fn enqueue_site_replication_retry_event(peer: &PeerInfo, path: &str, error: &S3Error) {
    enqueue_site_replication_retry_event_for_generation(peer, path, error, None).await
}

pub(crate) async fn enqueue_site_replication_retry_event_for_generation(
    peer: &PeerInfo,
    path: &str,
    error: &S3Error,
    generation: Option<u64>,
) {
    let peer_owned = peer.clone();
    let path_owned = path.to_string();
    let error_text = error.to_string();
    let result = update_site_replication_state(move |state| {
        // A peer that left the state can never drain its entries again
        // (remove_sites already pruned them); recording a late failure for it
        // would only pollute retry_stats until the queue cap evicts it.
        if state.peers.contains_key(&peer_owned.deployment_id) {
            upsert_site_replication_retry_event(&mut state.retry_queue, &peer_owned, &path_owned, &error_text, generation);
        }
        Ok(())
    })
    .await;

    if let Err(err) = result {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            peer = %peer.endpoint,
            path,
            error = ?err,
            "failed to persist site replication retry event"
        );
    }
}

pub(crate) fn retry_bucket_operation(path: &str) -> Option<String> {
    let (base_path, query) = path.split_once('?')?;
    if base_path != SITE_REPLICATION_PEER_BUCKET_OPS_PATH {
        return None;
    }

    form_urlencoded::parse(query.as_bytes()).find_map(|(key, value)| (key == "operation").then(|| value.into_owned()))
}

pub(crate) fn retry_event_replayed_by_bootstrap(event: &SiteReplicationRetryEvent) -> bool {
    matches!(
        retry_bucket_operation(&event.path).as_deref(),
        Some(SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING | SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION)
    )
}

/// Exponential backoff base for the background retry drain, aligned with the
/// reconcile cadence (`site_replication_reconcile::RECONCILE_INTERVAL`).
pub(crate) const SITE_REPLICATION_RETRY_DRAIN_BASE_BACKOFF_SECS: i64 = 600;

/// Backoff ceiling: a permanently failed peer is still probed daily.
pub(crate) const SITE_REPLICATION_RETRY_DRAIN_MAX_BACKOFF_SECS: i64 = 86_400;

/// What the background drain may do for one retry event. Everything not
/// representable here is operator territory (manual repair).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RetryDrainAction {
    /// Constant-path IAM item deliveries collapse into one queue entry per
    /// peer and their bodies are not persisted; the only faithful replay is
    /// the current IAM snapshot from the bootstrap plan.
    IamSnapshot,
    /// Same collapse for bucket-meta deliveries: replay the bucket metadata
    /// snapshot from the bootstrap plan.
    BucketMetadataSnapshot,
    /// A self-contained bucket op the bootstrap plan can re-derive for its
    /// bucket (`make-with-versioning` / `configure-replication`).
    BucketOpReplay { operation: String, bucket: String },
    /// Re-send the current peer records under a fresh edit generation.
    PeerEdit,
}

#[derive(Clone)]
pub(crate) enum RetrySnapshot {
    Iam(Vec<SRIAMItem>),
    BucketMetadata(Vec<SRBucketMeta>),
}

impl RetrySnapshot {
    pub(crate) fn from_plan(action: &RetryDrainAction, plan: &SiteReplicationBootstrapPlan) -> Option<Self> {
        match action {
            RetryDrainAction::IamSnapshot => Some(Self::Iam(plan.iam_items.clone())),
            RetryDrainAction::BucketMetadataSnapshot => Some(Self::BucketMetadata(plan.bucket_items.clone())),
            _ => None,
        }
    }

    pub(crate) fn fingerprint(&self) -> S3Result<Vec<Vec<u8>>> {
        let mut payloads = match self {
            Self::Iam(items) => items.iter().map(serde_json::to_vec).collect::<Result<Vec<_>, _>>(),
            Self::BucketMetadata(items) => items.iter().map(serde_json::to_vec).collect::<Result<Vec<_>, _>>(),
        }
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize retry snapshot failed: {err}")))?;
        payloads.sort_unstable();
        Ok(payloads)
    }

    pub(crate) fn replay_after_change(previous: &Self, fresh: &Self, observed_at: OffsetDateTime) -> Self {
        match (previous, fresh) {
            (Self::Iam(previous), Self::Iam(fresh)) => {
                let fresh_keys: HashSet<IamSnapshotKey> = fresh.iter().filter_map(iam_snapshot_key).collect();
                let mut replay = fresh.clone();
                for item in previous {
                    if iam_snapshot_key(item).is_some_and(|key| !fresh_keys.contains(&key)) {
                        replay.extend(iam_snapshot_tombstones(item, observed_at));
                    }
                }
                Self::Iam(replay)
            }
            (Self::BucketMetadata(previous), Self::BucketMetadata(fresh)) => {
                let fresh_keys: HashSet<(&str, &str)> = fresh
                    .iter()
                    .map(|item| (item.bucket.as_str(), item.r#type.as_str()))
                    .collect();
                let mut replay = fresh.clone();
                for item in previous {
                    if !fresh_keys.contains(&(item.bucket.as_str(), item.r#type.as_str())) {
                        replay.push(bucket_metadata_snapshot_tombstone(item, observed_at));
                    }
                }
                Self::BucketMetadata(replay)
            }
            _ => fresh.clone(),
        }
    }

    pub(crate) async fn send(&self, transport: &PeerTransport, access_key: &str, secret_key: &str) -> S3Result<()> {
        match self {
            Self::Iam(items) => {
                for item in items {
                    SiteReplicationRepairTask::Iam(item)
                        .send(transport, access_key, secret_key)
                        .await?;
                }
            }
            Self::BucketMetadata(items) => {
                for item in items {
                    SiteReplicationRepairTask::BucketMetadata(item)
                        .send(transport, access_key, secret_key)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Hash, PartialEq, Eq)]
pub(crate) enum IamSnapshotKey {
    Policy(String),
    User(String),
    Group(String),
    PolicyMapping { target: String, user_type: i64, is_group: bool },
}

pub(crate) fn iam_snapshot_key(item: &SRIAMItem) -> Option<IamSnapshotKey> {
    match item.r#type.as_str() {
        "policy" => Some(IamSnapshotKey::Policy(item.name.clone())),
        "iam-user" => item
            .iam_user
            .as_ref()
            .map(|user| IamSnapshotKey::User(user.access_key.clone())),
        "group-info" => item
            .group_info
            .as_ref()
            .map(|group| IamSnapshotKey::Group(group.update_req.group.clone())),
        "policy-mapping" => item.policy_mapping.as_ref().map(|mapping| IamSnapshotKey::PolicyMapping {
            target: mapping.user_or_group.clone(),
            user_type: mapping.user_type,
            is_group: mapping.is_group,
        }),
        _ => None,
    }
}

pub(crate) fn iam_snapshot_tombstones(item: &SRIAMItem, observed_at: OffsetDateTime) -> Vec<SRIAMItem> {
    let mut tombstone = item.clone();
    tombstone.updated_at = Some(observed_at);
    match item.r#type.as_str() {
        "policy" => tombstone.policy = None,
        "iam-user" => {
            if let Some(user) = tombstone.iam_user.as_mut() {
                user.is_delete_req = true;
                user.user_req = None;
            }
        }
        "group-info" => {
            let Some(group) = tombstone.group_info.as_mut() else {
                return Vec::new();
            };
            group.update_req.is_remove = true;
            if group.update_req.members.is_empty() {
                return vec![tombstone];
            }
            let mut delete = tombstone.clone();
            if let Some(group) = delete.group_info.as_mut() {
                group.update_req.members.clear();
            }
            return vec![tombstone, delete];
        }
        "policy-mapping" => {
            if let Some(mapping) = tombstone.policy_mapping.as_mut() {
                mapping.policy.clear();
            }
        }
        _ => return Vec::new(),
    }
    vec![tombstone]
}

pub(crate) fn bucket_metadata_snapshot_tombstone(item: &SRBucketMeta, observed_at: OffsetDateTime) -> SRBucketMeta {
    SRBucketMeta {
        r#type: item.r#type.clone(),
        bucket: item.bucket.clone(),
        updated_at: Some(observed_at),
        expiry_updated_at: Some(observed_at),
        api_version: item.api_version.clone(),
        derived_rule_contract: item.derived_rule_contract,
        ..Default::default()
    }
}

pub(crate) const SITE_REPLICATION_RETRY_SNAPSHOT_STABILITY_ATTEMPTS: usize = 3;

pub(crate) fn classify_site_replication_retry_event(event: &SiteReplicationRetryEvent) -> Option<RetryDrainAction> {
    let snapshot_action = match event.path.as_str() {
        SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH => Some(RetryDrainAction::IamSnapshot),
        SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH => Some(RetryDrainAction::BucketMetadataSnapshot),
        _ => None,
    };
    if snapshot_action.is_some() && event.last_error != SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER {
        return snapshot_action;
    }
    if event.path.starts_with("internal:") {
        // Marker records store payloads in `last_error` (legacy
        // pending-endpoint-refresh backup and snapshot liabilities); they are
        // not drainable delivery failures.
        return None;
    }
    if event.last_error == SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER {
        // Already snapshot-replayed once for this failure episode; a possible
        // deletion cannot be replayed from a snapshot, so re-sending daily
        // proves nothing. A new hook failure overwrites the marker.
        return None;
    }
    let base_path = event.path.split_once('?').map(|(base, _)| base).unwrap_or(&event.path);
    match base_path {
        "/rustfs/admin/v3/site-replication/peer/iam-item" => Some(RetryDrainAction::IamSnapshot),
        "/rustfs/admin/v3/site-replication/peer/bucket-meta" => Some(RetryDrainAction::BucketMetadataSnapshot),
        SITE_REPLICATION_PEER_EDIT_PATH => Some(RetryDrainAction::PeerEdit),
        SITE_REPLICATION_PEER_BUCKET_OPS_PATH => {
            let operation = retry_bucket_operation(&event.path)?;
            if !matches!(
                operation.as_str(),
                SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING | SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION
            ) {
                // Destructive ops (delete-bucket / force-delete-bucket) are
                // operator territory: replaying them against a peer whose
                // bucket was since recreated is irreversible.
                return None;
            }
            let bucket = retry_bucket_name(&event.path)?;
            Some(RetryDrainAction::BucketOpReplay { operation, bucket })
        }
        _ => None,
    }
}

pub(crate) fn retry_bucket_name(path: &str) -> Option<String> {
    let (_, query) = path.split_once('?')?;
    form_urlencoded::parse(query.as_bytes())
        .find_map(|(key, value)| (key == "bucket" && !value.is_empty()).then(|| value.into_owned()))
}

/// A collapsed retry event after a stable snapshot resend is escalated with
/// this marker instead of being cleared: the snapshot contains no task for a
/// failed deletion, so remote absence remains operator-visible. Collapsed
/// failures use an internal queue path so ordinary successes and older nodes
/// cannot settle an unrelated entity's liability.
pub(crate) const SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER: &str = "snapshot replayed; a failed deletion cannot be replayed from a snapshot — run site replication repair or re-deliver to settle";

/// Escalate a collapsed retry event after its snapshot resend succeeded,
/// unless a newer failure was recorded after `snapshot_updated_at` (that
/// failure belongs to a newer local commit the snapshot did not contain and
/// must keep the entry drain-eligible).
pub(crate) fn escalate_site_replication_retry_events_up_to(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
    snapshot_updated_at: Option<OffsetDateTime>,
) -> usize {
    let Some(marker_path) = collapsed_retry_queue_path(path) else {
        return 0;
    };

    if path != marker_path {
        queue.retain(|event| {
            if !retry_event_matches(event, peer, path) {
                return true;
            }
            matches!((event.updated_at, snapshot_updated_at), (Some(current), Some(seen)) if current > seen)
                || matches!((event.updated_at, snapshot_updated_at), (Some(_), None))
        });
    }

    let marker_index = queue.iter().position(|event| retry_event_matches(event, peer, marker_path));
    let marker_index = marker_index.unwrap_or_else(|| {
        queue.push(SiteReplicationRetryEvent {
            id: Uuid::new_v4().to_string(),
            peer_deployment_id: peer.deployment_id.clone(),
            peer_endpoint: peer.endpoint.clone(),
            path: marker_path.to_string(),
            updated_at: snapshot_updated_at,
            ..Default::default()
        });
        queue.len() - 1
    });
    let event = &mut queue[marker_index];
    let newer_failure_recorded = match (event.updated_at, snapshot_updated_at) {
        (Some(current), Some(seen)) => current > seen,
        (Some(_), None) => true,
        (None, _) => false,
    };
    if newer_failure_recorded && event.last_error != SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER {
        return 0;
    }
    event.failed = true;
    event.retry_count = event.retry_count.max(SITE_REPLICATION_RETRY_FAILED_AFTER);
    event.last_error = SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER.to_string();
    event.updated_at = Some(OffsetDateTime::now_utc());
    1
}

pub(crate) async fn escalate_site_replication_retry_event_up_to(
    peer: &PeerInfo,
    path: &str,
    snapshot_updated_at: Option<OffsetDateTime>,
) {
    let peer_owned = peer.clone();
    let path_owned = path.to_string();
    let result = update_site_replication_state(move |state| {
        escalate_site_replication_retry_events_up_to(&mut state.retry_queue, &peer_owned, &path_owned, snapshot_updated_at);
        Ok(())
    })
    .await;

    if let Err(err) = result {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            peer = %peer.endpoint,
            deployment_id = %peer.deployment_id,
            path,
            error = ?err,
            "failed to escalate site replication retry event"
        );
    }
}

/// Whether the drain may attempt this event now.
pub(crate) fn site_replication_retry_backoff_elapsed(event: &SiteReplicationRetryEvent, now: OffsetDateTime) -> bool {
    let Some(updated_at) = event.updated_at else {
        return true;
    };
    // 600 * 2^8 already exceeds the daily ceiling; capping the shift keeps
    // the arithmetic overflow-free for any persisted retry_count.
    let exponent = event.retry_count.saturating_sub(1).min(8);
    let delay = (SITE_REPLICATION_RETRY_DRAIN_BASE_BACKOFF_SECS << exponent).min(SITE_REPLICATION_RETRY_DRAIN_MAX_BACKOFF_SECS);
    now.unix_timestamp().saturating_sub(updated_at.unix_timestamp()) >= delay
}

/// The subset of the retry queue the background drain is allowed to touch.
pub(crate) fn actionable_site_replication_retry_events(
    state: &SiteReplicationState,
    now: OffsetDateTime,
) -> Vec<SiteReplicationRetryEvent> {
    state
        .retry_queue
        .iter()
        .filter(|event| classify_site_replication_retry_event(event).is_some())
        .filter(|event| state.peers.contains_key(&event.peer_deployment_id))
        .filter(|event| site_replication_retry_backoff_elapsed(event, now))
        .cloned()
        .collect()
}

/// Background consumer for the retry queue, run from the reconcile tick.
///
/// Scope: this settles "delivered once and failed" entries whose replay is
/// faithful (bucket ops, peer edits). Collapsed iam-item / bucket-meta
/// entries are snapshot-resent and then *escalated*, not cleared — a failed
/// deletion leaves no task in the snapshot, so remote absence stays unproven
/// until a later delivery or a manual repair. A hook that never fired (crash
/// between the local commit and the send) leaves no entry at all, so the
/// drain is not a full cross-site diff-heal; manual repair remains the
/// authoritative catch-all.
pub(crate) async fn drain_site_replication_retry_queue() {
    if let Err(err) = drain_site_replication_retry_queue_inner().await {
        warn!(
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            result = "retry_drain_failed",
            error = ?err,
            "admin site replication state"
        );
    }
}

pub(crate) async fn drain_site_replication_retry_queue_inner() -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    let actionable = actionable_site_replication_retry_events(&runtime.state, OffsetDateTime::now_utc());
    if actionable.is_empty() {
        return Ok(());
    }
    let Some(store) = current_object_store_handle() else {
        return Ok(());
    };
    if runtime.state.pending_endpoint_refresh.is_some()
        || runtime.state.pending_remove.is_some()
        || runtime.state.pending_rotation.is_some()
    {
        // The tick-level gate ran before the reconcilers; a multi-step flow
        // (endpoint refresh commits its pending marker without the lifecycle
        // guard) may have started since. Re-check on the fresh state.
        return Ok(());
    }
    // Serialize against operator repair execution. This does NOT close the
    // dry-run -> execute window (dry-run takes no lock): a drain settling a
    // replayable bucket-op entry in that window changes the preflight token
    // and execute fails safe with "preflight is stale" — the operator
    // re-runs the dry-run. Lock order matches repair: lifecycle guard (held
    // by the reconcile tick) -> repair execution lock -> state object lock
    // inside the send bookkeeping. An operator repair holding the lock makes
    // this tick skip after the lock-acquire timeout.
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        drain_site_replication_retry_queue_locked(runtime, actionable).await
    })
    .await
    .map_err(ApiError::from)?
}

pub(crate) async fn drain_site_replication_retry_queue_locked(
    runtime: SiteReplicationRuntime,
    events: Vec<SiteReplicationRetryEvent>,
) -> S3Result<()> {
    let needs_plan = events
        .iter()
        .any(|event| !matches!(classify_site_replication_retry_event(event), Some(RetryDrainAction::PeerEdit)));
    // The plan is a full local snapshot (buckets + IAM); build it once per
    // tick and only when a snapshot resend is actually due.
    let plan = if needs_plan {
        let info = build_sr_info(&runtime.state, &runtime.local_peer).await?;
        Some(site_replication_bootstrap_plan(&info)?)
    } else {
        None
    };

    let mut events_by_peer: BTreeMap<String, Vec<SiteReplicationRetryEvent>> = BTreeMap::new();
    for event in events {
        events_by_peer
            .entry(event.peer_deployment_id.clone())
            .or_default()
            .push(event);
    }

    let mut settled = 0usize;
    let mut failures = 0usize;
    for (deployment_id, peer_events) in events_by_peer {
        let Some(peer) = runtime.state.peers.get(&deployment_id) else {
            continue;
        };
        if deployment_id == runtime.local_peer.deployment_id
            || same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
        {
            continue;
        }
        let transport = match PeerTransport::for_runtime_peer(peer).await {
            Ok(transport) => transport,
            Err(err) => {
                // Record the attempt so backoff advances for an unreachable
                // peer instead of re-dialing it every tick.
                for event in &peer_events {
                    enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                }
                failures += peer_events.len();
                continue;
            }
        };
        for event in peer_events {
            let Some(action) = classify_site_replication_retry_event(&event) else {
                continue;
            };
            match drain_one_site_replication_retry_event(&runtime, peer, &transport, &event, action, plan.as_ref()).await {
                Ok(true) => settled += 1,
                Ok(false) => {}
                Err(_) => failures += 1,
            }
        }
    }

    if settled > 0 || failures > 0 {
        info!(
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            result = "retry_drain_settled",
            settled,
            failures,
            "admin site replication state"
        );
    }
    Ok(())
}

/// Replay one retry event against its peer. Returns `Ok(true)` when the
/// event was settled (delivered, or provably stale), `Ok(false)` when it was
/// skipped, and `Err` after a failed delivery (already re-queued with an
/// incremented retry count).
pub(crate) async fn drain_one_site_replication_retry_event(
    runtime: &SiteReplicationRuntime,
    peer: &PeerInfo,
    transport: &PeerTransport,
    event: &SiteReplicationRetryEvent,
    action: RetryDrainAction,
    plan: Option<&SiteReplicationBootstrapPlan>,
) -> S3Result<bool> {
    let access_key = &runtime.state.service_account_access_key;
    let secret_key = &runtime.service_account_secret_key;
    match action.clone() {
        RetryDrainAction::IamSnapshot | RetryDrainAction::BucketMetadataSnapshot => {
            let Some(plan) = plan else {
                return Ok(false);
            };
            let mut current_snapshot = RetrySnapshot::from_plan(&action, plan).expect("snapshot action has a snapshot");
            let mut replay = current_snapshot.clone();
            for _ in 0..SITE_REPLICATION_RETRY_SNAPSHOT_STABILITY_ATTEMPTS {
                let current_fingerprint = current_snapshot.fingerprint()?;
                if let Err(err) = replay.send(transport, access_key, secret_key).await {
                    enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                    return Err(err);
                }
                let fresh_info = build_sr_info(&runtime.state, &runtime.local_peer).await?;
                let fresh_plan = site_replication_bootstrap_plan(&fresh_info)?;
                let fresh_snapshot = RetrySnapshot::from_plan(&action, &fresh_plan).expect("snapshot action has a snapshot");
                if fresh_snapshot.fingerprint()? == current_fingerprint {
                    escalate_site_replication_retry_event_up_to(peer, &event.path, event.updated_at).await;
                    return Ok(true);
                }
                replay = RetrySnapshot::replay_after_change(&current_snapshot, &fresh_snapshot, OffsetDateTime::now_utc());
                current_snapshot = fresh_snapshot;
            }
            Ok(false)
        }
        RetryDrainAction::BucketOpReplay { operation, bucket } => {
            let Some(plan) = plan else {
                return Ok(false);
            };
            // Replay from the CURRENT plan, never the recorded path: the
            // recorded query can carry an expired one-shot bootstrap token or
            // a stale createdAt.
            let make_op = operation == SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING;
            let paths = if make_op {
                &plan.bucket_make_ops
            } else {
                &plan.bucket_configure_ops
            };
            let tasks: Vec<SiteReplicationRepairTask<'_>> = paths
                .iter()
                .filter(|path| retry_bucket_name(path).as_deref() == Some(bucket.as_str()))
                .map(|path| {
                    if make_op {
                        SiteReplicationRepairTask::BucketMake(path)
                    } else {
                        SiteReplicationRepairTask::Replication(path)
                    }
                })
                .collect();
            if tasks.is_empty() {
                // The bucket left the plan (deleted, or replication no longer
                // configured): the recorded intent is stale, settle it.
                dequeue_site_replication_retry_event(peer, &event.path).await;
                return Ok(true);
            }
            for task in &tasks {
                if let Err(err) = task.send(transport, access_key, secret_key).await {
                    enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                    return Err(err);
                }
            }
            dequeue_site_replication_retry_event(peer, &event.path).await;
            Ok(true)
        }
        RetryDrainAction::PeerEdit => {
            // The recorded generation is stale by definition — the receiver
            // fences it. Allocate a fresh generation and re-send the current
            // peer records (a superset of the failed body; the receiver
            // upserts), all inside one state transaction so the fence and the
            // bodies agree.
            let target_id = peer.deployment_id.clone();
            let (generation, bodies) = update_site_replication_state(move |state| {
                if !state.peers.contains_key(&target_id) {
                    return Ok((None, Vec::new()));
                }
                Ok((Some(next_peer_edit_generation(state)), state.peers.values().cloned().collect::<Vec<_>>()))
            })
            .await?;
            let Some(generation) = generation else {
                // Peer left between the snapshot and now; the queue entry was
                // already pruned by remove_sites.
                return Ok(false);
            };
            let local_deployment_id = Some(runtime.local_peer.deployment_id.as_str()).filter(|id| !id.is_empty());
            let edit_path = peer_edit_path_with_fence(local_deployment_id, generation);
            let delivery_fence = local_deployment_id.is_some().then_some(generation);
            for body in &bodies {
                if let Err(err) = PeerAdminRequest::put(&transport.connection, &edit_path, access_key)
                    .with_client(&transport.client)
                    .send(secret_key, body)
                    .await
                {
                    enqueue_site_replication_retry_event_for_generation(
                        peer,
                        SITE_REPLICATION_PEER_EDIT_PATH,
                        &err,
                        delivery_fence,
                    )
                    .await;
                    return Err(err);
                }
            }
            dequeue_site_replication_retry_event_for_generation(peer, SITE_REPLICATION_PEER_EDIT_PATH, delivery_fence).await;
            Ok(true)
        }
    }
}

/// Remove a retry event for (peer, path) from the queue on successful delivery.
/// This is a no-op (load + no-op persist skipped) when no matching entry exists,
/// avoiding unnecessary I/O on the common path.
pub(crate) async fn dequeue_site_replication_retry_event(peer: &PeerInfo, path: &str) {
    dequeue_site_replication_retry_event_for_generation(peer, path, None).await
}

pub(crate) async fn dequeue_site_replication_retry_event_for_generation(peer: &PeerInfo, path: &str, generation: Option<u64>) {
    let result = async {
        // Fast path: this sits on every successful hook broadcast, so probe
        // with a plain read first and only enter the locked RMW on a hit
        // (the transaction re-checks under the lock).
        let mut probe = load_site_replication_state().await?;
        if settle_site_replication_retry_events(&mut probe.retry_queue, peer, path, generation) == 0 {
            return Ok(());
        }
        let peer_owned = peer.clone();
        let path_owned = path.to_string();
        update_site_replication_state(move |state| {
            settle_site_replication_retry_events(&mut state.retry_queue, &peer_owned, &path_owned, generation);
            Ok(())
        })
        .await?;
        Ok::<_, S3Error>(())
    }
    .await;

    if let Err(err) = result {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            peer = %peer.endpoint,
            deployment_id = %peer.deployment_id,
            path,
            error = ?err,
            "failed to dequeue site replication retry event"
        );
    }
}
