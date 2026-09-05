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
use futures::{StreamExt, stream};

pub(crate) const SITE_REPLICATION_RETRY_QUEUE_LIMIT: usize = 256;

/// Attempts before an entry reports as `failed` in retryStats. Visibility
/// only: a `failed` entry stays drain-eligible, and the reachability probe
/// short-circuits its backoff once the peer answers again — so an early
/// `failed` mark is a timely operator signal, not a dead end.
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
    /// The latest delivery failure happened before an authenticated peer
    /// response was received (connect, DNS, or TLS). Such failures
    /// may bypass the expensive replay backoff only after a cheap devnull
    /// reachability probe proves the peer is back.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) peer_unreachable: bool,
    /// Whether every failure folded into this collapsed IAM entry had its
    /// deletion body (if it was a deletion) recorded in
    /// [`SiteReplicationState::iam_deletion_replays`]. Only then may a
    /// successful deletion replay plus a stable snapshot resend settle the
    /// entry; a legacy entry (or one degraded by record overflow) keeps the
    /// escalation semantics because an unrecorded deletion may hide in it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) deletions_recorded: bool,
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
        // Merged rows may span binaries: a row written without deletion
        // recording taints the merged entry, so only both-recorded merges
        // stay settleable.
        let deletions_recorded = existing.deletions_recorded && event.deletions_recorded;
        if event_is_newer {
            let retry_count = existing.retry_count.max(event.retry_count);
            *existing = event;
            existing.retry_count = retry_count;
        } else {
            existing.retry_count = existing.retry_count.max(event.retry_count);
        }
        existing.deletions_recorded = deletions_recorded;
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

pub(crate) fn settle_observed_site_replication_retry_event(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    observed: &SiteReplicationRetryEvent,
) -> usize {
    let before = queue.len();
    queue.retain(|current| {
        !(retry_event_matches(current, peer, &observed.path)
            && current.id == observed.id
            && current.updated_at == observed.updated_at)
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
    let peer_unreachable = retry_error_indicates_peer_unreachable(error);
    if let Some(event) = queue.iter_mut().find(|event| retry_event_matches(event, peer, path)) {
        // The id is the event revision used by probe promotion and replay
        // settlement. Refresh it on every failure so an older in-flight
        // success can never acknowledge the newer observation.
        event.id = Uuid::new_v4().to_string();
        event.retry_count = event.retry_count.saturating_add(1);
        event.failed = event.retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER;
        event.last_error = detail;
        event.updated_at = Some(now);
        event.peer_unreachable = peer_unreachable;
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
        peer_unreachable,
        deletions_recorded: false,
    });
    if queue.len() > SITE_REPLICATION_RETRY_QUEUE_LIMIT {
        let overflow = queue.len() - SITE_REPLICATION_RETRY_QUEUE_LIMIT;
        queue.drain(0..overflow);
    }
}

pub(crate) fn retry_error_indicates_peer_unreachable(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    let Some((_, request)) = error.split_once("peer request to ") else {
        return false;
    };
    let Some((_, failure)) = request.split_once(" failed ") else {
        return false;
    };
    failure.starts_with("(connect):") || failure.starts_with("(dns resolution):") || failure.starts_with("(tls handshake):")
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

pub(crate) const SITE_REPLICATION_PEER_IAM_ITEM_WIRE_PATH: &str = "/rustfs/admin/v3/site-replication/peer/iam-item";

/// Per-peer cap on recorded deletion bodies. Beyond it the peer's collapsed
/// IAM entry degrades to the escalation semantics (an unrecorded deletion may
/// exist), so the list stays bounded without silently dropping liability.
pub(crate) const SITE_REPLICATION_IAM_DELETION_REPLAY_LIMIT_PER_PEER: usize = 256;

/// One IAM deletion event whose delivery to `peer` failed, kept verbatim so
/// the retry drain can replay it before the snapshot resend. `entity`
/// collapses repeated deletions of the same entity into the newest body.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SiteReplicationIamDeletionReplay {
    pub(crate) id: String,
    pub(crate) peer_deployment_id: String,
    pub(crate) peer_endpoint: String,
    pub(crate) entity: String,
    pub(crate) item: Value,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) recorded_at: Option<OffsetDateTime>,
}

pub(crate) fn iam_deletion_replay_matches(record: &SiteReplicationIamDeletionReplay, peer: &PeerInfo) -> bool {
    record.peer_deployment_id == peer.deployment_id || record.peer_endpoint == peer.endpoint
}

/// The entity a deletion-shaped IAM item removes, or `None` for items that
/// create or update state (those are faithfully replayed by the snapshot
/// resend and need no record). Group member removal keys on the removed
/// member set: two removals from the same group are distinct events, not a
/// newer revision of one another.
pub(crate) fn iam_item_deletion_entity(item: &SRIAMItem) -> Option<String> {
    match item.r#type.as_str() {
        "policy" if item.policy.is_none() => Some(format!("policy:{}", item.name)),
        "iam-user" => item
            .iam_user
            .as_ref()
            .filter(|user| user.is_delete_req)
            .map(|user| format!("iam-user:{}", user.access_key)),
        "group-info" => item
            .group_info
            .as_ref()
            .filter(|group| group.update_req.is_remove)
            .map(|group| {
                let mut members = group.update_req.members.clone();
                members.sort_unstable();
                format!("group-remove:{}:{}", group.update_req.group, members.join(","))
            }),
        "policy-mapping" => item
            .policy_mapping
            .as_ref()
            .filter(|mapping| mapping.policy.is_empty())
            .map(|mapping| format!("policy-mapping:{}:{}:{}", mapping.user_or_group, mapping.user_type, mapping.is_group)),
        "service-account" => item
            .svc_acc_change
            .as_ref()
            .and_then(|change| change.delete.as_ref())
            .map(|delete| format!("svc-acc:{}", delete.access_key)),
        _ => None,
    }
}

/// Failure bookkeeping for one IAM item delivery: upsert the collapsed retry
/// event and, when the item is a deletion, record its body for replay. Both
/// live in the same state so the caller commits them in one transaction — a
/// retry entry can never exist whose deletion body was lost to a separate
/// failed write.
pub(crate) fn record_failed_iam_delivery(state: &mut SiteReplicationState, peer: &PeerInfo, item: &SRIAMItem, error: &str) {
    let existed = state
        .retry_queue
        .iter()
        .any(|event| retry_event_matches(event, peer, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH));
    upsert_site_replication_retry_event(&mut state.retry_queue, peer, SITE_REPLICATION_PEER_IAM_ITEM_WIRE_PATH, error, None);
    if !existed
        && let Some(event) = state
            .retry_queue
            .iter_mut()
            .find(|event| retry_event_matches(event, peer, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH))
    {
        // Fresh entry: every failure it will ever collapse goes through this
        // recording path, so a deletion replay plus a stable snapshot resend
        // can later settle it instead of escalating.
        event.deletions_recorded = true;
    }

    let Some(entity) = iam_item_deletion_entity(item) else {
        return;
    };
    let item_value = match serde_json::to_value(item) {
        Ok(value) => value,
        Err(_) => {
            degrade_iam_retry_event_to_escalation(state, peer);
            return;
        }
    };
    let now = OffsetDateTime::now_utc();
    if let Some(existing) = state
        .iam_deletion_replays
        .iter_mut()
        .find(|record| iam_deletion_replay_matches(record, peer) && record.entity == entity)
    {
        existing.item = item_value;
        existing.recorded_at = Some(now);
        return;
    }

    let per_peer = state
        .iam_deletion_replays
        .iter()
        .filter(|record| iam_deletion_replay_matches(record, peer))
        .count();
    if per_peer >= SITE_REPLICATION_IAM_DELETION_REPLAY_LIMIT_PER_PEER {
        // The record set is no longer complete for this peer; the entry must
        // escalate rather than settle. Drop the oldest record to stay
        // bounded — remaining records are still replayed best-effort.
        degrade_iam_retry_event_to_escalation(state, peer);
        if let Some(oldest) = state
            .iam_deletion_replays
            .iter()
            .enumerate()
            .filter(|(_, record)| iam_deletion_replay_matches(record, peer))
            .min_by_key(|(_, record)| record.recorded_at)
            .map(|(index, _)| index)
        {
            state.iam_deletion_replays.remove(oldest);
        }
    }
    state.iam_deletion_replays.push(SiteReplicationIamDeletionReplay {
        id: Uuid::new_v4().to_string(),
        peer_deployment_id: peer.deployment_id.clone(),
        peer_endpoint: peer.endpoint.clone(),
        entity,
        item: item_value,
        recorded_at: Some(now),
    });
}

pub(crate) fn degrade_iam_retry_event_to_escalation(state: &mut SiteReplicationState, peer: &PeerInfo) {
    if let Some(event) = state
        .retry_queue
        .iter_mut()
        .find(|event| retry_event_matches(event, peer, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH))
    {
        event.deletions_recorded = false;
    }
}

pub(crate) fn iam_deletion_replays_for_peer(
    state: &SiteReplicationState,
    peer: &PeerInfo,
) -> Vec<SiteReplicationIamDeletionReplay> {
    let mut records: Vec<SiteReplicationIamDeletionReplay> = state
        .iam_deletion_replays
        .iter()
        .filter(|record| iam_deletion_replay_matches(record, peer))
        .cloned()
        .collect();
    // Oldest first, so a later deletion of a recreated entity lands after
    // the earlier one.
    records.sort_by_key(|record| record.recorded_at);
    records
}

pub(crate) fn clear_iam_deletion_replays_for_peer(state: &mut SiteReplicationState, peer: &PeerInfo) {
    state
        .iam_deletion_replays
        .retain(|record| !iam_deletion_replay_matches(record, peer));
}

pub(crate) async fn record_failed_site_replication_iam_delivery(peer: &PeerInfo, item: &SRIAMItem, error: &S3Error) {
    let peer_owned = peer.clone();
    let item_owned = item.clone();
    let error_text = error.to_string();
    let deletion_entity = iam_item_deletion_entity(item);
    let result = update_site_replication_state(move |state| {
        // A departed peer can never drain its entries again (remove_sites
        // already pruned them) — mirror enqueue_site_replication_retry_event.
        if state.peers.contains_key(&peer_owned.deployment_id) {
            record_failed_iam_delivery(state, &peer_owned, &item_owned, &error_text);
        }
        Ok(())
    })
    .await;

    match result {
        Ok(()) => {
            if let Some(entity) = deletion_entity {
                warn!(
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    peer = %peer.endpoint,
                    deployment_id = %peer.deployment_id,
                    entity = %entity,
                    result = "iam_deletion_recorded_for_replay",
                    "IAM deletion delivery to peer failed; recorded for retry-drain replay"
                );
            }
        }
        Err(err) => {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                peer = %peer.endpoint,
                deployment_id = %peer.deployment_id,
                error = ?err,
                "failed to persist site replication IAM delivery failure"
            );
        }
    }
}

/// Post-replay settlement for a collapsed IAM entry: remove the replayed
/// deletion records and, when the entry's whole liability is provably
/// replayed (`deletions_recorded` and no residual records), remove the entry.
/// Anything else falls back to the escalation marker, and a failure stamped
/// after `snapshot_updated_at` keeps the entry drain-eligible untouched.
/// Returns whether the entry was fully settled.
pub(crate) fn settle_replayed_iam_retry_events(
    state: &mut SiteReplicationState,
    peer: &PeerInfo,
    path: &str,
    snapshot_updated_at: Option<OffsetDateTime>,
    replayed_record_ids: &[String],
) -> bool {
    state
        .iam_deletion_replays
        .retain(|record| !(iam_deletion_replay_matches(record, peer) && replayed_record_ids.contains(&record.id)));

    if collapsed_retry_queue_path(path) != Some(SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH) {
        return false;
    }
    let Some(index) = state
        .retry_queue
        .iter()
        .position(|event| retry_event_matches(event, peer, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH))
    else {
        return false;
    };
    let event = &state.retry_queue[index];
    let newer_failure_recorded = match (event.updated_at, snapshot_updated_at) {
        (Some(current), Some(seen)) => current > seen,
        (Some(_), None) => true,
        (None, _) => false,
    };
    if newer_failure_recorded && event.last_error != SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER {
        // The newer failure's own deletion (if any) has its own record; the
        // next drain pass replays it.
        return false;
    }
    let residual_records = state
        .iam_deletion_replays
        .iter()
        .any(|record| iam_deletion_replay_matches(record, peer));
    if event.deletions_recorded && !residual_records {
        state.retry_queue.remove(index);
        return true;
    }
    escalate_site_replication_retry_events_up_to(&mut state.retry_queue, peer, path, snapshot_updated_at);
    false
}

pub(crate) async fn settle_replayed_site_replication_iam_retry_event(
    peer: &PeerInfo,
    path: &str,
    snapshot_updated_at: Option<OffsetDateTime>,
    replayed_record_ids: Vec<String>,
) {
    let peer_owned = peer.clone();
    let path_owned = path.to_string();
    let result = update_site_replication_state(move |state| {
        Ok(settle_replayed_iam_retry_events(
            state,
            &peer_owned,
            &path_owned,
            snapshot_updated_at,
            &replayed_record_ids,
        ))
    })
    .await;

    match result {
        Ok(true) => {
            info!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                peer = %peer.endpoint,
                deployment_id = %peer.deployment_id,
                result = "iam_retry_event_settled",
                "recorded IAM deletions replayed and snapshot stable; collapsed retry entry settled"
            );
        }
        Ok(false) => {}
        Err(err) => {
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                peer = %peer.endpoint,
                deployment_id = %peer.deployment_id,
                path,
                error = ?err,
                "failed to settle replayed site replication IAM retry event"
            );
        }
    }
}

/// Drop a deletion record whose body no longer deserializes (it can never be
/// replayed) and degrade the peer's entry to escalation so the liability
/// stays operator-visible instead of silently settling.
pub(crate) async fn drop_corrupt_iam_deletion_replay(peer: &PeerInfo, record_id: &str) {
    let peer_owned = peer.clone();
    let record_id_owned = record_id.to_string();
    let result = update_site_replication_state(move |state| {
        state.iam_deletion_replays.retain(|record| record.id != record_id_owned);
        degrade_iam_retry_event_to_escalation(state, &peer_owned);
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
            record_id,
            error = ?err,
            "failed to drop corrupt site replication IAM deletion record"
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

/// Backoff ceiling for *replay attempts*: a permanently failed peer still
/// gets a full replay daily. Reachability is separately probed every tick
/// ([`promote_reachable_deferred_retry_events`]), so a peer that recovers
/// converges at the next tick instead of waiting out this ceiling.
pub(crate) const SITE_REPLICATION_RETRY_DRAIN_MAX_BACKOFF_SECS: i64 = 86_400;

/// Keep a background drain round below the lifecycle lock's 30-second wait
/// bound. Peer requests may each consume the full 10-second request timeout,
/// so a round may automatically replay only a small, indivisible request
/// chain. Larger snapshots and topology edits remain queued for operator
/// repair instead of monopolizing lifecycle admission.
pub(crate) const SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER: usize = 2;

/// Bound sockets for every retry pass. The lightweight pass also admits at
/// most this many peer request chains per round, bounding its lock hold time.
pub(crate) const SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY: usize = 4;

/// A replay shape the retry machinery can derive from persisted state. The
/// lightweight 30-second scheduler admits only bounded bucket-op chains;
/// snapshot and topology-wide work remains operator territory.
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

pub(crate) fn is_lightweight_retry_drain_action(action: &RetryDrainAction) -> bool {
    matches!(action, RetryDrainAction::BucketOpReplay { .. })
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

    pub(crate) async fn send(
        &self,
        peer: &PeerInfo,
        transport: &PeerTransport,
        access_key: &str,
        secret_key: &str,
    ) -> S3Result<bool> {
        match self {
            Self::Iam(items) => {
                for item in items {
                    if !send_retry_task_if_peer_current(
                        peer,
                        &SiteReplicationRepairTask::Iam(item),
                        transport,
                        access_key,
                        secret_key,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                }
            }
            Self::BucketMetadata(items) => {
                for item in items {
                    if !send_retry_task_if_peer_current(
                        peer,
                        &SiteReplicationRepairTask::BucketMetadata(item),
                        transport,
                        access_key,
                        secret_key,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}

pub(crate) async fn send_retry_task_if_peer_current(
    peer: &PeerInfo,
    task: &SiteReplicationRepairTask<'_>,
    transport: &PeerTransport,
    access_key: &str,
    secret_key: &str,
) -> S3Result<bool> {
    let body = match task {
        SiteReplicationRepairTask::Iam(item) => serde_json::to_value(item),
        SiteReplicationRepairTask::BucketMetadata(item) => serde_json::to_value(item),
        SiteReplicationRepairTask::BucketMake(_) | SiteReplicationRepairTask::Replication(_) => Ok(serde_json::json!({})),
    }
    .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize retry task failed: {err}")))?;
    send_retry_request_if_peer_current(peer, transport, task.path(), access_key, secret_key, body).await
}

pub(crate) async fn send_retry_request_if_peer_current(
    peer: &PeerInfo,
    transport: &PeerTransport,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: Value,
) -> S3Result<bool> {
    let peer = peer.clone();
    let transport = transport.clone();
    let path = path.to_string();
    let access_key = access_key.to_string();
    let secret_key = secret_key.to_string();
    with_site_replication_state_read_lock(move |state| async move {
        let current = state
            .peers
            .get(&peer.deployment_id)
            .is_some_and(|current| same_identity_endpoint(&current.endpoint, &peer.endpoint));
        if !current {
            return Ok(false);
        }
        PeerAdminRequest::put(&transport.connection, &path, &access_key)
            .with_client(&transport.client)
            .send(&secret_key, &body)
            .await?;
        Ok(true)
    })
    .await
}

async fn send_peer_edit_retry_if_peer_current(
    peer: &PeerInfo,
    transport: &PeerTransport,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: Value,
) -> S3Result<bool> {
    let peer_owned = peer.clone();
    let current = with_site_replication_state_read_lock(move |state| async move {
        Ok(state
            .peers
            .get(&peer_owned.deployment_id)
            .is_some_and(|current| same_identity_endpoint(&current.endpoint, &peer_owned.endpoint)))
    })
    .await?;
    if !current {
        return Ok(false);
    }

    // A peer-edit handler takes its own site's state write lock. Releasing
    // this site's read lock before the request prevents simultaneous A -> B
    // and B -> A retries from waiting on each other's write lock. The edit
    // generation carried by `path` fences a delivery overtaken by a newer
    // topology commit after this check.
    PeerAdminRequest::put(&transport.connection, path, access_key)
        .with_client(&transport.client)
        .send(secret_key, &body)
        .await?;
    Ok(true)
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

pub(crate) fn bucket_op_retry_replay_tasks<'a>(
    plan: &'a SiteReplicationBootstrapPlan,
    operation: &str,
    bucket: &str,
) -> S3Result<Vec<SiteReplicationRepairTask<'a>>> {
    let matches_bucket = |path: &&String| retry_bucket_name(path).as_deref() == Some(bucket);
    match operation {
        SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING => {
            let mut tasks = plan
                .bucket_make_ops
                .iter()
                .filter(matches_bucket)
                .map(|path| SiteReplicationRepairTask::BucketMake(path.as_str()))
                .collect::<Vec<_>>();
            if tasks.is_empty() {
                return Ok(tasks);
            }
            let configure_tasks = plan
                .bucket_configure_ops
                .iter()
                .filter(matches_bucket)
                .map(|path| SiteReplicationRepairTask::Replication(path.as_str()))
                .collect::<Vec<_>>();
            if configure_tasks.is_empty() {
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("site replication retry plan has no configure operation for bucket {bucket:?}"),
                ));
            }
            tasks.extend(
                plan.bucket_items
                    .iter()
                    .filter(|item| item.bucket == bucket)
                    .map(SiteReplicationRepairTask::BucketMetadata),
            );
            tasks.extend(configure_tasks);
            Ok(tasks)
        }
        SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION => Ok(plan
            .bucket_configure_ops
            .iter()
            .filter(matches_bucket)
            .map(|path| SiteReplicationRepairTask::Replication(path.as_str()))
            .collect()),
        _ => Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("unsupported site replication retry bucket operation {operation:?}"),
        )),
    }
}

pub(crate) fn retry_drain_request_count(action: &RetryDrainAction, plan: Option<&SiteReplicationBootstrapPlan>) -> usize {
    match action {
        RetryDrainAction::BucketOpReplay { operation, bucket } => plan
            .and_then(|plan| bucket_op_retry_replay_tasks(plan, operation, bucket).ok())
            .map_or(0, |tasks| tasks.len()),
        RetryDrainAction::IamSnapshot | RetryDrainAction::BucketMetadataSnapshot | RetryDrainAction::PeerEdit => usize::MAX,
    }
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

/// Replayable events currently held back only by backoff. The exponential
/// backoff exists to spare a *dead* peer the expensive replay (plan build,
/// snapshot resend) — it must not delay convergence to a peer that has
/// already RECOVERED, or a failure window ends in up to a day of silent
/// divergence (backlog#2071). Peer connection failures may be probed before
/// the normal replay backoff elapses; request timeouts and application
/// failures still wait at least one base interval so a reachable peer that
/// keeps rejecting a replay is not hammered faster than before.
pub(crate) fn deferred_site_replication_retry_events(
    state: &SiteReplicationState,
    now: OffsetDateTime,
) -> Vec<SiteReplicationRetryEvent> {
    state
        .retry_queue
        .iter()
        .filter(|event| classify_site_replication_retry_event(event).is_some())
        .filter(|event| state.peers.contains_key(&event.peer_deployment_id))
        .filter(|event| !site_replication_retry_backoff_elapsed(event, now))
        .filter(|event| {
            event.updated_at.is_none_or(|updated_at| {
                event.peer_unreachable
                    // Older binaries do not persist `peer_unreachable`. Parse
                    // only the locally-produced outer transport-error shape so
                    // rolling upgrades retain fast recovery without trusting
                    // an HTTP error body containing the same words.
                    || retry_error_indicates_peer_unreachable(&event.last_error)
                    || now.unix_timestamp().saturating_sub(updated_at.unix_timestamp())
                        >= SITE_REPLICATION_RETRY_DRAIN_BASE_BACKOFF_SECS
            })
        })
        .cloned()
        .collect()
}

/// Peer link-check upload drain: a bodiless-in-spirit POST every peer accepts
/// with the replication service account, discarding the payload. The cheapest
/// authenticated proof that the peer is reachable again.
pub(crate) const SITE_REPLICATION_PEER_DEVNULL_PATH: &str = "/rustfs/admin/v3/site-replication/devnull";

pub(crate) async fn probe_site_replication_peer_reachable(runtime: &SiteReplicationRuntime, peer: &PeerInfo) -> bool {
    let Ok(transport) = PeerTransport::for_runtime_peer(peer).await else {
        return false;
    };
    PeerAdminRequest::post(
        &transport.connection,
        SITE_REPLICATION_PEER_DEVNULL_PATH,
        &runtime.state.service_account_access_key,
    )
    .with_client(&transport.client)
    .send(&runtime.service_account_secret_key, &serde_json::json!({}))
    .await
    .is_ok()
}

/// Probe the peers whose whole backlog is deferred and promote the backlog of
/// every peer that answers. A probe failure advances nothing: retry counts
/// only move on real delivery attempts, so the per-event backoff is intact
/// when the peer is genuinely down.
pub(crate) fn mark_reachable_deferred_retry_events(
    state: &mut SiteReplicationState,
    recovered: &[SiteReplicationRetryEvent],
) -> usize {
    let mut promoted = 0;
    for recovered in recovered {
        if let Some(current) = state.retry_queue.iter_mut().find(|current| {
            current.id == recovered.id
                && current.peer_deployment_id == recovered.peer_deployment_id
                && current.path == recovered.path
                && current.updated_at == recovered.updated_at
        }) {
            current.updated_at = None;
            current.peer_unreachable = false;
            promoted += 1;
        }
    }
    promoted
}

pub(crate) async fn promote_reachable_deferred_retry_events(
    runtime: &SiteReplicationRuntime,
    actionable: &[SiteReplicationRetryEvent],
    deferred: Vec<SiteReplicationRetryEvent>,
) -> S3Result<()> {
    let due_peers: HashSet<String> = actionable.iter().map(|event| event.peer_deployment_id.clone()).collect();
    let mut deferred_by_peer: BTreeMap<String, Vec<SiteReplicationRetryEvent>> = BTreeMap::new();
    for event in deferred {
        if due_peers.contains(&event.peer_deployment_id) {
            // The peer is being dialed this tick anyway; its other events keep
            // their own backoff.
            continue;
        }
        deferred_by_peer
            .entry(event.peer_deployment_id.clone())
            .or_default()
            .push(event);
    }
    let probes = deferred_by_peer.into_iter().filter_map(|(deployment_id, events)| {
        let peer = runtime.state.peers.get(&deployment_id)?;
        if deployment_id == runtime.local_peer.deployment_id
            || same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
        {
            return None;
        }
        Some(async move {
            let reachable = probe_site_replication_peer_reachable(runtime, peer).await;
            (deployment_id, peer.endpoint.clone(), events, reachable)
        })
    });
    let mut recovered = Vec::new();
    let probe_results = stream::iter(probes)
        .buffer_unordered(SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for (deployment_id, peer_endpoint, events, reachable) in probe_results {
        if reachable {
            info!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "retry_backoff_probe_promoted",
                peer = %peer_endpoint,
                deployment_id = %deployment_id,
                promoted = events.len(),
                "peer reachable again; promoting backed-off retry events for replay"
            );
            recovered.extend(events);
        }
    }
    if recovered.is_empty() {
        return Ok(());
    }
    update_site_replication_state_when_changed(move |state| {
        let promoted = mark_reachable_deferred_retry_events(state, &recovered);
        Ok(if promoted == 0 {
            StateCommit::Unchanged(())
        } else {
            StateCommit::Changed(())
        })
    })
    .await
}

/// Operator-visible per-tick alert for retry entries that no longer converge
/// on their own: `failed` deliveries deep in backoff and snapshot-escalated
/// markers awaiting a repair. Healthy pending entries stay silent.
pub(crate) fn log_site_replication_retry_liabilities(state: &SiteReplicationState) {
    let escalated = state
        .retry_queue
        .iter()
        .filter(|event| event.last_error == SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER)
        .count();
    let failed = state
        .retry_queue
        .iter()
        .filter(|event| event.failed && event.last_error != SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER)
        .count();
    if failed == 0 && escalated == 0 {
        return;
    }
    let pending = state.retry_queue.len().saturating_sub(failed + escalated);
    let oldest_updated_at = state
        .retry_queue
        .iter()
        .filter(|event| event.failed || event.last_error == SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER)
        .filter_map(|event| event.updated_at)
        .min();
    warn!(
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
        event = EVENT_ADMIN_SITE_REPLICATION_STATE,
        failed,
        escalated,
        pending,
        oldest_updated_at = ?oldest_updated_at,
        recorded_deletions = state.iam_deletion_replays.len(),
        result = "retry_liabilities_outstanding",
        "site replication retry queue holds failed or escalated deliveries; peer convergence is degraded"
    );
}

/// Background consumer for the retry queue, run from the reconcile tick.
///
/// Scope: this settles "delivered once and failed" entries whose replay is
/// faithful (bucket ops, peer edits). Collapsed iam-item entries replay the
/// recorded deletion bodies and then the snapshot, which together cover every
/// failure the hook recorded, so a fully-recorded entry settles; an entry
/// with an unrecorded deletion (legacy rows, record overflow) is *escalated*
/// instead — remote absence stays unproven until a later delivery or a
/// manual repair. Collapsed bucket-meta entries keep the escalate-only
/// semantics. A hook that never fired (crash between the local commit and
/// the send) leaves no entry at all, so the drain is not a full cross-site
/// diff-heal; manual repair remains the authoritative catch-all.
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

/// Fast outage-recovery pass used by the 30-second scheduler. It limits work
/// to one bounded bucket-op chain per peer, builds no site-wide snapshot, and
/// runs reachability probes concurrently with replay for other peers.
pub(crate) async fn drain_site_replication_retry_queue_lightweight() {
    if let Err(err) = drain_site_replication_retry_queue_lightweight_inner().await {
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

async fn drain_site_replication_retry_queue_lightweight_inner() -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    log_site_replication_retry_liabilities(&runtime.state);
    if runtime.state.pending_endpoint_refresh.is_some()
        || runtime.state.pending_remove.is_some()
        || runtime.state.pending_rotation.is_some()
    {
        return Ok(());
    }
    let now = OffsetDateTime::now_utc();
    let mut actionable = actionable_site_replication_retry_events(&runtime.state, now);
    let mut deferred = deferred_site_replication_retry_events(&runtime.state, now);
    actionable.retain(|event| {
        classify_site_replication_retry_event(event).is_some_and(|action| is_lightweight_retry_drain_action(&action))
    });
    deferred.retain(|event| {
        classify_site_replication_retry_event(event).is_some_and(|action| is_lightweight_retry_drain_action(&action))
    });
    if actionable.is_empty() && deferred.is_empty() {
        return Ok(());
    }
    let Some(store) = current_object_store_handle() else {
        return Ok(());
    };

    // Probes are read-only and can consume the full request timeout. Keep
    // them outside repair coordination; promotion is fenced by event id and
    // timestamp, and the locked reload below decides what may actually send.
    promote_reachable_deferred_retry_events(&runtime, &actionable, deferred).await?;

    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        let Some(runtime) = runtime_site_replication_targets().await? else {
            return Ok(());
        };
        if runtime.state.pending_endpoint_refresh.is_some()
            || runtime.state.pending_remove.is_some()
            || runtime.state.pending_rotation.is_some()
        {
            return Ok(());
        }
        let now = OffsetDateTime::now_utc();
        let mut actionable = actionable_site_replication_retry_events(&runtime.state, now);
        actionable.retain(|event| {
            classify_site_replication_retry_event(event).is_some_and(|action| is_lightweight_retry_drain_action(&action))
        });
        if actionable.is_empty() {
            return Ok(());
        }
        drain_site_replication_retry_queue_lightweight_locked(Arc::new(runtime), actionable).await
    })
    .await
    .map_err(ApiError::from)?
}

pub(crate) async fn drain_site_replication_retry_queue_inner() -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    // The alert must fire even when nothing is drainable this tick —
    // escalated markers are exactly the entries the drain skips.
    log_site_replication_retry_liabilities(&runtime.state);
    let now = OffsetDateTime::now_utc();
    let actionable = actionable_site_replication_retry_events(&runtime.state, now);
    let deferred = deferred_site_replication_retry_events(&runtime.state, now);
    if actionable.is_empty() && deferred.is_empty() {
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

    // Persist successful recovery probes without monopolizing repair
    // coordination. The locked reload below observes those promotions and
    // replays them in this same round.
    promote_reachable_deferred_retry_events(&runtime, &actionable, deferred).await?;

    // Serialize against operator repair execution. Peer membership is
    // re-checked from a distributed state snapshot immediately before each
    // network request, so the caller need not hold the lifecycle guard while
    // a large snapshot is replayed. This does NOT close the
    // dry-run -> execute window (dry-run takes no lock): a drain settling a
    // replayable bucket-op entry in that window changes the preflight token
    // and execute fails safe with "preflight is stale" — the operator
    // re-runs the dry-run. The lock elects one server to replay the queue;
    // after acquiring it, reload state so a settled event or deleted bucket
    // cannot be replayed from this admission snapshot.
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        // Runtime and queue snapshots captured before this distributed lock
        // are only admission hints. Another node may have settled the event,
        // or a local bucket may have been deleted, while this node waited.
        let Some(runtime) = runtime_site_replication_targets().await? else {
            return Ok(());
        };
        if runtime.state.pending_endpoint_refresh.is_some()
            || runtime.state.pending_remove.is_some()
            || runtime.state.pending_rotation.is_some()
        {
            return Ok(());
        }
        let now = OffsetDateTime::now_utc();
        let actionable = actionable_site_replication_retry_events(&runtime.state, now);
        if actionable.is_empty() {
            return Ok(());
        }
        drain_site_replication_retry_queue_locked(runtime, actionable).await
    })
    .await
    .map_err(ApiError::from)?
}

async fn drain_site_replication_retry_queue_lightweight_locked(
    runtime: Arc<SiteReplicationRuntime>,
    events: Vec<SiteReplicationRetryEvent>,
) -> S3Result<()> {
    let mut events_by_peer: BTreeMap<String, Vec<SiteReplicationRetryEvent>> = BTreeMap::new();
    for event in events {
        events_by_peer
            .entry(event.peer_deployment_id.clone())
            .or_default()
            .push(event);
    }

    let peer_replays = events_by_peer
        .into_iter()
        .filter_map(|(deployment_id, peer_events)| {
            let peer = runtime.state.peers.get(&deployment_id)?.clone();
            if deployment_id == runtime.local_peer.deployment_id
                || same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
            {
                return None;
            }
            let runtime = Arc::clone(&runtime);
            Some(async move {
                let Some((event, action, bucket)) = peer_events.into_iter().find_map(|event| {
                    let action = classify_site_replication_retry_event(&event)?;
                    let bucket = match &action {
                        RetryDrainAction::BucketOpReplay { bucket, .. } => bucket.clone(),
                        _ => return None,
                    };
                    Some((event, action, bucket))
                }) else {
                    return (0, 0);
                };
                let plan = match site_replication_bucket_retry_plan(&bucket).await {
                    Ok(plan) => plan,
                    Err(err) => {
                        enqueue_site_replication_retry_event(&peer, &event.path, &err).await;
                        return (0, 1);
                    }
                };
                if retry_drain_request_count(&action, Some(&plan)) > SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER {
                    return (0, 0);
                }
                let transport = match PeerTransport::for_runtime_peer(&peer).await {
                    Ok(transport) => transport,
                    Err(err) => {
                        enqueue_site_replication_retry_event(&peer, &event.path, &err).await;
                        return (0, 1);
                    }
                };
                match drain_one_site_replication_retry_event(&runtime, &peer, &transport, &event, action, Some(&plan)).await {
                    Ok(true) => (1, 0),
                    Ok(false) => (0, 0),
                    Err(_) => (0, 1),
                }
            })
        })
        .take(SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY);

    let mut settled = 0usize;
    let mut failures = 0usize;
    let replay_results = stream::iter(peer_replays)
        .buffer_unordered(SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for (peer_settled, peer_failures) in replay_results {
        settled += peer_settled;
        failures += peer_failures;
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

    let runtime = Arc::new(runtime);
    let plan = plan.map(Arc::new);
    let peer_replays = events_by_peer.into_iter().filter_map(|(deployment_id, peer_events)| {
        let peer = runtime.state.peers.get(&deployment_id)?.clone();
        if deployment_id == runtime.local_peer.deployment_id
            || same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
        {
            return None;
        }
        let runtime = Arc::clone(&runtime);
        let plan = plan.as_ref().map(Arc::clone);
        Some(async move {
            let mut settled = 0usize;
            let mut failures = 0usize;
            let transport = match PeerTransport::for_runtime_peer(&peer).await {
                Ok(transport) => transport,
                Err(err) => {
                    // Record the attempt so backoff advances for an unreachable
                    // peer instead of re-dialing it every tick.
                    for event in &peer_events {
                        enqueue_site_replication_retry_event(&peer, &event.path, &err).await;
                    }
                    return (0, peer_events.len());
                }
            };
            for event in peer_events {
                let Some(action) = classify_site_replication_retry_event(&event) else {
                    continue;
                };
                match drain_one_site_replication_retry_event(&runtime, &peer, &transport, &event, action, plan.as_deref()).await {
                    Ok(true) => settled += 1,
                    Ok(false) => {}
                    Err(_) => failures += 1,
                }
            }
            (settled, failures)
        })
    });

    let mut settled = 0usize;
    let mut failures = 0usize;
    let replay_results = stream::iter(peer_replays)
        .buffer_unordered(SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for (peer_settled, peer_failures) in replay_results {
        settled += peer_settled;
        failures += peer_failures;
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
            // Replay recorded IAM deletion bodies BEFORE the snapshot: an
            // entity deleted and later recreated locally is restored by the
            // snapshot that follows, so the replay can never end below the
            // current local state (backlog#2071).
            let is_iam = matches!(action, RetryDrainAction::IamSnapshot);
            let mut replayed_record_ids = Vec::new();
            if is_iam {
                for record in iam_deletion_replays_for_peer(&runtime.state, peer) {
                    let Ok(item) = serde_json::from_value::<SRIAMItem>(record.item.clone()) else {
                        drop_corrupt_iam_deletion_replay(peer, &record.id).await;
                        continue;
                    };
                    match send_retry_task_if_peer_current(
                        peer,
                        &SiteReplicationRepairTask::Iam(&item),
                        transport,
                        access_key,
                        secret_key,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => return Ok(false),
                        Err(err) => {
                            enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                            return Err(err);
                        }
                    }
                    replayed_record_ids.push(record.id.clone());
                }
            }
            let mut current_snapshot = RetrySnapshot::from_plan(&action, plan).expect("snapshot action has a snapshot");
            let mut replay = current_snapshot.clone();
            for _ in 0..SITE_REPLICATION_RETRY_SNAPSHOT_STABILITY_ATTEMPTS {
                let current_fingerprint = current_snapshot.fingerprint()?;
                match replay.send(peer, transport, access_key, secret_key).await {
                    Ok(true) => {}
                    Ok(false) => return Ok(false),
                    Err(err) => {
                        enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                        return Err(err);
                    }
                }
                let fresh_info = build_sr_info(&runtime.state, &runtime.local_peer).await?;
                let fresh_plan = site_replication_bootstrap_plan(&fresh_info)?;
                let fresh_snapshot = RetrySnapshot::from_plan(&action, &fresh_plan).expect("snapshot action has a snapshot");
                if fresh_snapshot.fingerprint()? == current_fingerprint {
                    if is_iam {
                        settle_replayed_site_replication_iam_retry_event(
                            peer,
                            &event.path,
                            event.updated_at,
                            replayed_record_ids,
                        )
                        .await;
                    } else {
                        escalate_site_replication_retry_event_up_to(peer, &event.path, event.updated_at).await;
                    }
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
            let tasks = match bucket_op_retry_replay_tasks(plan, &operation, &bucket) {
                Ok(tasks) => tasks,
                Err(err) => {
                    enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                    return Err(err);
                }
            };
            if tasks.is_empty() {
                // The bucket left the plan (deleted, or replication no longer
                // configured): the recorded intent is stale, settle it.
                return Ok(dequeue_observed_site_replication_retry_event(peer, event).await);
            }
            for task in &tasks {
                match send_retry_task_if_peer_current(peer, task, transport, access_key, secret_key).await {
                    Ok(true) => {}
                    Ok(false) => return Ok(false),
                    Err(err) => {
                        enqueue_site_replication_retry_event(peer, &event.path, &err).await;
                        return Err(err);
                    }
                }
            }
            Ok(dequeue_observed_site_replication_retry_event(peer, event).await)
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
                let body = serde_json::to_value(body).map_err(|err| {
                    S3Error::with_message(S3ErrorCode::InternalError, format!("serialize retry peer edit failed: {err}"))
                })?;
                match send_peer_edit_retry_if_peer_current(peer, transport, &edit_path, access_key, secret_key, body).await {
                    Ok(true) => {}
                    Ok(false) => return Ok(false),
                    Err(err) => {
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

pub(crate) async fn dequeue_observed_site_replication_retry_event(peer: &PeerInfo, observed: &SiteReplicationRetryEvent) -> bool {
    let result = async {
        let mut probe = load_site_replication_state().await?;
        if settle_observed_site_replication_retry_event(&mut probe.retry_queue, peer, observed) == 0 {
            return Ok(false);
        }
        let peer_owned = peer.clone();
        let observed_owned = observed.clone();
        update_site_replication_state(move |state| {
            Ok(settle_observed_site_replication_retry_event(&mut state.retry_queue, &peer_owned, &observed_owned) > 0)
        })
        .await
    }
    .await;

    match result {
        Ok(settled) => settled,
        Err(err) => {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "retry_event_dequeue_failed",
                peer = %peer.endpoint,
                deployment_id = %peer.deployment_id,
                path = %observed.path,
                error = ?err,
                "failed to dequeue observed site replication retry event"
            );
            false
        }
    }
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
