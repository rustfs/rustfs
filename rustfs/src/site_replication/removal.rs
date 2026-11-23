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

//! Durable decommissioning contract shared by admin admission and peer delivery.

use super::*;
use crate::storage_api::site_replication::NamespaceLocking;
use rustfs_madmin::{SRRemoveV2Req, SRRemoveV2Status};
use rustfs_s3_types::s3_error;
use sha2::{Digest, Sha256};

pub(crate) const SITE_PEER_REMOVE_V2_PATH: &str = "/rustfs/admin/v3/site-replication/peer/remove-v2";
pub(crate) const SITE_REMOVE_V2_CAPABILITY_PATH: &str = "/rustfs/admin/v3/site-replication/remove-v2/capabilities";
/// This lock precedes bucket admission and the state transaction. Join admission uses the same lock.
pub(crate) const SITE_REMOVE_V2_ADMISSION_LOCK: &str = "config/site-replication/join-admission.lock";

/// Acquire lifecycle read admission without moving the caller to another task.
/// Notification context and cancellation/fault hooks belong to the calling task.
pub(crate) async fn acquire_site_removal_read_admission(store: Arc<ECStore>) -> S3Result<rustfs_lock::NamespaceLockGuard> {
    let lock = store
        .new_ns_lock(crate::storage_api::site_replication::RUSTFS_META_BUCKET, SITE_REMOVE_V2_ADMISSION_LOCK)
        .await
        .map_err(ApiError::from)?;
    lock.get_read_lock(crate::storage_api::site_replication::get_lock_acquire_timeout())
        .await
        .map_err(|error| S3Error::with_message(S3ErrorCode::SlowDown, format!("site removal admission unavailable: {error}")))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SiteRemovalPlan {
    pub(crate) id: String,
    pub(crate) coordinator: String,
    pub(crate) original_peers: BTreeMap<String, PeerInfo>,
    pub(crate) removed: BTreeSet<String>,
    pub(crate) decommissioned: BTreeSet<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) topology_at: OffsetDateTime,
    pub(crate) base_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PendingRemoveV2 {
    pub(crate) plan: SiteRemovalPlan,
    pub(crate) coordinator: bool,
    pub(crate) access_key: String,
    #[serde(default)]
    pub(crate) secret_candidates: Vec<String>,
    #[serde(default)]
    pub(crate) cleaned_peers: BTreeSet<String>,
    #[serde(default)]
    pub(crate) committed_peers: BTreeSet<String>,
    #[serde(default)]
    pub(crate) local_cleanup_complete: bool,
    #[serde(default)]
    pub(crate) commit_decided: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SiteRemovalReceipt {
    pub(crate) digest: String,
    pub(crate) plan: SiteRemovalPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SiteRemovalPeerRequest {
    pub(crate) phase: String,
    pub(crate) plan: SiteRemovalPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SiteRemovalPeerResponse {
    pub(crate) contract_version: u32,
    pub(crate) operation_id: String,
    pub(crate) digest: String,
    pub(crate) cleanup_complete: bool,
    pub(crate) committed: bool,
}

pub(crate) fn site_removal_digest(plan: &SiteRemovalPlan) -> S3Result<String> {
    let data = serde_json::to_vec(plan).map_err(|_| s3_error!(InternalError, "cannot encode site removal plan"))?;
    Ok(URL_SAFE_NO_PAD.encode_to_string(Sha256::digest(data)))
}

pub(crate) fn site_removal_preflight_token(plan: &SiteRemovalPlan, key: &[u8]) -> S3Result<String> {
    if key.is_empty() {
        return Err(s3_error!(InternalError, "empty decommission preflight signing key"));
    }
    let mut mac = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|_| s3_error!(InternalError, "invalid preflight signing key"))?;
    mac.update(b"rustfs:site-replication:decommission-preflight:v2\0");
    // Execution chooses the commit timestamp; the preview binds the identity, scope and base.
    let data = serde_json::to_vec(&(&plan.id, &plan.coordinator, &plan.removed, &plan.decommissioned, &plan.base_fingerprint))
        .map_err(|_| s3_error!(InternalError, "cannot encode site removal preflight"))?;
    mac.update(&data);
    Ok(URL_SAFE_NO_PAD.encode_to_string(mac.finalize().into_bytes()))
}

fn removal_base_fingerprint(state: &SiteReplicationState) -> S3Result<String> {
    // ACKs and candidate secrets may change during preflight without changing its scope.
    let legacy = state
        .pending_remove
        .as_ref()
        .map(|pending| (&pending.id, &pending.req, &pending.original_peers));
    let data = serde_json::to_vec(&(&state.peers, state.updated_at, legacy))
        .map_err(|_| s3_error!(InternalError, "cannot encode site removal precondition"))?;
    Ok(URL_SAFE_NO_PAD.encode_to_string(Sha256::digest(data)))
}

fn resolve_removal_names(peers: &BTreeMap<String, PeerInfo>, names: &[String]) -> S3Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for name in names {
        if name.is_empty() || name.trim() != name {
            return Err(s3_error!(
                InvalidRequest,
                "site names must be nonempty and have no surrounding whitespace"
            ));
        }
        let matches = peers.values().filter(|peer| peer.name == *name).collect::<Vec<_>>();
        let [peer] = matches.as_slice() else {
            return Err(s3_error!(
                InvalidRequest,
                "each requested site must identify exactly one configured deployment"
            ));
        };
        if !ids.insert(peer.deployment_id.clone()) {
            return Err(s3_error!(InvalidRequest, "duplicate site name"));
        }
    }
    Ok(ids)
}

pub(crate) fn plan_site_removal(
    state: &SiteReplicationState,
    local: &PeerInfo,
    req: &SRRemoveV2Req,
) -> S3Result<SiteRemovalPlan> {
    if !matches!(req.mode.as_str(), "dry-run" | "execute") {
        return Err(s3_error!(InvalidRequest, "mode must be dry-run or execute"));
    }
    if let Some(pending) = &state.pending_remove_v2 {
        if !pending.coordinator {
            return Err(s3_error!(InvalidRequest, "site removal is coordinated by another site"));
        }
        validate_removal_request_matches(&pending.plan, req)?;
        return Ok(pending.plan.clone());
    }
    if let Some(id) = &req.operation_id {
        if let Some(receipt) = state.remove_v2_receipts.get(id) {
            validate_removal_request_matches(&receipt.plan, req)?;
            return Ok(receipt.plan.clone());
        }
        if Uuid::parse_str(id).is_err() {
            return Err(s3_error!(InvalidRequest, "operationId must be a UUID"));
        }
    }
    if state.pending_rotation.is_some() || state.pending_endpoint_refresh.is_some() {
        return Err(s3_error!(InvalidRequest, "another site replication operation is pending"));
    }
    let original_peers = state
        .pending_remove
        .as_ref()
        .map(|pending| &pending.original_peers)
        .unwrap_or(&state.peers);
    if original_peers.len() < 2 || !original_peers.contains_key(&local.deployment_id) {
        return Err(s3_error!(InvalidRequest, "site replication topology is not configured"));
    }
    let removed = if req.all {
        if !req.sites.is_empty() {
            return Err(s3_error!(InvalidRequest, "sites must be empty when all=true"));
        }
        original_peers.keys().cloned().collect()
    } else {
        resolve_removal_names(original_peers, &req.sites)?
    };
    if removed.is_empty() {
        return Err(s3_error!(InvalidRequest, "at least one site must be removed"));
    }
    let decommissioned_names = if req.all { &req.decommissioned_sites } else { &req.sites };
    if !req.all && !req.decommissioned_sites.is_empty() {
        return Err(s3_error!(InvalidRequest, "decommissionedSites is only valid with all=true"));
    }
    let decommissioned = resolve_removal_names(original_peers, decommissioned_names)?;
    if decommissioned.is_empty() || decommissioned.contains(&local.deployment_id) || !decommissioned.is_subset(&removed) {
        return Err(s3_error!(InvalidRequest, "explicit decommissioned remote sites are required"));
    }
    if let Some(pending) = &state.pending_remove {
        let legacy_removed = if pending.req.remove_all || pending.req.site_names.contains(&state.name) {
            original_peers.keys().cloned().collect()
        } else {
            resolve_removal_names(original_peers, &pending.req.site_names)?
        };
        if legacy_removed != removed {
            return Err(s3_error!(InvalidRequest, "force removal must preserve the pending removal's site set"));
        }
    }
    let now = OffsetDateTime::now_utc();
    let topology_at = next_site_removal_timestamp(state, now)?;
    let base_fingerprint = removal_base_fingerprint(state)?;
    let identity = serde_json::to_vec(&(&local.deployment_id, &removed, &decommissioned, &base_fingerprint))
        .map_err(|_| s3_error!(InternalError, "cannot encode decommission identity"))?;
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity).to_string();
    if req.operation_id.as_ref().is_some_and(|requested| *requested != id) {
        return Err(s3_error!(PreconditionFailed, "operationId does not match the current removal preview"));
    }
    Ok(SiteRemovalPlan {
        id,
        coordinator: local.deployment_id.clone(),
        original_peers: original_peers.clone(),
        removed,
        decommissioned,
        topology_at,
        base_fingerprint,
    })
}

pub(crate) fn next_site_removal_timestamp(state: &SiteReplicationState, now: OffsetDateTime) -> S3Result<OffsetDateTime> {
    Ok(state
        .updated_at
        .into_iter()
        .chain(state.removal_v2_fence)
        .max()
        .filter(|stamp| *stamp >= now)
        .map(|stamp| {
            stamp
                .checked_add(time::Duration::nanoseconds(1))
                .ok_or_else(|| s3_error!(InvalidRequest, "topology timestamp exhausted"))
        })
        .transpose()?
        .unwrap_or(now))
}

fn validate_removal_request_matches(plan: &SiteRemovalPlan, req: &SRRemoveV2Req) -> S3Result<()> {
    let removed = if req.all {
        if !req.sites.is_empty() {
            return Err(s3_error!(InvalidRequest, "sites must be empty when all=true"));
        }
        plan.original_peers.keys().cloned().collect()
    } else {
        resolve_removal_names(&plan.original_peers, &req.sites)?
    };
    let names = if req.all { &req.decommissioned_sites } else { &req.sites };
    if (!req.all && !req.decommissioned_sites.is_empty())
        || removed != plan.removed
        || resolve_removal_names(&plan.original_peers, names)? != plan.decommissioned
        || req.operation_id.as_ref().is_some_and(|id| *id != plan.id)
    {
        return Err(s3_error!(InvalidRequest, "request does not match the recorded site removal"));
    }
    Ok(())
}

pub(crate) fn required_removal_peers(plan: &SiteRemovalPlan) -> BTreeSet<String> {
    plan.original_peers
        .keys()
        .filter(|id| **id != plan.coordinator && !plan.decommissioned.contains(*id))
        .cloned()
        .collect()
}

pub(crate) fn validate_removal_plan(plan: &SiteRemovalPlan, local_id: &str) -> S3Result<()> {
    if Uuid::parse_str(&plan.id).is_err()
        || !plan.original_peers.contains_key(&plan.coordinator)
        || !plan.original_peers.contains_key(local_id)
        || plan.decommissioned.contains(local_id)
        || plan.decommissioned.contains(&plan.coordinator)
        || plan.removed.is_empty()
        || plan.decommissioned.is_empty()
        || !plan.decommissioned.is_subset(&plan.removed)
        || plan.removed.iter().any(|id| !plan.original_peers.contains_key(id))
        || plan
            .original_peers
            .iter()
            .any(|(id, peer)| id.is_empty() || *id != peer.deployment_id)
    {
        return Err(s3_error!(InvalidRequest, "invalid site removal plan"));
    }
    Ok(())
}

pub(crate) fn admit_site_removal(
    state: &mut SiteReplicationState,
    plan: SiteRemovalPlan,
    local_id: &str,
    coordinator: bool,
) -> S3Result<()> {
    validate_removal_plan(&plan, local_id)?;
    if state.pending_remove_v2.is_some() || state.remove_v2_receipts.contains_key(&plan.id) {
        return Err(s3_error!(InvalidRequest, "site removal is already recorded"));
    }
    if state.pending_rotation.is_some() || state.pending_endpoint_refresh.is_some() {
        return Err(s3_error!(InvalidRequest, "another site replication operation is pending"));
    }
    if coordinator {
        if removal_base_fingerprint(state)? != plan.base_fingerprint {
            return Err(s3_error!(PreconditionFailed, "site removal preflight is stale"));
        }
    } else {
        if state.updated_at.is_some_and(|stamp| stamp >= plan.topology_at)
            || state.removal_v2_fence.is_some_and(|stamp| stamp >= plan.topology_at)
        {
            return Err(s3_error!(PreconditionFailed, "site removal was superseded by a newer topology"));
        }
        // A legacy remove --all may already have cleared this participant's state.
        // The authenticated retry can still clean its old derived bucket targets.
        if state.peers.iter().any(|(id, peer)| {
            plan.original_peers
                .get(id)
                .is_none_or(|original| !same_identity_endpoint(&peer.endpoint, &original.endpoint))
        }) || (!state.peers.is_empty()
            && plan
                .original_peers
                .keys()
                .any(|id| !plan.removed.contains(id) && !state.peers.contains_key(id)))
        {
            return Err(s3_error!(PreconditionFailed, "peer topology does not match the removal plan"));
        }
    }
    let access_key = state
        .pending_remove
        .as_ref()
        .map(|pending| pending.service_account_access_key.clone())
        .unwrap_or_else(|| state.service_account_access_key.clone());
    let secret_candidates = state
        .pending_remove
        .as_ref()
        .map(|pending| pending.secret_candidates.clone())
        .unwrap_or_default();
    state.peers.retain(|id, _| !plan.removed.contains(id));
    if plan.removed.contains(local_id) {
        state.peers.clear();
    }
    state.resync_status.retain(|id, _| state.peers.contains_key(id));
    state
        .retry_queue
        .retain(|event| state.peers.contains_key(&event.peer_deployment_id));
    state
        .iam_deletion_replays
        .retain(|event| state.peers.contains_key(&event.peer_deployment_id));
    state.updated_at = Some(plan.topology_at);
    state.removal_v2_fence = Some(
        state
            .removal_v2_fence
            .map_or(plan.topology_at, |stamp| stamp.max(plan.topology_at)),
    );
    state.pending_remove = None;
    state.pending_remove_v2 = Some(PendingRemoveV2 {
        plan,
        coordinator,
        access_key,
        secret_candidates,
        cleaned_peers: BTreeSet::new(),
        committed_peers: BTreeSet::new(),
        local_cleanup_complete: false,
        commit_decided: false,
    });
    Ok(())
}

pub(crate) fn finish_site_removal(state: &mut SiteReplicationState, id: &str) -> S3Result<()> {
    let Some(pending) = state.pending_remove_v2.as_ref().filter(|pending| pending.plan.id == id) else {
        return Err(s3_error!(PreconditionFailed, "site removal changed before completion"));
    };
    if !pending.local_cleanup_complete
        || !pending.commit_decided
        || (pending.coordinator && !required_removal_peers(&pending.plan).is_subset(&pending.committed_peers))
    {
        return Err(s3_error!(InvalidRequest, "site removal cleanup is not committed"));
    }
    let receipt = SiteRemovalReceipt {
        digest: site_removal_digest(&pending.plan)?,
        plan: pending.plan.clone(),
    };
    state.remove_v2_receipts.insert(id.to_string(), receipt);
    state.pending_remove_v2 = None;
    // The retained topology timestamp fences an evicted operation; old requests fail closed.
    while state.remove_v2_receipts.len() > 32 {
        let oldest = state
            .remove_v2_receipts
            .iter()
            .min_by_key(|(_, receipt)| receipt.plan.topology_at)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            state.remove_v2_receipts.remove(&oldest);
        }
    }
    Ok(())
}

pub(crate) fn site_removal_status(
    plan: &SiteRemovalPlan,
    pending: Option<&PendingRemoveV2>,
    token: String,
    complete: bool,
    errors: Vec<String>,
) -> SRRemoveV2Status {
    let pending_peers = pending
        .map(|pending| {
            let acked = if pending.commit_decided {
                &pending.committed_peers
            } else {
                &pending.cleaned_peers
            };
            required_removal_peers(plan).difference(acked).cloned().collect()
        })
        .unwrap_or_default();
    SRRemoveV2Status {
        contract_version: 2,
        operation_id: plan.id.clone(),
        status: if complete {
            "success"
        } else if pending.is_some() {
            "Partial"
        } else {
            "planned"
        }
        .to_string(),
        preflight_token: token,
        removed_deployment_ids: plan.removed.iter().cloned().collect(),
        decommissioned_deployment_ids: plan.decommissioned.iter().cloned().collect(),
        pending_peers,
        local_cleanup_complete: complete || pending.is_some_and(|pending| pending.local_cleanup_complete),
        errors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (SiteReplicationState, PeerInfo, SRRemoveV2Req) {
        let peers: BTreeMap<String, PeerInfo> = ["a", "b", "c"]
            .into_iter()
            .map(|id| {
                (
                    id.to_string(),
                    PeerInfo {
                        deployment_id: id.to_string(),
                        name: id.to_string(),
                        endpoint: format!("https://{id}.example.test"),
                        ..Default::default()
                    },
                )
            })
            .collect();
        let local = peers["a"].clone();
        (
            SiteReplicationState {
                name: "a".to_string(),
                peers,
                ..Default::default()
            },
            local,
            SRRemoveV2Req {
                mode: "dry-run".to_string(),
                sites: vec!["b".to_string()],
                ..Default::default()
            },
        )
    }

    #[test]
    fn decommission_skips_only_the_selected_site_and_waits_for_commit_ack() {
        let (mut state, local, req) = fixture();
        let plan = plan_site_removal(&state, &local, &req).expect("plan retirement");
        assert_eq!(required_removal_peers(&plan), BTreeSet::from(["c".to_string()]));
        admit_site_removal(&mut state, plan.clone(), "a", true).expect("admit");
        assert!(!state.peers.contains_key("b"));
        assert!(finish_site_removal(&mut state, &plan.id).is_err(), "cleanup must precede completion");
        let pending = state.pending_remove_v2.as_mut().expect("pending");
        pending.local_cleanup_complete = true;
        pending.cleaned_peers.insert("c".to_string());
        pending.commit_decided = true;
        assert!(finish_site_removal(&mut state, &plan.id).is_err(), "decision delivery must be confirmed");
        state
            .pending_remove_v2
            .as_mut()
            .expect("pending")
            .committed_peers
            .insert("c".to_string());
        finish_site_removal(&mut state, &plan.id).expect("finish");
        assert!(state.pending_remove_v2.is_none());
        assert_eq!(state.remove_v2_receipts[&plan.id].digest, site_removal_digest(&plan).expect("digest"));
    }

    #[test]
    fn decommission_recovers_legacy_removed_identity_and_preserves_scope() {
        let (mut state, local, mut req) = fixture();
        let original_peers = state.peers.clone();
        state.peers.remove("b");
        state.pending_remove = Some(PendingRemove {
            id: "legacy".to_string(),
            original_peers,
            req: SRRemoveReq {
                site_names: vec!["b".to_string()],
                ..Default::default()
            },
            ..Default::default()
        });
        let plan = plan_site_removal(&state, &local, &req).expect("recover removed peer");
        assert!(plan.decommissioned.contains("b"));
        req.sites = vec!["c".to_string()];
        assert!(plan_site_removal(&state, &local, &req).is_err(), "cannot change the legacy pending scope");
        admit_site_removal(&mut state, plan, "a", true).expect("upgrade journal");
        assert!(state.pending_remove.is_none());
        let restored = parse_site_replication_state(&serde_json::to_vec(&state).expect("encode")).expect("restore");
        assert!(restored.pending_remove_v2.is_some(), "restart must retain the recovery obligation");
    }

    #[test]
    fn decommission_preflight_binds_topology_but_not_background_ack_updates() {
        let (mut state, local, mut req) = fixture();
        let plan = plan_site_removal(&state, &local, &req).expect("plan");
        req.operation_id = Some(plan.id.clone());
        let replanned = plan_site_removal(&state, &local, &req).expect("execute plan");
        assert_eq!(
            site_removal_preflight_token(&plan, b"key").expect("token"),
            site_removal_preflight_token(&replanned, b"key").expect("token")
        );
        state.peers.get_mut("c").expect("peer").endpoint = "https://new-c.example.test".to_string();
        assert!(
            admit_site_removal(&mut state, plan, "a", true).is_err(),
            "stale preflight must fail closed"
        );
    }

    #[test]
    fn decommission_rejects_unknown_duplicate_local_and_inconsistent_selectors() {
        let (state, local, mut req) = fixture();
        for sites in [vec!["unknown"], vec!["b", "b"], vec!["a"], vec![" b"], vec![]] {
            req.sites = sites.into_iter().map(str::to_string).collect();
            assert!(plan_site_removal(&state, &local, &req).is_err());
        }
        req.sites.clear();
        req.all = true;
        assert!(plan_site_removal(&state, &local, &req).is_err(), "all does not waive every ACK");
        req.decommissioned_sites = vec!["b".to_string()];
        let plan = plan_site_removal(&state, &local, &req).expect("explicit all");
        assert_eq!(required_removal_peers(&plan), BTreeSet::from(["c".to_string()]));
    }

    #[test]
    fn decommission_peer_rejects_stale_replay_after_rejoin_and_keeps_receipt_on_restart() {
        let (state, local, req) = fixture();
        let plan = plan_site_removal(&state, &local, &req).expect("plan");
        let mut peer_state = state.clone();
        admit_site_removal(&mut peer_state, plan.clone(), "c", false).expect("peer prepare");
        let pending = peer_state.pending_remove_v2.as_mut().expect("pending");
        pending.local_cleanup_complete = true;
        pending.commit_decided = true;
        finish_site_removal(&mut peer_state, &plan.id).expect("peer commit");
        peer_state.peers = state.peers;
        peer_state.updated_at = Some(plan.topology_at + time::Duration::seconds(1));
        assert!(admit_site_removal(&mut peer_state, plan.clone(), "c", false).is_err());
        let loaded = parse_site_replication_state(&serde_json::to_vec(&peer_state).expect("encode")).expect("load");
        assert!(loaded.remove_v2_receipts.contains_key(&plan.id));
        assert!(loaded.peers.contains_key("b"), "a late request must not tear down the rejoined site");
    }

    #[test]
    fn decommission_fence_survives_receipt_eviction_and_clock_rollback() {
        let (state, local, req) = fixture();
        let plan = plan_site_removal(&state, &local, &req).expect("plan");
        let mut peer_state = state.clone();
        admit_site_removal(&mut peer_state, plan.clone(), "c", false).expect("admit");
        peer_state.pending_remove_v2 = None;
        peer_state.remove_v2_receipts.clear();
        peer_state.updated_at = None;
        peer_state.peers = state.peers;
        assert!(
            admit_site_removal(&mut peer_state, plan, "c", false).is_err(),
            "persistent fence must reject stale removal"
        );
    }
}
