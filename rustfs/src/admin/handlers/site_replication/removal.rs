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
use crate::runtime_sources::current_endpoints_handle;
use rustfs_madmin::SRRemoveV2Req;

pub(super) struct SiteRemovalV2Handler;
pub(super) struct SiteRemovalV2CapabilitiesHandler;
pub(super) struct SiteRemovalV2PeerHandler;

#[cfg(test)]
pub(super) static FAIL_NEXT_LOCAL_REMOVAL_CLEANUP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

async fn with_removal_admission<T, F, Fut>(
    store: std::sync::Arc<crate::storage_api::site_replication::ECStore>,
    operation: F,
) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
{
    with_config_object_write_lock(store, SITE_REMOVE_V2_ADMISSION_LOCK.to_string(), move |guard| async move {
        if guard.is_lock_lost() { return Err(s3_error!(PreconditionFailed, "decommission admission lock was lost")); }
        let signal = guard.lock_lost_signal();
        tokio::select! {
            biased;
            _ = async { if let Some(signal) = signal { signal.notified().await; } else { std::future::pending::<()>().await; } } => {
                Err(s3_error!(PreconditionFailed, "decommission admission lock was lost"))
            }
            result = operation() => {
                if guard.is_lock_lost() { Err(s3_error!(PreconditionFailed, "decommission admission lock was lost")) }
                else { result }
            }
        }
    }).await.map_err(|err| S3Error::from(ApiError::from(err)))?
}

#[async_trait::async_trait]
impl Operation for SiteRemovalV2CapabilitiesHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        if !req
            .uri()
            .query()
            .is_some_and(|query| form_urlencoded::parse(query.as_bytes()).any(|(key, value)| key == "local" && value == "true"))
        {
            require_local_removal_nodes(&cred).await?;
        }
        owned_json_response(StatusCode::OK, &serde_json::json!({"contractVersion": 2, "durableCleanup": true}))
    }
}

/// Probe every node directly; a load balancer hitting one upgraded node cannot
/// authorize a mixed-version site's destructive operation.
async fn require_local_removal_nodes(cred: &rustfs_credentials::Credentials) -> S3Result<()> {
    let Some(endpoints) = current_endpoints_handle() else {
        return Err(s3_error!(InternalError, "site topology is unavailable"));
    };
    let (nodes, local_host) = endpoints.peers();
    let state = load_site_replication_state().await?;
    let local = current_local_runtime_peer(&state);
    let scheme = if is_https_endpoint(&local.endpoint) { "https" } else { "http" };
    for node in nodes.into_iter().filter(|node| *node != local_host) {
        let peer = PeerInfo {
            endpoint: format!("{scheme}://{node}"),
            ..local.clone()
        };
        let connection = runtime_peer_connection(&peer)?;
        let body = PeerAdminRequest::get(
            &connection,
            "/rustfs/admin/v3/site-replication/remove-v2/capabilities?local=true",
            &cred.access_key,
        )
        .send_get(&cred.secret_key)
        .await?;
        let capability: Value =
            serde_json::from_slice(&body).map_err(|_| s3_error!(NotImplemented, "a node does not advertise durable removal"))?;
        if capability["contractVersion"] != 2 || capability["durableCleanup"] != true {
            return Err(s3_error!(NotImplemented, "all nodes must support durable removal v2"));
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl Operation for SiteRemovalV2Handler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let endpoint = site_replication_local_endpoint(req.uri(), req.headers());
        let input: SRRemoveV2Req = read_site_replication_json(req, "", false).await?;
        if input.mode == "dry-run" && input.preflight_token.is_some() {
            return Err(s3_error!(InvalidRequest, "dry-run does not accept preflightToken"));
        }
        if input.mode == "execute" {
            require_local_removal_nodes(&cred).await?;
        }
        let signing_key =
            current_token_signing_key().ok_or_else(|| s3_error!(InternalError, "token signing key is unavailable"))?;
        let _lifecycle = SiteReplicationLifecycleGuard::acquire().await?;
        let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store is unavailable"))?;
        let result = with_removal_admission(store, move || async move {
            let state = load_site_replication_state().await?;
            let local = local_peer_at_endpoint(endpoint, &state);
            let plan = plan_site_removal(&state, &local, &input)?;
            let token = site_removal_preflight_token(&plan, signing_key.as_bytes())?;
            let complete = state.remove_v2_receipts.contains_key(&plan.id);
            if input.mode == "dry-run" {
                return Ok(site_removal_status(&plan, state.pending_remove_v2.as_ref(), token, complete, Vec::new()));
            }
            if input.operation_id.as_deref() != Some(plan.id.as_str())
                || input
                    .preflight_token
                    .as_deref()
                    .is_none_or(|supplied| !crate::auth::constant_time_eq(supplied, &token))
            {
                return Err(s3_error!(
                    PreconditionFailed,
                    "execute requires the current operationId and preflightToken"
                ));
            }
            if complete {
                return Ok(site_removal_status(&plan, None, token, true, Vec::new()));
            }
            if state.pending_remove_v2.is_none() {
                probe_removal_peers(&plan, &state).await?;
                let planned = plan.clone();
                let local_id = local.deployment_id;
                update_site_replication_state(move |state| admit_site_removal(state, planned, &local_id, true)).await?;
            }
            let errors = drive_site_removal_v2_locked(&plan.id).await?;
            let final_state = load_site_replication_state().await?;
            Ok(site_removal_status(
                &plan,
                final_state.pending_remove_v2.as_ref(),
                token,
                final_state.remove_v2_receipts.contains_key(&plan.id),
                errors,
            ))
        })
        .await?;
        owned_json_response(StatusCode::OK, &result)
    }
}

async fn probe_removal_peers(plan: &SiteRemovalPlan, state: &SiteReplicationState) -> S3Result<()> {
    let access_key = state
        .pending_remove
        .as_ref()
        .map(|pending| pending.service_account_access_key.as_str())
        .unwrap_or(state.service_account_access_key.as_str());
    // A two-site decommission has no remote participant and requires no peer secret.
    let required = required_removal_peers(plan);
    if required.is_empty() {
        return Ok(());
    }
    let secret = site_replicator_service_account_secret(access_key).await?;
    for id in required {
        let peer = plan
            .original_peers
            .get(&id)
            .ok_or_else(|| s3_error!(InvalidRequest, "required peer is missing"))?;
        let connection = runtime_peer_connection(peer)?;
        let body = PeerAdminRequest::get(&connection, SITE_REMOVE_V2_CAPABILITY_PATH, access_key)
            .send_get(&secret)
            .await?;
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| s3_error!(NotImplemented, "peer does not advertise durable removal"))?;
        if value["contractVersion"] != 2 || value["durableCleanup"] != true {
            return Err(s3_error!(NotImplemented, "every participating peer must support durable removal v2"));
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl Operation for SiteRemovalV2PeerHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        let input: SiteRemovalPeerRequest = read_site_replication_json(req, "", false).await?;
        if !matches!(input.phase.as_str(), "prepare" | "commit") {
            return Err(s3_error!(InvalidRequest, "phase must be prepare or commit"));
        }
        let _lifecycle = SiteReplicationLifecycleGuard::acquire().await?;
        let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store is unavailable"))?;
        let result = with_removal_admission(store, move || async move {
            let state = load_site_replication_state().await?;
            let local = current_local_runtime_peer(&state);
            validate_removal_plan(&input.plan, &local.deployment_id)?;
            let digest = site_removal_digest(&input.plan)?;
            if let Some(receipt) = state.remove_v2_receipts.get(&input.plan.id) {
                if receipt.digest != digest {
                    return Err(s3_error!(PreconditionFailed, "operationId was used for another removal"));
                }
                return peer_removal_response(&input.plan, true, true);
            }
            if let Some(pending) = &state.pending_remove_v2 {
                if pending.coordinator || site_removal_digest(&pending.plan)? != digest {
                    return Err(s3_error!(PreconditionFailed, "another site removal is pending"));
                }
            } else {
                if input.phase == "commit" {
                    return Err(s3_error!(PreconditionFailed, "site removal was not prepared"));
                }
                let plan = input.plan.clone();
                let local_id = local.deployment_id;
                update_site_replication_state(move |state| admit_site_removal(state, plan, &local_id, false)).await?;
            }
            cleanup_site_removal_v2_local(&input.plan.id).await?;
            if input.phase == "commit" {
                let id = input.plan.id.clone();
                update_site_replication_state(move |state| {
                    let pending = state
                        .pending_remove_v2
                        .as_mut()
                        .filter(|pending| pending.plan.id == id)
                        .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed"))?;
                    pending.commit_decided = true;
                    finish_site_removal(state, &id)
                })
                .await?;
            }
            peer_removal_response(&input.plan, true, input.phase == "commit")
        })
        .await?;
        owned_json_response(StatusCode::OK, &result)
    }
}

fn peer_removal_response(plan: &SiteRemovalPlan, cleaned: bool, committed: bool) -> S3Result<SiteRemovalPeerResponse> {
    Ok(SiteRemovalPeerResponse {
        contract_version: 2,
        operation_id: plan.id.clone(),
        digest: site_removal_digest(plan)?,
        cleanup_complete: cleaned,
        committed,
    })
}

/// Called under the distributed lifecycle admission lock. Peer IO never holds the state lock.
pub(super) async fn drive_site_removal_v2_locked(id: &str) -> S3Result<Vec<String>> {
    let state = load_site_replication_state().await?;
    let Some(pending) = state.pending_remove_v2.as_ref().filter(|pending| pending.plan.id == id) else {
        return Ok(Vec::new());
    };
    if !pending.coordinator {
        cleanup_site_removal_v2_local(id).await?;
        return Ok(Vec::new());
    }
    let mut errors = Vec::new();
    if let Err(err) = cleanup_site_removal_v2_local(id).await {
        errors.push(summarize_peer_error_detail(&format!("local cleanup failed: {err}")));
    }
    for phase in ["prepare", "commit"] {
        let state = load_site_replication_state().await?;
        let Some(pending) = state.pending_remove_v2.as_ref().filter(|pending| pending.plan.id == id) else {
            break;
        };
        if phase == "commit" && !pending.commit_decided {
            break;
        }
        let peers = required_removal_peers(&pending.plan);
        let acked = if phase == "prepare" {
            &pending.cleaned_peers
        } else {
            &pending.committed_peers
        };
        let mut candidates = pending.secret_candidates.clone();
        if let Ok(secret) = site_replicator_service_account_secret(&pending.access_key).await {
            push_unique_secret_candidate(&mut candidates, secret);
        }
        if candidates != pending.secret_candidates {
            let id = id.to_string();
            let saved = candidates.clone();
            update_site_replication_state(move |state| {
                let pending = state
                    .pending_remove_v2
                    .as_mut()
                    .filter(|pending| pending.plan.id == id)
                    .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed before secret recording"))?;
                pending.secret_candidates = saved;
                Ok(())
            })
            .await?;
        }
        for peer_id in peers.difference(acked) {
            let peer = pending
                .plan
                .original_peers
                .get(peer_id)
                .ok_or_else(|| s3_error!(InvalidRequest, "required peer is missing"))?;
            let request = SiteRemovalPeerRequest {
                phase: phase.to_string(),
                plan: pending.plan.clone(),
            };
            let result = async {
                let connection = runtime_peer_connection(peer)?;
                let body = PeerAdminRequest::put(&connection, SITE_PEER_REMOVE_V2_PATH, &pending.access_key)
                    .send_with_secret_candidates(&candidates, &request)
                    .await?;
                let response: SiteRemovalPeerResponse = serde_json::from_slice(&body)
                    .map_err(|_| s3_error!(InternalError, "invalid durable removal acknowledgement"))?;
                if response.contract_version != 2
                    || response.operation_id != id
                    || response.digest != site_removal_digest(&pending.plan)?
                    || !response.cleanup_complete
                    || (phase == "commit" && !response.committed)
                {
                    return Err(s3_error!(InternalError, "durable removal acknowledgement does not match the operation"));
                }
                Ok::<(), S3Error>(())
            }
            .await;
            match result {
                Ok(()) => {
                    let id = id.to_string();
                    let peer_id = peer_id.clone();
                    let commit = phase == "commit";
                    update_site_replication_state_when_changed(move |state| {
                        let pending = state
                            .pending_remove_v2
                            .as_mut()
                            .filter(|pending| pending.plan.id == id)
                            .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed before acknowledgement"))?;
                        let inserted = if commit {
                            pending.committed_peers.insert(peer_id)
                        } else {
                            pending.cleaned_peers.insert(peer_id)
                        };
                        Ok(if inserted {
                            StateCommit::Changed(())
                        } else {
                            StateCommit::Unchanged(())
                        })
                    })
                    .await?;
                }
                Err(err) => errors.push(summarize_peer_error_detail(&format!("peer {} {phase} failed: {err}", peer.name))),
            }
        }
        if phase == "prepare" {
            let id = id.to_string();
            update_site_replication_state_when_changed(move |state| {
                let pending = state
                    .pending_remove_v2
                    .as_mut()
                    .filter(|pending| pending.plan.id == id)
                    .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed before decision"))?;
                if pending.commit_decided
                    || !pending.local_cleanup_complete
                    || !required_removal_peers(&pending.plan).is_subset(&pending.cleaned_peers)
                {
                    return Ok(StateCommit::Unchanged(()));
                }
                pending.commit_decided = true;
                Ok(StateCommit::Changed(()))
            })
            .await?;
        }
    }
    let id = id.to_string();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_remove_v2.as_ref().filter(|pending| pending.plan.id == id) else {
            return Ok(StateCommit::Unchanged(()));
        };
        if !pending.commit_decided || !required_removal_peers(&pending.plan).is_subset(&pending.committed_peers) {
            return Ok(StateCommit::Unchanged(()));
        }
        finish_site_removal(state, &id)?;
        Ok(StateCommit::Changed(()))
    })
    .await?;
    Ok(errors)
}

async fn cleanup_site_removal_v2_local(id: &str) -> S3Result<()> {
    let state = load_site_replication_state().await?;
    let pending = state
        .pending_remove_v2
        .as_ref()
        .filter(|pending| pending.plan.id == id)
        .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed before cleanup"))?;
    if pending.local_cleanup_complete {
        return Ok(());
    }
    let local = current_local_runtime_peer(&state);
    let removed: HashSet<String> = if pending.plan.removed.contains(&local.deployment_id) {
        pending
            .plan
            .original_peers
            .keys()
            .filter(|id| **id != local.deployment_id)
            .cloned()
            .collect()
    } else {
        pending.plan.removed.iter().cloned().collect()
    };
    let _bucket_operations = SITE_REPLICATION_BUCKET_OP_LOCK.write().await;
    #[cfg(test)]
    if FAIL_NEXT_LOCAL_REMOVAL_CLEANUP.swap(false, std::sync::atomic::Ordering::SeqCst) {
        return Err(s3_error!(InternalError, "injected local cleanup I/O failure"));
    }
    cleanup_removed_site_replication_buckets(&removed).await?;
    let id = id.to_string();
    update_site_replication_state(move |state| {
        let pending = state
            .pending_remove_v2
            .as_mut()
            .filter(|pending| pending.plan.id == id)
            .ok_or_else(|| s3_error!(PreconditionFailed, "site removal changed after cleanup"))?;
        pending.local_cleanup_complete = true;
        Ok(())
    })
    .await
}

pub(super) async fn resume_site_removal_v2(pending: &PendingRemoveV2) -> S3Result<()> {
    let store = current_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store is unavailable"))?;
    let id = pending.plan.id.clone();
    with_removal_admission(store, move || async move {
        let errors = drive_site_removal_v2_locked(&id).await?;
        if !errors.is_empty() {
            warn!(event = EVENT_ADMIN_SITE_REPLICATION_STATE, component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION, result = "decommission_still_pending", operation_id = %id,
                error_count = errors.len(), "site replication decommission is pending");
        }
        Ok::<(), S3Error>(())
    })
    .await
}
