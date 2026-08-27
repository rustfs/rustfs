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

pub(crate) const SITE_REPLICATION_PEER_EDIT_PATH: &str = "/rustfs/admin/v3/site-replication/peer/edit";

/// Peer-edit fencing token, carried as query parameters so a peer that predates
/// the fence simply ignores them (unknown query keys are dropped) and keeps the
/// previous last-writer-wins behaviour.
pub(crate) const SITE_REPLICATION_EDIT_ORIGIN_QUERY: &str = "editOrigin";

pub(crate) const SITE_REPLICATION_EDIT_GENERATION_QUERY: &str = "editGeneration";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct SiteReplicationState {
    pub(crate) name: String,
    pub(crate) service_account_access_key: String,
    #[serde(default, skip_serializing)]
    pub(crate) service_account_secret_key: String,
    pub(crate) service_account_parent: String,
    pub(crate) peers: BTreeMap<String, PeerInfo>,
    pub(crate) updated_at: Option<OffsetDateTime>,
    pub(crate) resync_status: BTreeMap<String, SRResyncOpStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pending_rotation: Option<PendingRotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pending_remove: Option<PendingRemove>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pending_endpoint_refresh: Option<PendingEndpointRefresh>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) retry_queue: Vec<SiteReplicationRetryEvent>,
    #[serde(default)]
    pub(crate) sync_state_initialized: bool,
    /// Fencing token for peer-edit delivery, allocated inside the state
    /// transaction (the distributed state-object lock). Two nodes of THIS
    /// site that accept admin edits concurrently therefore get strictly
    /// ordered generations, and a delivery that stalls can be recognised as
    /// stale by the receiving site.
    #[serde(default)]
    pub(crate) edit_generation: u64,
    /// Per-origin high-water mark of the peer edits already applied here,
    /// keyed by the origin site's deployment id. A delivery whose generation
    /// is not above the mark arrived out of order and must not overwrite the
    /// newer edit that already landed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) applied_edit_generations: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PendingEndpointRefresh {
    pub(crate) id: String,
    pub(crate) peer: PeerInfo,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) remote_peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) acked_deployment_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PendingRotation {
    pub(crate) id: String,
    pub(crate) access_key: String,
    pub(crate) parent: String,
    pub(crate) new_secret_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) secret_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) acked_deployment_ids: BTreeSet<String>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PendingRemove {
    pub(crate) id: String,
    pub(crate) req: SRRemoveReq,
    pub(crate) service_account_access_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) secret_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) original_peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) acked_deployment_ids: BTreeSet<String>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<OffsetDateTime>,
}

impl SiteReplicationState {
    pub(crate) fn enabled(&self) -> bool {
        self.peers.len() > 1
    }
}

pub(crate) fn parse_site_replication_state(data: &[u8]) -> S3Result<SiteReplicationState> {
    let mut state: SiteReplicationState = serde_json::from_slice(data)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication state: {e}")))?;
    state.peers = normalize_peer_map_by_identity(state.peers);
    // A peer-edit high-water mark only fences a CURRENT peer. A site that
    // leaves drops below two peers, which clears its own state object and
    // restarts its generation counter — a mark left over from the previous
    // membership must not reject the edits it sends after it rejoins. This
    // pruning covers departures THIS site observed; an origin removed
    // unilaterally elsewhere stays in this peer map with its mark, and the
    // wall-clock floor in `next_peer_edit_generation` is what lifts its
    // restarted counter over that mark. Dropping departed origins on load
    // also keeps the map bounded.
    state
        .applied_edit_generations
        .retain(|origin, _| state.peers.contains_key(origin));
    if !state.sync_state_initialized {
        if state.enabled() {
            mark_unknown_peer_sync_enabled(&mut state.peers);
        }
        state.sync_state_initialized = true;
    }
    Ok(state)
}

pub(crate) async fn load_site_replication_state() -> S3Result<SiteReplicationState> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_admin_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => parse_site_replication_state(&data),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

/// Whether this deployment participates in site replication (two or more
/// peers in the persisted state). Read by the S3 interface layer to gate
/// replication-config edits (MinIO `ErrReplicationDenyEditError` semantics,
/// issue #1948); a state-read failure propagates so the gate fails closed.
pub(crate) async fn site_replication_enabled() -> S3Result<bool> {
    Ok(load_site_replication_state().await?.enabled())
}

/// Deployment ids of the remote peers the reconciler derives a
/// `site-repl-<id>` rule for on every bucket (the same peer filter as
/// `build_site_replication_config`); empty when site replication is not
/// enabled. Read by the bucket usecase so an S3 replication-config edit keeps
/// exactly the reconciler-owned rules (issue #1948); a state-read failure
/// propagates so the edit fails closed.
pub(crate) async fn site_replication_edit_context() -> S3Result<(HashSet<String>, OperatorRuleContract)> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        // Enabled without a service account is a state this site cannot
        // broadcast from either; the peers are still the reconciler's.
        let state = load_site_replication_state().await?;
        if !state.enabled() {
            return Ok((HashSet::new(), OperatorRuleContract::Derived));
        }
        let peers = remote_peer_deployment_ids(&state, &current_local_runtime_peer(&state));
        return Ok((peers, OperatorRuleContract::Legacy));
    };
    let peers = remote_peer_deployment_ids(&runtime.state, &runtime.local_peer);
    let contract = site_replication_operator_rule_contract(&runtime).await;
    Ok((peers, contract))
}

/// Whether every remote peer merges replication configs under the derived
/// contract, probed through the peer capability endpoint. A peer that does
/// not (or cannot be asked) pins the cluster to [`OperatorRuleContract::Legacy`]
/// for this edit: consistency across sites wins over keeping the operator's
/// priority values, and the legacy merge keeps their order anyway.
pub(crate) async fn site_replication_operator_rule_contract(runtime: &SiteReplicationRuntime) -> OperatorRuleContract {
    let remote_peers: Vec<&PeerInfo> = runtime
        .state
        .peers
        .values()
        .filter(|peer| {
            peer.deployment_id != runtime.local_peer.deployment_id
                && !same_identity_endpoint(&peer.endpoint, &runtime.local_peer.endpoint)
        })
        .collect();
    let probes = futures::future::join_all(remote_peers.iter().map(|peer| async move {
        let transport = PeerTransport::for_runtime_peer(peer).await?;
        let (status, body) = PeerAdminRequest::put(
            &transport.connection,
            SITE_REPLICATION_PEER_DERIVED_RULE_CONTRACT_CAPABILITY_PATH,
            &runtime.state.service_account_access_key,
        )
        .with_client(&transport.client)
        .send_raw(&runtime.service_account_secret_key, Some(&()))
        .await?;
        peer_capability_response_supported(peer, status, &body)
    }))
    .await;
    operator_rule_contract_from_probes(remote_peers.into_iter().zip(probes))
}

pub(crate) fn operator_rule_contract_from_probes<'a>(
    probes: impl IntoIterator<Item = (&'a PeerInfo, S3Result<bool>)>,
) -> OperatorRuleContract {
    for (peer, probe) in probes {
        match probe {
            Ok(true) => {}
            Ok(false) => return OperatorRuleContract::Legacy,
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    result = "derived_rule_contract_probe_failed",
                    peer = %peer.endpoint,
                    error = %err,
                    "admin site replication state"
                );
                return OperatorRuleContract::Legacy;
            }
        }
    }
    OperatorRuleContract::Derived
}

pub(crate) fn remote_peer_deployment_ids(state: &SiteReplicationState, local_peer: &PeerInfo) -> HashSet<String> {
    state
        .peers
        .values()
        .filter(|peer| {
            peer.deployment_id != local_peer.deployment_id && !same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
        })
        .map(|peer| peer.deployment_id.clone())
        .collect()
}

/// Deployment ids of every site in the cluster, this one included: the set
/// a peer's derived rules can name (its rule towards this site carries this
/// site's id). Empty when site replication is not enabled.
pub(crate) async fn site_replication_deployment_ids() -> S3Result<HashSet<String>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() {
        return Ok(HashSet::new());
    }
    Ok(state.peers.values().map(|peer| peer.deployment_id.clone()).collect())
}

pub(crate) async fn load_site_replication_state_no_lock(store: Arc<ECStore>) -> S3Result<SiteReplicationState> {
    match read_config_no_lock(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => parse_site_replication_state(&data),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

/// Persist-or-clear under an already-held state object lock. Normalizes the
/// peer map exactly once (the historical persist path normalized twice with
/// two full clones — P2-22).
pub(crate) async fn persist_site_replication_state_no_lock(store: Arc<ECStore>, mut state: SiteReplicationState) -> S3Result<()> {
    state.peers = normalize_peer_map_by_identity(state.peers);
    if state.peers.len() <= 1 && state.pending_rotation.is_none() && state.pending_remove.is_none() {
        match delete_config_no_lock(store, SITE_REPLICATION_STATE_PATH).await {
            Ok(()) | Err(StorageError::ConfigNotFound) => Ok(()),
            Err(err) => Err(S3Error::with_message(S3ErrorCode::InternalError, format!("clear state failed: {err}"))),
        }
    } else {
        let data = serde_json::to_vec(&state)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize state failed: {e}")))?;
        save_config_no_lock(store, SITE_REPLICATION_STATE_PATH, data)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save state failed: {e}")))
    }
}

/// What a state transaction closure decided to do with the state it was
/// handed. `Unchanged` skips the write entirely: the ack markers and the
/// pending-clearing paths run on every retry and mostly find their pending id
/// already gone, and the retry queue shares this object — rewriting it byte
/// for byte only makes those misses contend with the writers that do have
/// something to say.
pub(crate) enum StateCommit<T> {
    Changed(T),
    Unchanged(T),
}

/// The site-replication state RMW transaction: load, mutate, persist — all
/// under the distributed state-object write lock (see
/// crate::site_replication::state_lock). No peer network calls and no other
/// config locks inside `update`; anything that has to talk to a peer belongs
/// between two transactions, with the precondition re-checked inside the
/// second one.
pub(crate) async fn update_site_replication_state<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationState) -> S3Result<T> + Send + 'static,
{
    update_site_replication_state_when_changed(move |state| update(state).map(StateCommit::Changed)).await
}

/// [`update_site_replication_state`] for closures that may find nothing to
/// do — see [`StateCommit`].
pub(crate) async fn update_site_replication_state_when_changed<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationState) -> S3Result<StateCommit<T>> + Send + 'static,
{
    with_site_replication_state_lock(move || async move {
        let store = current_object_store_handle()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
        let mut state = load_site_replication_state_no_lock(store.clone()).await?;
        match update(&mut state)? {
            StateCommit::Changed(result) => {
                persist_site_replication_state_no_lock(store, state).await?;
                Ok(result)
            }
            StateCommit::Unchanged(result) => Ok(result),
        }
    })
    .await
}

/// Test-only seeding of the state object. Every production write goes through
/// [`update_site_replication_state`] — this helper is `cfg(test)` so a new
/// call site cannot reintroduce the pre-P1-15 shape (load through one object
/// lock, save through another, with the mutation in between unprotected).
#[cfg(test)]
pub(crate) async fn save_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let mut normalized = state.clone();
    normalized.peers = normalize_peer_map_by_identity(normalized.peers);

    let data = serde_json::to_vec(&normalized)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize state failed: {e}")))?;
    save_admin_config(store, SITE_REPLICATION_STATE_PATH, data)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save state failed: {e}")))?;
    Ok(())
}

pub(crate) fn request_endpoint(uri: &Uri, headers: &HeaderMap) -> String {
    let scheme = get_source_scheme(headers)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
        })
        .or_else(|| uri.scheme_str().map(str::to_ascii_lowercase))
        .unwrap_or_else(|| {
            if runtime_tls_enabled() {
                "https".to_string()
            } else {
                "http".to_string()
            }
        });

    let host = headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| uri.authority().map(|value| value.as_str().to_string()))
        .or_else(|| {
            current_endpoints_handle().and_then(|endpoints| {
                endpoints
                    .as_ref()
                    .iter()
                    .flat_map(|pool| pool.endpoints.as_ref().iter())
                    .find(|endpoint| endpoint.is_local)
                    .map(|endpoint| endpoint.host_port())
            })
        })
        .unwrap_or_else(|| format!("127.0.0.1:{}", current_runtime_port()));

    format!("{scheme}://{host}")
}

pub(crate) fn runtime_console_port() -> Option<u16> {
    let console_address = get_config_snapshot()
        .map(|snapshot| snapshot.console_address.clone())
        .unwrap_or_else(|| rustfs_utils::get_env_str(ENV_RUSTFS_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ADDRESS));

    let parse_target = if console_address.starts_with(':') {
        format!("127.0.0.1{console_address}")
    } else {
        console_address
    };

    Url::parse(&format!("http://{parse_target}"))
        .ok()
        .and_then(|parsed| parsed.port_or_known_default())
}

pub(crate) fn site_replication_local_endpoint(uri: &Uri, headers: &HeaderMap) -> String {
    let endpoint = request_endpoint(uri, headers);
    match Url::parse(&endpoint) {
        Ok(mut parsed) => {
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return request_endpoint(&Uri::from_static("/"), &HeaderMap::new());
            }
            if parsed.port_or_known_default() == runtime_console_port() && parsed.set_port(Some(current_runtime_port())).is_ok() {
                parsed.to_string().trim_end_matches('/').to_string()
            } else {
                endpoint
            }
        }
        Err(_) => request_endpoint(&Uri::from_static("/"), &HeaderMap::new()),
    }
}

pub(crate) fn current_local_runtime_endpoint() -> String {
    site_replication_local_endpoint(&Uri::from_static("/"), &HeaderMap::new())
}

pub(crate) fn infer_site_name(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn stored_peer_tls_settings(stored_peer: Option<&PeerInfo>) -> (bool, String) {
    stored_peer
        .map(|peer| (peer.skip_tls_verify, peer.ca_cert_pem.clone()))
        .unwrap_or_default()
}

/// The local peer record as the given state describes it. Split out of
/// [`current_local_peer`] so a state transaction can rebuild it against the
/// state it just loaded: the request the endpoint came from cannot cross into
/// the transaction closure, but the endpoint itself can.
pub(crate) fn local_peer_at_endpoint(endpoint: String, state: &SiteReplicationState) -> PeerInfo {
    let deployment_id = current_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
    let stored_peer = state.peers.get(&deployment_id);
    let (skip_tls_verify, ca_cert_pem) = stored_peer_tls_settings(stored_peer);

    PeerInfo {
        endpoint: endpoint.clone(),
        name: if state.name.is_empty() {
            stored_peer
                .map(|peer| peer.name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| infer_site_name(&endpoint))
        } else {
            state.name.clone()
        },
        deployment_id,
        sync_state: stored_peer.map(|peer| peer.sync_state.clone()).unwrap_or(SyncStatus::Unknown),
        default_bandwidth: stored_peer.map(|peer| peer.default_bandwidth.clone()).unwrap_or_default(),
        replicate_ilm_expiry: stored_peer.is_some_and(|peer| peer.replicate_ilm_expiry),
        object_naming_mode: stored_peer.map(|peer| peer.object_naming_mode.clone()).unwrap_or_default(),
        skip_tls_verify,
        ca_cert_pem,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

pub(crate) fn current_local_runtime_peer(state: &SiteReplicationState) -> PeerInfo {
    local_peer_at_endpoint(current_local_runtime_endpoint(), state)
}

pub(crate) fn normalize_peer_map_by_identity(peers: BTreeMap<String, PeerInfo>) -> BTreeMap<String, PeerInfo> {
    normalize_peer_map_by_identity_with(peers, normalize_peer_info)
}

pub(crate) fn normalize_peer_info(mut peer: PeerInfo) -> PeerInfo {
    if peer.deployment_id.is_empty() {
        peer.deployment_id = deployment_id_for_endpoint(&peer.endpoint);
    }
    if peer.name.is_empty() {
        peer.name = infer_site_name(&peer.endpoint);
    }
    if peer.api_version.is_none() {
        peer.api_version = Some(SITE_REPL_API_VERSION.to_string());
    }
    peer
}

pub(crate) async fn site_replicator_service_account_secret(access_key: &str) -> S3Result<String> {
    let Some(iam_sys) = current_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    iam_sys
        .get_site_replicator_service_account_secret(access_key)
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

pub(crate) fn legacy_site_replicator_state_secret(state: &SiteReplicationState) -> Option<String> {
    (state.service_account_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT && !state.service_account_secret_key.is_empty())
        .then(|| state.service_account_secret_key.clone())
}

pub(crate) fn pending_endpoint_refresh(state: &SiteReplicationState) -> Option<PendingEndpointRefresh> {
    state.pending_endpoint_refresh.clone().or_else(|| {
        state
            .retry_queue
            .iter()
            .find(|event| event.path == SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH)
            .and_then(|event| serde_json::from_str(&event.last_error).ok())
    })
}

/// The wall clock in unix nanoseconds, clamped into u64. A pre-1970 (or
/// post-2554) clock yields 0, which makes the hybrid allocation below
/// degrade to the plain `previous + 1` counter — monotone, never panicking.
pub(crate) fn edit_generation_wall_clock() -> u64 {
    u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(0)
}

/// Allocate the next peer-edit generation as a hybrid logical clock:
/// `max(wall clock in unix nanoseconds, previous + 1)`. Called inside the
/// state transaction, so the value is handed out under the distributed
/// state-object lock and two nodes of this site can never take the same one
/// (`previous + 1` keeps the sequence strictly increasing even when two
/// allocations land in one clock tick, and keeps it monotone on a node
/// whose clock stepped backwards mid-lifetime).
///
/// The wall-clock floor is what survives the counter's death. A site
/// removed while unreachable — the receiver never dropped it from its peer
/// map, so the load-time mark pruning in `parse_site_replication_state`
/// never fired — that later rejoins recreates its state object with the
/// counter back at zero. A plain counter would then hand out generations
/// below the receiver's stale high-water mark and every delivery would be
/// silently fenced until the counter caught up. Jumping to wall time clears
/// that mark: every value the deleted lifetime handed out was capped by the
/// wall clock at its own allocation (or by a prior lifetime's cap, applied
/// inductively), so the recreated lifetime's first allocation exceeds them
/// all — while a pre-removal delivery still in flight stays below the new
/// floor and remains correctly fenced. Marks recorded by pre-hybrid
/// receivers (small plain-counter values) sit far below any wall-clock
/// value, so a restarted origin passes those too — the fix needs only the
/// sender upgraded, nothing on the wire or in the receiver changed.
///
/// A wall clock that regresses across a delete/recreate (the recreating
/// node's clock behind the clock that fed the previous lifetime) mints
/// below the stale mark and the origin stays fenced — but only until real
/// time passes the previous lifetime's last allocation, because every later
/// allocation takes the wall-clock floor again (and never longer than
/// [`PEER_EDIT_FENCE_STALENESS_WINDOW_NANOS`]: a regression past the window
/// leaves the mark implausibly distant and the origin runs unfenced
/// immediately). Bounded by the skew,
/// self-healing, and no rollback window beyond the plain counter's: a
/// delivery applies only at or above the receiver's mark, so the one
/// cross-lifetime interleaving that can apply stale content — a
/// pre-removal delivery whose generation lands above everything the
/// regressed new lifetime has minted — required the same straggler landing
/// above the mark under the plain counter, where the recreated counter's
/// low restart made it strictly easier to hit.
pub(crate) fn next_peer_edit_generation(state: &mut SiteReplicationState) -> u64 {
    state.edit_generation = edit_generation_wall_clock().max(state.edit_generation.saturating_add(1));
    state.edit_generation
}

/// Build the peer-edit request path carrying the fencing token. The bare
/// constant stays the retry-queue key: the query only fences the wire
/// delivery, and a per-generation key would make every retry event unique.
/// Without a local deployment id there is nothing to fence against, so the
/// unstamped path is sent and the receiver keeps its pre-fence behaviour.
pub(crate) fn peer_edit_path_with_fence(origin: Option<&str>, generation: u64) -> String {
    let Some(origin) = origin.filter(|origin| !origin.is_empty()) else {
        return SITE_REPLICATION_PEER_EDIT_PATH.to_string();
    };
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair(SITE_REPLICATION_EDIT_ORIGIN_QUERY, origin)
        .append_pair(SITE_REPLICATION_EDIT_GENERATION_QUERY, &generation.to_string())
        .finish();
    format!("{SITE_REPLICATION_PEER_EDIT_PATH}?{query}")
}
