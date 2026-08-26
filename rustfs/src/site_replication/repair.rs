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

pub(crate) const SITE_REPLICATION_REPAIR_STATE_PATH: &str = "config/site-replication/repair-state.json";

pub(crate) const SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH: &str = "config/site-replication/repair-execution.lock";

pub(crate) const SITE_REPLICATION_REPAIR_OPERATION_LIMIT: usize = 32;

pub(crate) const SITE_REPLICATION_REPAIR_IAM_FAMILY: &str = "iam";

pub(crate) const SITE_REPLICATION_REPAIR_BUCKET_FAMILY: &str = "bucket";

pub(crate) const SITE_REPLICATION_REPAIR_BUCKET_METADATA_FAMILY: &str = "bucket-metadata";

pub(crate) const SITE_REPLICATION_REPAIR_REPLICATION_FAMILY: &str = "replication";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) operations: BTreeMap<String, SiteReplicationRepairOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairOperation {
    pub(crate) operation_id: String,
    pub(crate) preflight_token: String,
    pub(crate) plan_token: String,
    pub(crate) status: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) sites: BTreeMap<String, SiteReplicationRepairSiteStatus>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairSiteStatus {
    pub(crate) deployment_id: String,
    pub(crate) name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) families: BTreeMap<String, SiteReplicationRepairFamilyStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairFamilyStatus {
    pub(crate) planned: usize,
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    #[serde(default)]
    pub(crate) retry_events: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) tasks: Vec<SiteReplicationRepairTaskStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairTaskStatus {
    pub(crate) task_id: String,
    pub(crate) status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SiteReplicationRepairRequest {
    pub(crate) mode: SiteReplicationRepairMode,
    #[serde(default)]
    pub(crate) preflight_token: Option<String>,
    #[serde(default)]
    pub(crate) operation_id: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SiteReplicationRepairMode {
    DryRun,
    Execute,
}

pub(crate) struct SiteReplicationRepairExecutionRequest {
    pub(crate) local_peer: PeerInfo,
    pub(crate) preflight_token: String,
    pub(crate) operation_id: String,
    pub(crate) signing_key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairPreflight {
    pub(crate) mode: &'static str,
    pub(crate) status: &'static str,
    pub(crate) preflight_token: String,
    pub(crate) retry_events: usize,
    pub(crate) sites: BTreeMap<String, SiteReplicationRepairSiteStatus>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairOperationResponse {
    pub(crate) mode: &'static str,
    pub(crate) operation_id: String,
    pub(crate) status: String,
    pub(crate) sites: BTreeMap<String, SiteReplicationRepairSiteResponse>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairSiteResponse {
    pub(crate) deployment_id: String,
    pub(crate) name: String,
    pub(crate) families: BTreeMap<String, SiteReplicationRepairFamilyResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SiteReplicationRepairFamilyResponse {
    pub(crate) planned: usize,
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    pub(crate) retry_events: usize,
    pub(crate) tasks: Vec<SiteReplicationRepairTaskStatus>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) errors: Vec<String>,
}

pub(crate) async fn load_site_replication_repair_state_from_store(store: Arc<ECStore>) -> S3Result<SiteReplicationRepairState> {
    match read_config_no_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH).await {
        Ok(data) => serde_json::from_slice(&data).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication repair state: {e}"))
        }),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationRepairState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication repair state: {err}"),
        )),
    }
}

pub(crate) async fn save_site_replication_repair_state_to_store(
    store: Arc<ECStore>,
    state: &SiteReplicationRepairState,
) -> S3Result<()> {
    let data = serde_json::to_vec(state)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair state failed: {e}")))?;
    save_config_no_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH, data)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save repair state failed: {e}")))
}

pub(crate) async fn read_site_replication_repair_state() -> S3Result<SiteReplicationRepairState> {
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    let read_store = store.clone();
    with_config_object_read_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH.to_string(), move || async move {
        load_site_replication_repair_state_from_store(read_store).await
    })
    .await
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock repair state failed: {e}")))?
}

pub(crate) async fn update_site_replication_repair_state<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationRepairState) -> S3Result<T> + Send + 'static,
{
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    let read_store = store.clone();
    let save_store = store.clone();
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH.to_string(), move || async move {
        let mut state = load_site_replication_repair_state_from_store(read_store).await?;
        let result = update(&mut state)?;
        save_site_replication_repair_state_to_store(save_store, &state).await?;
        Ok(result)
    })
    .await
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock repair state failed: {e}")))?
}

pub(crate) enum SiteReplicationRepairTask<'a> {
    Iam(&'a SRIAMItem),
    BucketMake(&'a str),
    BucketMetadata(&'a SRBucketMeta),
    Replication(&'a str),
}

impl SiteReplicationRepairTask<'_> {
    pub(crate) fn family(&self) -> &'static str {
        match self {
            Self::Iam(_) => SITE_REPLICATION_REPAIR_IAM_FAMILY,
            Self::BucketMake(_) => SITE_REPLICATION_REPAIR_BUCKET_FAMILY,
            Self::BucketMetadata(_) => SITE_REPLICATION_REPAIR_BUCKET_METADATA_FAMILY,
            Self::Replication(_) => SITE_REPLICATION_REPAIR_REPLICATION_FAMILY,
        }
    }

    pub(crate) fn path(&self) -> &str {
        match self {
            Self::Iam(_) => "/rustfs/admin/v3/site-replication/peer/iam-item",
            Self::BucketMake(path) | Self::Replication(path) => path,
            Self::BucketMetadata(_) => "/rustfs/admin/v3/site-replication/peer/bucket-meta",
        }
    }

    pub(crate) fn id(&self) -> S3Result<String> {
        let payload = match self {
            Self::Iam(item) => serde_json::to_vec(item),
            Self::BucketMake(_) | Self::Replication(_) => serde_json::to_vec(&serde_json::json!({})),
            Self::BucketMetadata(item) => serde_json::to_vec(item),
        }
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair task failed: {err}")))?;
        let mut digest = Sha256::new();
        digest.update(self.family().as_bytes());
        digest.update([0]);
        digest.update(self.path().as_bytes());
        digest.update([0]);
        digest.update(payload);
        Ok(URL_SAFE_NO_PAD.encode_to_string(digest.finalize()))
    }

    pub(crate) async fn send(&self, transport: &PeerTransport, access_key: &str, secret_key: &str) -> S3Result<Vec<u8>> {
        match self {
            Self::Iam(item) => {
                PeerAdminRequest::put(&transport.connection, self.path(), access_key)
                    .with_client(&transport.client)
                    .send(secret_key, item)
                    .await
            }
            Self::BucketMetadata(item) => {
                PeerAdminRequest::put(&transport.connection, self.path(), access_key)
                    .with_client(&transport.client)
                    .send(secret_key, item)
                    .await
            }
            Self::BucketMake(_) | Self::Replication(_) => {
                PeerAdminRequest::put(&transport.connection, self.path(), access_key)
                    .with_client(&transport.client)
                    .send(secret_key, &serde_json::json!({}))
                    .await
            }
        }
    }
}

pub(crate) fn site_replication_repair_tasks(plan: &SiteReplicationBootstrapPlan) -> Vec<(usize, SiteReplicationRepairTask<'_>)> {
    let mut tasks = Vec::with_capacity(
        plan.iam_items.len() + plan.bucket_make_ops.len() + plan.bucket_items.len() + plan.bucket_configure_ops.len(),
    );
    tasks.extend(
        plan.iam_items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, SiteReplicationRepairTask::Iam(item))),
    );
    tasks.extend(
        plan.bucket_make_ops
            .iter()
            .enumerate()
            .map(|(index, path)| (index, SiteReplicationRepairTask::BucketMake(path))),
    );
    tasks.extend(
        plan.bucket_items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, SiteReplicationRepairTask::BucketMetadata(item))),
    );
    tasks.extend(
        plan.bucket_configure_ops
            .iter()
            .enumerate()
            .map(|(index, path)| (index, SiteReplicationRepairTask::Replication(path))),
    );
    tasks
}

pub(crate) fn site_replication_repair_plan_token(
    state: &SiteReplicationState,
    plan: &SiteReplicationBootstrapPlan,
) -> S3Result<String> {
    let mut digest = Sha256::new();
    let snapshot = serde_json::to_vec(&(
        &state.name,
        &state.service_account_access_key,
        &state.peers,
        state.updated_at,
        state.sync_state_initialized,
    ))
    .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair snapshot failed: {err}")))?;
    digest.update(snapshot);
    for (_, task) in site_replication_repair_tasks(plan) {
        digest.update(task.id()?.as_bytes());
    }
    Ok(URL_SAFE_NO_PAD.encode_to_string(digest.finalize()))
}

pub(crate) fn site_replication_repair_preflight_token(
    state: &SiteReplicationState,
    plan: &SiteReplicationBootstrapPlan,
    signing_key: &[u8],
) -> S3Result<String> {
    if signing_key.is_empty() {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            "repair signing key is empty".to_string(),
        ));
    }
    let mut digest = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(signing_key)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid repair signing key".to_string()))?;
    digest.update(b"rustfs:site-replication:repair-preflight:v1\0");
    digest.update(site_replication_repair_plan_token(state, plan)?.as_bytes());
    for event in state
        .retry_queue
        .iter()
        .filter(|event| retry_event_replayed_by_bootstrap(event))
    {
        digest.update(event.id.as_bytes());
        digest.update(&[0]);
        digest.update(event.peer_deployment_id.as_bytes());
        digest.update(&[0]);
        digest.update(event.path.as_bytes());
        digest.update(&[0]);
    }
    Ok(URL_SAFE_NO_PAD.encode_to_string(digest.finalize().into_bytes()))
}

pub(crate) fn site_replication_repair_task_checkpoint_id(
    signing_key: &[u8],
    peer_deployment_id: &str,
    task: &SiteReplicationRepairTask<'_>,
) -> S3Result<String> {
    let mut digest = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(signing_key)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid repair signing key".to_string()))?;
    digest.update(b"rustfs:site-replication:repair-task:v1\0");
    digest.update(peer_deployment_id.as_bytes());
    digest.update(&[0]);
    digest.update(task.id()?.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode_to_string(digest.finalize().into_bytes()))
}

pub(crate) fn site_replication_repair_sites(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    plan: &SiteReplicationBootstrapPlan,
    signing_key: &[u8],
) -> S3Result<BTreeMap<String, SiteReplicationRepairSiteStatus>> {
    let mut planned = BTreeMap::new();
    let mut family_paths = BTreeMap::<String, BTreeSet<String>>::new();
    for (_, task) in site_replication_repair_tasks(plan) {
        let family = task.family().to_string();
        let family_status = planned
            .entry(task.family().to_string())
            .or_insert_with(SiteReplicationRepairFamilyStatus::default);
        family_status.planned += 1;
        family_paths.entry(family).or_default().insert(task.path().to_string());
    }

    let mut sites = BTreeMap::new();
    for peer in state.peers.values().filter(|peer| {
        peer.deployment_id != local_peer.deployment_id && !same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
    }) {
        let mut families = planned.clone();
        for (_, task) in site_replication_repair_tasks(plan) {
            let family = families
                .get_mut(task.family())
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair task family is missing".to_string()))?;
            family.tasks.push(SiteReplicationRepairTaskStatus {
                task_id: site_replication_repair_task_checkpoint_id(signing_key, &peer.deployment_id, &task)?,
                status: "planned".to_string(),
                error: None,
            });
        }
        for (family, status) in &mut families {
            status.retry_events = state
                .retry_queue
                .iter()
                .filter(|event| {
                    event.peer_deployment_id == peer.deployment_id
                        && retry_event_replayed_by_bootstrap(event)
                        && family_paths.get(family).is_some_and(|paths| paths.contains(&event.path))
                })
                .count();
        }
        sites.insert(
            peer.deployment_id.clone(),
            SiteReplicationRepairSiteStatus {
                deployment_id: peer.deployment_id.clone(),
                name: peer.name.clone(),
                families,
            },
        );
    }
    Ok(sites)
}

pub(crate) fn update_site_replication_repair_task(
    operation: &mut SiteReplicationRepairOperation,
    deployment_id: &str,
    family: &str,
    family_index: usize,
    result: Result<(), &str>,
) -> S3Result<()> {
    let site = operation
        .sites
        .get_mut(deployment_id)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation site is missing".to_string()))?;
    let family_status = site
        .families
        .get_mut(family)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation family is missing".to_string()))?;
    if family_status.succeeded != family_index {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            "repair operation task checkpoint is invalid".to_string(),
        ));
    }
    let task_status = family_status.tasks.get_mut(family_index).ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InternalError, "repair operation task checkpoint is missing".to_string())
    })?;
    family_status.failed = 0;
    family_status.errors.clear();
    match result {
        Ok(()) => {
            family_status.succeeded = family_status.succeeded.saturating_add(1);
            task_status.status = "succeeded".to_string();
            task_status.error = None;
        }
        Err(error) => {
            let error = classify_site_replication_repair_error(error).to_string();
            family_status.failed = 1;
            family_status.errors.push(error.clone());
            task_status.status = "failed".to_string();
            task_status.error = Some(error);
        }
    }
    Ok(())
}

pub(crate) fn site_replication_repair_task_pending(
    operation: &SiteReplicationRepairOperation,
    deployment_id: &str,
    family: &str,
    family_index: usize,
) -> S3Result<bool> {
    let site = operation
        .sites
        .get(deployment_id)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation site is missing".to_string()))?;
    let family = site
        .families
        .get(family)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation family is missing".to_string()))?;
    if family.succeeded > family_index {
        return Ok(false);
    }
    if family.succeeded < family_index {
        return Ok(false);
    }
    Ok(family.failed == 0)
}

pub(crate) fn prepare_site_replication_repair_retry(operation: &mut SiteReplicationRepairOperation) {
    for family in operation.sites.values_mut().flat_map(|site| site.families.values_mut()) {
        family.failed = 0;
        family.errors.clear();
        for task in &mut family.tasks {
            match task.status.as_str() {
                "succeeded" => task.status = "skipped".to_string(),
                "failed" => {
                    task.status = "planned".to_string();
                    task.error = None;
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn classify_site_replication_repair_error(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("accessdenied")
        || error.contains("signaturedoesnotmatch")
        || error.contains("unauthorized")
        || error.contains("forbidden")
        || error.contains("401")
        || error.contains("403")
    {
        "authorization-failed"
    } else if error.contains("timeout") {
        "remote-timeout"
    } else if error.contains("dns") {
        "remote-dns-failed"
    } else if error.contains("tls") || error.contains("certificate") {
        "remote-tls-failed"
    } else if error.contains("connect") {
        "remote-connect-failed"
    } else {
        "remote-operation-failed"
    }
}

pub(crate) fn summarize_site_replication_repair_operation(operation: &mut SiteReplicationRepairOperation) {
    let failed = operation
        .sites
        .values()
        .flat_map(|site| site.families.values())
        .any(|family| family.failed > 0);
    let complete = operation
        .sites
        .values()
        .all(|site| site.families.values().all(|family| family.succeeded == family.planned));
    operation.status = if complete {
        "success"
    } else if failed {
        "partial"
    } else {
        "running"
    }
    .to_string();
    operation.updated_at = Some(OffsetDateTime::now_utc());
    operation.completed_at = complete.then_some(OffsetDateTime::now_utc());
}

pub(crate) fn site_replication_repair_operation_response(
    operation: &SiteReplicationRepairOperation,
) -> SiteReplicationRepairOperationResponse {
    SiteReplicationRepairOperationResponse {
        mode: "execute",
        operation_id: operation.operation_id.clone(),
        status: operation.status.clone(),
        sites: operation
            .sites
            .iter()
            .map(|(deployment_id, site)| {
                (
                    deployment_id.clone(),
                    SiteReplicationRepairSiteResponse {
                        deployment_id: site.deployment_id.clone(),
                        name: site.name.clone(),
                        families: site
                            .families
                            .iter()
                            .map(|(family, status)| {
                                (
                                    family.clone(),
                                    SiteReplicationRepairFamilyResponse {
                                        planned: status.planned,
                                        succeeded: status.succeeded,
                                        failed: status.failed,
                                        retry_events: status.retry_events,
                                        tasks: status.tasks.clone(),
                                        errors: status.errors.clone(),
                                    },
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
        created_at: operation.created_at,
        updated_at: operation.updated_at,
        completed_at: operation.completed_at,
    }
}

pub(crate) fn prune_site_replication_repair_operations(operations: &mut BTreeMap<String, SiteReplicationRepairOperation>) {
    while operations.len() > SITE_REPLICATION_REPAIR_OPERATION_LIMIT {
        let Some(oldest) = operations
            .iter()
            .filter(|(_, operation)| operation.status == "success")
            .min_by_key(|(_, operation)| operation.created_at)
            .map(|(id, _)| id.clone())
        else {
            break;
        };
        operations.remove(&oldest);
    }
}

pub(crate) async fn persist_site_replication_repair_operation(operation: &SiteReplicationRepairOperation) -> S3Result<()> {
    let operation = operation.clone();
    update_site_replication_repair_state(move |state| {
        if let Some(existing) = state.operations.get(&operation.operation_id)
            && !constant_time_eq(&existing.preflight_token, &operation.preflight_token)
        {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        state.operations.insert(operation.operation_id.clone(), operation);
        prune_site_replication_repair_operations(&mut state.operations);
        Ok(())
    })
    .await
}

pub(crate) async fn persist_site_replication_repair_task(
    operation: &SiteReplicationRepairOperation,
    peer: &PeerInfo,
    family: &str,
    path: &str,
) -> S3Result<()> {
    persist_site_replication_repair_operation(operation).await?;

    let family_status = operation
        .sites
        .get(&peer.deployment_id)
        .and_then(|site| site.families.get(family))
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair task status is missing".to_string()))?;
    let failure = (family_status.failed > 0).then(|| {
        family_status
            .errors
            .first()
            .cloned()
            .unwrap_or_else(|| "remote-operation-failed".to_string())
    });
    let peer = peer.clone();
    let path = path.to_string();
    update_site_replication_state(move |state| {
        match failure.as_deref() {
            Some(error) => upsert_site_replication_retry_event(&mut state.retry_queue, &peer, &path, error, None),
            None => {
                dequeue_site_replication_retry_events_including_escalated(&mut state.retry_queue, &peer, &path);
            }
        }
        Ok(())
    })
    .await
}

pub(crate) fn admit_site_replication_repair_operation(
    repair_state: &mut SiteReplicationRepairState,
    operation_id: String,
    supplied_token: &str,
    candidate: SiteReplicationRepairOperation,
) -> S3Result<SiteReplicationRepairOperation> {
    if let Some(existing) = repair_state.operations.get(&operation_id) {
        if !constant_time_eq(&existing.preflight_token, supplied_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        if !constant_time_eq(&existing.plan_token, &candidate.plan_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::PreconditionFailed,
                "site replication repair plan changed after partial execution".to_string(),
            ));
        }
        return Ok(existing.clone());
    }
    if repair_state
        .operations
        .values()
        .any(|operation| operation.status == "running")
    {
        return Err(S3Error::with_message(
            S3ErrorCode::ClientTokenConflict,
            "another site replication repair is active".to_string(),
        ));
    }
    repair_state.operations.insert(operation_id, candidate.clone());
    prune_site_replication_repair_operations(&mut repair_state.operations);
    Ok(candidate)
}

pub(crate) async fn execute_site_replication_repair(
    request: SiteReplicationRepairExecutionRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        execute_site_replication_repair_locked(request).await
    })
    .await
    .map_err(|_| {
        S3Error::with_message(S3ErrorCode::ClientTokenConflict, "another site replication repair is active".to_string())
    })?
}

pub(crate) async fn execute_site_replication_repair_locked(
    request: SiteReplicationRepairExecutionRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "site replication is not configured"));
    }
    let info = build_sr_info(&state, &request.local_peer).await?;
    let plan = site_replication_bootstrap_plan(&info)?;
    let plan_token = site_replication_repair_plan_token(&state, &plan)?;
    let preflight_token = site_replication_repair_preflight_token(&state, &plan, request.signing_key.as_bytes())?;
    let sites = site_replication_repair_sites(&state, &request.local_peer, &plan, request.signing_key.as_bytes())?;

    let repair_state = read_site_replication_repair_state().await?;
    if let Some(existing) = repair_state.operations.get(&request.operation_id) {
        if !constant_time_eq(&existing.preflight_token, &request.preflight_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        if existing.status == "success" {
            return json_response(StatusCode::OK, &site_replication_repair_operation_response(existing));
        }
        if !constant_time_eq(&existing.plan_token, &plan_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::PreconditionFailed,
                "site replication repair plan changed after partial execution".to_string(),
            ));
        }
    } else if !constant_time_eq(&request.preflight_token, &preflight_token) {
        return Err(S3Error::with_message(
            S3ErrorCode::PreconditionFailed,
            "site replication repair preflight is stale".to_string(),
        ));
    }

    let now = OffsetDateTime::now_utc();
    let candidate = SiteReplicationRepairOperation {
        operation_id: request.operation_id.clone(),
        preflight_token,
        plan_token,
        status: "running".to_string(),
        sites,
        created_at: Some(now),
        updated_at: Some(now),
        completed_at: None,
    };
    let supplied_token = request.preflight_token;
    let operation_id = request.operation_id;
    let mut operation = update_site_replication_repair_state(move |repair_state| {
        admit_site_replication_repair_operation(repair_state, operation_id, &supplied_token, candidate)
    })
    .await?;
    if operation.status == "success" {
        return json_response(StatusCode::OK, &site_replication_repair_operation_response(&operation));
    }

    let service_account_secret_key = site_replicator_service_account_secret(&state.service_account_access_key).await?;
    prepare_site_replication_repair_retry(&mut operation);
    operation.status = "running".to_string();
    operation.completed_at = None;
    operation.updated_at = Some(OffsetDateTime::now_utc());
    persist_site_replication_repair_operation(&operation).await?;

    let tasks = site_replication_repair_tasks(&plan);
    for peer in state.peers.values().filter(|peer| {
        peer.deployment_id != request.local_peer.deployment_id
            && !same_identity_endpoint(&peer.endpoint, &request.local_peer.endpoint)
    }) {
        let transport = match PeerTransport::for_runtime_peer(peer).await {
            Ok(transport) => transport,
            Err(err) => {
                let error = err.to_string();
                for (family_index, task) in &tasks {
                    if !site_replication_repair_task_pending(&operation, &peer.deployment_id, task.family(), *family_index)? {
                        continue;
                    }
                    update_site_replication_repair_task(
                        &mut operation,
                        &peer.deployment_id,
                        task.family(),
                        *family_index,
                        Err(&error),
                    )?;
                    summarize_site_replication_repair_operation(&mut operation);
                    persist_site_replication_repair_task(&operation, peer, task.family(), task.path()).await?;
                }
                continue;
            }
        };

        for (family_index, task) in &tasks {
            if !site_replication_repair_task_pending(&operation, &peer.deployment_id, task.family(), *family_index)? {
                continue;
            }
            let result = task
                .send(&transport, &state.service_account_access_key, &service_account_secret_key)
                .await;
            let error = result.err().map(|err| err.to_string());
            update_site_replication_repair_task(
                &mut operation,
                &peer.deployment_id,
                task.family(),
                *family_index,
                match error.as_deref() {
                    Some(error) => Err(error),
                    None => Ok(()),
                },
            )?;
            summarize_site_replication_repair_operation(&mut operation);
            persist_site_replication_repair_task(&operation, peer, task.family(), task.path()).await?;
        }
    }

    summarize_site_replication_repair_operation(&mut operation);
    persist_site_replication_repair_operation(&operation).await?;
    json_response(StatusCode::OK, &site_replication_repair_operation_response(&operation))
}
