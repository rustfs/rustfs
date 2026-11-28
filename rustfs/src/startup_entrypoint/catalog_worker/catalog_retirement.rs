// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Private operator evidence and original application owners for D09 process tests.

use super::*;
use crate::app::vector_usecase::{CatalogWorkerBucketDeleteReplay, catalog_worker_capture_bucket_delete};
use std::sync::atomic::{AtomicBool, Ordering};

const MAX_CURSORS: usize = 32;
const MAX_GATE_EVENTS: usize = 256;
const MAX_GATE_PAUSE: Duration = Duration::from_secs(300);
type EvidenceReader = CatalogRetirementEvidenceReader;

#[derive(Default)]
pub(super) struct Registry {
    captured: BTreeMap<String, Arc<Captured>>,
    cursors: BTreeMap<String, (String, CatalogRetirementEvidenceCursor)>,
    gates: BTreeMap<String, Arc<Gate>>,
}

struct Captured {
    bucket: VectorBucketBindingV1,
    resource: Option<VectorMaintenanceResourceV1>,
    runtime_token: Option<String>,
    execution_identity: Option<VectorMaintenanceExecutionIdentityV1>,
    reader: EvidenceReader,
    replay: Mutex<Option<CatalogWorkerBucketDeleteReplay>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    AuthorityPublished,
    BeforeTargetDispatch,
    TargetNativeReturned,
    TargetProgressPublished,
    CatalogPreparationCompleted,
}
impl Phase {
    fn observed(self) -> CatalogRetirementObservedPhase {
        match self {
            Self::AuthorityPublished => CatalogRetirementObservedPhase::AuthorityPublished,
            Self::BeforeTargetDispatch => CatalogRetirementObservedPhase::BeforeTargetDispatch,
            Self::TargetNativeReturned => CatalogRetirementObservedPhase::TargetNativeReturned,
            Self::TargetProgressPublished => CatalogRetirementObservedPhase::TargetProgressPublished,
            Self::CatalogPreparationCompleted => CatalogRetirementObservedPhase::CatalogPreparationCompleted,
        }
    }
}

#[derive(Default, Serialize)]
struct GateState {
    hits: u32,
    arrivals: u32,
    released: bool,
    timed_out: bool,
    overflow: bool,
    cancelled: bool,
    observation_failed: bool,
    sequence: u32,
    observation: Option<CatalogRetirementObservation>,
}
struct Gate {
    captured_token: String,
    phase: CatalogRetirementObservedPhase,
    occurrence: u32,
    pause: Duration,
    released: AtomicBool,
    wake: Notify,
    state: Mutex<GateState>,
    observer: Mutex<Option<CatalogRetirementObservationHandle>>,
}
impl Gate {
    fn selected(&self, event: &CatalogRetirementObservation) -> bool {
        if event.phase != self.phase {
            return false;
        }
        match event.phase {
            CatalogRetirementObservedPhase::TargetNativeReturned => event.target.as_ref().is_some_and(|target| {
                matches!(target.fenced_delete, Some(CatalogRetirementObservedOutcome::Succeeded))
                    && matches!(target.subsequent_fence, Some(CatalogRetirementObservedOutcome::Succeeded))
            }),
            CatalogRetirementObservedPhase::TargetProgressPublished => event.target.as_ref().is_some_and(|target| {
                matches!(target.fenced_delete, Some(CatalogRetirementObservedOutcome::Succeeded))
                    && matches!(target.subsequent_fence, Some(CatalogRetirementObservedOutcome::Succeeded))
                    && matches!(target.progress_publish, Some(CatalogRetirementObservedOutcome::Succeeded))
                    && serde_json::to_value(&target.progress)
                        .ok()
                        .and_then(|value| {
                            value["targets"]
                                .get(usize::from(target.ordinal))
                                .and_then(|entry| entry["state"].as_str())
                                .map(str::to_owned)
                        })
                        .as_deref()
                        == Some("DELETE_ACKNOWLEDGED")
            }),
            _ => true,
        }
    }

    async fn checkpoint(&self, event: CatalogRetirementObservation) -> TableCatalogStoreResult<()> {
        let should_pause = {
            let mut state = self.state.lock().expect("cleanup observation state");
            if state.sequence == u32::try_from(MAX_GATE_EVENTS).expect("bounded gate events") {
                state.overflow = true;
                state.observation_failed = true;
                return Err(TableCatalogStoreError::Unavailable(
                    "Catalog cleanup observation event budget exhausted".into(),
                ));
            }
            state.sequence += 1;
            if !self.selected(&event) {
                return Ok(());
            }
            state.hits += 1;
            if state.hits == self.occurrence {
                state.arrivals += 1;
                state.observation = Some(event);
                true
            } else {
                false
            }
        };
        if !should_pause {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.pause;
        loop {
            let notified = self.wake.notified();
            if self.released.load(Ordering::Acquire) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                let mut state = self.state.lock().expect("cleanup observation state");
                state.timed_out = true;
                return Err(TableCatalogStoreError::Unavailable("Catalog cleanup observation pause timed out".into()));
            }
        }
    }
    fn release(&self, cancelled: bool) {
        self.released.store(true, Ordering::Release);
        let mut state = self.state.lock().expect("cleanup observation state");
        state.released = true;
        state.cancelled |= cancelled;
        drop(state);
        self.wake.notify_waiters();
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        for gate in self.gates.values() {
            gate.release(true);
        }
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.release(true);
    }
}

impl Worker {
    fn d09_captured(&self, token: &str) -> WorkerResult<Arc<Captured>> {
        self.state
            .lock()
            .expect("worker state")
            .catalog_retirement
            .captured
            .get(token)
            .cloned()
            .ok_or_else(|| invalid("unknown D09 captured_token"))
    }

    fn d09_retain(&self, captured: Captured) -> WorkerResult<Value> {
        let mut state = self.state.lock().expect("worker state");
        let registry = &mut state.catalog_retirement;
        if registry.captured.len() >= MAX_SESSIONS {
            return Err(invalid("D09 capture capacity reached"));
        }
        let token = Uuid::new_v4().to_string();
        let output = json!({"captured_token":token,"bucket":captured.bucket,"resource":captured.resource,
            "execution_identity":captured.execution_identity,"instance_id":self.store.id,"channel":"private_operator_evidence"});
        registry.captured.insert(token, Arc::new(captured));
        Ok(output)
    }

    pub(super) async fn d09_capture(&self, runtime_token: String, resource: VectorMaintenanceResourceV1) -> WorkerResult<Value> {
        resource.validate()?;
        let driver = self.maintenance(&runtime_token)?;
        let bucket = driver.resolve_bucket(&resource.bucket().public_name).await?;
        if &bucket != resource.bucket() {
            return Err(TableCatalogStoreError::Conflict("D09 capture bucket identity changed".into()).into());
        }
        let selected = match driver.resolve_resource(&bucket.public_name, resource.name()).await {
            Ok(selected) => selected,
            Err(_) => {
                let deleted = driver.inspect_deleted_resource(&resource, None).await?;
                if serde_json::to_value(&deleted)?["resource"] != serde_json::to_value(&resource)? {
                    return Err(invalid("D09 deleted capture differs from the selected resource"));
                }
                resource.clone()
            }
        };
        if selected != resource {
            return Err(TableCatalogStoreError::Conflict("D09 capture Index identity changed".into()).into());
        }
        let identity = driver.execution_identity().clone();
        let replay = catalog_worker_capture_bucket_delete(self.server_ctx.clone(), &bucket.to_handle()?, &identity).await?;
        let reader = EvidenceReader::new(
            EcStoreTableCatalogObjectBackend::new(self.store.clone()),
            bucket.clone(),
            Some(resource.clone()),
            None,
        )?;
        self.d09_retain(Captured {
            bucket,
            resource: Some(resource),
            runtime_token: Some(runtime_token),
            execution_identity: Some(identity),
            reader,
            replay: Mutex::new(Some(replay)),
        })
    }

    /// The existing private socket operator can reopen exact retained evidence after deletion.
    /// This path never creates an application authorization or a deletion owner.
    pub(super) async fn d09_reopen(
        &self,
        bucket: VectorBucketBindingV1,
        resource: Option<VectorMaintenanceResourceV1>,
        authority_ref: CatalogMetadataRefRecordV1,
    ) -> WorkerResult<Value> {
        bucket.to_handle()?;
        if resource.as_ref().is_some_and(|resource| resource.bucket() != &bucket) {
            return Err(invalid("D09 reopen resource differs from its complete bucket"));
        }
        let reader = EvidenceReader::new(
            EcStoreTableCatalogObjectBackend::new(self.store.clone()),
            bucket.clone(),
            resource.clone(),
            Some(authority_ref),
        )?;
        reader.start().await?;
        self.d09_retain(Captured {
            bucket,
            resource,
            runtime_token: None,
            execution_identity: None,
            reader,
            replay: Mutex::new(None),
        })
    }

    pub(super) async fn d09_publication_state(&self, captured_token: &str) -> WorkerResult<Value> {
        let captured = self.d09_captured(captured_token)?;
        captured.bucket.to_handle()?;
        let app = self
            .server_ctx
            .installed_app_context()
            .ok_or_else(|| invalid("missing installed worker context"))?;
        if !Arc::ptr_eq(&app.object_store(), &self.store) {
            return Err(invalid("publication observation store differs from the installed context"));
        }
        let client = crate::runtime_sources::current_lock_client().ok_or_else(|| invalid("missing local lock client"))?;
        let installed = app
            .lock_client()
            .handle()
            .ok_or_else(|| invalid("missing installed lock client"))?;
        if !Arc::ptr_eq(&client, &installed) {
            return Err(invalid("publication observation lock client differs from the installed context"));
        }
        let endpoints = self
            .store
            .instance_endpoints()
            .ok_or_else(|| invalid("missing actual lock endpoint topology"))?;
        let local_endpoints: usize = endpoints
            .as_ref()
            .iter()
            .map(|pool| pool.endpoints.as_ref().iter().filter(|endpoint| endpoint.is_local).count())
            .sum();
        if local_endpoints != 1 {
            return Err(invalid("publication observation requires one actual local endpoint"));
        }
        let local_node_name = crate::runtime_sources::current_local_node_name()
            .await
            .filter(|name| !name.is_empty())
            .ok_or_else(|| invalid("missing actual local lock owner name"))?;
        let resource = rustfs_lock::ObjectKey::new(
            captured.bucket.storage_bucket_id.clone(),
            rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH,
        );
        let leases = publication_leases(Some(client), &resource).await?;
        Ok(json!({
            "channel":"private_operator_evidence","captured_token":captured_token,
            "bucket":captured.bucket,"instance_id":self.store.id,
            "node":{"node_id":self.node_id,"boot_id":self.boot_id,"pid":std::process::id()},
            "local_node_name":local_node_name,"resource":resource,
            "client_local":true,"client_online":true,"leases":leases,
        }))
    }

    pub(super) async fn d09_snapshot(&self, captured_token: String, cursor: Option<String>) -> WorkerResult<Value> {
        let captured = self.d09_captured(&captured_token)?;
        let previous = if let Some(cursor) = cursor {
            let (owner, cursor) = self
                .state
                .lock()
                .expect("worker state")
                .catalog_retirement
                .cursors
                .remove(&cursor)
                .ok_or_else(|| invalid("unknown or consumed D09 snapshot cursor"))?;
            if owner != captured_token {
                return Err(invalid("D09 snapshot cursor belongs to another capture"));
            }
            cursor
        } else {
            captured.reader.start().await?
        };
        let (mut page, next) = captured.reader.next(previous).await?;
        let next_cursor = if let Some(next) = next {
            let mut state = self.state.lock().expect("worker state");
            let cursors = &mut state.catalog_retirement.cursors;
            if cursors.len() >= MAX_CURSORS {
                return Err(invalid("D09 snapshot cursor capacity reached"));
            }
            let token = Uuid::new_v4().to_string();
            cursors.insert(token.clone(), (captured_token, next));
            Some(token)
        } else {
            None
        };
        page["instance_id"] = json!(self.store.id);
        page["next_cursor"] = json!(next_cursor);
        page["channel"] = json!("private_operator_evidence");
        Ok(page)
    }

    pub(super) fn d09_gate_arm(
        &self,
        captured_token: String,
        phase: Phase,
        occurrence: NonZeroU32,
        pause_timeout_ms: u64,
    ) -> WorkerResult<Value> {
        let captured = self.d09_captured(&captured_token)?;
        let pause = Duration::from_millis(pause_timeout_ms);
        if occurrence.get() > u32::try_from(MAX_GATE_EVENTS).expect("bounded gate events")
            || pause.is_zero()
            || pause > MAX_GATE_PAUSE
        {
            return Err(invalid("D09 gate occurrence or pause exceeds the original bounds"));
        }
        if captured.execution_identity.is_none() {
            return Err(invalid("read-only D09 evidence cannot arm a mutation gate"));
        }
        let mut state = self.state.lock().expect("worker state");
        let gates = &mut state.catalog_retirement.gates;
        if gates.len() >= MAX_VECTOR_FAULTS {
            return Err(invalid("D09 gate capacity reached"));
        }
        let gate = Arc::new(Gate {
            captured_token: captured_token.clone(),
            phase: phase.observed(),
            occurrence: occurrence.get(),
            pause,
            released: AtomicBool::new(false),
            wake: Notify::new(),
            state: Mutex::new(GateState::default()),
            observer: Mutex::new(None),
        });
        let observer = Arc::downgrade(&gate);
        let handle = observe_catalog_retirement(
            captured.bucket.clone(),
            Arc::new(move |event| {
                let observer = observer.clone();
                Box::pin(async move {
                    if let Some(gate) = observer.upgrade() {
                        gate.checkpoint(event).await
                    } else {
                        Ok(())
                    }
                })
            }),
        )?;
        *gate.observer.lock().expect("cleanup observer handle") = Some(handle);
        let token = Uuid::new_v4().to_string();
        gates.insert(token.clone(), gate);
        Ok(json!({"fault_token":token,"captured_token":captured_token}))
    }

    fn d09_gate(&self, token: &str) -> WorkerResult<Arc<Gate>> {
        self.state
            .lock()
            .expect("worker state")
            .catalog_retirement
            .gates
            .get(token)
            .cloned()
            .ok_or_else(|| invalid("unknown D09 fault_token"))
    }
    pub(super) fn d09_gate_poll(&self, fault_token: &str) -> WorkerResult<Value> {
        let gate = self.d09_gate(fault_token)?;
        let mut value = serde_json::to_value(&*gate.state.lock().expect("cleanup observation state"))?;
        value["captured_token"] = json!(gate.captured_token);
        value["fault_token"] = json!(fault_token);
        value["managed_delete_diagnostics"] = gate
            .observer
            .lock()
            .expect("cleanup observer handle")
            .as_ref()
            .map(CatalogRetirementObservationHandle::managed_delete_diagnostics)
            .unwrap_or(Value::Null);
        Ok(value)
    }
    pub(super) async fn d09_delete_replay(&self, captured_token: &str, runtime_token: &str) -> WorkerResult<Value> {
        let captured = self.d09_captured(captured_token)?;
        let driver = self.maintenance(runtime_token)?;
        if captured.runtime_token.as_deref() != Some(runtime_token)
            || captured.execution_identity.as_ref() != Some(driver.execution_identity())
        {
            return Err(invalid("D09 deletion replay differs from its admitted runtime identity"));
        }
        let owner = captured
            .replay
            .lock()
            .expect("captured deletion owner")
            .take()
            .ok_or_else(|| invalid("D09 deletion owner is absent or already consumed"))?;
        let response = owner.execute(driver.execution_identity()).await?;
        if !matches!(response, crate::vector::protocol::Response::DeleteVectorBucket(_)) {
            return Err(invalid("D09 deletion returned another operation response"));
        }
        Ok(json!({"captured_token":captured_token,"bucket":captured.bucket,"completed":true}))
    }
}

async fn publication_leases(
    client: Option<Arc<dyn rustfs_lock::LockClient>>,
    resource: &rustfs_lock::ObjectKey,
) -> WorkerResult<Vec<Value>> {
    let client = client.ok_or_else(|| invalid("missing local publication lease client"))?;
    if !client.is_local().await || !client.is_online().await {
        return Err(invalid("publication lease client is not local and online"));
    }
    let mut selected = Vec::new();
    for lease in client.list_lock_leases().await {
        if &lease.resource != resource {
            continue;
        }
        if selected.len() == 2 {
            return Err(invalid("publication lease observation exceeds exact-resource bound"));
        }
        selected.push(json!({
            "resource":lease.resource,"lock_type":lease.lock_type,"owner":lease.owner,
            "guard_id":lease.guard_id,"acquired_at":lease.acquired_at,
            "remaining_ttl_ms":u64::try_from(lease.remaining_ttl.as_millis()).map_err(invalid)?,
        }));
    }
    Ok(selected)
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use rustfs_lock::{LockClient, LockRequest, LockType, ObjectKey, client::local::LocalClient};

    #[tokio::test]
    async fn d09_publication_lease_observation_requires_client_and_preserves_real_grants() {
        let client = Arc::new(LocalClient::new());
        let resource = ObjectKey::new(
            format!("d09-{}", Uuid::new_v4()),
            rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH,
        );
        assert!(publication_leases(None, &resource).await.is_err());
        assert!(
            publication_leases(Some(client.clone()), &resource)
                .await
                .expect("read exact local resource")
                .is_empty()
        );
        let request = LockRequest::new(resource.clone(), LockType::Exclusive, "actual-owner").with_ttl(Duration::from_secs(30));
        assert!(
            client
                .acquire_lock(&request)
                .await
                .expect("acquire actual local grant")
                .success
        );
        let before = client
            .check_status(&request.lock_id)
            .await
            .expect("read actual granted lock")
            .expect("actual grant exists");
        let observed = publication_leases(Some(client.clone()), &resource)
            .await
            .expect("read exact local resource");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0]["resource"], json!(resource));
        assert_eq!(observed[0]["owner"], "actual-owner");
        assert_eq!(observed[0]["lock_type"], "Exclusive");
        assert!(observed[0]["guard_id"].as_u64().is_some());
        assert!(observed[0]["remaining_ttl_ms"].as_u64().expect("remaining TTL milliseconds") > 0);
        assert_eq!(observed[0]["acquired_at"], json!(before.acquired_at));
        let foreign = ObjectKey::new("another-bucket", resource.object.clone());
        assert!(
            publication_leases(Some(client.clone()), &foreign)
                .await
                .expect("read another exact resource")
                .is_empty()
        );
        let after = client
            .check_status(&request.lock_id)
            .await
            .expect("read actual granted lock")
            .expect("actual grant exists");
        assert_eq!(after.id, before.id);
        assert_eq!(after.last_refreshed, before.last_refreshed);
        assert_eq!(after.expires_at, before.expires_at);
        assert_eq!(after.status, before.status);
        assert!(client.release(&request.lock_id).await.expect("release actual original grant"));
        assert!(
            publication_leases(Some(client), &resource)
                .await
                .expect("read original resource after release")
                .is_empty()
        );
    }
}
