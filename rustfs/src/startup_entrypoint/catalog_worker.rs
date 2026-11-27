// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Private native-test IPC attached to the server's installed storage instance.

use crate::app::vector_usecase::maintenance::fault_test::{
    MaintenanceDriver, MaintenanceWriteFaultRequest, MaintenanceWrittenResultRequest,
};
use crate::app::{RequestEnvelope, RequestHead, object::ManagedEncryption};
use crate::server::ExternalRequestContextLayer;
use crate::startup_services::{ManagedLanceObjects, managed_lance_objects};
use crate::storage_api::startup::services::{
    AccountNamespaceId, BucketHandle, BucketIncarnation, BucketKind, BucketPublicName, ECStore, MakeBucketOptions,
    ManagedObjectAccess, ManagedObjectOperation, ManagedTableId, ReqInfo, RequestContext, ServerContextSlot, make_managed_bucket,
};
use crate::storage_api::table::RUSTFS_META_BUCKET;
use crate::table_catalog::test_support::catalog_write_faults::{
    CatalogWriteFaultAction, CatalogWriteFaultHandle, CatalogWriteFaultPhase,
};
use crate::table_catalog::test_support::{
    real_catalog_backend::{FaultPause, FaultPhase, RealCatalogBackend},
    vector_catalog_paths, vector_publication_lock_resources,
};
use crate::table_catalog::*;
use crate::vector::fault_test::{FaultEventKind, FaultHandle, FaultPoint, FaultSnapshot, SdkRequestFailureSource};
use crate::vector::lance::{
    LanceManifestAdapter, LanceManifestError, ManagedManifestObjects, ManifestIoLimits, ManifestPrepareRequest,
    VectorDistanceMetric, VectorIndexManifestSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as _,
    io,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Notify, Semaphore},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tower::Layer as _;
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 1024 * 1024;
const MAX_SESSIONS: usize = 64;
const MAX_PREPARED: usize = 256;
const MAX_REQUESTS: usize = 4096;
const MAX_VECTOR_FAULTS: usize = 32;
const IO_TIMEOUT: Duration = Duration::from_secs(15);
type Backend = RealCatalogBackend<EcStoreTableCatalogObjectBackend<ECStore>>;

mod catalog_retirement;
mod table_sentinel;

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error(transparent)]
    VectorProtocol(#[from] crate::vector::protocol::ProtocolError),
    #[error(transparent)]
    VectorFault(#[from] crate::vector::fault_test::FaultError),
    #[error(transparent)]
    Maintenance(#[from] VectorMaintenanceExecutionError),
    #[error(transparent)]
    Catalog(#[from] TableCatalogStoreError),
    #[error(transparent)]
    Lance(#[from] LanceManifestError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid worker request: {0}")]
    Invalid(String),
    #[error("worker operation deadline elapsed; mutation outcome requires inspection")]
    Timeout,
    #[error("worker operation cancelled; mutation outcome requires inspection")]
    Cancelled,
}
type WorkerResult<T> = Result<T, WorkerError>;
fn invalid(error: impl std::fmt::Display) -> WorkerError {
    WorkerError::Invalid(error.to_string())
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Encryption {
    #[default]
    Unencrypted,
    Aes256,
    AwsKms {
        key_id: String,
    },
}
impl Encryption {
    fn managed(&self) -> ManagedEncryption {
        match self {
            Self::Unencrypted => ManagedEncryption::Unencrypted,
            Self::Aes256 => ManagedEncryption::Aes256,
            Self::AwsKms { key_id } => ManagedEncryption::AwsKms { key_id: key_id.clone() },
        }
    }
    fn descriptor(&self) -> Value {
        match self {
            Self::Unencrypted => Value::Null,
            Self::Aes256 => json!({"sseType": "AES256"}),
            Self::AwsKms { key_id } => json!({"sseType": "aws:kms", "kmsKeyArn": key_id}),
        }
    }
    fn xml(&self) -> WorkerResult<Vec<u8>> {
        let inner = match self {
            Self::Unencrypted => return Ok(Vec::new()),
            Self::Aes256 => "<SSEAlgorithm>AES256</SSEAlgorithm>".into(),
            Self::AwsKms { key_id } => {
                if key_id.is_empty() || key_id.len() > 2048 || key_id.chars().any(|c| c.is_control() || "<>&\"'".contains(c)) {
                    return Err(invalid("invalid test KMS key identifier"));
                }
                format!("<SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{key_id}</KMSMasterKeyID>")
            }
        };
        Ok(format!("<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>{inner}</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>").into_bytes())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    bucket: VectorBucketBindingV1,
    table_id: String,
    index_name: String,
    dimension: u32,
    encryption: Encryption,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExpectedHead {
    version_token: String,
    generation: u64,
    metadata: Option<CatalogMetadataRefRecordV1>,
    descriptor: Option<CatalogFormatDescriptorRecordV1>,
}
impl ExpectedHead {
    fn from_catalog(head: &CatalogExpectedHead) -> Self {
        Self {
            version_token: head.version_token().as_str().into(),
            generation: head.generation().get(),
            metadata: head.metadata().map(Into::into),
            descriptor: head.format_descriptor().map(Into::into),
        }
    }
    fn to_catalog(&self, session: &Session) -> WorkerResult<CatalogExpectedHead> {
        let mut entry = session.initial_entry();
        entry.version_token = self.version_token.clone();
        entry.generation = self.generation;
        entry.metadata_location = self.metadata.as_ref().map(|m| m.object_key.clone()).unwrap_or_default();
        Ok(CatalogExpectedHead::from_entry(
            &entry,
            self.metadata.as_ref().map(CatalogMetadataRef::try_from).transpose()?,
            self.descriptor.as_ref().map(CatalogFormatDescriptor::try_from).transpose()?,
        )?)
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Target {
    Staged,
    Idempotency,
    Head,
    Finalization,
    IdempotencyFinalization,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Before,
    After,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Effect {
    Pause,
    Fail,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceWritePhase {
    Before,
    AfterNative,
    AfterFence,
    BeforeResponse,
}
impl From<MaintenanceWritePhase> for CatalogWriteFaultPhase {
    fn from(phase: MaintenanceWritePhase) -> Self {
        match phase {
            MaintenanceWritePhase::Before => Self::Before,
            MaintenanceWritePhase::AfterNative => Self::AfterNative,
            MaintenanceWritePhase::AfterFence => Self::AfterFence,
            MaintenanceWritePhase::BeforeResponse => Self::BeforeResponse,
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceWriteStage {
    AnnBuild,
    ReserveFragments,
    Rewrite,
}
impl From<MaintenanceWriteStage> for VectorMaintenanceTestStage {
    fn from(stage: MaintenanceWriteStage) -> Self {
        match stage {
            MaintenanceWriteStage::AnnBuild => Self::AnnBuild,
            MaintenanceWriteStage::ReserveFragments => Self::ReserveFragments,
            MaintenanceWriteStage::Rewrite => Self::Rewrite,
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MaintenanceWriteTarget {
    Staged { stage: MaintenanceWriteStage },
    Head { stage: MaintenanceWriteStage },
    Finalization { stage: MaintenanceWriteStage },
    Job,
    Result,
    Progress { authorization: CatalogMetadataRefRecordV1 },
}
impl TryFrom<MaintenanceWriteTarget> for VectorMaintenanceTestWriteTarget {
    type Error = TableCatalogStoreError;
    fn try_from(target: MaintenanceWriteTarget) -> Result<Self, Self::Error> {
        Ok(match target {
            MaintenanceWriteTarget::Staged { stage } => Self::Staged(stage.into()),
            MaintenanceWriteTarget::Head { stage } => Self::Head(stage.into()),
            MaintenanceWriteTarget::Finalization { stage } => Self::Finalization(stage.into()),
            MaintenanceWriteTarget::Job => Self::Job,
            MaintenanceWriteTarget::Result => Self::Result,
            MaintenanceWriteTarget::Progress { authorization } => Self::Progress {
                authorization: (&authorization).try_into()?,
            },
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    D09Capture {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
    },
    D09Reopen {
        bucket: VectorBucketBindingV1,
        resource: Option<VectorMaintenanceResourceV1>,
        authority_ref: CatalogMetadataRefRecordV1,
    },
    D09Snapshot {
        captured_token: String,
        cursor: Option<String>,
    },
    D09PublicationState {
        captured_token: String,
    },
    D09GateArm {
        captured_token: String,
        phase: catalog_retirement::Phase,
        occurrence: NonZeroU32,
        pause_timeout_ms: u64,
    },
    D09GatePoll {
        fault_token: String,
    },
    D09GateRelease {
        fault_token: String,
    },
    D09DeleteReplay {
        captured_token: String,
        runtime_token: String,
    },

    Ping,
    VectorHttpStart,
    MaintenanceOpen {
        execution_account: String,
    },
    MaintenanceTick {
        runtime_token: String,
    },
    MaintenanceCollect {
        runtime_token: String,
    },
    MaintenancePoll {
        runtime_token: String,
        operation_token: String,
    },
    MaintenanceStop {
        runtime_token: String,
    },
    VectorBucketResolve {
        runtime_token: String,
        bucket_name: String,
    },
    MaintenanceGcLockSelector {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
    },
    MaintenanceGcLockObserve {
        runtime_token: String,
    },
    VectorResolve {
        runtime_token: String,
        bucket_name: String,
        index_name: String,
    },
    VectorInspect {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        cursor: Option<String>,
    },
    VectorDeletedInspect {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        cursor: Option<String>,
    },
    TableSentinelCreate,
    TableSentinelOpen {
        identity: table_sentinel::Identity,
    },
    TableSentinelRead {
        sentinel_token: String,
    },
    M4WriteFaultArm {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
        attempt: NonZeroU16,
        target: MaintenanceWriteTarget,
        occurrence: NonZeroU16,
        phase: MaintenanceWritePhase,
        effect: Effect,
        pause_timeout_ms: Option<u64>,
    },
    M4WriteFaultPoll {
        fault_token: String,
    },
    M4WrittenResultRead {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
        attempt: NonZeroU16,
        content_sha256: String,
        content_length: u64,
    },
    M4WriteFaultRelease {
        fault_token: String,
    },
    M4FaultArm {
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        point: FaultPoint,
        occurrence: NonZeroU32,
        pause_timeout_ms: u64,
    },
    M4FaultPoll {
        fault_token: String,
    },
    M4FaultWait {
        fault_token: String,
        wait_timeout_ms: u64,
    },
    M4FaultRelease {
        fault_token: String,
    },
    SdkCandidateRetain {
        runtime_token: String,
        fault_token: String,
    },
    SdkCandidateInspect {
        runtime_token: String,
        fault_token: String,
    },
    SdkCandidateObject {
        runtime_token: String,
        fault_token: String,
        reference_ordinal: usize,
        content_offset: Option<u64>,
    },
    SdkCandidateQuota {
        runtime_token: String,
        fault_token: String,
    },
    SdkCandidateRelease {
        runtime_token: String,
        fault_token: String,
    },
    Create {
        public_name: String,
        index_name: String,
        dimension: u32,
        #[serde(default)]
        encryption: Encryption,
    },
    Open {
        identity: Identity,
    },
    Prepare {
        session_id: String,
        commit_id: String,
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        base: Option<Box<ExpectedHead>>,
    },
    Commit {
        prepared_token: String,
    },
    CandidateBeforeArm {
        session_id: String,
        commit_id: String,
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        pause_timeout_ms: Option<u64>,
    },
    Head {
        session_id: String,
    },
    Read {
        session_id: String,
    },
    Recovery {
        session_id: String,
    },
    Recover {
        session_id: String,
    },
    RawRead {
        session_id: String,
        bucket: String,
        key: String,
    },
    RawWrite {
        session_id: String,
        bucket: String,
        key: String,
        expected_etag: String,
        bytes_base64: String,
    },
    FaultArm {
        prepared_token: String,
        target: Target,
        phase: Phase,
        effect: Effect,
        #[serde(default)]
        pause_timeout_ms: Option<u64>,
    },
    FaultRelease {
        fault_id: String,
    },
    Events {
        session_id: String,
        #[serde(default)]
        after_sequence: usize,
    },
    Cancel {
        target_request_id: String,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u16,
    nonce: String,
    request_id: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    action: Action,
}

struct Session {
    identity: Identity,
    access: ManagedObjectAccess,
    resource: CatalogResourceIdentity,
    name: CatalogName,
    namespace: Namespace,
    index: VectorIndexManifestSpec,
    objects: Arc<ManagedLanceObjects>,
    backend: Backend,
    catalog: Arc<ObjectTableCatalogStore<Backend>>,
    adapter: LanceManifestAdapter<Backend>,
    writable_keys: Mutex<BTreeSet<String>>,
}
impl Session {
    fn bucket(&self) -> &str {
        self.access.bucket().storage_bucket_id().as_str()
    }
    fn head_key(&self) -> String {
        vector_catalog_paths().table_entry_path(self.bucket(), &self.namespace, &self.name)
    }
    fn initial_entry(&self) -> TableEntry {
        TableEntry {
            vector_index_definition: None,
            vector_maintenance_binding: None,
            vector_head_binding: None,
            version: 2,
            table_bucket: self.bucket().into(),
            namespace: "indexes".into(),
            table: self.name.as_str().into(),
            table_id: self.identity.table_id.clone(),
            table_uuid: self.identity.table_id.clone(),
            format: "lance".into(),
            format_version: 1,
            warehouse_location: format!("s3://{}/{}", self.bucket(), self.resource.dataset_prefix().trim_end_matches('/')),
            metadata_location: String::new(),
            version_token: "initial-token".into(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }
    async fn current_head(&self) -> WorkerResult<CatalogHead> {
        let head = self
            .catalog
            .read_catalog_head(self.access.bucket(), &self.namespace, &self.name)
            .await?;
        if head.resource_identity() != &self.resource {
            return Err(
                TableCatalogStoreError::Conflict("worker session resource differs from persisted Catalog head".into()).into(),
            );
        }
        Ok(head)
    }
    fn check_raw(&self, bucket: &str, key: &str, write: bool) -> WorkerResult<()> {
        CatalogObjectKey::try_new(key.into())?;
        let catalog_key = bucket == RUSTFS_META_BUCKET && self.writable_keys.lock().expect("registered keys").contains(key);
        let dataset_key = !write && bucket == self.bucket() && key.starts_with(&self.resource.dataset_prefix());
        if !catalog_key && !dataset_key {
            return Err(invalid("raw object is outside this registered fixture"));
        }
        Ok(())
    }
    async fn raw_read(&self, bucket: &str, key: &str) -> WorkerResult<Option<TableCatalogObject>> {
        self.check_raw(bucket, key, false)?;
        self.backend.revalidate_bucket_handle(self.access.bucket()).await?;
        if bucket == self.bucket() {
            return Ok(Some(
                self.objects
                    .get(&self.access, &CatalogObjectKey::try_new(key.into())?, None, None)
                    .await?,
            ));
        }
        // A paused publisher owns the Catalog write lock; evidence reads must remain possible.
        Ok(self
            .backend
            .read_object_unlocked_limited(bucket, key, MAX_OBJECT_BYTES)
            .await?)
    }
    async fn head_value(&self) -> WorkerResult<Value> {
        let head = self.current_head().await?;
        let key = self.head_key();
        let raw = self
            .raw_read(RUSTFS_META_BUCKET, &key)
            .await?
            .ok_or_else(|| invalid("head disappeared"))?;
        let entry: TableEntry = serde_json::from_slice(&raw.data)?;
        if entry.version_token != head.version_token().as_str()
            || entry.generation != head.generation().get()
            || entry.metadata_location != head.metadata().object_key().as_str()
            || entry.table_id != self.identity.table_id
        {
            return Err(TableCatalogStoreError::Conflict("head changed during evidence read".into()).into());
        }
        Ok(json!({"entry": entry, "expected_head": ExpectedHead::from_catalog(&head.expected_head()),
            "metadata": metadata_value(head.metadata()), "raw": object_value(RUSTFS_META_BUCKET, &key, &raw)}))
    }
}
#[derive(Clone)]
struct Prepared {
    session: Arc<Session>,
    commit: PreparedCatalogCommit,
}
#[derive(Serialize)]
struct Operation {
    request_id: String,
    action: String,
    started_micros: u128,
    completed_micros: Option<u128>,
    status: String,
    error: Option<Value>,
    #[serde(skip)]
    cancel: CancellationToken,
}

enum PauseHandle {
    Backend(FaultPause),
    Candidate(Arc<Notify>),
}

struct CandidateBefore {
    session_id: String,
    commit_id: String,
    idempotency_key: Option<String>,
    base: ExpectedHead,
    release: Arc<Notify>,
    timeout: Duration,
    hit: bool,
}

#[derive(Serialize)]
struct CandidateEvent {
    sequence: usize,
    elapsed_micros: u128,
    session_id: String,
    fault_id: String,
    request_id: String,
    commit_id: String,
    phase: &'static str,
}

struct CandidatePauseGuard<'a> {
    worker: &'a Worker,
    session_id: &'a str,
    fault_id: &'a str,
    request_id: &'a str,
    commit_id: &'a str,
    completed: bool,
}

impl Drop for CandidatePauseGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.worker.candidate_event(self, "before-cancelled");
        }
    }
}

enum VectorFaultEvidence {
    Live(Arc<FaultHandle>),
    Completed(Box<FaultSnapshot>),
}

struct VectorFaultState {
    evidence: VectorFaultEvidence,
    receipt: Option<Uuid>,
    receipt_busy: bool,
}

struct VectorFaultRecord {
    runtime_token: String,
    resource: VectorMaintenanceResourceV1,
    state: Mutex<VectorFaultState>,
}

impl VectorFaultRecord {
    fn new(runtime_token: String, resource: VectorMaintenanceResourceV1, handle: FaultHandle) -> Self {
        Self {
            runtime_token,
            resource,
            state: Mutex::new(VectorFaultState {
                evidence: VectorFaultEvidence::Live(Arc::new(handle)),
                receipt: None,
                receipt_busy: false,
            }),
        }
    }

    fn snapshot(&self) -> FaultSnapshot {
        match &self.state.lock().expect("fault record").evidence {
            VectorFaultEvidence::Live(handle) => handle.snapshot(),
            VectorFaultEvidence::Completed(snapshot) => snapshot.as_ref().clone(),
        }
    }

    fn release(&self) -> WorkerResult<FaultSnapshot> {
        let state = self.state.lock().expect("fault record");
        if state.receipt_busy {
            return Err(invalid("SDK receipt operation is in progress"));
        }
        Ok(match &state.evidence {
            VectorFaultEvidence::Live(handle) => handle.release(),
            VectorFaultEvidence::Completed(snapshot) => snapshot.as_ref().clone(),
        })
    }

    async fn wait(&self, timeout: Duration) -> WorkerResult<FaultSnapshot> {
        let handle = {
            let state = self.state.lock().expect("fault record");
            match &state.evidence {
                VectorFaultEvidence::Live(handle) => handle.clone(),
                VectorFaultEvidence::Completed(snapshot) => return Ok(snapshot.as_ref().clone()),
            }
        };
        Ok(handle.wait_arrival(timeout).await?)
    }

    fn begin_receipt(self: &Arc<Self>, runtime_token: &str) -> WorkerResult<SdkReceiptMutation> {
        if self.runtime_token != runtime_token {
            return Err(invalid("SDK candidate belongs to another maintenance runtime"));
        }
        let mut state = self.state.lock().expect("fault record");
        if state.receipt_busy {
            return Err(invalid("SDK receipt operation is in progress"));
        }
        state.receipt_busy = true;
        Ok(SdkReceiptMutation { record: self.clone() })
    }
}

/// Cancellation releases only the short receipt-operation admission. Evidence and owners stay retained.
struct SdkReceiptMutation {
    record: Arc<VectorFaultRecord>,
}

impl Drop for SdkReceiptMutation {
    fn drop(&mut self) {
        self.record.state.lock().expect("fault record").receipt_busy = false;
    }
}

fn sdk_candidate_token(
    snapshot: &FaultSnapshot,
    instance_id: Uuid,
    resource: &VectorMaintenanceResourceV1,
    execution_account: &str,
) -> WorkerResult<Uuid> {
    if snapshot.point != FaultPoint::SdkCandidatePrepared
        || snapshot.instance_id != instance_id
        || snapshot.resource.bucket != *resource.bucket()
        || &snapshot.resource.table_id != resource.table_id()
        || snapshot.resource.format != "lance"
        || snapshot.hits != u64::from(snapshot.occurrence)
        || snapshot.arrivals != 1
        || snapshot.completions != 1
        || !snapshot.injected_before_catalog
        || snapshot.released
        || snapshot.timed_out
        || snapshot.cancelled
        || snapshot.observation_failed
        || snapshot.overflow
        || snapshot.handle_dropped
        || snapshot.sdk_request_failure_error.is_some()
    {
        return Err(invalid("SDK candidate requires its complete successful original fault observation"));
    }
    let observation = snapshot
        .observation
        .as_ref()
        .ok_or_else(|| invalid("SDK candidate observation is absent"))?;
    let candidate = observation
        .sdk_candidate
        .as_ref()
        .ok_or_else(|| invalid("SDK candidate evidence is absent"))?;
    let request = observation
        .sdk_request_failure
        .as_ref()
        .ok_or_else(|| invalid("SDK request association is absent"))?;
    if request.observation_token != candidate.observation_token
        || request.commit_id != candidate.commit_id
        || request.request_digest_sha256 != candidate.request_digest_sha256
        || request.account != execution_account
        || request.request_id.is_nil()
        || request.source != SdkRequestFailureSource::InjectedBeforeCatalog
        || snapshot
            .events
            .iter()
            .filter(|event| matches!(event.kind, FaultEventKind::FailureInjected))
            .count()
            != 1
        || snapshot
            .events
            .iter()
            .filter(|event| matches!(event.kind, FaultEventKind::SdkRequestFailureObserved))
            .count()
            != 1
    {
        return Err(invalid("SDK candidate association differs from its original owner or execution account"));
    }
    Ok(candidate.observation_token)
}

#[derive(Default)]
struct State {
    catalog_retirement: catalog_retirement::Registry,
    maintenance_write_faults: BTreeMap<String, Arc<CatalogWriteFaultHandle>>,
    vector_faults: BTreeMap<String, Arc<VectorFaultRecord>>,
    maintenance: BTreeMap<String, Arc<MaintenanceDriver>>,
    sessions: BTreeMap<String, Arc<Session>>,
    table_sentinels: BTreeMap<String, Option<table_sentinel::Identity>>,
    prepared: BTreeMap<String, Prepared>,
    pauses: BTreeMap<String, PauseHandle>,
    candidate_before: BTreeMap<String, CandidateBefore>,
    candidate_events: Vec<CandidateEvent>,
    operations: BTreeMap<String, Operation>,
}
impl State {
    fn insert_vector_fault(&mut self, token: String, record: Arc<VectorFaultRecord>) -> WorkerResult<()> {
        if self.vector_faults.len() >= MAX_VECTOR_FAULTS || self.vector_faults.contains_key(&token) {
            return Err(invalid("M4 fault evidence budget exhausted or token already exists"));
        }
        self.vector_faults.insert(token, record);
        Ok(())
    }
}

struct Worker {
    store: Arc<ECStore>,
    server_ctx: Arc<ServerContextSlot>,
    node_id: String,
    nonce: String,
    boot_id: Uuid,
    started: Instant,
    state: Mutex<State>,
    shutdown: CancellationToken,
    vector_http: tokio::sync::Mutex<Option<VectorHttp>>,
}

struct VectorHttp {
    endpoint: String,
    region: String,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl Drop for VectorHttp {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub(super) struct CatalogWorkerGuard {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    socket: PathBuf,
    socket_inode: u64,
}
impl Drop for CatalogWorkerGuard {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
        if std::fs::symlink_metadata(&self.socket).is_ok_and(|metadata| metadata.ino() == self.socket_inode) {
            let _ = std::fs::remove_file(&self.socket);
        }
    }
}

pub(super) async fn start_if_configured(
    store: Arc<ECStore>,
    server_ctx: Arc<ServerContextSlot>,
    shutdown: CancellationToken,
) -> io::Result<Option<CatalogWorkerGuard>> {
    let socket = std::env::var_os("RUSTFS_CATALOG_WORKER_SOCKET");
    let node_id = std::env::var_os("RUSTFS_CATALOG_WORKER_NODE_ID");
    let nonce = std::env::var_os("RUSTFS_CATALOG_WORKER_NONCE");
    if socket.is_none() && node_id.is_none() && nonce.is_none() {
        return Ok(None);
    }
    let config_error =
        || io::Error::new(io::ErrorKind::InvalidInput, "all private Catalog worker environment fields are required");
    let socket = PathBuf::from(socket.ok_or_else(config_error)?);
    let node_id = node_id.ok_or_else(config_error)?.into_string().map_err(|_| config_error())?;
    let nonce = nonce.ok_or_else(config_error)?.into_string().map_err(|_| config_error())?;
    if !socket.is_absolute() || node_id.is_empty() || node_id.len() > 128 || nonce.len() < 32 || nonce.len() > 256 {
        return Err(config_error());
    }
    let metadata = std::fs::symlink_metadata(socket.parent().ok_or_else(config_error)?)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "worker socket requires a private 0700 parent directory",
        ));
    }
    let app = server_ctx
        .installed_app_context()
        .ok_or_else(|| io::Error::other("worker requires installed server context"))?;
    if !Arc::ptr_eq(&app.object_store(), &store) {
        return Err(io::Error::other("worker store differs from server context"));
    }
    if let Ok(existing) = std::fs::symlink_metadata(&socket) {
        if !existing.file_type().is_socket() || existing.uid() != metadata.uid() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "worker socket path is not an owned socket",
            ));
        }
        match UnixStream::connect(&socket).await {
            Ok(_) => return Err(io::Error::new(io::ErrorKind::AddrInUse, "worker socket is already active")),
            Err(error) if matches!(error.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound) => {
                std::fs::remove_file(&socket)?
            }
            Err(error) => return Err(error),
        }
    }
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let socket_metadata = std::fs::symlink_metadata(&socket)?;
    if socket_metadata.uid() != metadata.uid() {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "worker directory has another owner"));
    }
    let socket_inode = socket_metadata.ino();
    let shutdown = shutdown.child_token();
    let worker = Arc::new(Worker {
        store,
        server_ctx,
        node_id,
        nonce,
        boot_id: Uuid::new_v4(),
        started: Instant::now(),
        state: Mutex::default(),
        shutdown: shutdown.clone(),
        vector_http: tokio::sync::Mutex::new(None),
    });
    let stop = shutdown.clone();
    let task = tokio::spawn(async move {
        let permits = Arc::new(Semaphore::new(32));
        let mut requests = JoinSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                Some(_) = requests.join_next(), if !requests.is_empty() => {},
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    if !stream.peer_cred().is_ok_and(|credentials| credentials.uid() == socket_metadata.uid()) { continue; }
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let worker = worker.clone();
                    requests.spawn(async move { let _permit = permit; let _ = worker.serve(stream).await; });
                }
            }
        }
        requests.abort_all();
    });
    Ok(Some(CatalogWorkerGuard {
        shutdown,
        task,
        socket,
        socket_inode,
    }))
}

impl Worker {
    fn maintenance(&self, token: &str) -> WorkerResult<Arc<MaintenanceDriver>> {
        self.state
            .lock()
            .expect("worker state")
            .maintenance
            .get(token)
            .cloned()
            .ok_or_else(|| invalid("unknown maintenance runtime_token"))
    }

    fn vector_fault(&self, token: &str) -> WorkerResult<Arc<VectorFaultRecord>> {
        self.state
            .lock()
            .expect("worker state")
            .vector_faults
            .get(token)
            .cloned()
            .ok_or_else(|| invalid("unknown M4 fault_token"))
    }

    async fn retain_sdk_candidate(&self, runtime_token: &str, fault_token: &str) -> WorkerResult<Value> {
        let driver = self.maintenance(runtime_token)?;
        let record = self.vector_fault(fault_token)?;
        let _mutation = record.begin_receipt(runtime_token)?;
        let handle = {
            let state = record.state.lock().expect("fault record");
            if state.receipt.is_some() {
                return Err(invalid("SDK candidate receipt is already retained"));
            }
            match &state.evidence {
                VectorFaultEvidence::Live(handle) => handle.clone(),
                VectorFaultEvidence::Completed(_) => return Err(invalid("SDK candidate fault is already completed")),
            }
        };
        let token =
            sdk_candidate_token(&handle.snapshot(), self.store.id, &record.resource, driver.execution_identity().account())?;
        let receipt = handle.observed_sdk_candidate(token)?;
        if receipt.instance_id() != self.store.id || receipt.resource() != &record.resource.identity()? {
            return Err(invalid("SDK candidate receipt differs from its original arm resource"));
        }
        let registration = driver.retain_sdk_candidate(&record.resource, &handle, token).await?;
        // App insertion has completed. Retain its token even if final evidence validation fails,
        // so the explicit release command can clean up without discarding the original live owner.
        let mut state = record.state.lock().expect("fault record");
        state.receipt = Some(token);
        let snapshot = handle.snapshot();
        if sdk_candidate_token(&snapshot, self.store.id, &record.resource, driver.execution_identity().account())? != token {
            return Err(invalid("SDK candidate token changed during registration"));
        }
        let output = json!({"fault_token":fault_token,"registration":registration,"observation":snapshot});
        state.evidence = VectorFaultEvidence::Completed(Box::new(snapshot));
        drop(state);
        drop(handle);
        Ok(output)
    }

    fn sdk_candidate_owner(&self, runtime_token: &str, fault_token: &str) -> WorkerResult<(Arc<MaintenanceDriver>, Uuid)> {
        let driver = self.maintenance(runtime_token)?;
        let record = self.vector_fault(fault_token)?;
        if record.runtime_token != runtime_token {
            return Err(invalid("SDK candidate belongs to another maintenance runtime"));
        }
        let state = record.state.lock().expect("fault record");
        if state.receipt_busy {
            return Err(invalid("SDK receipt operation is in progress"));
        }
        let VectorFaultEvidence::Completed(snapshot) = &state.evidence else {
            return Err(invalid("SDK candidate registration is not complete"));
        };
        let token = state.receipt.ok_or_else(|| invalid("SDK candidate receipt was released"))?;
        if sdk_candidate_token(snapshot, self.store.id, &record.resource, driver.execution_identity().account())? != token {
            return Err(invalid("SDK candidate receipt token differs from its preserved evidence"));
        }
        Ok((driver, token))
    }

    async fn release_sdk_candidate(&self, runtime_token: &str, fault_token: &str) -> WorkerResult<Value> {
        let driver = self.maintenance(runtime_token)?;
        let record = self.vector_fault(fault_token)?;
        let _mutation = record.begin_receipt(runtime_token)?;
        let token = record
            .state
            .lock()
            .expect("fault record")
            .receipt
            .ok_or_else(|| invalid("SDK candidate receipt is not retained"))?;
        driver.release_sdk_candidate(token).await?;
        record.state.lock().expect("fault record").receipt = None;
        Ok(json!({"fault_token":fault_token,"observation_token":token,"released":true}))
    }

    async fn start_vector_http(&self) -> WorkerResult<Value> {
        let mut owned = self.vector_http.lock().await;
        if owned.is_none() {
            let app = self
                .server_ctx
                .installed_app_context()
                .ok_or_else(|| invalid("application is not installed"))?;
            let root = app
                .action_credentials()
                .get()
                .ok_or_else(|| invalid("signature verifier is unavailable"))?;
            let region = app
                .region()
                .get()
                .map(|region| region.to_string())
                .unwrap_or_else(|| rustfs_config::RUSTFS_REGION.to_owned());
            let vector = crate::server::vector::assemble(
                crate::server::vector::VectorAssembly {
                    server_ctx: self.server_ctx.clone(),
                    root_access_key: root.access_key.clone(),
                    root_secret_key: root.secret_key.clone(),
                    region: Some(region.clone()),
                    body_read_timeout: Duration::from_secs(10),
                },
                ManifestIoLimits {
                    max_manifest_bytes: NonZeroU64::new(64 * 1024 * 1024).expect("positive manifest limit"),
                    max_concurrent_reads: NonZeroUsize::new(4).expect("positive read concurrency"),
                },
                NonZeroUsize::new(3).expect("positive write attempts"),
            )
            .map_err(invalid)?;
            let fallback = tower::service_fn(|_request: http::Request<hyper::body::Incoming>| {
                let mut response = http::Response::new(crate::server::EdgeBody::default());
                *response.status_mut() = http::StatusCode::NOT_FOUND;
                std::future::ready(Ok::<_, crate::server::EdgeError>(response))
            });
            let service = crate::server::vector::VectorDispatch::enabled_for_test(fallback, vector);
            let service = ExternalRequestContextLayer::new(false).layer(service);
            let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let shutdown = self.shutdown.child_token();
            let stop = shutdown.clone();
            let task = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        incoming = listener.accept() => {
                            let (stream, peer) = incoming?;
                            if !peer.ip().is_loopback() {
                                continue;
                            }
                            let service = service.clone();
                            let connection = hyper::server::conn::http1::Builder::new()
                                .keep_alive(false)
                                .serve_connection(
                                    hyper_util::rt::TokioIo::new(stream),
                                    hyper_util::service::TowerToHyperService::new(service),
                                );
                            connections.spawn(connection);
                        }
                        completed = connections.join_next(), if !connections.is_empty() => {
                            completed.expect("nonempty HTTP connection set").map_err(io::Error::other)?
                                .map_err(io::Error::other)?;
                        }
                    }
                }
                connections.shutdown().await;
                Ok(())
            });
            *owned = Some(VectorHttp {
                endpoint,
                region,
                shutdown,
                task,
            });
        }
        let http = owned.as_ref().expect("HTTP listener is installed");
        if http.task.is_finished() {
            return Err(invalid("Vector HTTP listener terminated"));
        }
        Ok(json!({"endpoint":http.endpoint,"region":http.region,"node_id":self.node_id,
            "boot_id":self.boot_id,"pid":std::process::id()}))
    }

    fn candidate_event(&self, pause: &CandidatePauseGuard<'_>, phase: &'static str) {
        let mut state = self.state.lock().expect("worker state");
        let sequence = state.candidate_events.len() + 1;
        state.candidate_events.push(CandidateEvent {
            sequence,
            elapsed_micros: self.started.elapsed().as_micros(),
            session_id: pause.session_id.into(),
            fault_id: pause.fault_id.into(),
            request_id: pause.request_id.into(),
            commit_id: pause.commit_id.into(),
            phase,
        });
    }

    async fn pause_before_candidate(
        &self,
        session_id: &str,
        commit_id: &str,
        idempotency_key: Option<&str>,
        base: &ExpectedHead,
        request_id: &str,
    ) -> WorkerResult<()> {
        let selected = {
            let mut state = self.state.lock().expect("worker state");
            let selected = state
                .candidate_before
                .iter_mut()
                .find(|(_, rule)| !rule.hit && rule.session_id == session_id && rule.commit_id == commit_id);
            match selected {
                Some((fault_id, rule)) => {
                    if rule.base != *base || rule.idempotency_key.as_deref() != idempotency_key {
                        return Err(invalid("candidate pause requires its exact unused replay input"));
                    }
                    rule.hit = true;
                    Some((fault_id.clone(), rule.release.clone(), rule.timeout))
                }
                None => None,
            }
        };
        let Some((fault_id, release, timeout)) = selected else {
            return Ok(());
        };
        let mut guard = CandidatePauseGuard {
            worker: self,
            session_id,
            fault_id: &fault_id,
            request_id,
            commit_id,
            completed: false,
        };
        self.candidate_event(&guard, "before-arrived");
        if tokio::time::timeout(timeout, release.notified()).await.is_err() {
            self.candidate_event(&guard, "before-timeout");
            guard.completed = true;
            return Err(WorkerError::Timeout);
        }
        self.candidate_event(&guard, "before-released");
        guard.completed = true;
        Ok(())
    }

    fn session(&self, id: &str) -> WorkerResult<Arc<Session>> {
        self.state
            .lock()
            .expect("worker state")
            .sessions
            .get(id)
            .cloned()
            .ok_or_else(|| invalid("unknown session_id"))
    }
    fn prepared(&self, id: &str) -> WorkerResult<Prepared> {
        self.state
            .lock()
            .expect("worker state")
            .prepared
            .get(id)
            .cloned()
            .ok_or_else(|| invalid("unknown prepared_token"))
    }
    async fn serve(&self, stream: UnixStream) -> WorkerResult<()> {
        let (reader, mut writer) = stream.into_split();
        let mut line = Vec::new();
        let mut reader = BufReader::new(reader).take((MAX_FRAME_BYTES + 1) as u64);
        tokio::time::timeout(IO_TIMEOUT, reader.read_until(b'\n', &mut line))
            .await
            .map_err(|_| WorkerError::Timeout)??;
        if line.len() > MAX_FRAME_BYTES || line.last() != Some(&b'\n') {
            return Err(invalid("request frame exceeds limit or has no newline"));
        }
        let request: Request = serde_json::from_slice(&line)?;
        if request.version != 1 || request.nonce != self.nonce {
            return Err(invalid("worker protocol or nonce mismatch"));
        }
        let request_id = request.request_id.clone();
        let result = self.execute(request).await;
        let response = match result {
            Ok(value) => json!({"status":"ok", "result": value, "error": null}),
            Err(error) => json!({"status":"error", "result": null, "error": error_value(&error)}),
        };
        let mut response = response.as_object().expect("response object").clone();
        response.extend(
            json!({"version":1,"request_id":request_id,"node_id":self.node_id,"boot_id":self.boot_id,"pid":std::process::id()})
                .as_object()
                .expect("identity object")
                .clone(),
        );
        let mut bytes = serde_json::to_vec(&response)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(invalid("response exceeds frame budget"));
        }
        bytes.push(b'\n');
        tokio::time::timeout(IO_TIMEOUT, writer.write_all(&bytes))
            .await
            .map_err(|_| WorkerError::Timeout)??;
        Ok(())
    }
    async fn execute(&self, request: Request) -> WorkerResult<Value> {
        if request.request_id.is_empty() || request.request_id.len() > 128 || request.request_id.chars().any(char::is_control) {
            return Err(invalid("request_id must be a bounded printable string"));
        }
        let timeout_ms = request.timeout_ms.unwrap_or(120_000);
        if !(1..=900_000).contains(&timeout_ms) {
            return Err(invalid("timeout_ms must be between 1 and 900000"));
        }
        let cancel = CancellationToken::new();
        {
            let mut state = self.state.lock().expect("worker state");
            if state.operations.len() >= MAX_REQUESTS || state.operations.contains_key(&request.request_id) {
                return Err(invalid("request_id was already used or request budget is exhausted"));
            }
            let action = serde_json::to_value(&request.action)?["op"]
                .as_str()
                .expect("tagged action")
                .to_owned();
            state.operations.insert(
                request.request_id.clone(),
                Operation {
                    request_id: request.request_id.clone(),
                    action,
                    started_micros: self.started.elapsed().as_micros(),
                    completed_micros: None,
                    status: "running".into(),
                    error: None,
                    cancel: cancel.clone(),
                },
            );
        }
        let result = tokio::select! {
            _ = cancel.cancelled() => Err(WorkerError::Cancelled),
            result = tokio::time::timeout(Duration::from_millis(timeout_ms), self.dispatch(request.action, &request.request_id)) => result.unwrap_or(Err(WorkerError::Timeout)),
        };
        let mut state = self.state.lock().expect("worker state");
        let operation = state.operations.get_mut(&request.request_id).expect("registered operation");
        operation.completed_micros = Some(self.started.elapsed().as_micros());
        operation.status = if result.is_ok() { "ok" } else { "error" }.into();
        operation.error = result.as_ref().err().map(error_value);
        result
    }
    async fn new_session(&self, identity: Identity) -> WorkerResult<Arc<Session>> {
        if self.state.lock().expect("worker state").sessions.len() >= MAX_SESSIONS {
            return Err(invalid("session budget exhausted"));
        }
        let bucket = identity.bucket.to_handle()?;
        if !bucket.account_namespace_id().as_str().is_empty() {
            return Err(invalid("worker requires the configured empty account namespace"));
        }
        let table_id = ManagedTableId::try_new(identity.table_id.clone()).map_err(invalid)?;
        let name = CatalogName::parse(&identity.index_name).map_err(invalid)?;
        let resource = CatalogResourceIdentity::try_new(bucket.clone(), table_id.clone(), CatalogFormat::Lance)?;
        let access = ManagedObjectAccess::try_from_parts(
            bucket,
            table_id,
            resource.dataset_prefix(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Range,
            ]
            .into_iter()
            .collect(),
        )
        .map_err(invalid)?;
        let index = VectorIndexManifestSpec {
            schema_version: 1,
            dimension: NonZeroU32::new(identity.dimension).ok_or_else(|| invalid("dimension must be positive"))?,
            distance_metric: VectorDistanceMetric::Cosine,
            non_filterable_metadata_keys: BTreeSet::new(),
            encryption_descriptor_sha256: CatalogContentSha256::digest(&serde_json::to_vec(&identity.encryption.descriptor())?),
        };
        let limits = ManifestIoLimits {
            max_manifest_bytes: NonZeroU64::new(MAX_OBJECT_BYTES as u64).expect("positive budget"),
            max_concurrent_reads: NonZeroUsize::new(8).expect("positive concurrency"),
        };
        let head = RequestHead {
            method: http::Method::POST,
            uri: http::Uri::from_static("/"),
            headers: http::HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        };
        let mut extensions = http::Extensions::new();
        let context = RequestContext::fallback();
        extensions.insert(context.clone());
        extensions.insert(self.server_ctx.clone());
        extensions.insert(ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: "native-catalog-test".into(),
                ..Default::default()
            }),
            bucket: Some(access.bucket().storage_bucket_id().as_str().into()),
            request_context: Some(context),
            ..Default::default()
        });
        let objects = managed_lance_objects(
            access.clone(),
            Arc::new(RequestEnvelope::from_parts(head, extensions)),
            identity.encryption.managed(),
            limits,
        )?;
        let backend = RealCatalogBackend::new(EcStoreTableCatalogObjectBackend::new_with_managed_candidate_reader(
            self.store.clone(),
            objects.clone(),
        ));
        let catalog = Arc::new(ObjectTableCatalogStore::for_vector_scope(backend.clone(), access.bucket().clone()).await?);
        let adapter =
            LanceManifestAdapter::new(catalog.clone(), objects.clone(), access.clone(), name.clone(), index.clone(), limits)?;
        let namespace = Namespace::parse("indexes").map_err(invalid)?;
        let head_key = vector_catalog_paths().table_entry_path(access.bucket().storage_bucket_id().as_str(), &namespace, &name);
        Ok(Arc::new(Session {
            identity,
            access,
            resource,
            name,
            namespace,
            index,
            objects,
            backend,
            catalog,
            adapter,
            writable_keys: Mutex::new([head_key].into_iter().collect()),
        }))
    }
    fn register(&self, session: Arc<Session>) -> WorkerResult<String> {
        let mut state = self.state.lock().expect("worker state");
        if state.sessions.len() >= MAX_SESSIONS {
            return Err(invalid("session budget exhausted"));
        }
        let id = Uuid::new_v4().to_string();
        state.sessions.insert(id.clone(), session);
        Ok(id)
    }
    async fn dispatch(&self, action: Action, request_id: &str) -> WorkerResult<Value> {
        match action {
            Action::D09Capture { runtime_token, resource } => self.d09_capture(runtime_token, resource).await,
            Action::D09Reopen {
                bucket,
                resource,
                authority_ref,
            } => self.d09_reopen(bucket, resource, authority_ref).await,
            Action::D09Snapshot { captured_token, cursor } => self.d09_snapshot(captured_token, cursor).await,
            Action::D09PublicationState { captured_token } => self.d09_publication_state(&captured_token).await,
            Action::D09GateArm {
                captured_token,
                phase,
                occurrence,
                pause_timeout_ms,
            } => self.d09_gate_arm(captured_token, phase, occurrence, pause_timeout_ms),
            Action::D09GatePoll { fault_token } => self.d09_gate_poll(&fault_token),
            Action::D09GateRelease { fault_token } => self.d09_gate_release(&fault_token),
            Action::D09DeleteReplay {
                captured_token,
                runtime_token,
            } => self.d09_delete_replay(&captured_token, &runtime_token).await,

            Action::VectorDeletedInspect {
                runtime_token,
                resource,
                cursor,
            } => Ok(serde_json::to_value(
                self.maintenance(&runtime_token)?
                    .inspect_deleted_resource(&resource, cursor.as_deref())
                    .await?,
            )?),
            Action::TableSentinelCreate => self.create_table_sentinel().await,
            Action::TableSentinelOpen { identity } => self.open_table_sentinel(identity).await,
            Action::TableSentinelRead { sentinel_token } => self.read_table_sentinel(&sentinel_token).await,
            Action::M4WriteFaultArm {
                runtime_token,
                resource,
                job_id,
                attempt,
                target,
                occurrence,
                phase,
                effect,
                pause_timeout_ms,
            } => {
                let action = match (effect, pause_timeout_ms) {
                    (Effect::Fail, None) => CatalogWriteFaultAction::Fail,
                    (Effect::Pause, Some(timeout)) if (1..=120_000).contains(&timeout) => {
                        CatalogWriteFaultAction::Pause(Duration::from_millis(timeout))
                    }
                    _ => return Err(invalid("invalid Catalog write fault effect or pause deadline")),
                };
                if self.state.lock().expect("worker state").maintenance_write_faults.len() >= 32 {
                    return Err(invalid("Catalog write fault evidence budget exhausted"));
                }
                let handle = self
                    .maintenance(&runtime_token)?
                    .arm_write_fault(
                        &resource,
                        MaintenanceWriteFaultRequest {
                            job_id,
                            attempt,
                            target: target.try_into()?,
                            occurrence,
                            phase: phase.into(),
                            action,
                        },
                    )
                    .await?;
                let mut state = self.state.lock().expect("worker state");
                if state.maintenance_write_faults.len() >= 32 {
                    return Err(invalid("Catalog write fault evidence budget exhausted"));
                }
                let token = Uuid::new_v4().to_string();
                let observation = serde_json::to_value(handle.snapshot())?;
                state.maintenance_write_faults.insert(token.clone(), Arc::new(handle));
                Ok(json!({"fault_token": token, "observation": observation}))
            }
            Action::M4WrittenResultRead {
                runtime_token,
                resource,
                job_id,
                attempt,
                content_sha256,
                content_length,
            } => Ok(serde_json::to_value(
                self.maintenance(&runtime_token)?
                    .read_written_result(
                        &resource,
                        MaintenanceWrittenResultRequest {
                            job_id,
                            attempt,
                            content_sha256,
                            content_length,
                        },
                    )
                    .await?,
            )?),
            Action::M4WriteFaultPoll { fault_token } => {
                let state = self.state.lock().expect("worker state");
                let handle = state
                    .maintenance_write_faults
                    .get(&fault_token)
                    .ok_or_else(|| invalid("unknown Catalog write fault token"))?;
                Ok(serde_json::to_value(handle.snapshot())?)
            }
            Action::M4WriteFaultRelease { fault_token } => {
                let state = self.state.lock().expect("worker state");
                let handle = state
                    .maintenance_write_faults
                    .get(&fault_token)
                    .ok_or_else(|| invalid("unknown Catalog write fault token"))?;
                handle.release();
                Ok(serde_json::to_value(handle.snapshot())?)
            }
            Action::M4FaultArm {
                runtime_token,
                resource,
                point,
                occurrence,
                pause_timeout_ms,
            } => {
                if !(1..=300_000).contains(&pause_timeout_ms) {
                    return Err(invalid("M4 fault timeout must be between 1 and 300000 milliseconds"));
                }
                // Admission validates the exact incarnation and current permissions before registration.
                self.maintenance(&runtime_token)?.inspect_resource(&resource, None).await?;
                let mut state = self.state.lock().expect("worker state");
                if state.vector_faults.len() >= MAX_VECTOR_FAULTS {
                    return Err(invalid("M4 fault evidence budget exhausted"));
                }
                let handle = crate::vector::fault_test::arm(
                    self.store.id,
                    resource.identity()?,
                    point,
                    occurrence,
                    Duration::from_millis(pause_timeout_ms),
                )?;
                let token = Uuid::new_v4().to_string();
                let observation = serde_json::to_value(handle.snapshot())?;
                state.insert_vector_fault(token.clone(), Arc::new(VectorFaultRecord::new(runtime_token, resource, handle)))?;
                Ok(json!({"fault_token":token,"observation":observation}))
            }
            Action::M4FaultPoll { fault_token } => Ok(serde_json::to_value(self.vector_fault(&fault_token)?.snapshot())?),
            Action::M4FaultWait {
                fault_token,
                wait_timeout_ms,
            } => {
                if !(1..=30_000).contains(&wait_timeout_ms) {
                    return Err(invalid("M4 fault observation timeout must be between 1 and 30000 milliseconds"));
                }
                let record = self.vector_fault(&fault_token)?;
                Ok(serde_json::to_value(record.wait(Duration::from_millis(wait_timeout_ms)).await?)?)
            }
            Action::M4FaultRelease { fault_token } => Ok(serde_json::to_value(self.vector_fault(&fault_token)?.release()?)?),
            Action::SdkCandidateRetain {
                runtime_token,
                fault_token,
            } => self.retain_sdk_candidate(&runtime_token, &fault_token).await,
            Action::SdkCandidateInspect {
                runtime_token,
                fault_token,
            } => {
                let (driver, token) = self.sdk_candidate_owner(&runtime_token, &fault_token)?;
                Ok(serde_json::to_value(driver.inspect_sdk_candidate(token).await?)?)
            }
            Action::SdkCandidateObject {
                runtime_token,
                fault_token,
                reference_ordinal,
                content_offset,
            } => {
                let (driver, token) = self.sdk_candidate_owner(&runtime_token, &fault_token)?;
                Ok(serde_json::to_value(
                    driver
                        .inspect_sdk_candidate_object(token, reference_ordinal, content_offset)
                        .await?,
                )?)
            }
            Action::SdkCandidateQuota {
                runtime_token,
                fault_token,
            } => {
                let (driver, token) = self.sdk_candidate_owner(&runtime_token, &fault_token)?;
                Ok(serde_json::to_value(driver.inspect_sdk_candidate_quota(token).await?)?)
            }
            Action::SdkCandidateRelease {
                runtime_token,
                fault_token,
            } => self.release_sdk_candidate(&runtime_token, &fault_token).await,
            Action::VectorHttpStart => self.start_vector_http().await,
            Action::MaintenanceOpen { execution_account } => {
                let app = self
                    .server_ctx
                    .installed_app_context()
                    .ok_or_else(|| invalid("application is not installed"))?;
                let driver = Arc::new(MaintenanceDriver::admit(app, &execution_account).await?);
                let token = Uuid::new_v4().to_string();
                let identity = serde_json::to_value(driver.execution_identity())?;
                let mut state = self.state.lock().expect("worker state");
                if state.maintenance.len() >= MAX_SESSIONS {
                    return Err(invalid("maintenance runtime budget exhausted"));
                }
                state.maintenance.insert(token.clone(), driver);
                Ok(json!({"runtime_token":token,"execution_identity":identity}))
            }
            Action::MaintenanceTick { runtime_token } => {
                let token = self.maintenance(&runtime_token)?.tick()?;
                Ok(json!({"operation_token":token}))
            }
            Action::MaintenanceCollect { runtime_token } => {
                let token = self.maintenance(&runtime_token)?.collect()?;
                Ok(json!({"operation_token":token}))
            }
            Action::MaintenancePoll {
                runtime_token,
                operation_token,
            } => Ok(serde_json::to_value(self.maintenance(&runtime_token)?.poll(&operation_token)?)?),
            Action::MaintenanceStop { runtime_token } => {
                Ok(serde_json::to_value(self.maintenance(&runtime_token)?.stop().await?)?)
            }
            Action::VectorBucketResolve {
                runtime_token,
                bucket_name,
            } => {
                let driver = self.maintenance(&runtime_token)?;
                let bucket = driver.resolve_bucket(&bucket_name).await?;
                Ok(json!({
                    "bucket": bucket,
                    "execution_identity": driver.execution_identity(),
                    "instance_id": self.store.id,
                }))
            }
            Action::MaintenanceGcLockSelector {
                runtime_token,
                resource,
                job_id,
            } => Ok(serde_json::to_value(
                self.maintenance(&runtime_token)?
                    .read_gc_lock_selector(&resource, job_id)
                    .await?,
            )?),
            Action::MaintenanceGcLockObserve { runtime_token } => {
                let observation = self.maintenance(&runtime_token)?.observe_gc_lock_acquisitions().await?;
                Ok(json!({
                    "instance_id": self.store.id,
                    "process_id": std::process::id(),
                    "boot_id": self.boot_id,
                    "observation": observation,
                }))
            }
            Action::VectorResolve {
                runtime_token,
                bucket_name,
                index_name,
            } => Ok(serde_json::to_value(
                self.maintenance(&runtime_token)?
                    .resolve_resource(&bucket_name, &index_name)
                    .await?,
            )?),
            Action::VectorInspect {
                runtime_token,
                resource,
                cursor,
            } => Ok(serde_json::to_value(
                self.maintenance(&runtime_token)?
                    .inspect_resource(&resource, cursor.as_deref())
                    .await?,
            )?),
            Action::Ping => {
                let endpoints = self
                    .store
                    .instance_endpoints()
                    .ok_or_else(|| invalid("store has no instance endpoints"))?;
                let pools: Vec<_> = endpoints
                    .as_ref()
                    .iter()
                    .map(|pool| {
                        json!({
                            "set_count": pool.set_count, "drives_per_set": pool.drives_per_set,
                            "endpoints": pool.endpoints.as_ref().iter().map(|endpoint| json!({
                                "url": endpoint.to_string(), "is_local": endpoint.is_local,
                                "pool_index": endpoint.pool_idx, "set_index": endpoint.set_idx, "disk_index": endpoint.disk_idx,
                            })).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                Ok(json!({"node_id": self.node_id, "boot_id": self.boot_id, "pid": std::process::id(),
                    "store_id": self.store.id, "store_context_matches": self.server_ctx.installed_app_context()
                        .is_some_and(|app| Arc::ptr_eq(&app.object_store(), &self.store)), "instance_endpoints": pools}))
            }
            Action::Create {
                public_name,
                index_name,
                dimension,
                encryption,
            } => {
                if !(1..=4096).contains(&dimension) {
                    return Err(invalid("dimension must be between 1 and 4096"));
                }
                CatalogName::parse(&index_name).map_err(invalid)?;
                let account = AccountNamespaceId::empty();
                let name = BucketPublicName::try_new(public_name).map_err(invalid)?;
                let region = self
                    .store
                    .region()
                    .map(|region| region.to_string())
                    .unwrap_or_else(|| "us-east-1".into());
                let bucket = BucketHandle::try_from_parts(
                    BucketKind::Vector,
                    account.clone(),
                    region.clone(),
                    name.clone(),
                    crate::app::bucket_usecase::vector_storage_bucket_id(&account, &region, &name).map_err(invalid)?,
                    BucketIncarnation::try_new(Uuid::new_v4()).map_err(invalid)?,
                )
                .map_err(invalid)?;
                make_managed_bucket(&self.store, &bucket, &encryption.xml()?, &[], &MakeBucketOptions::default())
                    .await
                    .map_err(invalid)?;
                let identity = Identity {
                    bucket: VectorBucketBindingV1::from_handle(&bucket)?,
                    table_id: Uuid::new_v4().to_string(),
                    index_name,
                    dimension,
                    encryption,
                };
                let session = self.new_session(identity).await?;
                session
                    .catalog
                    .put_table_bucket(TableBucketEntry {
                        vector_bucket_binding: Some(session.identity.bucket.clone()),
                        version: 2,
                        table_bucket: session.bucket().into(),
                        catalog_type: "s3vectors".into(),
                        warehouse_root: format!("s3://{}/", session.bucket()),
                        state: TableCatalogEntryState::Active,
                        properties: BTreeMap::new(),
                        active_rename_id: None,
                        created_at: None,
                        updated_at: None,
                    })
                    .await?;
                session
                    .catalog
                    .create_namespace(NamespaceEntry {
                        version: 1,
                        table_bucket: session.bucket().into(),
                        namespace: session.namespace.public_name(),
                        namespace_id: session.namespace.storage_id(),
                        state: TableCatalogEntryState::Active,
                        properties: BTreeMap::new(),
                        created_at: None,
                        updated_at: None,
                    })
                    .await?;
                let mut entry = session.initial_entry();
                let prepared = session
                    .adapter
                    .prepare_empty_index(
                        &session.access,
                        ManifestPrepareRequest {
                            parts: CatalogCommitParts {
                                resource_identity: session.resource.clone(),
                                namespace: session.namespace.clone(),
                                name: session.name.clone(),
                                expected_head: CatalogExpectedHead::from_entry(&entry, None, None)?,
                                commit_id: CatalogCommitId::try_new(Uuid::new_v4().to_string())?,
                                operation: "native-test".into(),
                                idempotency_key: None,
                                writer: None,
                            },
                            index: session.index.clone(),
                        },
                    )
                    .await?;
                entry.metadata_location = prepared.candidate_metadata().object_key().as_str().into();
                entry.vector_head_binding = Some(VectorHeadBindingV1::try_new(
                    &session.resource,
                    prepared.candidate_metadata(),
                    prepared.format_descriptor(),
                )?);
                session.catalog.create_table(entry).await?;
                let head = session.head_value().await?;
                let session_id = self.register(session.clone())?;
                Ok(json!({"session_id":session_id,"identity":session.identity,"head":head}))
            }
            Action::Open { identity } => {
                let session = self.new_session(identity).await?;
                session.adapter.load_current(&session.access, &session.resource).await?;
                let head = session.head_value().await?;
                let session_id = self.register(session.clone())?;
                Ok(json!({"session_id":session_id,"identity":session.identity,"head":head}))
            }
            Action::CandidateBeforeArm {
                session_id,
                commit_id,
                idempotency_key,
                pause_timeout_ms,
            } => {
                let session = self.session(&session_id)?;
                validated_commit_id(&commit_id)?;
                let timeout_ms = pause_timeout_ms.unwrap_or(600_000);
                if !(1..=900_000).contains(&timeout_ms) {
                    return Err(invalid("pause timeout must be between 1 and 900000"));
                }
                let selected = session.adapter.load_current(&session.access, &session.resource).await?;
                let head = session.current_head().await?;
                if head.metadata() != &selected.reference {
                    return Err(TableCatalogStoreError::Conflict("head changed while arming candidate preparation".into()).into());
                }
                let base = ExpectedHead::from_catalog(&head.expected_head());
                base.to_catalog(&session)?;
                let fault_id = Uuid::new_v4().to_string();
                let replay = Action::Prepare {
                    session_id: session_id.clone(),
                    commit_id: commit_id.clone(),
                    idempotency_key: idempotency_key.clone(),
                    base: Some(Box::new(base.clone())),
                };
                let release = Arc::new(Notify::new());
                let mut state = self.state.lock().expect("worker state");
                if state.candidate_before.len() >= MAX_PREPARED
                    || state
                        .candidate_before
                        .values()
                        .any(|rule| rule.session_id == session_id && rule.commit_id == commit_id)
                {
                    return Err(invalid("candidate pause already registered or candidate pause budget exhausted"));
                }
                state.pauses.insert(fault_id.clone(), PauseHandle::Candidate(release.clone()));
                state.candidate_before.insert(
                    fault_id.clone(),
                    CandidateBefore {
                        session_id,
                        commit_id,
                        idempotency_key,
                        base: base.clone(),
                        release,
                        timeout: Duration::from_millis(timeout_ms),
                        hit: false,
                    },
                );
                Ok(json!({"fault_id":fault_id,"replay":replay,"expected_head":base}))
            }
            Action::Prepare {
                session_id,
                commit_id,
                idempotency_key,
                base,
            } => {
                let session = self.session(&session_id)?;
                if self.state.lock().expect("worker state").prepared.len() >= MAX_PREPARED {
                    return Err(invalid("prepared token budget exhausted"));
                }
                let base = match base {
                    Some(base) => *base,
                    None => ExpectedHead::from_catalog(&session.current_head().await?.expected_head()),
                };
                let prepare_request = ManifestPrepareRequest {
                    parts: CatalogCommitParts {
                        resource_identity: session.resource.clone(),
                        namespace: session.namespace.clone(),
                        name: session.name.clone(),
                        expected_head: base.to_catalog(&session)?,
                        commit_id: validated_commit_id(&commit_id)?,
                        operation: "native-test".into(),
                        idempotency_key: idempotency_key.clone(),
                        writer: None,
                    },
                    index: session.index.clone(),
                };
                self.pause_before_candidate(&session_id, &commit_id, idempotency_key.as_deref(), &base, request_id)
                    .await?;
                let prepared = session.adapter.prepare_empty_index(&session.access, prepare_request).await?;
                let paths = prepared_paths(&session, &prepared);
                {
                    let mut keys = session.writable_keys.lock().expect("registered keys");
                    keys.insert(paths["commit_log"].as_str().expect("commit path").into());
                    if let Some(key) = paths["idempotency"].as_str() {
                        keys.insert(key.into());
                    }
                }
                let reference = prepared.candidate_metadata();
                let readback = session
                    .objects
                    .get(
                        &session.access,
                        reference.object_key(),
                        None,
                        reference.body_revision().map(|revision| revision.as_uuid()),
                    )
                    .await?;
                verify_reference(reference, &readback)?;
                let token = Uuid::new_v4().to_string();
                let replay = Action::Prepare {
                    session_id,
                    commit_id,
                    idempotency_key,
                    base: Some(Box::new(base.clone())),
                };
                let mut candidate = metadata_value(reference);
                candidate["bucket"] = session.bucket().into();
                candidate["uri"] = format!("s3://{}/{}", session.bucket(), reference.object_key().as_str()).into();
                let response = json!({"prepared_token": token, "replay": replay, "candidate": candidate,
                    "candidate_readback":object_value(session.bucket(),reference.object_key().as_str(),&readback),
                    "expected_head":base,"descriptor":CatalogFormatDescriptorRecordV1::from(prepared.format_descriptor()),
                    "commit_id":prepared.commit_id().as_str(),"request_digest":prepared.request_digest().to_hex(),"paths":paths});
                let mut state = self.state.lock().expect("worker state");
                if state.prepared.len() >= MAX_PREPARED {
                    return Err(invalid("prepared token budget exhausted"));
                }
                state.prepared.insert(
                    token,
                    Prepared {
                        session,
                        commit: prepared,
                    },
                );
                Ok(response)
            }
            Action::Commit { prepared_token } => {
                let prepared = self.prepared(&prepared_token)?;
                let result = prepared.session.catalog.commit_prepared(prepared.commit).await?;
                Ok(json!({"table":result.table,"commit_log":result.commit_log}))
            }
            Action::Head { session_id } => self.session(&session_id)?.head_value().await,
            Action::Read { session_id } => {
                let session = self.session(&session_id)?;
                let selected = session.adapter.load_current(&session.access, &session.resource).await?;
                let manifest = session
                    .objects
                    .get(
                        &session.access,
                        selected.reference.object_key(),
                        None,
                        selected.reference.body_revision().map(|revision| revision.as_uuid()),
                    )
                    .await?;
                verify_reference(&selected.reference, &manifest)?;
                let head = session.head_value().await?;
                if head["metadata"]["sha256"] != selected.reference.content_sha256().to_hex()
                    || head["metadata"]["object_key"] != selected.reference.object_key().as_str()
                {
                    return Err(
                        TableCatalogStoreError::Conflict("head changed while opening the selected manifest".into()).into(),
                    );
                }
                Ok(json!({"head":head,"manifest_version":selected.manifest_version,
                    "manifest":object_value(session.bucket(),selected.reference.object_key().as_str(),&manifest)}))
            }
            Action::Recovery { session_id } => {
                let session = self.session(&session_id)?;
                session.current_head().await?;
                Ok(serde_json::to_value(
                    session
                        .catalog
                        .plan_table_commit_recovery(session.bucket(), "indexes", session.name.as_str())
                        .await?,
                )?)
            }
            Action::Recover { session_id } => {
                let session = self.session(&session_id)?;
                session.current_head().await?;
                Ok(serde_json::to_value(
                    session
                        .catalog
                        .recover_table_commits(session.bucket(), "indexes", session.name.as_str())
                        .await?,
                )?)
            }
            Action::RawRead { session_id, bucket, key } => {
                let object = self.session(&session_id)?.raw_read(&bucket, &key).await?;
                Ok(json!({"object":object.as_ref().map(|object| object_value(&bucket,&key,object))}))
            }
            Action::RawWrite {
                session_id,
                bucket,
                key,
                expected_etag,
                bytes_base64,
            } => {
                let session = self.session(&session_id)?;
                session.check_raw(&bucket, &key, true)?;
                if expected_etag.is_empty() {
                    return Err(invalid("raw write requires expected_etag"));
                }
                let bytes = base64_simd::STANDARD
                    .decode_to_vec(bytes_base64.as_bytes())
                    .map_err(invalid)?;
                if bytes.len() > MAX_OBJECT_BYTES {
                    return Err(invalid("raw object exceeds byte budget"));
                }
                let before = session
                    .raw_read(&bucket, &key)
                    .await?
                    .ok_or_else(|| invalid("raw write requires an existing object"))?;
                if before.etag.as_deref() != Some(&expected_etag) {
                    return Err(TableCatalogStoreError::Conflict("raw write ETag changed".into()).into());
                }
                session
                    .backend
                    .put_object(&bucket, &key, bytes.clone(), TableCatalogPutPrecondition::IfMatch(expected_etag))
                    .await?;
                let after = session
                    .raw_read(&bucket, &key)
                    .await?
                    .ok_or_else(|| invalid("raw write readback is absent"))?;
                if after.data != bytes {
                    return Err(TableCatalogStoreError::Conflict("raw write readback differs".into()).into());
                }
                Ok(json!({"before":object_value(&bucket,&key,&before),"after":object_value(&bucket,&key,&after)}))
            }
            Action::FaultArm {
                prepared_token,
                target,
                phase,
                effect,
                pause_timeout_ms,
            } => {
                let prepared = self.prepared(&prepared_token)?;
                let paths = prepared_paths(&prepared.session, &prepared.commit);
                let (key, attempt) = match target {
                    Target::Head => (paths["head"].as_str(), 1),
                    Target::Staged => (paths["commit_log"].as_str(), 1),
                    Target::Finalization => (paths["commit_log"].as_str(), 2),
                    Target::Idempotency => (paths["idempotency"].as_str(), 1),
                    Target::IdempotencyFinalization => (paths["idempotency"].as_str(), 2),
                };
                let key = key.ok_or_else(|| invalid("target requires an idempotency key"))?;
                let timeout = pause_timeout_ms.unwrap_or(600_000);
                if !(1..=900_000).contains(&timeout) {
                    return Err(invalid("pause timeout must be between 1 and 900000"));
                }
                let fault_id = Uuid::new_v4().to_string();
                let fault_phase = match phase {
                    Phase::Before => FaultPhase::Before,
                    Phase::After => FaultPhase::After,
                };
                match effect {
                    Effect::Fail => prepared.session.backend.fail(RUSTFS_META_BUCKET, key, attempt, fault_phase),
                    Effect::Pause => {
                        let pause = prepared.session.backend.pause_with_timeout(
                            RUSTFS_META_BUCKET,
                            key,
                            attempt,
                            fault_phase,
                            Duration::from_millis(timeout),
                        );
                        self.state
                            .lock()
                            .expect("worker state")
                            .pauses
                            .insert(fault_id.clone(), PauseHandle::Backend(pause));
                    }
                }
                Ok(
                    json!({"fault_id":fault_id,"bucket":RUSTFS_META_BUCKET,"key":key,"attempt":attempt,"phase":phase,"effect":effect}),
                )
            }
            Action::FaultRelease { fault_id } => {
                let pause = self
                    .state
                    .lock()
                    .expect("worker state")
                    .pauses
                    .remove(&fault_id)
                    .ok_or_else(|| invalid("unknown pause fault_id"))?;
                match pause {
                    PauseHandle::Backend(pause) => pause.release(),
                    PauseHandle::Candidate(release) => release.notify_one(),
                }
                Ok(json!({"fault_id":fault_id,"released":true}))
            }
            Action::Events {
                session_id,
                after_sequence,
            } => {
                let session = self.session(&session_id)?;
                let events: Vec<_> = session
                    .backend
                    .events()
                    .into_iter()
                    .filter(|event| event.sequence > after_sequence)
                    .take(256)
                    .collect();
                let observations = session.backend.observations();
                let state = self.state.lock().expect("worker state");
                let candidate_events: Vec<_> = state
                    .candidate_events
                    .iter()
                    .filter(|event| event.session_id == session_id)
                    .collect();
                Ok(
                    json!({"events":events,"observations":observations,"candidate_events":candidate_events,"operations":state.operations.values().collect::<Vec<_>>() }),
                )
            }
            Action::Cancel { target_request_id } => {
                let state = self.state.lock().expect("worker state");
                let operation = state
                    .operations
                    .get(&target_request_id)
                    .ok_or_else(|| invalid("unknown target request_id"))?;
                let pending = operation.completed_micros.is_none();
                if pending {
                    operation.cancel.cancel();
                }
                Ok(json!({"target_request_id":target_request_id,"cancel_requested":pending,"status":operation.status}))
            }
        }
    }
}

fn validated_commit_id(value: &str) -> WorkerResult<CatalogCommitId> {
    let id = Uuid::parse_str(value).map_err(invalid)?;
    if id.is_nil() || id.to_string() != value {
        return Err(invalid("commit_id must be a canonical non-nil UUID"));
    }
    Ok(CatalogCommitId::try_new(value.into())?)
}

fn prepared_paths(session: &Session, prepared: &PreparedCatalogCommit) -> Value {
    let paths = vector_catalog_paths();
    json!({"bucket":RUSTFS_META_BUCKET,"head":session.head_key(),
        "commit_log":paths.commit_log_entry_path(session.bucket(),&session.identity.table_id,prepared.commit_id().as_str()),
        "idempotency":prepared.idempotency_key().map(|key|paths.commit_idempotency_entry_path(session.bucket(),&session.identity.table_id,key)),
        "lock_resources": vector_publication_lock_resources(session.bucket(), &session.namespace, &session.name).into_iter()
            .map(|(bucket,object)|json!({"bucket":bucket,"object":object,"version":null})).collect::<Vec<_>>()})
}
fn metadata_value(reference: &CatalogMetadataRef) -> Value {
    let mut value = serde_json::to_value(CatalogMetadataRefRecordV1::from(reference)).expect("metadata serialization");
    value["sha256"] = reference.content_sha256().to_hex().into();
    value["length"] = reference.content_length().into();
    value
}
fn object_value(bucket: &str, key: &str, object: &TableCatalogObject) -> Value {
    json!({"bucket":bucket,"key":key,"length":object.data.len(),"sha256":CatalogContentSha256::digest(&object.data).to_hex(),
        "body_revision":object.body_revision,"etag":object.etag,"bytes_base64":base64_simd::STANDARD.encode_to_string(&object.data)})
}
fn verify_reference(reference: &CatalogMetadataRef, object: &TableCatalogObject) -> WorkerResult<()> {
    if &CatalogMetadataRef::from_read(reference.object_key().clone(), object)? != reference {
        return Err(TableCatalogStoreError::Conflict("exact manifest read differs from Catalog content identity".into()).into());
    }
    Ok(())
}
fn error_value(error: &WorkerError) -> Value {
    fn catalog_class(error: &TableCatalogStoreError) -> String {
        match error {
            TableCatalogStoreError::NotFound(_) => "NotFound",
            TableCatalogStoreError::NamespaceNotFound(_) => "NamespaceNotFound",
            TableCatalogStoreError::TableNotFound(_) => "TableNotFound",
            TableCatalogStoreError::AlreadyExists(_) => "AlreadyExists",
            TableCatalogStoreError::Conflict(_) => "Conflict",
            TableCatalogStoreError::Invalid(_) => "Invalid",
            TableCatalogStoreError::Unsupported(_) => "Unsupported",
            TableCatalogStoreError::Unavailable(_) => "Unavailable",
            TableCatalogStoreError::LockUnavailable { .. } => "LockUnavailable",
            TableCatalogStoreError::Internal(_) => "Internal",
            TableCatalogStoreError::Io { classification, .. } => return format!("Io::{classification:?}"),
        }
        .into()
    }
    let class = match error {
        WorkerError::VectorFault(_) => "VectorFault".into(),
        WorkerError::VectorProtocol(_) => "VectorProtocol".into(),
        WorkerError::Maintenance(_) => "Maintenance".into(),
        WorkerError::Catalog(error) | WorkerError::Lance(LanceManifestError::Catalog(error)) => catalog_class(error),
        WorkerError::Lance(_) => "Lance".into(),
        WorkerError::Io(_) => "Io".into(),
        WorkerError::Json(_) => "Json".into(),
        WorkerError::Invalid(_) => "InvalidRequest".into(),
        WorkerError::Timeout => "Timeout".into(),
        WorkerError::Cancelled => "Cancelled".into(),
    };
    let mut sources = Vec::new();
    let mut source = error.source();
    while let Some(error) = source {
        if sources.len() == 8 {
            break;
        }
        sources.push(error.to_string());
        source = error.source();
    }
    let category = match error {
        WorkerError::Maintenance(error) => Some(error.category()),
        _ => None,
    };
    let mut output = json!({"class":class,"category":category,"message":error.to_string(),"sources":sources});
    if let WorkerError::VectorProtocol(error) = error {
        output["code"] = json!(error.code().name());
        output["status"] = json!(error.status_code().as_u16());
    }
    let mut source = error.source();
    let mut typed_sources = Vec::new();
    for _ in 0..64 {
        let Some(current) = source else {
            break;
        };
        if let Some(crate::app::object::ManagedObjectError::Api(api)) =
            current.downcast_ref::<crate::app::object::ManagedObjectError>()
        {
            typed_sources.push(json!({"type":"ManagedObjectError::Api","code":api.code.as_str()}));
        }
        source = current.source();
    }
    if !typed_sources.is_empty() {
        output["typed_sources"] = json!(typed_sources);
    }
    if source.is_some() {
        output["typed_source_chain_limit"] = json!(true);
    }
    output
}

#[cfg(test)]
mod tests {
    mod d09_observation_tests;

    use super::*;
    use crate::app::gating_test_env::run_large_stack_test;
    use aws_sdk_s3vectors::{
        Client,
        config::{BehaviorVersion, Credentials, Region},
        types::{DataType, DistanceMetric, PutInputVector, VectorData},
    };
    use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};
    use sha2::{Digest as _, Sha256};

    struct SdkWorkerFixture {
        worker: Worker,
        client: Client,
        runtime_token: String,
        resource: VectorMaintenanceResourceV1,
        bucket: String,
    }

    async fn call(worker: &Worker, action: Action) -> WorkerResult<Value> {
        worker
            .execute(Request {
                version: 1,
                nonce: worker.nonce.clone(),
                request_id: Uuid::new_v4().to_string(),
                timeout_ms: Some(60_000),
                action,
            })
            .await
    }

    impl SdkWorkerFixture {
        async fn new() -> Self {
            let (app, credentials) = crate::app::vector_usecase::catalog_worker_test_identity().await;
            let server_ctx = ServerContextSlot::new();
            assert!(server_ctx.install(app.clone()));
            let worker = Worker {
                store: app.object_store(),
                server_ctx,
                node_id: "sdk-worker-test".into(),
                nonce: Uuid::new_v4().to_string(),
                boot_id: Uuid::new_v4(),
                started: Instant::now(),
                state: Mutex::default(),
                shutdown: CancellationToken::new(),
                vector_http: tokio::sync::Mutex::new(None),
            };
            let http = call(&worker, Action::VectorHttpStart).await.unwrap();
            let http_client = aws_smithy_http_client::Builder::new().build_with_connector_fn(|settings, components| {
                let mut builder = aws_smithy_http_client::Connector::builder()
                    .proxy_config(aws_smithy_http_client::proxy::ProxyConfig::disabled());
                builder.set_connector_settings(settings.cloned());
                builder.set_sleep_impl(components.and_then(|runtime| runtime.sleep_impl()));
                builder.build_http()
            });
            let client = Client::from_conf(
                aws_sdk_s3vectors::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new(http["region"].as_str().unwrap().to_owned()))
                    .endpoint_url(http["endpoint"].as_str().unwrap())
                    .credentials_provider(Credentials::new(
                        credentials.access_key.clone(),
                        credentials.secret_key,
                        None,
                        None,
                        "sdk-worker-test",
                    ))
                    .http_client(http_client)
                    .retry_config(RetryConfig::disabled())
                    .timeout_config(
                        TimeoutConfig::builder()
                            .operation_timeout(Duration::from_secs(30))
                            .connect_timeout(Duration::from_secs(5))
                            .read_timeout(Duration::from_secs(30))
                            .build(),
                    )
                    .build(),
            );
            let bucket = format!("worker-{}", Uuid::new_v4().simple());
            client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();
            client
                .create_index()
                .vector_bucket_name(&bucket)
                .index_name("worker.index")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .send()
                .await
                .unwrap();
            let runtime = call(
                &worker,
                Action::MaintenanceOpen {
                    execution_account: credentials.access_key,
                },
            )
            .await
            .unwrap();
            let runtime_token = runtime["runtime_token"].as_str().unwrap().to_owned();
            let resource = serde_json::from_value(
                call(
                    &worker,
                    Action::VectorResolve {
                        runtime_token: runtime_token.clone(),
                        bucket_name: bucket.clone(),
                        index_name: "worker.index".into(),
                    },
                )
                .await
                .unwrap(),
            )
            .unwrap();
            Self {
                worker,
                client,
                runtime_token,
                resource,
                bucket,
            }
        }

        async fn arm(&self, runtime_token: &str) -> String {
            call(
                &self.worker,
                Action::M4FaultArm {
                    runtime_token: runtime_token.into(),
                    resource: self.resource.clone(),
                    point: FaultPoint::SdkCandidatePrepared,
                    occurrence: NonZeroU32::MIN,
                    pause_timeout_ms: 60_000,
                },
            )
            .await
            .unwrap()["fault_token"]
                .as_str()
                .unwrap()
                .to_owned()
        }

        async fn fail_put(&self, fault_token: &str) -> Value {
            let error = self
                .client
                .put_vectors()
                .vector_bucket_name(&self.bucket)
                .index_name("worker.index")
                .vectors(
                    PutInputVector::builder()
                        .key("candidate-row")
                        .data(VectorData::Float32(vec![1.0, 2.0]))
                        .build()
                        .unwrap(),
                )
                .send()
                .await
                .unwrap_err();
            let response = error.raw_response().expect("SDK received the actual HTTP error");
            assert!(response.status().as_u16() >= 500);
            let http_request_id = response
                .headers()
                .get("x-amz-request-id")
                .expect("original response request ID");
            let snapshot = call(
                &self.worker,
                Action::M4FaultPoll {
                    fault_token: fault_token.into(),
                },
            )
            .await
            .unwrap();
            assert_eq!(snapshot["observation"]["sdk_request_failure"]["http_request_id"], http_request_id);
            assert_eq!(snapshot["injected_before_catalog"], true);
            assert_eq!(snapshot["arrivals"], 1);
            assert_eq!(snapshot["completions"], 1);
            assert!(snapshot["sdk_request_failure_error"].is_null());
            snapshot
        }
    }

    #[test]
    #[serial_test::serial]
    fn sdk_worker_receipt_preserves_evidence_releases_gate_and_keeps_capacity() {
        run_large_stack_test("sdk-worker-receipt", || async {
            let fixture = SdkWorkerFixture::new().await;
            let worker = &fixture.worker;
            let token = fixture.arm(&fixture.runtime_token).await;
            let before = fixture.fail_put(&token).await;
            let retained = call(
                worker,
                Action::SdkCandidateRetain {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(retained["observation"], before);
            let observation_token = retained["registration"]["observation_token"].clone();
            assert_eq!(observation_token, before["observation"]["sdk_candidate"]["observation_token"]);
            let other_runtime = call(
                worker,
                Action::MaintenanceOpen {
                    execution_account: before["observation"]["sdk_request_failure"]["account"]
                        .as_str()
                        .unwrap()
                        .into(),
                },
            )
            .await
            .unwrap()["runtime_token"]
                .as_str()
                .unwrap()
                .to_owned();
            for action in [
                Action::SdkCandidateRetain {
                    runtime_token: other_runtime.clone(),
                    fault_token: token.clone(),
                },
                Action::SdkCandidateInspect {
                    runtime_token: other_runtime.clone(),
                    fault_token: token.clone(),
                },
                Action::SdkCandidateObject {
                    runtime_token: other_runtime.clone(),
                    fault_token: token.clone(),
                    reference_ordinal: 0,
                    content_offset: None,
                },
                Action::SdkCandidateQuota {
                    runtime_token: other_runtime.clone(),
                    fault_token: token.clone(),
                },
                Action::SdkCandidateRelease {
                    runtime_token: other_runtime,
                    fault_token: token.clone(),
                },
            ] {
                assert!(
                    matches!(call(worker, action).await, Err(WorkerError::Invalid(ref message)) if message == "SDK candidate belongs to another maintenance runtime")
                );
            }
            assert!(
                call(
                    worker,
                    Action::SdkCandidateRetain {
                        runtime_token: fixture.runtime_token.clone(),
                        fault_token: token.clone()
                    }
                )
                .await
                .is_err()
            );
            for action in [
                Action::M4FaultPoll {
                    fault_token: token.clone(),
                },
                Action::M4FaultWait {
                    fault_token: token.clone(),
                    wait_timeout_ms: 1,
                },
                Action::M4FaultRelease {
                    fault_token: token.clone(),
                },
            ] {
                assert_eq!(call(worker, action).await.unwrap(), before);
            }
            let catalog = call(
                worker,
                Action::SdkCandidateInspect {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(catalog["observation_token"], observation_token);
            assert!(catalog["commit"].is_null());
            let references = before["observation"]["sdk_candidate"]["references"].as_array().unwrap();
            assert!(!references.is_empty());
            let head = call(
                worker,
                Action::SdkCandidateObject {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                    reference_ordinal: 0,
                    content_offset: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(head["expected_object"], references[0]);
            assert!(head["body_revision"].is_string());
            assert!(head["content_base64"].is_null());
            let body = call(
                worker,
                Action::SdkCandidateObject {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                    reference_ordinal: 0,
                    content_offset: Some(0),
                },
            )
            .await
            .unwrap();
            let bytes = base64_simd::STANDARD
                .decode_to_vec(body["content_base64"].as_str().unwrap())
                .unwrap();
            assert_eq!(
                body["chunk_sha256"],
                hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower)
            );
            assert_eq!(body["observation_token"], observation_token);
            let quota = call(
                worker,
                Action::SdkCandidateQuota {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(quota["observation_token"], observation_token);
            assert!(
                call(
                    worker,
                    Action::SdkCandidateObject {
                        runtime_token: fixture.runtime_token.clone(),
                        fault_token: token.clone(),
                        reference_ordinal: references.len(),
                        content_offset: None,
                    }
                )
                .await
                .is_err()
            );
            let rearmed = fixture.arm(&fixture.runtime_token).await;
            assert_ne!(rearmed, token);
            // Unarrived real gates exercise the shared worker capacity; no observation is fabricated.
            for _ in 2..MAX_VECTOR_FAULTS {
                let handle = crate::vector::fault_test::arm(
                    Uuid::new_v4(),
                    fixture.resource.identity().unwrap(),
                    FaultPoint::SdkCandidatePrepared,
                    NonZeroU32::MIN,
                    Duration::from_secs(60),
                )
                .unwrap();
                worker
                    .state
                    .lock()
                    .unwrap()
                    .insert_vector_fault(
                        Uuid::new_v4().to_string(),
                        Arc::new(VectorFaultRecord::new(fixture.runtime_token.clone(), fixture.resource.clone(), handle)),
                    )
                    .unwrap();
            }
            assert_eq!(worker.state.lock().unwrap().vector_faults.len(), MAX_VECTOR_FAULTS);
            let capacity_error = call(
                worker,
                Action::M4FaultArm {
                    runtime_token: fixture.runtime_token.clone(),
                    resource: fixture.resource.clone(),
                    point: FaultPoint::QuerySnapshotOpened,
                    occurrence: NonZeroU32::MIN,
                    pause_timeout_ms: 60_000,
                },
            )
            .await
            .unwrap_err();
            assert!(
                matches!(capacity_error, WorkerError::Invalid(ref message) if message == "M4 fault evidence budget exhausted")
            );
            let released = call(
                worker,
                Action::SdkCandidateRelease {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap();
            assert_eq!(released["observation_token"], observation_token);
            assert_eq!(released["released"], true);
            assert!(
                call(
                    worker,
                    Action::SdkCandidateInspect {
                        runtime_token: fixture.runtime_token.clone(),
                        fault_token: token.clone()
                    }
                )
                .await
                .is_err()
            );
            assert!(
                call(
                    worker,
                    Action::SdkCandidateRelease {
                        runtime_token: fixture.runtime_token.clone(),
                        fault_token: token.clone()
                    }
                )
                .await
                .is_err()
            );
            assert_eq!(call(worker, Action::M4FaultPoll { fault_token: token }).await.unwrap(), before);
            assert_eq!(worker.state.lock().unwrap().vector_faults.len(), MAX_VECTOR_FAULTS);
        });
    }

    #[test]
    #[serial_test::serial]
    fn sdk_worker_rejects_other_runtime_account_and_unknown_tokens() {
        run_large_stack_test("sdk-worker-identity", || async {
            let fixture = SdkWorkerFixture::new().await;
            let worker = &fixture.worker;
            let (other_app, other_credentials) = crate::app::vector_usecase::catalog_worker_test_identity().await;
            assert!(Arc::ptr_eq(&other_app.object_store(), &worker.store));
            let other = call(
                worker,
                Action::MaintenanceOpen {
                    execution_account: other_credentials.access_key,
                },
            )
            .await
            .unwrap();
            let other_runtime = other["runtime_token"].as_str().unwrap();
            let token = fixture.arm(other_runtime).await;
            let before = fixture.fail_put(&token).await;
            let wrong_runtime = call(
                worker,
                Action::SdkCandidateRetain {
                    runtime_token: fixture.runtime_token.clone(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap_err();
            assert!(
                matches!(wrong_runtime, WorkerError::Invalid(ref message) if message == "SDK candidate belongs to another maintenance runtime")
            );
            let wrong_account = call(
                worker,
                Action::SdkCandidateRetain {
                    runtime_token: other_runtime.into(),
                    fault_token: token.clone(),
                },
            )
            .await
            .unwrap_err();
            assert!(
                matches!(wrong_account, WorkerError::Invalid(ref message) if message == "SDK candidate association differs from its original owner or execution account")
            );
            let record = worker.vector_fault(&token).unwrap();
            {
                let state = record.state.lock().unwrap();
                assert!(matches!(state.evidence, VectorFaultEvidence::Live(_)));
                assert!(state.receipt.is_none());
                assert!(!state.receipt_busy);
            }
            assert_eq!(
                call(
                    worker,
                    Action::M4FaultPoll {
                        fault_token: token.clone()
                    }
                )
                .await
                .unwrap(),
                before
            );
            for action in [
                Action::SdkCandidateRetain {
                    runtime_token: other_runtime.into(),
                    fault_token: "unknown".into(),
                },
                Action::SdkCandidateInspect {
                    runtime_token: other_runtime.into(),
                    fault_token: "unknown".into(),
                },
                Action::SdkCandidateObject {
                    runtime_token: other_runtime.into(),
                    fault_token: "unknown".into(),
                    reference_ordinal: 0,
                    content_offset: None,
                },
                Action::SdkCandidateQuota {
                    runtime_token: other_runtime.into(),
                    fault_token: "unknown".into(),
                },
                Action::SdkCandidateRelease {
                    runtime_token: other_runtime.into(),
                    fault_token: "unknown".into(),
                },
            ] {
                assert!(
                    matches!(call(worker, action).await, Err(WorkerError::Invalid(ref message)) if message == "unknown M4 fault_token")
                );
            }
            let admission = record.begin_receipt(other_runtime).unwrap();
            assert!(record.begin_receipt(other_runtime).is_err());
            assert!(
                call(
                    worker,
                    Action::M4FaultRelease {
                        fault_token: token.clone()
                    }
                )
                .await
                .is_err()
            );
            drop(admission);
            assert!(!record.state.lock().unwrap().receipt_busy);
            assert_eq!(record.snapshot().events.len(), before["events"].as_array().unwrap().len());
            assert_eq!(
                call(worker, Action::M4FaultRelease { fault_token: token }).await.unwrap()["released"],
                true
            );
        });
    }

    #[test]
    fn sdk_worker_commands_reject_caller_supplied_authority_and_object_paths() {
        for op in [
            "sdk_candidate_retain",
            "sdk_candidate_inspect",
            "sdk_candidate_object",
            "sdk_candidate_quota",
            "sdk_candidate_release",
        ] {
            let mut input = json!({"op":op,"runtime_token":"runtime","fault_token":"fault"});
            if op == "sdk_candidate_object" {
                input["reference_ordinal"] = json!(0);
                input["content_offset"] = Value::Null;
            }
            assert!(serde_json::from_value::<Action>(input.clone()).is_ok());
            for field in [
                "object_key",
                "descriptor",
                "resource",
                "scope",
                "execution_account",
                "observation_token",
            ] {
                let mut invalid = input.clone();
                invalid[field] = json!("caller-selected");
                assert!(serde_json::from_value::<Action>(invalid).is_err(), "{op} accepted {field}");
            }
        }
    }

    #[test]
    fn sdk_worker_errors_keep_original_managed_api_codes() {
        for code in [
            rustfs_s3_types::S3ErrorCode::NoSuchKey,
            rustfs_s3_types::S3ErrorCode::AccessDenied,
        ] {
            let message = "original managed read failure";
            let error = WorkerError::Maintenance(VectorMaintenanceExecutionError::Executor {
                category: VectorMaintenanceFailureCode::StorageUnavailable,
                source: Box::new(crate::app::object::ManagedObjectError::Api(crate::error::ApiError {
                    code: code.clone(),
                    message: message.into(),
                    source: None,
                })),
            });
            let output = error_value(&error);
            assert_eq!(output["class"], "Maintenance");
            assert_eq!(output["category"], "STORAGE_UNAVAILABLE");
            assert!(output["sources"].as_array().unwrap().contains(&json!(message)));
            assert_eq!(output["typed_sources"], json!([{"type":"ManagedObjectError::Api","code":code.as_str()}]));
        }
        assert!(error_value(&invalid("NoSuchKey"))["typed_sources"].is_null());
    }
}

#[cfg(test)]
mod observation_action_tests {
    use super::*;

    #[test]
    fn observation_actions_reject_caller_supplied_instance_authority() {
        for mut action in [
            json!({"op":"vector_bucket_resolve","runtime_token":"runtime","bucket_name":"vectors"}),
            json!({"op":"maintenance_gc_lock_observe","runtime_token":"runtime"}),
        ] {
            assert!(serde_json::from_value::<Action>(action.clone()).is_ok());
            action
                .as_object_mut()
                .unwrap()
                .insert("instance_id".into(), json!(Uuid::new_v4()));
            assert!(serde_json::from_value::<Action>(action).is_err());
        }
    }
}
