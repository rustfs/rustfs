//! Local checkpoints are captured from this writer's owned transaction only.
use crate::{
    LogId, NativeLogWriter, NativeSnapshotError, NativeSnapshotView, StoreError,
    checkpoint_baseline::{self, BaselineMetadata},
    snapshot,
};
use redb::{ReadableDatabase, ReadableTable, StorageBackend};
use std::{
    fs::File,
    io::{self, Seek, Write},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Copy, Debug)]
pub struct CheckpointLimits {
    pub snapshot: snapshot::Limits,
    pub staging_bytes: u64,
}

impl CheckpointLimits {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        let rows = self.snapshot;
        if rows.total_bytes == 0
            || rows.membership_bytes == 0
            || rows.membership_bytes > 512 * 1024
            || rows.key_bytes == 0
            || rows.key_bytes > 1024 * 1024
            || rows.value_bytes == 0
            || rows.value_bytes > 1024 * 1024
            || rows.records == 0
            || self.staging_bytes < 4 * 1024 * 1024
        {
            return Err(StoreError::ResourceLimit);
        }
        Ok(())
    }
}

impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            snapshot: snapshot::Limits {
                total_bytes: 256 * 1024 * 1024,
                membership_bytes: 512 * 1024,
                key_bytes: 8192,
                value_bytes: 8192,
                records: 1_000_000,
            },
            staging_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointReceipt {
    generation: u64,
    cut: LogId,
}

impl CheckpointReceipt {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn cut(&self) -> LogId {
        self.cut
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Capture(#[from] NativeSnapshotError),
    #[error(transparent)]
    Snapshot(#[from] snapshot::SnapshotError),
    #[error("checkpoint filesystem failure")]
    Io(#[from] io::Error),
    #[error("checkpoint worker failed")]
    Worker(#[source] tokio::task::JoinError),
    #[error("checkpoint store lock poisoned")]
    Poisoned,
    #[error("checkpoint conflict: {0}")]
    Conflict(&'static str),
    #[error("checkpoint cancelled before publication")]
    Cancelled,
}

/// Enforces the file-length budget before redb can allocate or write a page.
#[derive(Debug)]
pub(crate) struct BoundedBackend {
    inner: redb::backends::FileBackend,
    maximum: u64,
}

impl BoundedBackend {
    fn new(file: File, maximum: u64) -> Result<Self, redb::DatabaseError> {
        Ok(Self {
            inner: redb::backends::FileBackend::new(file)?,
            maximum,
        })
    }

    fn check(&self, end: u64) -> io::Result<()> {
        if end > self.maximum {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "checkpoint staging budget"));
        }
        Ok(())
    }
}

impl StorageBackend for BoundedBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check(len)?;
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let length = u64::try_from(data.len()).map_err(io::Error::other)?;
        self.check(
            offset
                .checked_add(length)
                .ok_or_else(|| io::Error::other("checkpoint write overflow"))?,
        )?;
        self.inner.write(offset, data)
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, redb::BackendError> {
        self.inner.try_lock_range(start, end)
    }
    fn try_lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, redb::BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), redb::BackendError> {
        self.inner.lock_range(start, end)
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), redb::BackendError> {
        self.inner.lock_shared_range(start, end)
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), redb::BackendError> {
        self.inner.unlock_range(start, end)
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, redb::BackendError> {
        self.inner.query_lock_range(start, end)
    }
}

pub(crate) fn recovery_scratch(maximum: u64) -> Result<(tempfile::TempDir, redb::Database), StoreError> {
    let directory = tempfile::tempdir()?;
    let database = stage_database(directory.path().join("stage.redb"), maximum)?;
    Ok((directory, database))
}

pub(crate) fn stage_database(path: std::path::PathBuf, maximum: u64) -> Result<redb::Database, StoreError> {
    let file = File::options().read(true).write(true).create_new(true).open(path)?;
    let backend = BoundedBackend::new(file, maximum)?;
    Ok(redb::Database::builder()
        .set_cache_size(8 * 1024 * 1024)
        .create_with_backend(backend)?)
}

struct PayloadFile {
    file: File,
    maximum: u64,
    written: u64,
}

impl Write for PayloadFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(bytes.len()).map_err(io::Error::other)?;
        let end = self
            .written
            .checked_add(length)
            .ok_or_else(|| io::Error::other("checkpoint payload overflow"))?;
        if end > self.maximum {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "checkpoint payload budget"));
        }
        let count = self.file.write(bytes)?;
        self.written = self
            .written
            .checked_add(u64::try_from(count).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("checkpoint payload overflow"))?;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub(crate) struct StageSink<'a>(pub(crate) &'a redb::WriteTransaction);

impl snapshot::StagingSink for StageSink<'_> {
    fn row(&mut self, section: snapshot::Section, key: &[u8], value: &[u8]) -> io::Result<()> {
        let definition = match section {
            snapshot::Section::Heads => checkpoint_baseline::HEADS,
            snapshot::Section::Versions => checkpoint_baseline::VERSIONS,
            snapshot::Section::Events => checkpoint_baseline::EVENTS,
            snapshot::Section::Operations => checkpoint_baseline::OPERATIONS,
        };
        let mut table = self.0.open_table(definition).map_err(io::Error::other)?;
        if table.insert(key, value).map_err(io::Error::other)?.is_some() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "duplicate checkpoint key"));
        }
        Ok(())
    }
}

// This type has no external constructors, deserialization, or path-based import.
// Its source remains the owned local read transaction, not the decoded artifact.
struct LocalStage {
    source: NativeSnapshotView,
    database: redb::Database,
    _directory: tempfile::TempDir,
    metadata: BaselineMetadata,
}

fn capture_stage(source: NativeSnapshotView, limits: CheckpointLimits) -> Result<LocalStage, CheckpointError> {
    limits.validate()?;
    source
        .header()
        .applied
        .ok_or(CheckpointError::Conflict("empty applied cut"))?;
    let directory = tempfile::tempdir()?;
    let payload = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(directory.path().join("capture.csnp"))?;
    let artifact = source.stream_local_capability(
        PayloadFile {
            file: payload,
            maximum: limits.staging_bytes.min(limits.snapshot.total_bytes),
            written: 0,
        },
        limits.snapshot,
    )?;
    let remaining = limits
        .staging_bytes
        .checked_sub(artifact.payload.written)
        .ok_or(CheckpointError::Conflict("staging budget"))?;
    let mut payload = artifact.payload.file;
    payload.sync_all()?;
    payload.rewind()?;
    let database = stage_database(directory.path().join("stage.redb"), remaining)?;
    let transaction = database.begin_write().map_err(StoreError::from)?;
    for definition in [
        checkpoint_baseline::HEADS,
        checkpoint_baseline::VERSIONS,
        checkpoint_baseline::EVENTS,
        checkpoint_baseline::OPERATIONS,
    ] {
        transaction.open_table(definition).map_err(StoreError::from)?;
    }
    let header = snapshot::decode_capability_artifact(
        snapshot::CapabilityArtifact {
            manifest: artifact.manifest,
            payload,
        },
        &mut StageSink(&transaction),
        limits.snapshot,
    )?;
    std::fs::remove_file(directory.path().join("capture.csnp"))?;
    if header != *source.header() {
        return Err(CheckpointError::Conflict("capture header"));
    }
    let metadata = BaselineMetadata {
        generation: 1,
        header,
        limits,
    };
    let encoded = checkpoint_baseline::encode_metadata(&metadata)?;
    {
        let mut meta = transaction.open_table(crate::META).map_err(StoreError::from)?;
        meta.insert("schema", 7u64.to_be_bytes().as_slice())
            .map_err(StoreError::from)?;
        meta.insert("checkpoint_baseline", encoded.as_slice())
            .map_err(StoreError::from)?;
    }
    transaction.commit().map_err(StoreError::StagingCommit)?;
    let stage = LocalStage {
        source,
        database,
        _directory: directory,
        metadata,
    };
    Ok(stage)
}

fn verify_local_stage(stage: &LocalStage) -> Result<(), CheckpointError> {
    let transaction = stage.database.begin_read().map_err(StoreError::from)?;
    for (source_definition, stage_definition) in [
        (crate::application::HEADS, checkpoint_baseline::HEADS),
        (crate::application::VERSIONS, checkpoint_baseline::VERSIONS),
        (crate::application::EVENTS, checkpoint_baseline::EVENTS),
        (crate::application::OPERATIONS, checkpoint_baseline::OPERATIONS),
    ] {
        let source = stage
            .source
            .transaction()
            .open_table(source_definition)
            .map_err(StoreError::from)?;
        let staged = transaction.open_table(stage_definition).map_err(StoreError::from)?;
        let mut source_rows = source.iter().map_err(StoreError::from)?;
        let mut staged_rows = staged.iter().map_err(StoreError::from)?;
        loop {
            match (source_rows.next(), staged_rows.next()) {
                (None, None) => break,
                (Some(source), Some(staged)) => {
                    let (source_key, source_value) = source.map_err(StoreError::from)?;
                    let (stage_key, stage_value) = staged.map_err(StoreError::from)?;
                    if source_key.value() != stage_key.value() || source_value.value() != stage_value.value() {
                        return Err(CheckpointError::Conflict("staging differs from owned local capture"));
                    }
                }
                _ => return Err(CheckpointError::Conflict("staging row count")),
            }
        }
    }
    let staged_meta = transaction.open_table(crate::META).map_err(StoreError::from)?;
    let staged_metadata =
        checkpoint_baseline::read_metadata(&staged_meta)?.ok_or(CheckpointError::Conflict("missing staging metadata"))?;
    if checkpoint_baseline::encode_metadata(&staged_metadata)? != checkpoint_baseline::encode_metadata(&stage.metadata)? {
        return Err(CheckpointError::Conflict("staging metadata differs from local capture"));
    }
    let stage_bytes = std::fs::metadata(stage._directory.path().join("stage.redb"))?.len();
    let index_budget = stage
        .metadata
        .limits
        .staging_bytes
        .checked_sub(stage_bytes)
        .ok_or(CheckpointError::Conflict("staging disk budget"))?;
    checkpoint_baseline::validate_baseline_with_budget(&transaction, &stage.metadata, index_budget)?;
    Ok(())
}

fn resource_error(error: CheckpointError) -> CheckpointError {
    use std::error::Error;
    if matches!(&error, CheckpointError::Store(StoreError::CommitIndeterminate(_))) {
        return error;
    }
    let mut source: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if current
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::StorageFull)
        {
            return CheckpointError::Store(StoreError::ResourceLimit);
        }
        source = current.source();
    }
    error
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
type CaptureHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
type StageHook = Box<dyn FnOnce(&redb::Database) + Send>;
#[cfg(test)]
static CAPTURE_HOOKS: std::sync::Mutex<std::collections::BTreeMap<usize, CaptureHook>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());
#[cfg(test)]
static STAGE_HOOKS: std::sync::Mutex<std::collections::BTreeMap<usize, StageHook>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
pub(crate) struct CheckpointHook {
    owner: usize,
    stage: bool,
}
#[cfg(test)]
impl Drop for CheckpointHook {
    fn drop(&mut self) {
        if self.stage {
            STAGE_HOOKS.lock().expect("stage hooks").remove(&self.owner);
        } else {
            CAPTURE_HOOKS.lock().expect("capture hooks").remove(&self.owner);
        }
    }
}

#[cfg(test)]
pub(crate) fn after_local_capture(writer: &NativeLogWriter, hook: impl FnOnce() + Send + 'static) -> CheckpointHook {
    let owner = Arc::as_ptr(&writer.store).addr();
    assert!(
        CAPTURE_HOOKS
            .lock()
            .expect("capture hooks")
            .insert(owner, Box::new(hook))
            .is_none()
    );
    CheckpointHook { owner, stage: false }
}

#[cfg(test)]
pub(crate) fn after_local_stage(writer: &NativeLogWriter, hook: impl FnOnce(&redb::Database) + Send + 'static) -> CheckpointHook {
    let owner = Arc::as_ptr(&writer.store).addr();
    assert!(
        STAGE_HOOKS
            .lock()
            .expect("stage hooks")
            .insert(owner, Box::new(hook))
            .is_none()
    );
    CheckpointHook { owner, stage: true }
}

#[cfg(test)]
fn run_capture_hook(owner: &Arc<std::sync::Mutex<crate::Store>>) {
    let hook = CAPTURE_HOOKS
        .lock()
        .expect("capture hooks")
        .remove(&Arc::as_ptr(owner).addr());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn run_stage_hook(owner: &Arc<std::sync::Mutex<crate::Store>>, database: &redb::Database) {
    let hook = STAGE_HOOKS.lock().expect("stage hooks").remove(&Arc::as_ptr(owner).addr());
    if let Some(hook) = hook {
        hook(database);
    }
}

impl NativeLogWriter {
    pub async fn checkpoint_local(&self, limits: CheckpointLimits) -> Result<CheckpointReceipt, CheckpointError> {
        self.checkpoint_local_inner(limits).await.map_err(resource_error)
    }

    async fn checkpoint_local_inner(&self, limits: CheckpointLimits) -> Result<CheckpointReceipt, CheckpointError> {
        limits.validate()?;
        let admission = self.checkpoint_admission.clone().lock_owned().await;
        let owner = self.store.clone();
        let stage = tokio::task::spawn_blocking(move || {
            let source = NativeSnapshotView::acquire(owner.clone())?;
            #[cfg(test)]
            run_capture_hook(&owner);
            let stage = capture_stage(source, limits)?;
            #[cfg(test)]
            run_stage_hook(&owner, &stage.database);
            verify_local_stage(&stage)?;
            Ok::<_, CheckpointError>((stage, admission))
        })
        .await
        .map_err(CheckpointError::Worker)??;
        let ordering = self.ordering.clone().lock_owned().await;
        let owner = self.store.clone();
        let cancellation = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let cancelled = cancellation.0.clone();
        tokio::task::spawn_blocking(move || {
            // The blocking owner retains both guards until publication finishes,
            // even if the async caller is dropped while redb is committing.
            let _ordering = ordering;
            let (stage, _admission) = stage;
            if cancelled.load(Ordering::Acquire) {
                return Err(CheckpointError::Cancelled);
            }
            if !Arc::ptr_eq(&owner, stage.source.owner()) {
                return Err(CheckpointError::Conflict("local capture owner"));
            }
            let mut store = owner.lock().map_err(|_| CheckpointError::Poisoned)?;
            let transaction = store.write_transaction()?;
            let mut meta = transaction.open_table(crate::META).map_err(StoreError::from)?;
            let schema = crate::number(
                meta.get("schema")
                    .map_err(StoreError::from)?
                    .ok_or(StoreError::CorruptRecord)?
                    .value(),
            )?;
            if !matches!(schema, 6 | 7) {
                return Err(StoreError::UnsupportedSchema.into());
            }
            let previous = checkpoint_baseline::read_metadata(&meta)?;
            if crate::application::read_binding(&meta)? != stage.metadata.header.binding {
                return Err(CheckpointError::Conflict("live capture binding changed"));
            }
            let identity = crate::decode_identity(
                meta.get("identity")
                    .map_err(StoreError::from)?
                    .ok_or(StoreError::CorruptRecord)?
                    .value(),
            )?;
            if identity.cluster_id != stage.metadata.header.cluster_id {
                return Err(CheckpointError::Conflict("live capture cluster changed"));
            }
            let cut = stage
                .metadata
                .header
                .applied
                .ok_or(CheckpointError::Conflict("empty applied cut"))?;
            let applied = crate::application::read_applied(&meta)?.ok_or(StoreError::CorruptRecord)?;
            let logs = transaction.open_table(crate::LOGS).map_err(StoreError::from)?;
            let committed = crate::committed::validate_committed(&meta, &logs)?.ok_or(StoreError::CorruptRecord)?;
            if cut.index > applied.index
                || (cut.index == applied.index && cut != applied)
                || applied.index > committed.index
                || (applied.index == committed.index && applied != committed)
            {
                return Err(CheckpointError::Conflict("live applied and committed bounds"));
            }
            drop(logs);
            if let Some(old) = previous.as_ref().and_then(|previous| previous.header.applied)
                && (old.index > cut.index || (old.index == cut.index && old != cut))
            {
                return Err(CheckpointError::Conflict("published checkpoint cut"));
            }
            let mut metadata = stage.metadata;
            metadata.generation = previous.map_or(Ok(1), |old| {
                old.generation
                    .checked_add(1)
                    .ok_or(CheckpointError::Conflict("checkpoint generation exhausted"))
            })?;
            let stage_transaction = stage.database.begin_read().map_err(StoreError::from)?;
            for (source_definition, definition) in [
                (crate::application::HEADS, checkpoint_baseline::HEADS),
                (crate::application::VERSIONS, checkpoint_baseline::VERSIONS),
                (crate::application::EVENTS, checkpoint_baseline::EVENTS),
                (crate::application::OPERATIONS, checkpoint_baseline::OPERATIONS),
            ] {
                let captured = stage
                    .source
                    .transaction()
                    .open_table(source_definition)
                    .map_err(StoreError::from)?;
                let staged = stage_transaction.open_table(definition).map_err(StoreError::from)?;
                let mut source_rows = captured.iter().map_err(StoreError::from)?;
                let mut stage_rows = staged.iter().map_err(StoreError::from)?;
                let mut target = transaction.open_table(definition).map_err(StoreError::from)?;
                target.retain(|_, _| false).map_err(StoreError::from)?;
                // Recheck disk bytes while copying; the earlier relationship
                // validation remains outside the writer ordering interval.
                loop {
                    match (source_rows.next(), stage_rows.next()) {
                        (None, None) => break,
                        (Some(source), Some(staged)) => {
                            let (source_key, source_value) = source.map_err(StoreError::from)?;
                            let (key, value) = staged.map_err(StoreError::from)?;
                            if source_key.value() != key.value() || source_value.value() != value.value() {
                                return Err(CheckpointError::Conflict("publication differs from local capture"));
                            }
                            target.insert(key.value(), value.value()).map_err(StoreError::from)?;
                        }
                        _ => return Err(CheckpointError::Conflict("publication row count")),
                    }
                }
            }
            let encoded = checkpoint_baseline::encode_metadata(&metadata)?;
            meta.insert("checkpoint_baseline", encoded.as_slice())
                .map_err(StoreError::from)?;
            meta.insert("schema", 7u64.to_be_bytes().as_slice())
                .map_err(StoreError::from)?;
            crate::raft_state_machine::publish_local_meta(&mut meta, &metadata)?;
            drop(meta);
            if cancelled.load(Ordering::Acquire) {
                return Err(CheckpointError::Cancelled);
            }
            store.commit(transaction)?;
            Ok(CheckpointReceipt {
                generation: metadata.generation,
                cut,
            })
        })
        .await
        .map_err(CheckpointError::Worker)?
    }
}
