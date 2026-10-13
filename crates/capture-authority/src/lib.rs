//! Synchronous, exclusive local authority storage. Run from a blocking worker.
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

mod application;
mod checkpoint_baseline;
mod committed;
mod native_checkpoint;
#[cfg(test)]
mod native_checkpoint_tests;
pub use native_checkpoint::{CheckpointError, CheckpointLimits, CheckpointReceipt};
mod created;
mod source_page;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub mod source_reader;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod source_reader_tests;
pub use source_page::{EventCursor, EventPage, MAX_EVENT_PAGE, SourceEvent};
mod native_snapshot;
mod native_snapshot_data;
pub use native_snapshot_data::NativeSnapshotData;
mod native_snapshot_install;
mod raft_state_machine;
pub use raft_state_machine::NativeRaftStateMachine;
#[cfg(test)]
mod native_snapshot_install_tests;
mod raft_log_storage;
#[cfg(test)]
mod raft_state_machine_tests;
pub use raft_log_storage::NativeRaftLogStore;
mod node;
#[cfg(test)]
mod raft_log_storage_tests;
pub use native_snapshot::{NativeSnapshotError, NativeSnapshotView};
mod raft_types;
pub mod snapshot;
pub use created::{
    CaptureBinding, CreatedApplyResult, CreatedEvent, CreatedOperation, CreatedOperationProvenance, CreatedResult,
    CreatedVersion, DecideCreated, MAX_OBJECT_KEY_LENGTH, ObjectIdentity, PreparedIdentity,
};
pub use node::{CaptureNode, CaptureNodeError};
pub use raft_types::{
    CaptureCommand, CaptureMembership, CaptureRaftEntry, CaptureResponse, CaptureResponseKind, CaptureTypeConfig, MembershipError,
};
pub use snapshot::{CapabilityArtifact, CapabilityManifest};
#[cfg(test)]
mod application_tests;
mod truncate;

/// OpenRaft log reader backed by exclusive local authority storage.
#[derive(Clone)]
pub struct NativeLogReader {
    store: std::sync::Arc<std::sync::Mutex<Store>>,
}

impl NativeLogReader {
    pub fn new(store: Store) -> Self {
        Self {
            store: std::sync::Arc::new(std::sync::Mutex::new(store)),
        }
    }
}

/// Async native writes and readers sharing one durable Store.
#[derive(Clone)]
pub struct NativeLogWriter {
    store: std::sync::Arc<std::sync::Mutex<Store>>,
    ordering: std::sync::Arc<tokio::sync::Mutex<()>>,
    snapshot_receives: std::sync::Arc<raft_state_machine::ReceiveState>,
    checkpoint_admission: std::sync::Arc<tokio::sync::Mutex<()>>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    source_reader_admission: std::sync::Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeWriteError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("native log writer lock poisoned")]
    Poisoned,
    #[error("native log writer worker failed")]
    Worker(#[source] tokio::task::JoinError),
}

impl NativeLogWriter {
    pub fn new(store: Store) -> Self {
        Self {
            store: std::sync::Arc::new(std::sync::Mutex::new(store)),
            ordering: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            snapshot_receives: std::sync::Arc::new(raft_state_machine::ReceiveState::default()),
            checkpoint_admission: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_reader_admission: std::sync::Arc::new(tokio::sync::Semaphore::new(source_reader::MAX_CONNECTIONS)),
        }
    }

    pub async fn snapshot_view(&self) -> Result<NativeSnapshotView, NativeSnapshotError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || NativeSnapshotView::acquire(store))
            .await
            .map_err(NativeSnapshotError::Worker)?
    }

    pub fn reader(&self) -> NativeLogReader {
        NativeLogReader {
            store: self.store.clone(),
        }
    }

    async fn write<T, F>(&self, write: F) -> Result<T, NativeWriteError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    {
        // Lock order is ordering then Store. The worker owns the ordering guard
        // until commit completes, even when its awaiting caller is cancelled.
        let ordering = self.ordering.clone().lock_owned().await;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _ordering = ordering;
            let mut store = store.lock().map_err(|_| NativeWriteError::Poisoned)?;
            write(&mut store).map_err(NativeWriteError::Store)
        })
        .await
        .map_err(NativeWriteError::Worker)?
    }

    pub async fn save_vote(&self, vote: openraft::Vote<u64>) -> Result<(), NativeWriteError> {
        self.write(move |store| store.save_vote(vote.into())).await
    }

    pub async fn read_vote(&self) -> Result<Option<openraft::Vote<u64>>, NativeWriteError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let store = store.lock().map_err(|_| NativeWriteError::Poisoned)?;
            store
                .read_vote()
                .map(|vote| vote.map(Into::into))
                .map_err(NativeWriteError::Store)
        })
        .await
        .map_err(NativeWriteError::Worker)?
    }

    pub async fn append_native(&self, entries: Vec<CaptureRaftEntry>) -> Result<(), NativeWriteError> {
        self.write(move |store| store.append_native(&entries)).await
    }

    pub async fn truncate_suffix(&self, from: u64) -> Result<(), NativeWriteError> {
        self.write(move |store| store.truncate_suffix(from)).await
    }

    /// Apply entries already committed by Raft consensus. Only the trusted Raft
    /// apply path may call this; local persistence does not prove quorum.
    pub async fn apply_committed(&self, entries: Vec<CaptureRaftEntry>) -> Result<Vec<CaptureResponse>, NativeWriteError> {
        self.write(move |store| {
            if !store.writable {
                return Err(StoreError::WriteDisabled);
            }
            let (last_applied, _) = store.native_application_state()?;
            if entries.is_empty() {
                return Ok(Vec::new());
            }
            for entry in &entries {
                if store.read_native_entry(entry.log_id.index)?.as_ref() != Some(entry) {
                    return Err(StoreError::InvalidCommittedLog);
                }
            }
            for pair in entries.windows(2) {
                if pair[0].log_id.index.checked_add(1) != Some(pair[1].log_id.index) {
                    return Err(StoreError::InvalidApplySequence);
                }
            }
            let first = entries[0].log_id.index;
            match last_applied {
                None if first != 0 => return Err(StoreError::InvalidApplySequence),
                Some(last) if first > last.index && last.index.checked_add(1) != Some(first) => {
                    return Err(StoreError::InvalidApplySequence);
                }
                _ => {}
            }
            // Validate the entire request before either durable marker advances.
            let end = entries[entries.len() - 1].log_id;
            if store.read_committed()?.is_none_or(|committed| committed.index < end.index) {
                store.save_committed(end.into())?;
            }
            entries
                .into_iter()
                .map(|entry| store.apply_next_native(entry.log_id.into()))
                .collect()
        })
        .await
    }

    /// Purge only the exact durable baseline selected by this local checkpoint.
    pub async fn purge_checkpoint(&self, receipt: &CheckpointReceipt) -> Result<(), NativeWriteError> {
        let generation = receipt.generation();
        let cut = receipt.cut();
        self.write(move |store| store.purge_checkpoint(generation, cut)).await
    }

    pub async fn applied_state(&self) -> Result<(Option<LogId>, CaptureMembership), NativeWriteError> {
        self.write(|store| {
            if !store.writable {
                return Err(StoreError::WriteDisabled);
            }
            store.native_application_state()
        })
        .await
    }
}

#[derive(Debug, thiserror::Error)]
enum NativeReaderError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("native log reader lock poisoned")]
    Poisoned,
    #[error("native log reader worker failed")]
    Worker,
}

fn native_read_error(error: NativeReaderError) -> openraft::StorageError<u64> {
    openraft::StorageError::IO {
        source: openraft::StorageIOError::read_logs(openraft::AnyError::new(&error)),
    }
}

async fn native_read_worker<F>(
    store: std::sync::Arc<std::sync::Mutex<Store>>,
    read: F,
) -> Result<Vec<CaptureRaftEntry>, openraft::StorageError<u64>>
where
    F: FnOnce(&Store) -> Result<Vec<CaptureRaftEntry>, StoreError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let store = store.lock().map_err(|_| native_read_error(NativeReaderError::Poisoned))?;
        read(&store).map_err(|error| native_read_error(NativeReaderError::Store(error)))
    })
    .await
    .map_err(|_| native_read_error(NativeReaderError::Worker))?
}

impl openraft::RaftLogReader<CaptureTypeConfig> for NativeLogReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<CaptureRaftEntry>, openraft::StorageError<u64>> {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        native_read_worker(self.store.clone(), move |store| store.read_native_range(bounds)).await
    }
}

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const VOTE: TableDefinition<u8, &[u8]> = TableDefinition::new("vote");
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeIdentity {
    pub cluster_id: [u8; 16],
    pub node_id: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRecord {
    pub term: u64,
    pub voted_node: u64,
    pub committed: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogId {
    pub term: u64,
    pub leader_node: u64,
    pub index: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub id: LogId,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogState {
    pub first: Option<LogId>,
    pub last: Option<LogId>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("unsupported schema")]
    UnsupportedSchema,
    #[error("corrupt record")]
    CorruptRecord,
    #[error("invalid event page limit")]
    InvalidEventPage,
    #[error("invalid membership")]
    InvalidMembership(#[from] MembershipError),
    #[error("invalid log sequence")]
    InvalidLogSequence,
    #[error("invalid committed log")]
    InvalidCommittedLog,
    #[error("committed regression")]
    CommittedRegression,
    #[error("committed log protected")]
    CommittedLogProtected,
    #[error("checkpoint recovery resource limit exceeded")]
    ResourceLimit,
    #[error("checkpoint generation or cut changed")]
    StaleCheckpoint,
    #[error("capture binding not pinned")]
    BindingNotPinned,
    #[error("capture binding conflict")]
    BindingConflict,
    #[error("invalid application sequence")]
    InvalidApplySequence,
    #[error("log index exhausted")]
    IndexExhausted,
    #[error("store already initialized")]
    AlreadyInitialized,
    #[error("store missing")]
    MissingStore,
    #[error("checkpoint staging commit failed")]
    StagingCommit(#[source] redb::CommitError),
    #[error("commit outcome unknown; close and reopen")]
    CommitIndeterminate(#[source] redb::CommitError),
    #[error("handle requires reopening")]
    WriteDisabled,
    #[error("filesystem failure")]
    Io(#[from] io::Error),
    #[error("database failure")]
    Database(#[from] redb::DatabaseError),
    #[error("transaction failure")]
    Transaction(#[from] redb::TransactionError),
    #[error("table failure")]
    Table(#[from] redb::TableError),
    #[error("durability configuration failure")]
    Durability(#[from] redb::SetDurabilityError),
    #[error("storage failure")]
    Storage(#[from] redb::StorageError),
}
impl StoreError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::IdentityMismatch => "identity_mismatch",
            Self::InvalidEventPage => "invalid_event_page",
            Self::UnsupportedSchema => "unsupported_schema",
            Self::CorruptRecord => "corrupt_record",
            Self::InvalidMembership(_) => "invalid_membership",
            Self::InvalidLogSequence => "invalid_log_sequence",
            Self::InvalidCommittedLog => "invalid_committed_log",
            Self::CommittedRegression => "committed_regression",
            Self::CommittedLogProtected => "committed_log_protected",
            Self::ResourceLimit => "resource_limit",
            Self::StaleCheckpoint => "stale_checkpoint",
            Self::BindingNotPinned => "binding_not_pinned",
            Self::BindingConflict => "binding_conflict",
            Self::InvalidApplySequence => "invalid_apply_sequence",
            Self::IndexExhausted => "index_exhausted",
            Self::AlreadyInitialized => "already_initialized",
            Self::MissingStore => "missing_store",
            Self::StagingCommit(_) => "staging_commit",
            Self::CommitIndeterminate(_) => "commit_indeterminate",
            Self::WriteDisabled => "write_disabled",
            Self::Io(_) => "io",
            Self::Database(_) => "database",
            Self::Transaction(_) => "transaction",
            Self::Table(_) => "table",
            Self::Storage(_) => "storage",
            Self::Durability(_) => "durability",
        }
    }
}

pub struct Store {
    database: Database,
    writable: bool,
}
impl Store {
    pub fn initialize(path: impl AsRef<Path>, identity: NodeIdentity) -> Result<Self, StoreError> {
        validate_identity(identity)?;
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    StoreError::AlreadyInitialized
                } else {
                    e.into()
                }
            })?;
        let database = Database::builder().create_file(file)?;
        let store = Self::initialize_database(database, identity)?;
        File::open(path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
        Ok(store)
    }
    fn initialize_database(database: Database, identity: NodeIdentity) -> Result<Self, StoreError> {
        let mut store = Self {
            database,
            writable: true,
        };
        let tx = store.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            meta.insert("schema", 1u64.to_be_bytes().as_slice())?;
            meta.insert("identity", encode_identity(identity).as_slice())?;
            tx.open_table(VOTE)?;
            tx.open_table(LOGS)?;
            tx.open_table(source_page::EVENT_SEQUENCE)?;
        }
        store.commit(tx)?;
        Ok(store)
    }
    pub fn open(path: impl AsRef<Path>, expected: NodeIdentity) -> Result<Self, StoreError> {
        validate_identity(expected)?;
        let path = path.as_ref();
        if !path.exists() {
            return Err(StoreError::MissingStore);
        }
        let database = Database::open(path)?;
        Self::from_database(database, expected)
    }
    /// Reopen with an explicit temporary-space budget when the retained suffix
    /// has outgrown the budget recorded at the last checkpoint.
    pub fn open_with_checkpoint_limits(
        path: impl AsRef<Path>,
        expected: NodeIdentity,
        limits: CheckpointLimits,
    ) -> Result<Self, StoreError> {
        validate_identity(expected)?;
        limits.validate()?;
        let path = path.as_ref();
        if !path.exists() {
            return Err(StoreError::MissingStore);
        }
        Self::from_database_with_limits(Database::open(path)?, expected, Some(limits))
    }

    fn from_database(database: Database, expected: NodeIdentity) -> Result<Self, StoreError> {
        Self::from_database_with_limits(database, expected, None)
    }

    fn from_database_with_limits(
        database: Database,
        expected: NodeIdentity,
        limits: Option<CheckpointLimits>,
    ) -> Result<Self, StoreError> {
        let mut store = Self {
            database,
            writable: true,
        };
        let tx = store.database.begin_read()?;
        let meta = tx.open_table(META)?;
        let schema = meta.get("schema")?.ok_or(StoreError::CorruptRecord)?;
        if !matches!(number(schema.value())?, 1..=7) {
            return Err(StoreError::UnsupportedSchema);
        }
        let identity = meta.get("identity")?.ok_or(StoreError::CorruptRecord)?;
        if decode_identity(identity.value())? != expected {
            return Err(StoreError::IdentityMismatch);
        }
        let vote = tx.open_table(VOTE)?;
        for row in vote.iter()? {
            let (key, value) = row?;
            if key.value() != 0 {
                return Err(StoreError::CorruptRecord);
            }
            decode_vote(value.value())?;
        }
        let logs = tx.open_table(LOGS)?;
        let purged = checkpoint_baseline::read_purged(&meta)?;
        let mut next = purged.map_or(Some(0), |id| id.index.checked_add(1));
        let mut last = purged;
        for row in logs.iter()? {
            let (key, value) = row?;
            if number(schema.value())? == 7 && value.value().len() > 1024 * 1024 {
                return Err(StoreError::CorruptRecord);
            }
            let entry = decode_log(value.value())?;
            if next != Some(key.value()) || entry.id.index != key.value() {
                return Err(if matches!(number(schema.value())?, 4 | 6 | 7) {
                    StoreError::CorruptRecord
                } else {
                    StoreError::InvalidLogSequence
                });
            }
            if matches!(number(schema.value())?, 4 | 6 | 7) {
                application::decode_native_entry(entry.clone())?;
            }
            next = key.value().checked_add(1);
            last = Some(entry.id);
        }
        committed::validate_committed(&meta, &logs)?;
        if number(schema.value())? == 7 {
            checkpoint_baseline::validate_recovery_with_limits(&tx, limits)?;
        } else {
            application::validate_application(&tx)?;
        }
        let tail = meta.get("tail")?.map(|v| decode_id(v.value())).transpose()?;
        if tail != last {
            return Err(if matches!(number(schema.value())?, 4 | 6 | 7) {
                StoreError::CorruptRecord
            } else {
                StoreError::InvalidLogSequence
            });
        }
        drop(schema);
        drop(identity);
        drop(logs);
        drop(vote);
        drop(meta);
        drop(tx);
        store.ensure_event_index()?;
        Ok(store)
    }
    fn write_transaction(&self) -> Result<WriteTransaction, StoreError> {
        if !self.writable {
            return Err(StoreError::WriteDisabled);
        }
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.set_two_phase_commit(true);
        Ok(tx)
    }
    fn commit(&mut self, tx: WriteTransaction) -> Result<(), StoreError> {
        tx.commit().map_err(|e| {
            self.writable = false;
            StoreError::CommitIndeterminate(e)
        })
    }
    fn purge_checkpoint(&mut self, generation: u64, cut: LogId) -> Result<(), StoreError> {
        self.purge_native_prefix(cut, Some((generation, cut)))
    }

    /// The selected durable baseline authorizes only an exact covered log prefix.
    /// Caller holds NativeLogWriter ordering and Store ownership throughout validation/commit.
    fn purge_native_prefix(&mut self, through: LogId, expected: Option<(u64, LogId)>) -> Result<(), StoreError> {
        if !self.writable {
            return Err(StoreError::WriteDisabled);
        }
        let read = self.database.begin_read()?;
        let meta = read.open_table(META)?;
        let selected = checkpoint_baseline::read_metadata(&meta)?.ok_or(StoreError::StaleCheckpoint)?;
        let Some(cut) = selected.header.applied else {
            checkpoint_baseline::validate_recovery(&read)?;
            return Err(StoreError::StaleCheckpoint);
        };
        if expected.is_some_and(|(generation, id)| generation != selected.generation || id != cut) {
            return Err(StoreError::StaleCheckpoint);
        }
        if through.index > cut.index || (through.index == cut.index && through != cut) {
            return Err(StoreError::StaleCheckpoint);
        }
        checkpoint_baseline::validate_recovery(&read)?;
        if let Some(purged) = checkpoint_baseline::read_purged(&meta)? {
            if through == purged {
                return Ok(());
            }
            if through.index <= purged.index {
                return Err(StoreError::InvalidCommittedLog);
            }
        }
        let logs = read.open_table(LOGS)?;
        let record = logs.get(through.index)?.ok_or(StoreError::InvalidCommittedLog)?;
        if decode_log(record.value())?.id != through {
            return Err(StoreError::InvalidCommittedLog);
        }
        let generation = selected.generation;
        drop(record);
        drop(logs);
        drop(meta);
        drop(read);
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let selected = checkpoint_baseline::read_metadata(&meta)?.ok_or(StoreError::StaleCheckpoint)?;
            if selected.generation != generation || selected.header.applied != Some(cut) {
                return Err(StoreError::StaleCheckpoint);
            }
            let mut logs = tx.open_table(LOGS)?;
            let record = logs.get(through.index)?.ok_or(StoreError::InvalidCommittedLog)?;
            if decode_log(record.value())?.id != through {
                return Err(StoreError::InvalidCommittedLog);
            }
            drop(record);
            logs.retain(|index, _| index > through.index)?;
            tx.open_table(application::APPLY_RESULTS)?
                .retain(|index, _| index > through.index)?;
            meta.insert(checkpoint_baseline::PURGED_KEY, checkpoint_baseline::encode_purged(through).as_slice())?;
        }
        self.commit(tx)
    }

    pub fn read_vote(&self) -> Result<Option<VoteRecord>, StoreError> {
        let tx = self.database.begin_read()?;
        let table = tx.open_table(VOTE)?;
        table.get(0)?.map(|v| decode_vote(v.value())).transpose()
    }
    pub fn save_vote(&mut self, vote: VoteRecord) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            tx.open_table(VOTE)?.insert(0, encode_vote(vote).as_slice())?;
        }
        self.commit(tx)
    }
    pub fn append(&mut self, entries: &[LogEntry]) -> Result<(), StoreError> {
        if !self.writable {
            return Err(StoreError::WriteDisabled);
        }
        let tx = self.database.begin_read()?;
        if matches!(
            number(tx.open_table(META)?.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())?,
            4 | 6 | 7
        ) {
            return Err(StoreError::UnsupportedSchema);
        }
        drop(tx);
        if entries.is_empty() {
            return Ok(());
        }
        let tail = self.log_state()?.last;
        let mut next = next_index(tail.map(|id| id.index))?;
        for (position, entry) in entries.iter().enumerate() {
            if entry.id.index != next {
                return Err(StoreError::InvalidLogSequence);
            }
            if position + 1 < entries.len() {
                next = next_index(Some(next))?;
            }
        }
        let encoded: Vec<Vec<u8>> = entries.iter().map(encode_log).collect::<Result<_, _>>()?;
        let tx = self.write_transaction()?;
        {
            let mut logs = tx.open_table(LOGS)?;
            for (entry, bytes) in entries.iter().zip(&encoded) {
                logs.insert(entry.id.index, bytes.as_slice())?;
            }
            let last = entries.last().ok_or(StoreError::InvalidLogSequence)?;
            tx.open_table(META)?.insert("tail", encode_id(last.id).as_slice())?;
        }
        self.commit(tx)
    }
    pub fn read_entry(&self, index: u64) -> Result<Option<LogEntry>, StoreError> {
        let tx = self.database.begin_read()?;
        let table = tx.open_table(LOGS)?;
        table
            .get(index)?
            .map(|v| {
                let entry = decode_log(v.value())?;
                if entry.id.index != index {
                    return Err(StoreError::CorruptRecord);
                }
                Ok(entry)
            })
            .transpose()
    }
    pub fn read_entries(&self, start: u64, end: u64) -> Result<Vec<LogEntry>, StoreError> {
        if start > end {
            return Err(StoreError::InvalidLogSequence);
        }
        let tx = self.database.begin_read()?;
        let logs = tx.open_table(LOGS)?;
        let mut entries = Vec::new();
        for row in logs.range(start..end)? {
            let (key, value) = row?;
            let entry = decode_log(value.value())?;
            if entry.id.index != key.value() {
                return Err(StoreError::CorruptRecord);
            }
            entries.push(entry);
        }
        Ok(entries)
    }
    pub fn log_state(&self) -> Result<LogState, StoreError> {
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(META)?;
        let last = meta.get("tail")?.map(|v| decode_id(v.value())).transpose()?;
        let logs = tx.open_table(LOGS)?;
        let first = logs
            .first()?
            .map(|(_, value)| decode_log(value.value()).map(|entry| entry.id))
            .transpose()?;
        Ok(LogState { first, last })
    }
}
fn validate_identity(id: NodeIdentity) -> Result<(), StoreError> {
    if id.cluster_id == [0; 16] {
        Err(StoreError::CorruptRecord)
    } else {
        Ok(())
    }
}
fn next_index(tail: Option<u64>) -> Result<u64, StoreError> {
    tail.map_or(Ok(0), |index| index.checked_add(1).ok_or(StoreError::IndexExhausted))
}
fn number(bytes: &[u8]) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| StoreError::CorruptRecord)?))
}
fn encode_identity(id: NodeIdentity) -> Vec<u8> {
    let mut b = id.cluster_id.to_vec();
    b.extend(id.node_id.to_be_bytes());
    b
}
fn decode_identity(b: &[u8]) -> Result<NodeIdentity, StoreError> {
    if b.len() != 24 {
        return Err(StoreError::CorruptRecord);
    }
    let id = NodeIdentity {
        cluster_id: b[..16].try_into().map_err(|_| StoreError::CorruptRecord)?,
        node_id: number(&b[16..])?,
    };
    validate_identity(id)?;
    Ok(id)
}
fn encode_vote(v: VoteRecord) -> Vec<u8> {
    let mut b = v.term.to_be_bytes().to_vec();
    b.extend(v.voted_node.to_be_bytes());
    b.push(u8::from(v.committed));
    b
}
fn decode_vote(b: &[u8]) -> Result<VoteRecord, StoreError> {
    if b.len() != 17 || b[16] > 1 {
        return Err(StoreError::CorruptRecord);
    }
    Ok(VoteRecord {
        term: number(&b[..8])?,
        voted_node: number(&b[8..16])?,
        committed: b[16] == 1,
    })
}
fn encode_id(id: LogId) -> Vec<u8> {
    let mut b = id.term.to_be_bytes().to_vec();
    b.extend(id.leader_node.to_be_bytes());
    b.extend(id.index.to_be_bytes());
    b
}
fn decode_id(b: &[u8]) -> Result<LogId, StoreError> {
    if b.len() != 24 {
        return Err(StoreError::CorruptRecord);
    }
    Ok(LogId {
        term: number(&b[..8])?,
        leader_node: number(&b[8..16])?,
        index: number(&b[16..])?,
    })
}
fn encode_log(entry: &LogEntry) -> Result<Vec<u8>, StoreError> {
    let mut b = encode_id(entry.id);
    b.extend(
        u64::try_from(entry.payload.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes(),
    );
    b.extend(&entry.payload);
    Ok(b)
}
fn decode_log(b: &[u8]) -> Result<LogEntry, StoreError> {
    if b.len() < 32 {
        return Err(StoreError::CorruptRecord);
    }
    let len = usize::try_from(number(&b[24..32])?).map_err(|_| StoreError::CorruptRecord)?;
    if len.checked_add(32) != Some(b.len()) {
        return Err(StoreError::CorruptRecord);
    }
    Ok(LogEntry {
        id: decode_id(&b[..24])?,
        payload: b[32..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;
    use std::{
        io::{BufRead, BufReader, Read, Write},
        process::{Command, Stdio},
        sync::{Arc, Condvar, Mutex, mpsc},
        time::Duration,
    };
    #[derive(Debug, Default)]
    pub(super) struct Gate {
        arrivals: Mutex<usize>,
        changed: Condvar,
    }
    impl Gate {
        pub(super) fn wait(&self) {
            let mut arrivals = self.arrivals.lock().expect("gate");
            *arrivals += 1;
            self.changed.notify_all();
            let (_arrivals, timeout) = self
                .changed
                .wait_timeout_while(arrivals, Duration::from_secs(20), |n| *n < 2)
                .expect("gate wait");
            assert!(!timeout.timed_out(), "test IO rendezvous timed out");
        }
    }
    fn identity() -> NodeIdentity {
        NodeIdentity {
            cluster_id: [7; 16],
            node_id: 0,
        }
    }
    fn u19_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("reader runtime")
    }

    fn u19_assert_io(error: openraft::StorageError<u64>) {
        match error {
            openraft::StorageError::IO { source } => {
                assert!(source.to_string().starts_with("when Read Logs:"), "U19_READER_ERROR: {source}");
            }
            other => panic!("U19_READER_ERROR: unexpected error {other:?}"),
        }
    }

    fn u21_fixture() -> (tempfile::TempDir, CrashBackend, NativeLogWriter, Vec<CaptureRaftEntry>, CaptureMembership) {
        let (dir, backend, mut store) = u17a_store();
        let (entry, membership) = u17a_membership(1);
        let entries = vec![u17a_blank(0), entry, u17a_created(2, u17a_command())];
        store.append_native(&entries).expect("persist native batch");
        (dir, backend, NativeLogWriter::new(store), entries, membership)
    }

    #[test]
    fn u21_apply_native_batch() {
        let (_dir, backend, writer, entries, membership) = u21_fixture();
        let result = u19_runtime().block_on(writer.apply_committed(entries));
        assert!(result.is_ok(), "U21_APPLY_SUCCESS: {result:?}");
        let responses = result.expect("batch responses");
        let expected = {
            let store = writer.store.lock().expect("store");
            (0..3)
                .map(|i| {
                    store
                        .native_apply_response(i)
                        .expect("receipt")
                        .expect("U21_APPLY_DURABLE persisted receipt")
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(responses, expected, "U21_APPLY_DURABLE");
        assert_eq!(responses[0], CaptureResponse::blank(u17a_id(0)).expect("Blank"));
        assert_eq!(responses[1], CaptureResponse::membership(u17a_id(1)).expect("Membership"));
        {
            let store = writer.store.lock().expect("store");
            assert_eq!(
                responses[2],
                CaptureResponse::created(store.created_apply_result(2).expect("Created").expect("durable Created"))
                    .expect("Created response")
            );
            assert!(
                store
                    .created_operation(u17a_command().operation_id)
                    .expect("operation")
                    .is_some(),
                "U21_APPLY_DURABLE"
            );
        }
        assert_eq!(
            u19_runtime().block_on(writer.applied_state()).expect("checkpoint"),
            (Some(u17a_id(2)), membership.clone())
        );
        backend.cut();
        drop(writer);
        let store = Store::open(&backend.path, identity()).expect("cut reopen");
        assert_eq!(store.native_application_state().expect("reopened pair"), (Some(u17a_id(2)), membership));
        for (i, response) in expected.iter().enumerate() {
            assert_eq!(
                store
                    .native_apply_response(u64::try_from(i).expect("index"))
                    .expect("reopened receipt")
                    .as_ref(),
                Some(response)
            );
        }
    }

    #[test]
    fn u21_apply_batch_rejects_mismatch() {
        let (_dir, backend, writer, entries, _) = u21_fixture();
        let old = u19_runtime().block_on(writer.applied_state()).expect("old pair");
        let mut payload = entries.clone();
        payload[1].payload = openraft::EntryPayload::Blank;
        let mut id = entries.clone();
        id[1].log_id = LogId { term: 12, ..u17a_id(1) }.into();
        let mut command = u17a_command();
        command.prepared.content_length += 1;
        let mut changed = entries.clone();
        changed[2] = u17a_created(2, command);
        let syncs = backend.media.lock().expect("media").syncs;
        for invalid in [
            payload,
            id,
            changed,
            vec![entries[0].clone(), entries[2].clone()],
            vec![entries[1].clone(), entries[0].clone()],
            vec![entries[0].clone(), entries[0].clone()],
            vec![entries[2].clone()],
        ] {
            assert!(u19_runtime().block_on(writer.apply_committed(invalid)).is_err(), "U21_ENTRY_MATCH");
            assert_eq!(
                u19_runtime().block_on(writer.applied_state()).expect("unchanged pair"),
                old,
                "U21_ENTRY_MATCH"
            );
            let store = writer.store.lock().expect("store");
            assert_eq!(store.read_committed().expect("commit unchanged"), None, "U21_ENTRY_MATCH");
            assert_eq!(
                store
                    .created_operation(u17a_command().operation_id)
                    .expect("business unchanged"),
                None,
                "U21_ENTRY_MATCH"
            );
        }
        assert_eq!(backend.media.lock().expect("media").syncs, syncs);
    }

    #[test]
    fn u21_apply_replay_prefix() {
        let (_dir, backend, writer, entries, membership) = u21_fixture();
        let runtime = u19_runtime();
        let prefix = runtime
            .block_on(writer.apply_committed(entries[..2].to_vec()))
            .expect("prefix");
        let responses = runtime
            .block_on(writer.apply_committed(entries.clone()))
            .expect("prefix replay and suffix");
        assert_eq!(responses[..2], prefix);
        let before = {
            let store = writer.store.lock().expect("store");
            (
                store.created_operation(u17a_command().operation_id).expect("operation"),
                store.created_event(u17a_command().operation_id).expect("event"),
                store
                    .created_version(&u17a_command().object, u17a_command().prepared.version_id)
                    .expect("version"),
            )
        };
        let syncs = backend.media.lock().expect("media").syncs;
        assert_eq!(
            runtime
                .block_on(writer.apply_committed(entries.clone()))
                .expect("full replay"),
            responses
        );
        assert_eq!(
            runtime
                .block_on(writer.apply_committed(entries[..2].to_vec()))
                .expect("short replay"),
            prefix
        );
        let store = writer.store.lock().expect("store");
        assert_eq!(store.read_committed().expect("no regression"), Some(u17a_id(2)));
        assert_eq!(store.native_application_state().expect("pair"), (Some(u17a_id(2)), membership));
        assert_eq!(
            (
                store.created_operation(u17a_command().operation_id).expect("operation"),
                store.created_event(u17a_command().operation_id).expect("event"),
                store
                    .created_version(&u17a_command().object, u17a_command().prepared.version_id)
                    .expect("version")
            ),
            before
        );
        assert_eq!(backend.media.lock().expect("media").syncs, syncs);
    }

    #[test]
    fn u21_apply_empty_failure_fence() {
        let (_dir, backend, writer, _, _) = u21_fixture();
        let syncs = backend.media.lock().expect("media").syncs;
        assert_eq!(u19_runtime().block_on(writer.apply_committed(vec![])).expect("empty"), vec![]);
        assert_eq!(backend.media.lock().expect("media").syncs, syncs);
        let (_dir, _backend, store) = media_store();
        let legacy = NativeLogWriter::new(store);
        assert!(matches!(
            u19_runtime().block_on(legacy.apply_committed(vec![])),
            Err(NativeWriteError::Store(StoreError::UnsupportedSchema))
        ));
        legacy.store.lock().expect("store").writable = false;
        assert!(matches!(
            u19_runtime().block_on(legacy.apply_committed(vec![])),
            Err(NativeWriteError::Store(StoreError::WriteDisabled))
        ));
    }

    #[test]
    fn u21_apply_cut_and_retry() {
        let runtime = u19_runtime();
        let (_dir, backend, writer, entries, _) = u21_fixture();
        backend.media.lock().expect("media").syncs = 0;
        let expected = runtime
            .block_on(writer.apply_committed(entries))
            .expect("measure real successful apply");
        let syncs = backend.media.lock().expect("media").syncs;
        assert!(syncs > 0, "real apply syncs");
        for sync in 1..=syncs {
            for after in [false, true] {
                let (_dir, backend, writer, entries, membership) = u21_fixture();
                backend.arm(sync, after, false);
                assert!(
                    matches!(
                        runtime.block_on(writer.apply_committed(entries.clone())),
                        Err(NativeWriteError::Store(StoreError::CommitIndeterminate(_)))
                    ),
                    "cut must not ACK {sync}/{after}"
                );
                assert_eq!(backend.media.lock().expect("media").syncs, sync, "actual boundary reached");
                assert!(
                    matches!(
                        runtime.block_on(writer.applied_state()),
                        Err(NativeWriteError::Store(StoreError::WriteDisabled))
                    ),
                    "U21_CHECKPOINT_FENCE"
                );
                backend.cut();
                drop(writer);
                let store = Store::open(&backend.path, identity()).expect("reopen interrupted apply");
                let pair = store.native_application_state().expect("recovered pair");
                if let Some(last) = pair.0 {
                    assert!(last.index <= 2);
                    assert_eq!(
                        pair.1,
                        if last.index >= 1 {
                            membership.clone()
                        } else {
                            CaptureMembership::new(None, Vec::new(), std::collections::BTreeMap::new()).expect("empty membership")
                        }
                    );
                    for i in 0..=last.index {
                        assert_eq!(
                            store.native_apply_response(i).expect("recovered receipt"),
                            Some(expected[usize::try_from(i).expect("index")].clone())
                        );
                    }
                } else {
                    assert_eq!(
                        pair.1,
                        CaptureMembership::new(None, Vec::new(), std::collections::BTreeMap::new()).expect("empty membership")
                    );
                }
                let retry = NativeLogWriter::new(store);
                assert_eq!(
                    runtime
                        .block_on(retry.apply_committed(entries))
                        .expect("retry complete batch"),
                    expected
                );
                assert_eq!(
                    runtime.block_on(retry.applied_state()).expect("retry checkpoint"),
                    (Some(u17a_id(2)), membership)
                );
            }
        }
    }

    #[test]
    fn u21_apply_worker_and_order() {
        let (_dir, backend, writer, entries, _) = u21_fixture();
        backend.media.lock().expect("media").sync_threads.clear();
        let executor = std::thread::current().id();
        let (entered, resume) = backend.pause_sync(1, false);
        u19_runtime().block_on(async {
            let first_writer = writer.clone();
            let first = tokio::spawn(async move { first_writer.apply_committed(entries).await });
            tokio::task::spawn_blocking(move || entered.wait())
                .await
                .expect("apply entered sync");
            first.abort();
            assert!(first.await.expect_err("cancelled await").is_cancelled());
            assert!(writer.ordering.try_lock().is_err(), "cancel retains ordering guard");
            let second_writer = writer.clone();
            let second = tokio::spawn(async move { second_writer.save_vote(vote().into()).await });
            tokio::task::yield_now().await;
            assert!(!second.is_finished(), "later write waits");
            tokio::task::spawn_blocking(move || resume.wait())
                .await
                .expect("release sync");
            second.await.expect("later task").expect("later durable write");
            assert_eq!(writer.applied_state().await.expect("completed apply").0, Some(u17a_id(2)));
        });
        let media = backend.media.lock().expect("media");
        assert!(!media.sync_threads.is_empty());
        assert!(media.sync_threads.iter().all(|thread| *thread != executor));
    }

    #[test]
    fn u21_checkpoint_read_coherent() {
        let (_dir, backend, writer, entries, membership) = u21_fixture();
        let old = u19_runtime().block_on(writer.applied_state()).expect("old pair");
        let (entered, resume) = backend.pause_sync(1, false);
        u19_runtime().block_on(async {
            let apply_writer = writer.clone();
            let apply = tokio::spawn(async move { apply_writer.apply_committed(entries).await });
            tokio::task::spawn_blocking(move || entered.wait())
                .await
                .expect("apply entered");
            let read_writer = writer.clone();
            let read = tokio::spawn(async move { read_writer.applied_state().await });
            tokio::task::yield_now().await;
            assert!(!read.is_finished(), "checkpoint waits for store transaction");
            tokio::task::spawn_blocking(move || resume.wait())
                .await
                .expect("release apply");
            apply.await.expect("apply task").expect("apply");
            let pair = read.await.expect("read task").expect("read pair");
            assert!(pair == old || pair == (Some(u17a_id(2)), membership), "full coherent pair");
        });
    }

    #[test]
    fn u20_vote_durable_roundtrip() {
        let (_dir, backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        let expected = openraft::Vote::from(vote());
        u19_runtime().block_on(async {
            writer.save_vote(expected).await.expect("U20_VOTE_DURABLE save");
            assert_eq!(writer.read_vote().await.expect("read vote"), Some(expected), "U20_VOTE_DURABLE");
        });
        drop(writer);
        backend.cut();
        let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen vote");
        assert_eq!(reopened.read_vote().expect("durable vote"), Some(vote()), "U20_VOTE_DURABLE");
    }

    #[test]
    fn u20_native_append_reader_visibility() {
        use openraft::RaftLogReader;
        let (_dir, backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())];
        let mut reader = writer.reader();
        u19_runtime().block_on(async {
            assert!(writer.append_native(entries.clone()).await.is_ok(), "U20_WRITE_SUCCESS");
            assert_eq!(reader.try_get_log_entries(..).await.expect("shared reader"), entries);
        });
        drop(reader);
        drop(writer);
        backend.cut();
        let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen entries");
        assert_eq!(reopened.read_native_range(..).expect("durable entries"), entries);
    }

    #[test]
    fn u20_native_truncate_replacement() {
        use openraft::RaftLogReader;
        let (_dir, backend, mut store) = u17a_store();
        let original = vec![u17a_blank(0), u17a_blank(1), u17a_blank(2)];
        store.append_native(&original).expect("committed fixture entries");
        store.save_committed(u17a_id(0)).expect("committed prefix");
        let writer = NativeLogWriter::new(store);
        let replacement = u17a_created(1, u17a_command());
        let expected = vec![original[0].clone(), replacement.clone()];
        u19_runtime().block_on(async {
            assert!(matches!(
                writer.truncate_suffix(0).await,
                Err(NativeWriteError::Store(StoreError::CommittedLogProtected))
            ));
            writer.truncate_suffix(1).await.expect("truncate suffix");
            writer.append_native(vec![replacement]).await.expect("replace suffix");
            assert_eq!(writer.reader().try_get_log_entries(..).await.expect("replacement reader"), expected);
        });
        drop(writer);
        backend.cut();
        let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen replacement");
        assert_eq!(reopened.read_native_range(..).expect("durable replacement"), expected);
    }

    #[test]
    fn u20_worker_thread() {
        let (_dir, backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        backend.media.lock().expect("media").sync_threads.clear();
        let executor = std::thread::current().id();
        u19_runtime().block_on(writer.save_vote(vote().into())).expect("worker vote");
        let media = backend.media.lock().expect("media");
        assert!(!media.sync_threads.is_empty(), "U20_WORKER_THREAD no real sync");
        assert!(media.sync_threads.iter().all(|thread| *thread != executor), "U20_WORKER_THREAD");
    }

    #[test]
    fn u20_cancelled_write_stays_serialized() {
        let (_dir, backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        let (entered, resume) = backend.pause_sync(1, false);
        u19_runtime().block_on(async {
            let first_writer = writer.clone();
            let first = tokio::spawn(async move { first_writer.save_vote(vote().into()).await });
            tokio::task::spawn_blocking(move || entered.wait())
                .await
                .expect("first sync entered");
            first.abort();
            assert!(first.await.expect_err("cancel first await").is_cancelled());
            let held = writer.ordering.try_lock().is_err();
            let second_writer = writer.clone();
            let second = tokio::spawn(async move { second_writer.append_native(vec![u17a_blank(0)]).await });
            tokio::task::yield_now().await;
            let waiting = !second.is_finished();
            tokio::task::spawn_blocking(move || resume.wait())
                .await
                .expect("release sync");
            second.await.expect("second task").expect("second commit");
            assert!(held && waiting, "U20_WRITE_ORDER cancelled caller released ordering");
            assert_eq!(writer.read_vote().await.expect("first committed vote"), Some(vote().into()));
        });
    }

    #[test]
    fn u20_failed_commit_disables_mutations() {
        let (_dir, backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        backend.arm(1, false, false);
        u19_runtime().block_on(async {
            assert!(
                matches!(
                    writer.save_vote(vote().into()).await,
                    Err(NativeWriteError::Store(StoreError::CommitIndeterminate(_)))
                ),
                "U20_WRITE_FAILURE"
            );
            assert!(matches!(
                writer.save_vote(vote().into()).await,
                Err(NativeWriteError::Store(StoreError::WriteDisabled))
            ));
            assert!(matches!(
                writer.append_native(vec![u17a_blank(0)]).await,
                Err(NativeWriteError::Store(StoreError::WriteDisabled))
            ));
            assert!(matches!(
                writer.truncate_suffix(0).await,
                Err(NativeWriteError::Store(StoreError::WriteDisabled))
            ));
        });
    }

    #[test]
    fn u20_worker_poison_errors() {
        let (_dir, _backend, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        u19_runtime().block_on(async {
            assert!(matches!(
                writer.write::<(), _>(|_| panic!("injected worker panic")).await,
                Err(NativeWriteError::Worker(_))
            ));
            assert!(matches!(writer.save_vote(vote().into()).await, Err(NativeWriteError::Poisoned)));
            assert!(matches!(writer.append_native(vec![]).await, Err(NativeWriteError::Poisoned)));
            assert!(matches!(writer.truncate_suffix(0).await, Err(NativeWriteError::Poisoned)));
            assert!(matches!(writer.read_vote().await, Err(NativeWriteError::Poisoned)));
        });
    }

    #[test]
    fn u19_reader_roundtrip() {
        use openraft::RaftLogReader;
        let (_dir, _backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())];
        store.append_native(&entries).expect("append reader fixture");
        let mut reader = NativeLogReader::new(store);
        let read = u19_runtime().block_on(reader.try_get_log_entries(..)).expect("trait read");
        assert_eq!(read, entries, "U19_TRAIT_READ_SUCCESS");
    }

    #[test]
    fn u19_reader_bounds_reopen() {
        use openraft::RaftLogReader;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let (dir, backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())];
        store.append_native(&entries).expect("append range fixture");
        drop(store);
        drop(backend);
        let store = Store::open(dir.path().join("db"), identity()).expect("reopen reader file");
        let mut reader = NativeLogReader::new(store);
        u19_runtime().block_on(async {
            assert_eq!(
                reader.try_get_log_entries(1..2).await.expect("bounded"),
                entries[1..2],
                "U19_READER_RANGE"
            );
            assert_eq!(reader.try_get_log_entries(..=1).await.expect("inclusive"), entries[..2]);
            assert_eq!(
                reader
                    .try_get_log_entries((Excluded(0), Included(2)))
                    .await
                    .expect("excluded"),
                entries[1..]
            );
            assert_eq!(reader.try_get_log_entries(2..).await.expect("unbounded end"), entries[2..]);
            assert_eq!(
                reader
                    .try_get_log_entries((Unbounded, Included(u64::MAX)))
                    .await
                    .expect("max"),
                entries
            );
            for range in [
                (Included(2), Excluded(2)),
                (Included(2), Included(1)),
                (Excluded(u64::MAX), Unbounded),
            ] {
                assert!(reader.try_get_log_entries(range).await.expect("empty bounds").is_empty());
            }
            let start = 1;
            let end = 2;
            assert_eq!(
                reader
                    .try_get_log_entries((Included(&start), Excluded(&end)))
                    .await
                    .expect("borrowed bounds"),
                entries[1..2]
            );
        });
    }

    #[test]
    fn u19_reader_legacy_corrupt() {
        use openraft::RaftLogReader;
        let (_legacy_dir, _legacy_backend, legacy) = u17a_bound();
        let mut legacy = NativeLogReader::new(legacy);
        let runtime = u19_runtime();
        u19_assert_io(
            runtime
                .block_on(legacy.try_get_log_entries(0..0))
                .expect_err("U19_READER_ERROR: legacy empty range"),
        );
        let (_dir, _backend, mut store) = u17a_store();
        store.append_native(&[u17a_blank(0)]).expect("append corruption fixture");
        let tx = store.database.begin_write().expect("corruption transaction");
        {
            let mut logs = tx.open_table(LOGS).expect("logs");
            logs.insert(0, b"corrupt".as_slice()).expect("corrupt row");
        }
        tx.commit().expect("persist corruption");
        let mut reader = NativeLogReader::new(store);
        u19_assert_io(
            runtime
                .block_on(reader.try_get_log_entries(..))
                .expect_err("U19_READER_ERROR: corrupt row"),
        );
    }

    #[test]
    fn u19_reader_worker_thread() {
        use openraft::RaftLogReader;
        let (_dir, _backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0)];
        store.append_native(&entries).expect("append worker fixture");
        let mut reader = NativeLogReader::new(store);
        let runtime = u19_runtime();
        let async_thread = std::thread::current().id();
        let (sender, receiver) = mpsc::channel();
        runtime.block_on(async {
            let read = native_read_worker(reader.store.clone(), move |store| {
                sender.send(std::thread::current().id()).expect("send worker thread");
                store.read_native_range(..)
            })
            .await
            .expect("worker read");
            assert_eq!(read, entries);
            assert_ne!(receiver.recv().expect("worker thread"), async_thread, "U19_BLOCKING_THREAD");
            assert_eq!(reader.try_get_log_entries(..).await.expect("trait read"), entries);
        });
    }

    #[test]
    fn u19_reader_worker_errors() {
        use openraft::RaftLogReader;
        let (_dir, _backend, store) = u17a_store();
        let mut reader = NativeLogReader::new(store);
        let runtime = u19_runtime();
        let error = runtime
            .block_on(native_read_worker(reader.store.clone(), |_| panic!("test worker panic")))
            .expect_err("worker panic must fail");
        assert!(error.to_string().contains("native log reader worker failed"));
        u19_assert_io(error);
        let error = runtime
            .block_on(reader.try_get_log_entries(..))
            .expect_err("poison must fail");
        assert!(error.to_string().contains("native log reader lock poisoned"));
        u19_assert_io(error);
    }

    fn provenance_promote(store: &mut Store) {
        let result = store.promote_created_provenance();
        assert_eq!(result.as_ref().map(|_| ()).map_err(StoreError::code), Ok(()), "U26_PROVENANCE_ENABLED");
    }

    fn provenance_assert(store: &Store, command: &DecideCreated, first: LogId, result: &CreatedResult) {
        let actual = store
            .created_operation_provenance(command.operation_id)
            .expect("provenance read")
            .expect("permanent operation");
        assert_eq!(actual.first_decision_log_id, first, "U26_FIRST_ID_STICKY");
        assert_eq!(actual.operation.canonical_command, command.encode().expect("canonical bytes"));
        assert_eq!(&actual.operation.result, result);
        assert_eq!(
            store.created_operation(command.operation_id).expect("compatible projection"),
            Some(actual.operation)
        );
    }

    fn provenance_commands() -> Vec<DecideCreated> {
        let success = u17a_command();
        let mut conflict = success.clone();
        conflict.binding_revision = 99;
        let mut binding = conflict.clone();
        binding.operation_id = [11; 16];
        binding.expected_head = Some([12; 16]);
        let mut head = success.clone();
        head.operation_id = [13; 16];
        let mut version = success.clone();
        version.operation_id = [14; 16];
        version.expected_head = Some(success.prepared.version_id);
        vec![
            success.clone(),
            success,
            conflict,
            binding.clone(),
            head.clone(),
            version.clone(),
            binding,
            head,
            version,
        ]
    }

    fn provenance_history(native: bool) -> (tempfile::TempDir, CrashBackend, Store, Vec<DecideCreated>) {
        let (dir, backend, mut store) = u17a_bound();
        store.save_vote(vote()).expect("nonempty provenance fixture vote");
        let commands = provenance_commands();
        if native {
            store.enable_native_empty().expect("native fixture");
            store
                .append_native(
                    &commands
                        .iter()
                        .enumerate()
                        .map(|(i, c)| u17a_created(u64::try_from(i).expect("index"), c.clone()))
                        .collect::<Vec<_>>(),
                )
                .expect("native prefix");
        } else {
            store
                .append(
                    &commands
                        .iter()
                        .enumerate()
                        .map(|(i, c)| LogEntry {
                            id: u17a_id(u64::try_from(i).expect("index")),
                            payload: c.encode().expect("legacy canonical"),
                        })
                        .collect::<Vec<_>>(),
                )
                .expect("legacy prefix");
        }
        store.save_committed(u17a_id(8)).expect("complete commit");
        for i in 0..9 {
            if native {
                store.apply_next_native(u17a_id(i)).expect("native apply");
            } else {
                store.apply_next_created(u17a_id(i)).expect("legacy apply");
            }
        }
        (dir, backend, store, commands)
    }

    #[test]
    fn provenance_first_decisions_and_retries_reopen() {
        let (_dir, backend, mut store) = u17a_store();
        provenance_promote(&mut store);
        assert_eq!(committed_schema(&store), 6);
        let commands = provenance_commands();
        let entries = commands
            .iter()
            .enumerate()
            .map(|(i, c)| u17a_created(u64::try_from(i).expect("index"), c.clone()))
            .collect::<Vec<_>>();
        store.append_native(&entries).expect("Created entries");
        store.save_committed(u17a_id(8)).expect("committed Created");
        for i in 0..9 {
            store.apply_next_native(u17a_id(i)).expect("decision and retry");
        }
        let expected = [
            (
                0usize,
                0u64,
                CreatedResult::Created {
                    version_id: [3; 16],
                    event_id: [1; 16],
                    decision_log_id: u17a_id(0),
                },
            ),
            (3, 3, CreatedResult::BindingMismatch),
            (
                4,
                4,
                CreatedResult::HeadMismatch {
                    actual_head: Some([3; 16]),
                },
            ),
            (5, 5, CreatedResult::VersionConflict),
        ];
        for (index, first, result) in &expected {
            provenance_assert(&store, &commands[*index], u17a_id(*first), result);
        }
        assert_eq!(
            store
                .native_apply_response(2)
                .expect("conflict receipt")
                .expect("receipt")
                .created_result(),
            Some(&CreatedResult::OperationConflict)
        );
        drop(store);
        let store = Store::open(&backend.path, identity()).expect("reopen full provenance replay");
        for (index, first, result) in &expected {
            provenance_assert(&store, &commands[*index], u17a_id(*first), result);
        }
        assert_eq!(store.created_event(commands[3].operation_id).expect("negative event"), None);
        assert_eq!(encode_id(u17a_id(3)).len(), 24);
    }

    #[test]
    fn provenance_native_controls_preserve_state() {
        let (_dir, backend, mut store) = u17a_store();
        provenance_promote(&mut store);
        let (member, membership) = u17a_membership(1);
        let entries = vec![u17a_blank(0), member, u17a_created(2, u17a_command()), u17a_blank(3)];
        let writer = NativeLogWriter::new(store);
        let runtime = u19_runtime();
        runtime.block_on(writer.append_native(entries.clone())).expect("async append");
        let responses = runtime
            .block_on(writer.apply_committed(entries))
            .expect("async committed apply");
        assert_eq!(responses[0], CaptureResponse::blank(u17a_id(0)).expect("blank"));
        assert_eq!(responses[1], CaptureResponse::membership(u17a_id(1)).expect("membership"));
        assert_eq!(
            runtime.block_on(writer.applied_state()).expect("applied pair"),
            (Some(u17a_id(3)), membership.clone())
        );
        {
            let store = writer.store.lock().expect("store");
            assert_eq!(
                store
                    .database
                    .begin_read()
                    .expect("read")
                    .open_table(application::OPERATIONS)
                    .expect("operations")
                    .len()
                    .expect("count"),
                1
            );
            assert_eq!(store.created_apply_result(0).expect("blank projection"), None);
            assert_eq!(store.created_apply_result(1).expect("membership projection"), None);
        }
        let view = runtime.block_on(writer.snapshot_view()).expect("schema6 native view");
        drop(view);
        backend.cut();
        drop(writer);
        let store = Store::open(&backend.path, identity()).expect("async ACK cut reopen");
        assert_eq!(store.native_application_state().expect("recovered pair"), (Some(u17a_id(3)), membership));
        for (i, response) in responses.iter().enumerate() {
            assert_eq!(
                store
                    .native_apply_response(u64::try_from(i).expect("index"))
                    .expect("receipt")
                    .as_ref(),
                Some(response),
                "U26_DURABLE_ACK"
            );
        }
        assert_eq!(
            store
                .created_operation_provenance([1; 16])
                .expect("durable operation")
                .expect("operation")
                .first_decision_log_id,
            u17a_id(2),
            "U26_DURABLE_ACK"
        );
    }

    #[test]
    fn provenance_promotes_schema3_and_schema4_exact_history() {
        for native in [false, true] {
            let (_dir, backend, mut store, commands) = provenance_history(native);
            let old = u17b_snapshot(&store);
            assert_eq!(
                store
                    .created_operation_provenance([1; 16])
                    .expect_err("old schema has no provenance")
                    .code(),
                "unsupported_schema"
            );
            provenance_promote(&mut store);
            assert_eq!(committed_schema(&store), if native { 6 } else { 5 });
            let new = u17b_snapshot(&store);
            let metadata_without_schema = |rows: &Vec<(Vec<u8>, Vec<u8>)>| {
                rows.iter()
                    .filter(|(key, _)| key.as_slice() != b"schema")
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                metadata_without_schema(&old[0]),
                metadata_without_schema(&new[0]),
                "only schema metadata changes"
            );
            for i in 1..6 {
                assert_eq!(old[i], new[i], "promotion preserves logs, responses and business state");
            }
            assert_eq!(old[7], new[7], "promotion preserves vote");
            for (old_row, new_row) in old[6].iter().zip(&new[6]) {
                assert_eq!(old_row.0, new_row.0);
                assert_eq!(&new_row.1[..5], b"COPR\x01");
                assert_eq!(&new_row.1[29..], old_row.1.as_slice(), "canonical old value must remain byte exact");
            }
            for (i, first) in [(0usize, 0u64), (3, 3), (4, 4), (5, 5)] {
                let row = store
                    .created_operation_provenance(commands[i].operation_id)
                    .expect("promoted read")
                    .expect("operation");
                assert_eq!(row.first_decision_log_id, u17a_id(first), "U26_PROMOTION_FIRST_ID");
            }
            let once = u17b_snapshot(&store);
            provenance_promote(&mut store);
            assert_eq!(u17b_snapshot(&store), once);
            drop(store);
            let mut store = Store::open(&backend.path, identity()).expect("reopen promoted history");
            let mut suffix = commands[0].clone();
            suffix.operation_id = [20; 16];
            suffix.prepared.version_id = [21; 16];
            suffix.expected_head = Some([3; 16]);
            if native {
                store
                    .append_native(&[u17a_created(9, suffix.clone())])
                    .expect("native suffix");
            } else {
                store
                    .append(&[LogEntry {
                        id: u17a_id(9),
                        payload: suffix.encode().expect("suffix"),
                    }])
                    .expect("legacy suffix");
            }
            store.save_committed(u17a_id(9)).expect("suffix committed");
            if native {
                store.apply_next_native(u17a_id(9)).expect("native suffix apply");
            } else {
                store.apply_next_created(u17a_id(9)).expect("legacy suffix apply");
            }
            assert_eq!(
                store
                    .created_operation_provenance(suffix.operation_id)
                    .expect("suffix provenance")
                    .expect("suffix operation")
                    .first_decision_log_id,
                u17a_id(9)
            );
        }
    }

    #[test]
    fn provenance_rejects_incomplete_or_forged_prefix_atomically() {
        use application::{APPLY_RESULTS, EVENTS, HEADS, OPERATIONS, VERSIONS};
        for native in [false, true] {
            for corruption in 0..9 {
                let (_dir, _backend, mut store, commands) = provenance_history(native);
                let tx = store.database.begin_write().expect("corruption transaction");
                match corruption {
                    0 => {
                        tx.open_table(LOGS).expect("logs").remove(0).expect("missing prefix");
                    }
                    1 => {
                        tx.open_table(APPLY_RESULTS)
                            .expect("responses")
                            .remove(3)
                            .expect("missing negative response");
                    }
                    2 => {
                        let mut logs = tx.open_table(LOGS).expect("logs");
                        let raw = logs.get(3).expect("get").expect("row").value().to_vec();
                        let mut entry = decode_log(&raw).expect("log");
                        entry.id.term += 1;
                        logs.insert(3, encode_log(&entry).expect("encode").as_slice())
                            .expect("wrong full ID");
                    }
                    3 => {
                        let mut operations = tx.open_table(OPERATIONS).expect("operations");
                        let raw = operations
                            .get(commands[3].operation_id.as_slice())
                            .expect("get")
                            .expect("row")
                            .value()
                            .to_vec();
                        operations.insert([99; 16].as_slice(), raw.as_slice()).expect("orphan");
                    }
                    4 => {
                        tx.open_table(EVENTS)
                            .expect("events")
                            .remove([1; 16].as_slice())
                            .expect("missing event");
                    }
                    5 => {
                        let mut meta = tx.open_table(META).expect("meta");
                        meta.insert("last_applied", encode_id(u17a_id(7)).as_slice())
                            .expect("wrong applied ID");
                    }
                    6 => {
                        tx.open_table(HEADS)
                            .expect("heads")
                            .remove(crate::created::object_bytes(&commands[0].object).expect("object").as_slice())
                            .expect("missing head");
                    }
                    7 => {
                        let mut versions = tx.open_table(VERSIONS).expect("versions");
                        let key = versions
                            .iter()
                            .expect("iter")
                            .next()
                            .expect("version")
                            .expect("row")
                            .0
                            .value()
                            .to_vec();
                        versions.remove(key.as_slice()).expect("missing version");
                    }
                    _ => {
                        let mut logs = tx.open_table(LOGS).expect("logs");
                        let raw = logs.get(2).expect("get").expect("row").value().to_vec();
                        let mut entry = decode_log(&raw).expect("log");
                        entry.payload = if native {
                            decode_log(logs.get(3).expect("source log").expect("source row").value())
                                .expect("source entry")
                                .payload
                        } else {
                            commands[3].encode().expect("canonical")
                        };
                        logs.insert(2, encode_log(&entry).expect("encode").as_slice())
                            .expect("forged early conflict");
                    }
                }
                tx.commit().expect("inject corruption");
                let before = u17b_snapshot(&store);
                assert_eq!(
                    store
                        .promote_created_provenance()
                        .expect_err("corrupt promotion refused")
                        .code(),
                    "corrupt_record"
                );
                assert_eq!(u17b_snapshot(&store), before, "rejection must have zero writes");
            }
        }
    }

    #[test]
    fn provenance_empty_first_commit_never_downgrades() {
        for native in [false, true] {
            let (_dir, backend, mut store) = u17a_bound();
            provenance_promote(&mut store);
            assert_eq!(committed_schema(&store), 5);
            if native {
                store.enable_native_empty().expect("empty5 explicit native6");
                store.append_native(&[u17a_blank(0)]).expect("blank");
            } else {
                store
                    .append(&[LogEntry {
                        id: u17a_id(0),
                        payload: u17a_command().encode().expect("command"),
                    }])
                    .expect("legacy");
            }
            store.save_committed(u17a_id(0)).expect("first committed");
            assert_eq!(committed_schema(&store), if native { 6 } else { 5 }, "U26_SCHEMA_NEVER_DOWNGRADES");
            if native {
                store.apply_next_native(u17a_id(0)).expect("blank apply");
            } else {
                store.apply_next_created(u17a_id(0)).expect("legacy apply");
                assert_eq!(
                    store.enable_native_empty().expect_err("populated5 forbidden").code(),
                    "unsupported_schema"
                );
                assert_eq!(
                    store
                        .migrate_legacy_created_to_native()
                        .expect_err("U17B cannot reinterpret5")
                        .code(),
                    "unsupported_schema"
                );
            }
            drop(store);
            let store = Store::open(&backend.path, identity()).expect("reopen schema");
            assert_eq!(committed_schema(&store), if native { 6 } else { 5 }, "U26_SCHEMA_NEVER_DOWNGRADES");
            if !native {
                let writer = NativeLogWriter::new(store);
                assert!(
                    u19_runtime().block_on(writer.snapshot_view()).is_err(),
                    "schema5 cannot expose a native view"
                );
            }
        }
    }

    #[test]
    fn provenance_old_binary_and_unknown_versions_reject() {
        for native in [false, true] {
            for corruption in 0..10 {
                let (_dir, backend, mut store, _) = provenance_history(native);
                provenance_promote(&mut store);
                let tx = store.database.begin_write().expect("corruption");
                if corruption == 0 {
                    tx.open_table(META)
                        .expect("meta")
                        .insert("schema", 8u64.to_be_bytes().as_slice())
                        .expect("future schema");
                } else {
                    let mut operations = tx.open_table(application::OPERATIONS).expect("operations");
                    let key = [11; 16];
                    let mut raw = operations
                        .get(key.as_slice())
                        .expect("get")
                        .expect("negative")
                        .value()
                        .to_vec();
                    match corruption {
                        1 => raw[4] = 2,
                        2 => {
                            raw.pop();
                        }
                        3 => raw.push(0),
                        4 => raw[5..29].fill(0),
                        5 => raw[5..29].copy_from_slice(&encode_id(u17a_id(6))),
                        6 => {
                            let mut id = u17a_id(3);
                            id.leader_node += 1;
                            raw[5..29].copy_from_slice(&encode_id(id));
                        }
                        7 => {
                            let mut id = u17a_id(3);
                            id.term += 1;
                            raw[5..29].copy_from_slice(&encode_id(id));
                        }
                        8 => raw[29..37].copy_from_slice(&u64::MAX.to_be_bytes()),
                        _ => raw = raw[29..].to_vec(),
                    }
                    operations.insert(key.as_slice(), raw.as_slice()).expect("bad envelope");
                }
                tx.commit().expect("persist corruption");
                drop(store);
                assert_eq!(
                    Store::open(&backend.path, identity())
                        .err()
                        .expect("strict open rejects")
                        .code(),
                    if corruption == 0 {
                        "unsupported_schema"
                    } else {
                        "corrupt_record"
                    }
                );
            }
        }
        // The independent accepted old-binary probe is run by the root task.
    }

    #[test]
    fn provenance_promotion_and_apply_powercut_atomic() {
        for native in [false, true] {
            for promotion in [false, true] {
                let fixture = || {
                    let (dir, backend, mut store, _) = provenance_history(native);
                    if !promotion {
                        provenance_promote(&mut store);
                        let mut c = u17a_command();
                        c.operation_id = [30; 16];
                        c.binding_revision = 99;
                        if native {
                            store.append_native(&[u17a_created(9, c)]).expect("pending native");
                        } else {
                            store
                                .append(&[LogEntry {
                                    id: u17a_id(9),
                                    payload: c.encode().expect("pending canonical"),
                                }])
                                .expect("pending legacy");
                        }
                        store.save_committed(u17a_id(9)).expect("pending commit");
                    }
                    (dir, backend, store)
                };
                let action = |store: &mut Store| -> Result<(), StoreError> {
                    if promotion {
                        store.promote_created_provenance()
                    } else if native {
                        store.apply_next_native(u17a_id(9)).map(|_| ())
                    } else {
                        store.apply_next_created(u17a_id(9)).map(|_| ())
                    }
                };
                let (_dir, backend, mut store) = fixture();
                let old = u17b_snapshot(&store);
                backend.media.lock().expect("reset syncs").syncs = 0;
                let result = action(&mut store);
                assert_eq!(result.as_ref().map(|_| ()).map_err(StoreError::code), Ok(()), "U26_PROVENANCE_ENABLED");
                let reached = backend.media.lock().expect("syncs").syncs;
                assert_eq!(reached > 0, true, "U26_DURABLE_ACK");
                let new = u17b_snapshot(&store);
                backend.cut();
                drop(store);
                let database = Database::open(&backend.path).expect("ACK database recovery");
                assert_eq!(u17b_snapshot_database(&database), new, "U26_DURABLE_ACK");
                drop(database);
                let (_dir, backend, mut store) = fixture();
                backend.arm(1, false, false);
                assert_eq!(
                    action(&mut store).expect_err("uncertain provenance commit").code(),
                    "commit_indeterminate"
                );
                let fenced = u17b_snapshot(&store);
                let mutations: [fn(&mut Store) -> Result<(), StoreError>; 11] = [
                    Store::promote_created_provenance,
                    |store| store.append(&[]),
                    |store| store.append_native(&[]),
                    |store| store.save_vote(vote()),
                    |store| store.save_committed(u17a_id(8)),
                    |store| store.apply_next_created(u17a_id(0)).map(|_| ()),
                    |store| store.apply_next_native(u17a_id(0)).map(|_| ()),
                    |store| store.pin_capture_binding(u17a_binding()),
                    Store::enable_native_empty,
                    Store::migrate_legacy_created_to_native,
                    |store| store.truncate_suffix(9),
                ];
                for mutation in mutations {
                    assert_eq!(
                        mutation(&mut store)
                            .expect_err("uncertain handle must fence every mutation")
                            .code(),
                        "write_disabled",
                        "U26_WRITE_FENCE"
                    );
                    assert_eq!(u17b_snapshot(&store), fenced, "fenced mutation must not change any table");
                }
                let writer = NativeLogWriter::new(store);
                let result = u19_runtime().block_on(writer.apply_committed(vec![]));
                assert!(
                    matches!(result, Err(NativeWriteError::Store(StoreError::WriteDisabled))),
                    "U26_WRITE_FENCE async apply"
                );
                assert_eq!(u17b_snapshot(&writer.store.lock().expect("fenced writer")), fenced);
                drop(writer);
                for sync in 1..=reached {
                    for after in [false, true] {
                        let (_dir, backend, mut store) = fixture();
                        assert_eq!(u17b_snapshot(&store), old);
                        backend.arm(sync, after, true);
                        let result = action(&mut store);
                        let actual_syncs = backend.media.lock().expect("reached cut").syncs;
                        assert!(actual_syncs >= sync, "cut must actually reach target sync");
                        backend.cut();
                        drop(store);
                        match Database::open(&backend.path) {
                            Ok(database) => {
                                let actual = u17b_snapshot_database(&database);
                                assert!(actual == old || actual == new, "physical all-table atomicity");
                                if result.is_ok() {
                                    assert_eq!(actual, new, "U26_DURABLE_ACK");
                                }
                                let recovered = Store::from_database(database, identity())
                                    .expect("logical corruption cannot hide mixed state");
                                assert_eq!(u17b_snapshot(&recovered), actual);
                            }
                            Err(redb::DatabaseError::Storage(redb::StorageError::Corrupted(_))) => {
                                assert!(result.is_err(), "U26_DURABLE_ACK");
                            }
                            Err(error) => panic!("unexpected physical recovery error: {error:?}"),
                        }
                    }
                }
            }
            let (_dir, _backend, mut expected_store, _) = provenance_history(native);
            provenance_promote(&mut expected_store);
            let expected = u17b_snapshot(&expected_store);
            let dir = tempfile::tempdir().expect("child provenance directory");
            let path = dir.path().join("db");
            let mut store = Store::initialize(&path, identity()).expect("child database");
            store.pin_capture_binding(u17a_binding()).expect("child binding");
            drop(store);
            // Process termination after ACK is separate from the physical media cuts above.
            let mut process = child(&path, if native { "provenance-native" } else { "provenance-legacy" });
            ack(&mut process);
            process.kill().expect("SIGKILL acknowledged child");
            assert!(!process.wait().expect("reap killed child").success());
            let reopened = Store::open(&path, identity()).expect("independent child ACK reopen");
            assert_eq!(committed_schema(&reopened), if native { 6 } else { 5 });
            assert_eq!(u17b_snapshot(&reopened), expected, "U26_DURABLE_ACK complete child state");
        }
    }

    fn u17b_snapshot(store: &Store) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        u17b_snapshot_database(&store.database)
    }
    fn u17b_snapshot_database(database: &Database) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        use crate::application::{APPLY_RESULTS, EVENTS, HEADS, OPERATIONS, VERSIONS};
        let tx = database.begin_read().expect("snapshot transaction");
        let meta = tx.open_table(META).expect("snapshot metadata");
        let mut snapshot = vec![
            meta.iter()
                .expect("metadata iterator")
                .map(|row| {
                    let (key, value) = row.expect("metadata row");
                    (key.value().as_bytes().to_vec(), value.value().to_vec())
                })
                .collect(),
        ];
        for table in [LOGS, APPLY_RESULTS] {
            snapshot.push(
                tx.open_table(table)
                    .expect("snapshot indexed table")
                    .iter()
                    .expect("indexed iterator")
                    .map(|row| {
                        let (key, value) = row.expect("indexed row");
                        (key.value().to_be_bytes().to_vec(), value.value().to_vec())
                    })
                    .collect(),
            );
        }
        for table in [HEADS, VERSIONS, EVENTS, OPERATIONS] {
            snapshot.push(
                tx.open_table(table)
                    .expect("snapshot business table")
                    .iter()
                    .expect("business iterator")
                    .map(|row| {
                        let (key, value) = row.expect("business row");
                        (key.value().to_vec(), value.value().to_vec())
                    })
                    .collect(),
            );
        }
        snapshot.push(
            tx.open_table(VOTE)
                .expect("snapshot vote table")
                .iter()
                .expect("vote iterator")
                .map(|row| {
                    let (key, value) = row.expect("vote row");
                    (key.value().to_be_bytes().to_vec(), value.value().to_vec())
                })
                .collect(),
        );
        snapshot
    }
    fn u17b_history() -> (tempfile::TempDir, CrashBackend, Store, Vec<DecideCreated>) {
        let (dir, backend, mut store) = u17a_bound();
        let first = u17a_command();
        let mut rejected = first.clone();
        rejected.operation_id = [10; 16];
        rejected.prepared.version_id = [11; 16];
        let mut second = first.clone();
        second.operation_id = [12; 16];
        second.prepared.version_id = [13; 16];
        second.expected_head = Some(first.prepared.version_id);
        second.prepared.content_length = 8193;
        let mut suffix = second.clone();
        suffix.operation_id = [14; 16];
        suffix.prepared.version_id = [15; 16];
        suffix.expected_head = Some(second.prepared.version_id);
        let commands = vec![first.clone(), first, rejected, second, suffix];
        let entries = commands
            .iter()
            .enumerate()
            .map(|(index, command)| LogEntry {
                id: u17a_id(u64::try_from(index).expect("fixture index")),
                payload: command.encode().expect("canonical legacy Created"),
            })
            .collect::<Vec<_>>();
        store.append(&entries).expect("legacy history and suffix");
        store.save_committed(u17a_id(4)).expect("committed suffix");
        for index in 0..4 {
            store.apply_next_created(u17a_id(index)).expect("legacy history apply");
        }
        assert_eq!(committed_schema(&store), 3);
        (dir, backend, store, commands)
    }
    #[test]
    fn u17b_native_migration_populated_suffix() {
        let (_dir, backend, mut store, commands) = u17b_history();
        let before = u17b_snapshot(&store);
        let results = (0..4)
            .map(|index| {
                store
                    .created_apply_result(index)
                    .expect("legacy result")
                    .expect("applied result")
            })
            .collect::<Vec<_>>();
        assert!(
            store.migrate_legacy_created_to_native().is_ok(),
            "U17B_MIGRATION_SUCCESS canonical populated schema3 must migrate"
        );
        assert_eq!(committed_schema(&store), 4);
        assert_eq!(&u17b_snapshot(&store)[3..], &before[3..], "business bytes preserved");
        assert_eq!(store.capture_binding().expect("binding"), Some(u17a_binding()));
        assert_eq!(store.log_state().expect("tail").last, Some(u17a_id(4)));
        assert_eq!(store.read_committed().expect("committed"), Some(u17a_id(4)));
        assert_eq!(
            store.native_application_state().expect("application pair"),
            (
                Some(u17a_id(3)),
                CaptureMembership::new(None, vec![], std::collections::BTreeMap::new()).expect("empty membership")
            )
        );
        assert_eq!(
            store.read_native_entries(0, 5).expect("all entries"),
            commands
                .iter()
                .enumerate()
                .map(|(index, command)| u17a_created(u64::try_from(index).expect("index"), command.clone()))
                .collect::<Vec<_>>()
        );
        for result in results {
            assert_eq!(
                store.created_apply_result(result.log_id.index).expect("migrated result"),
                Some(result.clone())
            );
            assert_eq!(
                store
                    .native_apply_response(result.log_id.index)
                    .expect("native result")
                    .expect("response")
                    .created_result(),
                Some(&result.result)
            );
        }
        assert!(store.native_apply_response(4).expect("suffix response").is_none());
        assert_eq!(
            store
                .created_operation(commands[0].operation_id)
                .expect("first operation")
                .expect("operation")
                .result,
            CreatedResult::Created {
                version_id: commands[0].prepared.version_id,
                event_id: commands[0].operation_id,
                decision_log_id: u17a_id(0)
            }
        );
        drop(store);
        let mut store = Store::open(&backend.path, identity()).expect("migrated reopen");
        assert_eq!(&u17b_snapshot(&store)[3..], &before[3..]);
        let response = store.apply_next_native(u17a_id(4)).expect("real suffix application");
        assert_eq!(
            response.created_result(),
            Some(&CreatedResult::Created {
                version_id: commands[4].prepared.version_id,
                event_id: commands[4].operation_id,
                decision_log_id: u17a_id(4)
            })
        );
        assert_eq!(
            store.created_head(&commands[4].object).expect("suffix head"),
            Some(commands[4].prepared.version_id)
        );
        assert_eq!(store.last_applied().expect("suffix frontier"), Some(u17a_id(4)));
    }
    #[test]
    fn u17b_native_migration_opaque_refusal() {
        let canonical = u17a_command().encode().expect("canonical command");
        let mut noncanonical = canonical.clone();
        noncanonical.push(0);
        for payload in [
            b"opaque payload".to_vec(),
            canonical[..canonical.len() - 1].to_vec(),
            noncanonical,
        ] {
            let (_dir, backend, mut store) = u17a_bound();
            store
                .append(&[LogEntry { id: u17a_id(0), payload }])
                .expect("invalid legacy suffix append");
            let before = u17b_snapshot(&store);
            assert!(store.migrate_legacy_created_to_native().is_err(), "invalid payload must refuse");
            assert_eq!(u17b_snapshot(&store), before, "refusal changes no table bytes");
            drop(store);
            let store = Store::open(&backend.path, identity()).expect("refused suffix reopen");
            assert_eq!(u17b_snapshot(&store), before);
            assert_eq!(committed_schema(&store), 3);
        }
    }
    #[test]
    fn u17b_native_migration_empty_idempotent() {
        let (_dir, backend, store) = u17a_bound();
        drop(store);
        let mut store = Store::from_database(backend.database(), identity()).expect("open legacy without upgrade");
        assert_eq!(committed_schema(&store), 3);
        store.migrate_legacy_created_to_native().expect("empty migration");
        assert_eq!(committed_schema(&store), 4);
        let before = u17b_snapshot(&store);
        backend.media.lock().expect("reset migration syncs").syncs = 0;
        // Reopen through the instrumented backend before checking the no-commit path.
        drop(store);
        let mut store = Store::from_database(backend.database(), identity()).expect("native reopen");
        backend.media.lock().expect("reset reopen syncs").syncs = 0;
        store.migrate_legacy_created_to_native().expect("native repeat");
        assert_eq!(backend.media.lock().expect("repeat syncs").syncs, 0, "repeat must not commit");
        assert_eq!(u17b_snapshot(&store), before);
        store.writable = false;
        assert_eq!(
            store
                .migrate_legacy_created_to_native()
                .expect_err("disabled even on schema4")
                .code(),
            "write_disabled"
        );
    }
    #[test]
    fn u17b_native_migration_ack_and_cut() {
        let (_dir, backend, mut store, _) = u17b_history();
        let old = u17b_snapshot(&store);
        backend.media.lock().expect("reset migration syncs").syncs = 0;
        store.migrate_legacy_created_to_native().expect("migration ACK");
        let reached = backend.media.lock().expect("actual migration syncs").syncs;
        assert!(reached > 0, "migration must reach durability IO");
        let new = u17b_snapshot(&store);
        backend.cut();
        drop(store);
        assert_eq!(u17b_snapshot(&Store::open(&backend.path, identity()).expect("ACK recovery")), new);
        for sync in 1..=reached {
            for after in [false, true] {
                let (_dir, backend, mut store, _) = u17b_history();
                assert_eq!(u17b_snapshot(&store), old);
                let (entered, resume) = backend.pause_sync(sync, after);
                let writer = std::thread::spawn(move || {
                    let result = store.migrate_legacy_created_to_native();
                    (store, result)
                });
                entered.wait();
                backend.cut();
                resume.wait();
                let (store, result) = writer.join().expect("migration cut writer");
                drop(store);
                // A logical open refusal must not hide a physically durable mixed migration.
                if let Ok(database) = Database::open(&backend.path) {
                    let actual = u17b_snapshot_database(&database);
                    assert!(actual == old || actual == new, "U17B_ATOMIC_SCHEMA_MIGRATION");
                }
                if let Some(store) = u17a_cut_recovery(&backend) {
                    let actual = u17b_snapshot(&store);
                    assert!(
                        actual == old || actual == new,
                        "migration cut must never mix schema/log/result/business tables"
                    );
                    if result.is_ok() {
                        assert_eq!(actual, new, "ACK cannot recover old state");
                    }
                } else {
                    assert!(result.is_err(), "ACK recovery must succeed");
                }
            }
        }
        let (_dir, backend, mut store, _) = u17b_history();
        backend.arm(1, false, false);
        assert_eq!(
            store.migrate_legacy_created_to_native().expect_err("uncertain commit").code(),
            "commit_indeterminate"
        );
        assert_eq!(
            store
                .migrate_legacy_created_to_native()
                .expect_err("migration disabled")
                .code(),
            "write_disabled"
        );
        assert_eq!(store.append_native(&[]).expect_err("native append disabled").code(), "write_disabled");
        assert_eq!(store.append(&[]).expect_err("legacy append disabled").code(), "write_disabled");
        assert_eq!(
            store.apply_next_native(u17a_id(4)).expect_err("native apply disabled").code(),
            "write_disabled"
        );
        assert_eq!(
            store
                .apply_next_created(u17a_id(4))
                .expect_err("legacy apply disabled")
                .code(),
            "write_disabled"
        );
        assert_eq!(
            store
                .pin_capture_binding(u17a_binding())
                .expect_err("binding disabled")
                .code(),
            "write_disabled"
        );
        assert_eq!(store.save_committed(u17a_id(4)).expect_err("committed disabled").code(), "write_disabled");
        assert_eq!(store.save_vote(vote()).expect_err("vote disabled").code(), "write_disabled");
        assert_eq!(store.truncate_suffix(5).expect_err("truncate disabled").code(), "write_disabled");
    }
    fn u17a_id(index: u64) -> LogId {
        LogId {
            term: 11,
            leader_node: 7,
            index,
        }
    }
    fn u17a_binding() -> CaptureBinding {
        CaptureBinding {
            revision: 9,
            binding_id: [6; 16],
            target_id: [7; 16],
        }
    }
    fn u17a_bound() -> (tempfile::TempDir, CrashBackend, Store) {
        let (dir, backend, mut store) = media_store();
        store.pin_capture_binding(u17a_binding()).expect("pin empty schema3 fixture");
        (dir, backend, store)
    }
    fn u17a_store() -> (tempfile::TempDir, CrashBackend, Store) {
        let (dir, backend, mut store) = u17a_bound();
        store.enable_native_empty().expect("enable native empty fixture");
        (dir, backend, store)
    }
    fn u17a_blank(index: u64) -> CaptureRaftEntry {
        CaptureRaftEntry {
            log_id: u17a_id(index).into(),
            payload: openraft::EntryPayload::Blank,
        }
    }
    fn u17a_command() -> DecideCreated {
        DecideCreated {
            operation_id: [1; 16],
            object: ObjectIdentity {
                bucket_incarnation: [2; 16],
                key: b"native-object".to_vec(),
            },
            expected_head: None,
            prepared: PreparedIdentity {
                version_id: [3; 16],
                preparation_id: [4; 16],
                content_digest: [5; 32],
                content_length: 4097,
            },
            binding_revision: 9,
        }
    }
    fn u17a_created(index: u64, command: DecideCreated) -> CaptureRaftEntry {
        CaptureRaftEntry {
            log_id: u17a_id(index).into(),
            payload: openraft::EntryPayload::Normal(CaptureCommand::try_created(command).expect("checked native Created")),
        }
    }
    fn u17a_membership(index: u64) -> (CaptureRaftEntry, CaptureMembership) {
        use std::collections::{BTreeMap, BTreeSet};
        let membership = CaptureMembership::new(
            Some(u17a_id(index)),
            vec![BTreeSet::from([7, 8]), BTreeSet::from([8, 9])],
            BTreeMap::from([
                (7, CaptureNode::new("https://peer-7.example:7007".into()).expect("peer 7")),
                (8, CaptureNode::new("http://peer-8.example:7008".into()).expect("peer 8")),
                (9, CaptureNode::new("https://peer-9.example:7009".into()).expect("peer 9")),
                (10, CaptureNode::new("https://learner.example:7010".into()).expect("learner")),
            ]),
        )
        .expect("full joint membership");
        let stored = openraft::StoredMembership::try_from(&membership).expect("native membership conversion");
        (
            CaptureRaftEntry {
                log_id: u17a_id(index).into(),
                payload: openraft::EntryPayload::Membership(stored.membership().clone()),
            },
            membership,
        )
    }
    #[derive(Default)]
    struct U27aSink(Vec<(snapshot::Section, Vec<u8>, Vec<u8>)>);
    impl snapshot::StagingSink for U27aSink {
        fn row(&mut self, section: snapshot::Section, key: &[u8], value: &[u8]) -> io::Result<()> {
            self.0.push((section, key.to_vec(), value.to_vec()));
            Ok(())
        }
    }

    fn u27a_manifest(payload: &[u8]) -> snapshot::CapabilityManifest {
        use sha2::{Digest, Sha256};
        let mut bytes = [0; 56];
        bytes[..4].copy_from_slice(b"CCAP");
        bytes[4] = 1;
        bytes[5] = 1;
        bytes[6..14].copy_from_slice(&6u64.to_be_bytes());
        bytes[14] = 1;
        bytes[15] = 1;
        bytes[16..24].copy_from_slice(&u64::try_from(payload.len()).expect("small test payload length").to_be_bytes());
        bytes[24..].copy_from_slice(&Sha256::digest(payload));
        snapshot::CapabilityManifest { bytes }
    }

    fn u27a_fixture() -> (tempfile::TempDir, CrashBackend, Store, Vec<DecideCreated>) {
        let (dir, backend, mut store) = u17a_store();
        provenance_promote(&mut store);
        let commands = provenance_commands();
        let mut entries = vec![u17a_blank(0), u17a_membership(1).0];
        for (index, command) in commands.iter().enumerate() {
            entries.push(u17a_created(u64::try_from(index).expect("fixture index") + 2, command.clone()));
        }
        u24_apply(&mut store, &entries);
        assert_eq!(committed_schema(&store), 6, "schema6 fixture setup");
        (dir, backend, store, commands)
    }

    fn u27a_rows(store: &Store) -> Vec<(Vec<u8>, Vec<u8>)> {
        let tx = store.database.begin_read().expect("operation fixture transaction");
        let table = tx.open_table(application::OPERATIONS).expect("operation fixture table");
        let rows = table
            .iter()
            .expect("operation fixture cursor")
            .map(|row| {
                let (key, value) = row.expect("operation fixture row");
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect();
        rows
    }

    fn u27a_artifact(writer: &NativeLogWriter) -> snapshot::CapabilityArtifact<Vec<u8>> {
        let view = u19_runtime()
            .block_on(writer.snapshot_view())
            .expect("capability fixture view");
        let result = view.stream_capability_artifact(Vec::new(), u24_limits());
        assert_eq!(result.is_ok(), true, "U27A_CAPABILITY_ENABLED");
        result.expect("asserted capability production")
    }

    fn u27a_decode(
        artifact: snapshot::CapabilityArtifact<Vec<u8>>,
    ) -> Result<(snapshot::Header, U27aSink), snapshot::SnapshotError> {
        let mut sink = U27aSink::default();
        let header = snapshot::decode_capability_artifact(
            snapshot::CapabilityArtifact {
                manifest: artifact.manifest,
                payload: io::Cursor::new(artifact.payload),
            },
            &mut sink,
            u24_limits(),
        )?;
        Ok((header, sink))
    }

    #[test]
    fn u27a_checkpoint_schema6_copr_stream() {
        let (_dir, _backend, store, commands) = u27a_fixture();
        let expected = u27a_rows(&store);
        let writer = NativeLogWriter::new(store);
        let artifact = u27a_artifact(&writer);
        assert_eq!(
            artifact.manifest.encode(),
            u27a_manifest(&artifact.payload).encode(),
            "U27A_PAYLOAD_BINDING"
        );
        let decoded = u27a_decode(artifact);
        assert_eq!(decoded.is_ok(), true, "U27A_COPR_BYTES");
        let (header, sink) = decoded.expect("asserted capability consumer");
        assert_eq!(header.applied, Some(u17a_id(10)), "U27A_COPR_BYTES");
        assert_eq!(header.membership.log_id(), &Some(u17a_id(1)), "U27A_COPR_BYTES");
        let operations: Vec<_> = sink
            .0
            .into_iter()
            .filter(|row| row.0 == snapshot::Section::Operations)
            .map(|(_, key, value)| (key, value))
            .collect();
        assert_eq!(operations, expected, "U27A_COPR_BYTES");
        assert_eq!(operations.len(), 4, "U27A_COPR_BYTES");
        for (command_index, first_index) in [(0, 2), (3, 5), (4, 6), (5, 7)] {
            let command = &commands[command_index];
            let value = &operations
                .iter()
                .find(|(key, _)| key.as_slice() == command.operation_id.as_slice())
                .expect("canonical operation")
                .1;
            assert_eq!(&value[..5], b"COPR\x01", "U27A_COPR_BYTES");
            assert_eq!(&value[5..29], encode_id(u17a_id(first_index)), "U27A_FIRST_ID");
            let stored = writer
                .store
                .lock()
                .expect("fixture store")
                .created_operation_provenance(command.operation_id)
                .expect("provenance read")
                .expect("operation");
            assert_eq!(stored.first_decision_log_id, u17a_id(first_index), "U27A_FIRST_ID");
            assert_eq!(
                stored.operation.canonical_command,
                command.encode().expect("original command"),
                "U27A_COPR_BYTES"
            );
        }
        let store = writer.store.lock().expect("conflict receipt store");
        assert_eq!(
            store
                .native_apply_response(4)
                .expect("receipt")
                .expect("response")
                .created_result(),
            Some(&CreatedResult::OperationConflict),
            "U27A_FIRST_ID"
        );
    }

    #[test]
    fn u27a_checkpoint_schema4_legacy_unchanged() {
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())]);
        let before = u17b_snapshot(&store);
        // Fixed bytes exported by the accepted pre-capability binary for this schema4 fixture.
        const LEGACY_PAYLOAD_HEX: &str = concat!(
            "43534e5001010707070707070707070707070707070701000000000000000b00000000000000070000000000000002000000000000010e434d454d01",
            "01000000000000000b000000000000000700000000000000010000000200000002000000000000000700000000000000080000000200000000000000",
            "08000000000000000900000004000000000000000700000024434e4f44010000001b68747470733a2f2f706565722d372e6578616d706c653a373030",
            "37000000000000000800000023434e4f44010000001a687474703a2f2f706565722d382e6578616d706c653a37303038000000000000000900000024",
            "434e4f44010000001b68747470733a2f2f706565722d392e6578616d706c653a37303039000000000000000a00000025434e4f44010000001c687474",
            "70733a2f2f6c6561726e65722e6578616d706c653a373031300000000000000009060606060606060606060606060606060707070707070707070707",
            "070707070701000000000000000100000000000000210000000000000010020202020202020202020202020202020000000d6e61746976652d6f626a",
            "656374030303030303030303030303030303030200000000000000010000000000000031000000000000006802020202020202020202020202020202",
            "0000000d6e61746976652d6f626a65637403030303030303030303030303030303030303030303030303030303030303030404040404040404040404",
            "040404040405050505050505050505050505050505050505050505050505050505050505050000000000001001000000000000000b00000000000000",
            "0700000000000000020000000000000009030000000000000001000000000000001000000000000000a9010101010101010101010101010101010202",
            "02020202020202020202020202020000000d6e61746976652d6f626a6563740303030303030303030303030303030304040404040404040404040404",
            "04040405050505050505050505050505050505050505050505050505050505050505050000000000001001000000000000000b000000000000000700",
            "000000000000020000000000000009060606060606060606060606060606060707070707070707070707070707070704000000000000000100000000",
            "0000001000000000000000c8010101010101010101010101010101010000000000000087435254440101010101010101010101010101010101020202",
            "020202020202020202020202020000000d6e61746976652d6f626a656374000303030303030303030303030303030304040404040404040404040404",
            "040404050505050505050505050505050505050505050505050505050505050505050500000000000010010000000000000009000303030303030303",
            "030303030303030301010101010101010101010101010101000000000000000b000000000000000700000000000000024f43d9e18a25d702afdacde7",
            "e9b00f87177c40b11232952e61ddb2aa18411bce",
        );
        let original: Vec<u8> = LEGACY_PAYLOAD_HEX
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digits = std::str::from_utf8(pair).expect("fixed legacy hex");
                u8::from_str_radix(digits, 16).expect("fixed legacy byte")
            })
            .collect();
        let writer = NativeLogWriter::new(store);
        let view = u19_runtime().block_on(writer.snapshot_view()).expect("legacy view");
        let payload = view.stream(Vec::new(), u24_limits()).expect("original stream");
        assert_eq!(payload, original, "U27A_V1_OPAQUE");
        assert_eq!(&payload[..6], b"CSNP\x01\x01", "U27A_V1_OPAQUE");
        let view = u19_runtime()
            .block_on(writer.snapshot_view())
            .expect("legacy capability view");
        let result = view.stream_capability_artifact(Vec::new(), u24_limits());
        assert_eq!(
            matches!(result, Err(NativeSnapshotError::Store(StoreError::UnsupportedSchema))),
            true,
            "U27A_SCHEMA_GATE"
        );
        let store = writer.store.lock().expect("legacy unchanged store");
        assert_eq!(committed_schema(&store), 4, "U27A_SCHEMA_GATE");
        assert_eq!(u17b_snapshot(&store), before, "U27A_SCHEMA_GATE");
    }

    #[test]
    fn u27a_checkpoint_codec_mismatch_rejected() {
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_created(0, u17a_command())]);
        let writer = NativeLogWriter::new(store);
        let old = u19_runtime()
            .block_on(writer.snapshot_view())
            .expect("legacy codec view")
            .stream(Vec::new(), u24_limits())
            .expect("legacy codec payload");
        assert_eq!(
            u27a_decode(snapshot::CapabilityArtifact {
                manifest: u27a_manifest(&old),
                payload: old
            })
            .is_err(),
            true,
            "U27A_CODEC_DISPATCH"
        );
        let (_dir6, _backend6, store6, _) = u27a_fixture();
        let writer6 = NativeLogWriter::new(store6);
        let artifact = u27a_artifact(&writer6);
        for (offset, wrong) in [(6, 5), (14, 0), (14, 2), (15, 0), (15, 2)] {
            let mut manifest = artifact.manifest.clone();
            if offset == 6 {
                manifest.bytes[6..14].copy_from_slice(&u64::from(wrong).to_be_bytes());
            } else {
                manifest.bytes[offset] = wrong;
            }
            assert_eq!(
                u27a_decode(snapshot::CapabilityArtifact {
                    manifest,
                    payload: artifact.payload.clone()
                })
                .is_err(),
                true,
                "U27A_CODEC_DISPATCH"
            );
        }
    }

    #[test]
    fn u27a_checkpoint_manifest_binding_rejected() {
        struct ShortWriter {
            bytes: Vec<u8>,
            interrupted: bool,
        }
        impl Write for ShortWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                let length = bytes.len().min(3);
                self.bytes.extend_from_slice(&bytes[..length]);
                Ok(length)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (_dir, _backend, store, _) = u27a_fixture();
        let writer = NativeLogWriter::new(store);
        let view = u19_runtime().block_on(writer.snapshot_view()).expect("short output view");
        let result = view.stream_capability_artifact(
            ShortWriter {
                bytes: Vec::new(),
                interrupted: false,
            },
            u24_limits(),
        );
        assert_eq!(result.is_ok(), true, "U27A_PAYLOAD_BINDING");
        let artifact = result.expect("asserted short write producer");
        assert_eq!(
            artifact.manifest.encode(),
            u27a_manifest(&artifact.payload.bytes).encode(),
            "U27A_PAYLOAD_BINDING"
        );
        let encoded = artifact.manifest.encode();
        assert_eq!(snapshot::CapabilityManifest::decode(&encoded).is_ok(), true, "U27A_PAYLOAD_BINDING");
        for size in [0, 55, 57] {
            let mut raw = encoded.to_vec();
            raw.resize(size, 0);
            assert_eq!(snapshot::CapabilityManifest::decode(&raw).is_err(), true, "U27A_PAYLOAD_BINDING");
        }
        for offset in [0, 4, 5, 14, 15] {
            let mut raw = encoded;
            raw[offset] = 255;
            assert_eq!(snapshot::CapabilityManifest::decode(&raw).is_err(), true, "U27A_PAYLOAD_BINDING");
        }
        for offset in [16, 23, 24, 55] {
            let mut manifest = artifact.manifest.clone();
            manifest.bytes[offset] ^= 1;
            assert_eq!(
                u27a_decode(snapshot::CapabilityArtifact {
                    manifest,
                    payload: artifact.payload.bytes.clone()
                })
                .is_err(),
                true,
                "U27A_PAYLOAD_BINDING"
            );
        }
        let mut corrupt = artifact.payload.bytes.clone();
        *corrupt.last_mut().expect("footer byte") ^= 1;
        assert_eq!(
            u27a_decode(snapshot::CapabilityArtifact {
                manifest: artifact.manifest.clone(),
                payload: corrupt
            })
            .is_err(),
            true,
            "U27A_PAYLOAD_BINDING"
        );
        let mut extra = artifact.payload.bytes.clone();
        extra.push(0);
        assert_eq!(
            u27a_decode(snapshot::CapabilityArtifact {
                manifest: artifact.manifest,
                payload: extra
            })
            .is_err(),
            true,
            "U27A_PAYLOAD_BINDING"
        );
    }

    #[test]
    fn u27a_checkpoint_negative_bytes_first_id() {
        let (_dir, _backend, mut store, commands) = u27a_fixture();
        let before = u27a_rows(&store);
        let mut later = commands[0].clone();
        later.operation_id = [20; 16];
        later.prepared.version_id = [21; 16];
        later.expected_head = Some(commands[0].prepared.version_id);
        u24_apply(
            &mut store,
            &[
                u17a_created(11, later),
                u17a_created(12, commands[4].clone()),
                u17a_created(13, commands[2].clone()),
            ],
        );
        assert_eq!(
            store.created_head(&commands[0].object).expect("changed head"),
            Some([21; 16]),
            "U27A_FIRST_ID"
        );
        let after = u27a_rows(&store);
        for command_index in [3, 4, 5] {
            let key = commands[command_index].operation_id.to_vec();
            let old = before.iter().find(|row| row.0 == key).expect("first negative bytes");
            assert_eq!(after.iter().find(|row| row.0 == key), Some(old), "U27A_FIRST_ID");
        }
        assert_eq!(
            store
                .native_apply_response(13)
                .expect("conflict response")
                .expect("receipt")
                .created_result(),
            Some(&CreatedResult::OperationConflict),
            "U27A_FIRST_ID"
        );
        let writer = NativeLogWriter::new(store);
        let (_, sink) = u27a_decode(u27a_artifact(&writer)).expect("capability preserves negatives");
        let captured: Vec<_> = sink
            .0
            .into_iter()
            .filter(|row| row.0 == snapshot::Section::Operations)
            .map(|(_, key, value)| (key, value))
            .collect();
        assert_eq!(captured, after, "U27A_FIRST_ID");
    }

    #[test]
    fn u27a_checkpoint_v1_framing_is_opaque() {
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_blank(0)]);
        let tx = store.database.begin_read().expect("opaque header transaction");
        let (applied, membership) = application::native_application_state(&tx).expect("opaque application pair");
        let header = snapshot::Header {
            cluster_id: identity().cluster_id,
            applied,
            membership,
            binding: u17a_binding(),
        };
        let mut payload = Vec::new();
        snapshot::encode(
            &mut payload,
            &header,
            [0, 0, 0, 1],
            [snapshot::Row {
                section: snapshot::Section::Operations,
                key: b"o",
                value: b"opaque-operation",
            }],
            u24_limits(),
        )
        .expect("original opaque encode");
        let mut sink = U27aSink::default();
        let original = snapshot::decode(payload.as_slice(), &mut sink, u24_limits());
        assert_eq!(original.is_ok(), true, "U27A_V1_OPAQUE");
        assert_eq!(sink.0[0].2, b"opaque-operation", "U27A_V1_OPAQUE");
        assert_eq!(
            u27a_decode(snapshot::CapabilityArtifact {
                manifest: u27a_manifest(&payload),
                payload
            })
            .is_err(),
            true,
            "U27A_V1_OPAQUE"
        );
    }

    fn u27b_rows(bytes: Vec<u8>, expected: &[Vec<(Vec<u8>, Vec<u8>)>], cut: LogId, member: LogId) {
        let mut sink = U27aSink::default();
        let header = snapshot::decode(io::Cursor::new(bytes), &mut sink, u24_limits()).expect("decode captured bytes");
        assert_eq!(header.applied, Some(cut), "U27B_SAME_TX");
        assert_eq!(header.membership, u17a_membership(member.index).1, "U27B_SAME_TX");
        assert_eq!(header.cluster_id, identity().cluster_id, "U27B_SAME_TX");
        assert_eq!(header.binding, u17a_binding(), "U27B_SAME_TX");
        for (section, rows) in [
            snapshot::Section::Heads,
            snapshot::Section::Versions,
            snapshot::Section::Events,
            snapshot::Section::Operations,
        ]
        .into_iter()
        .zip(&expected[3..7])
        {
            let actual: Vec<_> = sink
                .0
                .iter()
                .filter(|row| row.0 == section)
                .map(|row| (row.1.clone(), row.2.clone()))
                .collect();
            assert_eq!(&actual, rows, "U27B_SAME_TX");
        }
    }

    #[test]
    fn u27b_checkpoint_same_tx_cut() {
        let (_dir, _backend, store, _) = u27a_fixture();
        let before = u17b_snapshot(&store);
        let writer = NativeLogWriter::new(store);
        let owner = writer.store.clone();
        let _hook = native_snapshot::after_capture(move || {
            let mut store = owner.lock().expect("live corruption owner");
            let tx = store.write_transaction().expect("live corruption transaction");
            tx.open_table(META)
                .expect("live metadata")
                .remove("committed")
                .expect("remove live marker");
            store.commit(tx).expect("durable live corruption");
            assert!(
                matches!(store.read_committed(), Err(StoreError::CorruptRecord)),
                "live transaction sees missing committed"
            );
        });
        let old = NativeSnapshotView::acquire(writer.store.clone());
        assert_eq!(old.is_ok(), true, "U27B_SAME_TX");
        u27b_rows(
            old.expect("asserted old view")
                .stream(Vec::new(), u24_limits())
                .expect("old stream"),
            &before,
            u17a_id(10),
            u17a_id(1),
        );
        assert!(
            matches!(
                NativeSnapshotView::acquire(writer.store.clone()),
                Err(NativeSnapshotError::Store(StoreError::CorruptRecord))
            ),
            "U27B_SAME_TX"
        );
        drop(_hook);

        let (_dir2, _backend2, store, _) = u27a_fixture();
        let before = u17b_snapshot(&store);
        let writer = NativeLogWriter::new(store);
        let old = NativeSnapshotView::acquire(writer.store.clone()).expect("normal old view");
        let after = {
            let mut store = writer.store.lock().expect("normal writer");
            let mut command = u17a_command();
            command.operation_id = [81; 16];
            command.object.key = b"u27b-next-object".to_vec();
            command.prepared.version_id = [82; 16];
            u24_apply(&mut store, &[u17a_membership(11).0, u17a_created(12, command)]);
            u17b_snapshot(&store)
        };
        for i in 3..7 {
            assert_ne!(before[i], after[i], "U27B_SAME_TX new table changes");
        }
        u27b_rows(
            old.stream(Vec::new(), u24_limits()).expect("normal old stream"),
            &before,
            u17a_id(10),
            u17a_id(1),
        );
        u27b_rows(
            NativeSnapshotView::acquire(writer.store.clone())
                .expect("normal fresh view")
                .stream(Vec::new(), u24_limits())
                .expect("normal fresh stream"),
            &after,
            u17a_id(12),
            u17a_id(11),
        );
    }

    #[test]
    fn u27b_checkpoint_bounds() {
        for case in 0..4 {
            let (_dir, _backend, mut store, _) = u27a_fixture();
            let tx = store.write_transaction().expect("bounds fixture transaction");
            {
                let mut meta = tx.open_table(META).expect("bounds metadata");
                match case {
                    0 => {
                        meta.insert("committed", encode_id(u17a_id(9)).as_slice())
                            .expect("committed below applied");
                    }
                    1 => {
                        let mut applied = vec![1];
                        applied.extend(encode_id(LogId { term: 12, ..u17a_id(10) }));
                        meta.insert("last_applied", applied.as_slice())
                            .expect("applied term mismatch");
                    }
                    2 => {
                        let mut applied = vec![1];
                        applied.extend(encode_id(LogId {
                            leader_node: 8,
                            ..u17a_id(10)
                        }));
                        meta.insert("last_applied", applied.as_slice())
                            .expect("applied leader mismatch");
                    }
                    _ => {
                        meta.remove("committed").expect("missing committed fixture");
                    }
                }
            }
            store.commit(tx).expect("persist bounds fixture");
            let expected_applied = match case {
                1 => LogId { term: 12, ..u17a_id(10) },
                2 => LogId {
                    leader_node: 8,
                    ..u17a_id(10)
                },
                _ => u17a_id(10),
            };
            assert_eq!(
                store.native_application_state().expect("valid applied encoding").0,
                Some(expected_applied),
                "U27B_FULL_CUT decoded fixture"
            );
            let before = u17b_snapshot(&store);
            let writer = NativeLogWriter::new(store);
            let result = NativeSnapshotView::acquire(writer.store.clone());
            assert_eq!(
                matches!(result, Err(NativeSnapshotError::Store(StoreError::CorruptRecord))),
                true,
                "U27B_FULL_CUT case {case}"
            );
            assert_eq!(
                u17b_snapshot(&writer.store.lock().expect("unchanged bounds owner")),
                before,
                "U27B_FULL_CUT zero writes"
            );
        }
        let (_dir, _backend, store, _) = u27a_fixture();
        let writer = NativeLogWriter::new(store);
        assert!(NativeSnapshotView::acquire(writer.store.clone()).is_ok(), "U27B_FULL_CUT equal full ID");
        u19_runtime()
            .block_on(writer.write(|store| {
                store.append_native(&[u17a_blank(11)])?;
                store.save_committed(u17a_id(11))
            }))
            .expect("advance committed only");
        assert!(
            NativeSnapshotView::acquire(writer.store.clone()).is_ok(),
            "U27B_FULL_CUT applied below committed"
        );

        for schema6 in [false, true] {
            let (_dir, _backend, mut store) = u17a_store();
            if schema6 {
                provenance_promote(&mut store);
            }
            let schema = if schema6 { 6 } else { 4 };
            let before = u17b_snapshot(&store);
            let writer = NativeLogWriter::new(store);
            let bytes = NativeSnapshotView::acquire(writer.store.clone())
                .expect("legal empty native view")
                .stream(Vec::new(), u24_limits())
                .expect("empty bare stream");
            let mut sink = U27aSink::default();
            let header = snapshot::decode(io::Cursor::new(bytes), &mut sink, u24_limits()).expect("empty decode");
            assert_eq!(header.applied, None, "U27B_FULL_CUT empty applied");
            assert!(
                header.membership.log_id().is_none()
                    && header.membership.configs().is_empty()
                    && header.membership.nodes().is_empty(),
                "U27B_FULL_CUT empty membership"
            );
            assert!(sink.0.is_empty(), "U27B_FULL_CUT empty rows");
            let capability = NativeSnapshotView::acquire(writer.store.clone())
                .expect("empty capability view")
                .stream_capability_artifact(Vec::new(), u24_limits());
            if schema6 {
                assert!(capability.is_ok(), "U27B_FULL_CUT explicit schema6");
            } else {
                assert!(
                    matches!(capability, Err(NativeSnapshotError::Store(StoreError::UnsupportedSchema))),
                    "U27B_FULL_CUT schema4 gate"
                );
            }
            assert_eq!(
                committed_schema(&writer.store.lock().expect("schema owner")),
                schema,
                "U27B_FULL_CUT no promotion"
            );
            assert_eq!(
                u17b_snapshot(&writer.store.lock().expect("empty unchanged owner")),
                before,
                "U27B_FULL_CUT zero writes"
            );
            u19_runtime()
                .block_on(writer.write(|store| {
                    store.append_native(&[u17a_blank(0)])?;
                    store.save_committed(u17a_id(0))
                }))
                .expect("empty applied with committed log");
            assert!(
                NativeSnapshotView::acquire(writer.store.clone()).is_ok(),
                "U27B_FULL_CUT committed unapplied accepted"
            );
        }
        let (_dir, _backend, mut store) = u17a_store();
        let tx = store.write_transaction().expect("missing applied fixture");
        tx.open_table(META)
            .expect("empty metadata")
            .remove("last_applied")
            .expect("remove empty applied encoding");
        store.commit(tx).expect("commit missing applied");
        let writer = NativeLogWriter::new(store);
        assert!(
            matches!(
                NativeSnapshotView::acquire(writer.store.clone()),
                Err(NativeSnapshotError::Store(StoreError::CorruptRecord))
            ),
            "U27B_FULL_CUT missing empty encoding"
        );
    }

    #[test]
    fn u27b_checkpoint_retained_log_identity() {
        for case in 0..5 {
            let (_dir, _backend, mut store, _) = u27a_fixture();
            let tx = store.write_transaction().expect("retained log fixture");
            {
                let mut logs = tx.open_table(LOGS).expect("retained log table");
                match case {
                    0 => {
                        logs.remove(10).expect("missing committed log");
                    }
                    1 => {
                        logs.insert(10, [0xff].as_slice()).expect("malformed committed log");
                    }
                    2 | 3 => {
                        let raw = logs
                            .get(10)
                            .expect("original committed log")
                            .expect("retained log")
                            .value()
                            .to_vec();
                        let mut entry = decode_log(&raw).expect("valid original log");
                        if case == 2 {
                            entry.id.term += 1;
                        } else {
                            entry.id.leader_node += 1;
                        }
                        logs.insert(10, encode_log(&entry).expect("encoded mismatching log").as_slice())
                            .expect("mismatching committed log");
                    }
                    _ => {}
                }
            }
            store.commit(tx).expect("persist retained log fixture");
            let before = u17b_snapshot(&store);
            let writer = NativeLogWriter::new(store);
            let result = NativeSnapshotView::acquire(writer.store.clone());
            if case == 4 {
                assert!(result.is_ok(), "U27B_COMMITTED_RECORD complete identity");
            } else {
                assert_eq!(
                    matches!(result, Err(NativeSnapshotError::Store(StoreError::CorruptRecord))),
                    true,
                    "U27B_COMMITTED_RECORD case {case}"
                );
            }
            assert_eq!(
                u17b_snapshot(&writer.store.lock().expect("retained log owner")),
                before,
                "U27B_COMMITTED_RECORD zero writes"
            );
        }
    }

    #[test]
    fn u27b_checkpoint_owner_and_read_lifetime() {
        let (_dir, _backend, store, _) = u27a_fixture();
        let before = u17b_snapshot(&store);
        let writer = NativeLogWriter::new(store);
        let view = NativeSnapshotView::acquire(writer.store.clone()).expect("owned captured view");
        drop(writer);
        u27b_rows(
            view.stream(Vec::new(), u24_limits()).expect("view outlives external writer"),
            &before,
            u17a_id(10),
            u17a_id(1),
        );

        struct PausedOutput {
            entered: Option<mpsc::Sender<()>>,
            release: mpsc::Receiver<()>,
            bytes: Vec<u8>,
        }
        impl Write for PausedOutput {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).map_err(io::Error::other)?;
                    self.release.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (_dir2, _backend2, store, _) = u27a_fixture();
        let before = u17b_snapshot(&store);
        let writer = NativeLogWriter::new(store);
        let view = NativeSnapshotView::acquire(writer.store.clone()).expect("paused view");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let stream = std::thread::spawn(move || {
            view.stream(
                PausedOutput {
                    entered: Some(entered_tx),
                    release: release_rx,
                    bytes: Vec::new(),
                },
                u24_limits(),
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let owner_available = writer.store.try_lock().is_ok();
        let (done_tx, done_rx) = mpsc::channel();
        let writer_for_thread = writer.clone();
        let write = std::thread::spawn(move || {
            let result = u19_runtime().block_on(writer_for_thread.write(|store| {
                u24_apply(store, &[u17a_blank(11)]);
                Ok(())
            }));
            let _ = done_tx.send(result.is_ok());
        });
        let done = done_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_tx.send(());
        let streamed = stream.join().expect("paused stream joined");
        write.join().expect("writer joined after release");
        assert!(entered.is_ok(), "U27B_OWNER output rendezvous");
        assert!(owner_available, "U27B_OWNER owner try_lock while output paused: {owner_available}");
        assert!(
            matches!(done, Ok(true)),
            "U27B_OWNER writer ACK before release: done={done:?}, owner_available={owner_available}"
        );
        u27b_rows(streamed.expect("paused stream completed").bytes, &before, u17a_id(10), u17a_id(1));
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "u27b output failure"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let result = NativeSnapshotView::acquire(writer.store.clone())
            .expect("failed output view")
            .stream(Broken, u24_limits());
        assert!(
            matches!(result, Err(NativeSnapshotError::Snapshot(snapshot::SnapshotError::Io(ref error))) if error.kind() == io::ErrorKind::BrokenPipe),
            "U27B_OWNER typed output failure"
        );
        u19_runtime()
            .block_on(writer.write(|store| {
                u24_apply(store, &[u17a_blank(12)]);
                Ok(())
            }))
            .expect("write after failed output");
        assert!(
            NativeSnapshotView::acquire(writer.store.clone())
                .expect("capture after failed output")
                .stream(Vec::new(), u24_limits())
                .is_ok(),
            "U27B_OWNER capture after failed output"
        );
    }

    fn u24_limits() -> snapshot::Limits {
        snapshot::Limits {
            total_bytes: 1_000_000,
            membership_bytes: 100_000,
            key_bytes: 100_000,
            value_bytes: 100_000,
            records: 1000,
        }
    }
    fn u24_apply(store: &mut Store, entries: &[CaptureRaftEntry]) {
        store.append_native(entries).expect("append snapshot fixture");
        store
            .save_committed(entries.last().expect("nonempty fixture").log_id.into())
            .expect("commit snapshot fixture");
        for entry in entries {
            store.apply_next_native(entry.log_id.into()).expect("apply snapshot fixture");
        }
    }
    fn u24_expected(store: &Store) -> Vec<u8> {
        let tx = store.database.begin_read().expect("fixture read transaction");
        let (applied, membership) = application::native_application_state(&tx).expect("fixture application head");
        let header = snapshot::Header {
            cluster_id: identity().cluster_id,
            applied,
            membership,
            binding: u17a_binding(),
        };
        let definitions = [
            application::HEADS,
            application::VERSIONS,
            application::EVENTS,
            application::OPERATIONS,
        ];
        let mut counts = [0; 4];
        for (i, definition) in definitions.iter().enumerate() {
            counts[i] = tx
                .open_table(*definition)
                .expect("fixture table")
                .len()
                .expect("fixture count");
        }
        let mut encoder = snapshot::Encoder::new(Vec::new(), &header, counts, u24_limits()).expect("fixture encoder");
        for (definition, section) in definitions.into_iter().zip([
            snapshot::Section::Heads,
            snapshot::Section::Versions,
            snapshot::Section::Events,
            snapshot::Section::Operations,
        ]) {
            let table = tx.open_table(definition).expect("fixture rows");
            for row in table.iter().expect("fixture iterator") {
                let (key, value) = row.expect("fixture row");
                encoder
                    .row(snapshot::Row {
                        section,
                        key: key.value(),
                        value: value.value(),
                    })
                    .expect("fixture encode row");
            }
        }
        encoder.finish().expect("fixture complete")
    }
    #[test]
    fn u24_coherent_view() {
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())]);
        let expected_a = u24_expected(&store);
        let writer = NativeLogWriter::new(store);
        let runtime = u19_runtime();
        let result = runtime.block_on(writer.snapshot_view());
        assert!(result.is_ok(), "U24_COHERENT_VIEW: acquisition failed");
        let view = result.expect("A view");
        let mut command = u17a_command();
        command.operation_id = [8; 16];
        command.object.key = b"second-object".to_vec();
        command.prepared.version_id = [9; 16];
        let expected_b = {
            let mut store = writer.store.lock().expect("B store");
            u24_apply(&mut store, &[u17a_membership(3).0, u17a_created(4, command)]);
            u24_expected(&store)
        };
        assert_eq!(view.stream(Vec::new(), u24_limits()).expect("A stream"), expected_a, "U24_COHERENT_VIEW");
        let new_view = runtime.block_on(writer.snapshot_view()).expect("B view");
        assert_eq!(new_view.stream(Vec::new(), u24_limits()).expect("B stream"), expected_b);
        assert_ne!(expected_a, expected_b, "B changes all tables and application membership cut");
    }
    #[test]
    fn u24_stream_releases_writer() {
        struct BlockingOutput {
            entered: Option<mpsc::Sender<()>>,
            release: mpsc::Receiver<()>,
        }
        impl Write for BlockingOutput {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).map_err(io::Error::other)?;
                    self.release.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_blank(0)]);
        let writer = NativeLogWriter::new(store);
        let view = u19_runtime().block_on(writer.snapshot_view()).expect("stream view");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let stream = std::thread::spawn(move || {
            view.stream(
                BlockingOutput {
                    entered: Some(entered_tx),
                    release: release_rx,
                },
                u24_limits(),
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        let (done_tx, done_rx) = mpsc::channel();
        let write = std::thread::spawn(move || {
            let result = u19_runtime().block_on(writer.write(|store| {
                u24_apply(store, &[u17a_created(1, u17a_command())]);
                Ok(())
            }));
            let _ = done_tx.send(result.is_ok());
        });
        let done = done_rx.recv_timeout(Duration::from_secs(5));
        let _ = release_tx.send(());
        let streamed = stream.join().expect("stream joined");
        write.join().expect("writer joined");
        assert!(entered.is_ok(), "output entered rendezvous");
        assert!(matches!(done, Ok(true)), "U24_STREAM_RELEASES_WRITER done={done:?}");
        assert!(streamed.is_ok(), "stream completed");
    }
    #[test]
    fn u24_view_schema_fence() {
        let (_dir, _backend, store) = u17a_bound();
        let writer = NativeLogWriter::new(store);
        assert!(
            matches!(
                u19_runtime().block_on(writer.snapshot_view()),
                Err(NativeSnapshotError::Store(StoreError::UnsupportedSchema))
            ),
            "U24_SCHEMA_FENCE"
        );
        assert_eq!(committed_schema(&writer.store.lock().expect("schema unchanged")), 3);
        let (_dir2, _backend2, mut store) = u17a_store();
        store.writable = false;
        let writer = NativeLogWriter::new(store);
        assert!(
            matches!(
                u19_runtime().block_on(writer.snapshot_view()),
                Err(NativeSnapshotError::Store(StoreError::WriteDisabled))
            ),
            "U24_WRITE_FENCE"
        );
    }
    #[test]
    fn u24_view_lifetime_errors() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "snapshot test output"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (_dir, _backend, mut store) = u17a_store();
        u24_apply(&mut store, &[u17a_blank(0)]);
        let expected = u24_expected(&store);
        let writer = NativeLogWriter::new(store);
        let view = u19_runtime().block_on(writer.snapshot_view()).expect("owned view");
        drop(writer);
        assert_eq!(view.stream(Vec::new(), u24_limits()).expect("view outlives writer"), expected);
        let (_dir2, _backend2, store) = u17a_store();
        let writer = NativeLogWriter::new(store);
        let view = u19_runtime().block_on(writer.snapshot_view()).expect("error view");
        let result = view.stream(Broken, u24_limits());
        match result {
            Err(NativeSnapshotError::Snapshot(snapshot::SnapshotError::Io(error))) => {
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe)
            }
            _ => panic!("U24_TYPED_IO_ERROR"),
        }
        u19_runtime()
            .block_on(writer.write(|store| {
                u24_apply(store, &[u17a_blank(0)]);
                Ok(())
            }))
            .expect("write after failed stream");
        u19_runtime()
            .block_on(writer.snapshot_view())
            .expect("view after failed stream")
            .stream(Vec::new(), u24_limits())
            .expect("stream after failed output");
    }
    #[test]
    fn u18_native_read_roundtrip() {
        let (_dir, _backend, mut store) = u17a_store();
        let command = u17a_command();
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, command.clone())];
        store.append_native(&entries).expect("append native fixture");
        let result = store.read_native_range(..);
        assert!(result.is_ok(), "U18_NATIVE_READ_SUCCESS: {result:?}");
        let read = result.expect("native read");
        assert_eq!(read, entries, "full identities and payloads");
        assert_eq!(
            store.read_entries(2, 3).expect("canonical row")[0].payload[14..],
            command.encode().expect("canonical command")
        );
    }
    #[test]
    fn u18_native_read_range_bounds() {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let (_dir, _backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())];
        store.append_native(&entries).expect("append range fixture");
        assert_eq!(store.read_native_range(1..2).expect("bounded read"), entries[1..2], "U18_RANGE_BOUNDARY");
        assert_eq!(store.read_native_range(..=1).expect("inclusive end"), entries[..2]);
        assert_eq!(store.read_native_range((Excluded(0), Included(2))).expect("excluded start"), entries[1..]);
        assert_eq!(store.read_native_range(2..).expect("unbounded end"), entries[2..]);
        assert_eq!(store.read_native_range(..).expect("all"), entries);
        for range in [
            (Included(2), Excluded(2)),
            (Included(2), Included(1)),
            (Unbounded, Excluded(0)),
            (Excluded(u64::MAX), Unbounded),
            (Included(u64::MAX), Included(u64::MAX)),
        ] {
            assert!(store.read_native_range(range).expect("empty mathematical range").is_empty());
        }
    }
    #[test]
    fn u18_native_read_reopen() {
        let (dir, backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_membership(1).0, u17a_created(2, u17a_command())];
        store.append_native(&entries).expect("append reopen fixture");
        drop(store);
        drop(backend);
        let store = Store::open(dir.path().join("db"), identity()).expect("reopen native file");
        assert_eq!(store.read_native_range(..).expect("reopened native read"), entries);
    }
    #[test]
    fn u18_native_read_legacy_rejected() {
        let (_dir, _backend, store) = u17a_bound();
        assert!(
            matches!(store.read_native_range(0..0), Err(StoreError::UnsupportedSchema)),
            "U18_SCHEMA_GUARD"
        );
        assert!(matches!(store.read_native_range(..), Err(StoreError::UnsupportedSchema)));
    }
    #[test]
    fn u18_native_read_corrupt_row() {
        let (_dir, _backend, mut store) = u17a_store();
        store.append_native(&[u17a_blank(0)]).expect("append corruption fixture");
        let original = store.read_entry(0).expect("read stored entry").expect("present entry");
        let tx = store.database.begin_write().expect("corruption transaction");
        {
            let mut logs = tx.open_table(LOGS).expect("logs");
            let bytes = logs.get(0).expect("get row").expect("present row").value().to_vec();
            logs.insert(1, bytes.as_slice()).expect("mismatched index row");
        }
        tx.commit().expect("persist mismatched row");
        assert!(matches!(store.read_native_range(1..=1), Err(StoreError::CorruptRecord)));
        let tx = store.database.begin_write().expect("malformed transaction");
        {
            let mut logs = tx.open_table(LOGS).expect("logs");
            let mut entry = original;
            entry.payload = b"CENR\x01".to_vec();
            let bytes = encode_log(&entry).expect("encode malformed native envelope");
            logs.insert(0, bytes.as_slice()).expect("malformed native row");
        }
        tx.commit().expect("persist malformed native row");
        assert!(matches!(store.read_native_range(..1), Err(StoreError::CorruptRecord)));
        let tx = store.database.begin_write().expect("missing-table transaction");
        assert!(tx.delete_table(LOGS).expect("delete logs table"));
        tx.commit().expect("persist missing logs table");
        assert!(store.read_native_entries(0, 0).is_err(), "U18_EMPTY_RANGE_TABLE_ERROR");
    }
    #[test]
    fn u17a_native_enable_empty_idempotent() {
        let (_dir, backend, mut store) = u17a_bound();
        assert_eq!(committed_schema(&store), 3);
        assert!(store.enable_native_empty().is_ok(), "U17A_ENABLE_SUCCESS empty bound schema3 must enable");
        assert_eq!(committed_schema(&store), 4);
        let empty = CaptureMembership::new(None, vec![], std::collections::BTreeMap::new()).expect("empty membership");
        assert_eq!(store.native_application_state().expect("empty native state"), (None, empty));
        let syncs = backend.media.lock().expect("sync count").syncs;
        store.enable_native_empty().expect("idempotent enable");
        assert_eq!(backend.media.lock().expect("sync count").syncs, syncs);
        drop(store);
        let reopened = Store::open(&backend.path, identity()).expect("schema4 reopen");
        assert_eq!(committed_schema(&reopened), 4);
        drop(reopened);
        let (_dir, backend, mut store) = u17a_bound();
        store.append(&batch()).expect("opaque legacy suffix");
        assert_eq!(
            store.enable_native_empty().expect_err("populated must refuse").code(),
            "unsupported_schema"
        );
        assert_eq!(store.read_entries(0, 3).expect("unchanged legacy logs"), batch());
        drop(store);
        assert_eq!(
            committed_schema(&Store::open(&backend.path, identity()).expect("no automatic upgrade")),
            3
        );
    }
    #[test]
    fn u17a_native_append_batch_atomic() {
        let (_dir, _backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_created(1, u17a_command()), u17a_membership(2).0];
        assert!(
            store.append_native(&entries).is_ok(),
            "U17A_APPEND_SUCCESS valid heterogeneous batch must append"
        );
        assert_eq!(store.read_native_entries(0, 3).expect("full read"), entries);
        assert_eq!(store.read_native_entry(3).expect("absent read"), None);
        assert_eq!(store.read_native_entries(2, 1).expect_err("reverse range").code(), "invalid_log_sequence");
        let bad = vec![u17a_blank(3), u17a_blank(5)];
        let rejected = store.append_native(&bad);
        assert_eq!(
            store.read_native_entry(3).expect("no partial append"),
            None,
            "U17A_APPEND_BATCH_NO_PARTIAL"
        );
        assert_eq!(store.log_state().expect("atomic tail").last, Some(u17a_id(2)));
        assert_eq!(rejected.expect_err("invalid batch").code(), "invalid_log_sequence");
        let bad_membership = CaptureRaftEntry {
            log_id: u17a_id(4).into(),
            payload: openraft::EntryPayload::Membership(openraft::Membership::new(
                vec![std::collections::BTreeSet::from([7])],
                std::collections::BTreeMap::<u64, CaptureNode>::new(),
            )),
        };
        assert_eq!(
            store
                .append_native(&[u17a_blank(3), bad_membership])
                .expect_err("missing voter metadata")
                .code(),
            "invalid_membership"
        );
        assert_eq!(store.read_native_entry(3).expect("invalid membership batch atomic"), None);
    }
    #[test]
    fn u17a_native_heterogeneous_order_replay() {
        let (_dir, backend, mut store) = u17a_store();
        let (membership_entry, membership) = u17a_membership(2);
        let entries = vec![
            u17a_blank(0),
            u17a_created(1, u17a_command()),
            membership_entry,
            u17a_blank(3),
        ];
        store.append_native(&entries).expect("persist heterogeneous prefix");
        store.save_committed(u17a_id(3)).expect("commit prefix");
        assert!(
            store.apply_next_native(u17a_id(0)).is_ok(),
            "U17A_APPLY_SUCCESS committed Blank must apply"
        );
        assert_eq!(store.last_applied().expect("Blank advances"), Some(u17a_id(0)));
        for index in 1..=3 {
            store.apply_next_native(u17a_id(index)).expect("ordered native apply");
        }
        assert_eq!(
            store.native_application_state().expect("final pair"),
            (Some(u17a_id(3)), membership.clone())
        );
        assert_eq!(store.created_apply_result(0).expect("blank projection"), None);
        assert_eq!(store.created_apply_result(2).expect("membership projection"), None);
        let syncs = backend.media.lock().expect("sync count").syncs;
        for index in 0..=3 {
            let response = store.apply_next_native(u17a_id(index)).expect("exact replay");
            assert_eq!(store.native_apply_response(index).expect("receipt"), Some(response));
        }
        store.enable_native_empty().expect("U17A_ENABLE_VALID_NATIVE_REPLAY");
        assert_eq!(backend.media.lock().expect("sync count").syncs, syncs);
        drop(store);
        let reopened = Store::open(&backend.path, identity()).expect("heterogeneous reopen");
        assert_eq!(
            reopened.native_application_state().expect("reopened pair"),
            (Some(u17a_id(3)), membership)
        );
        assert_eq!(reopened.read_native_entries(0, 4).expect("reopened prefix"), entries);
    }
    #[test]
    fn u17a_native_envelope_golden_strict() {
        let (_dir, backend, mut store) = u17a_store();
        let command = u17a_command();
        let empty = CaptureMembership::new(Some(u17a_id(2)), vec![], std::collections::BTreeMap::new()).expect("empty config");
        let stored = openraft::StoredMembership::try_from(&empty).expect("empty conversion");
        let entries = [
            u17a_blank(0),
            u17a_created(1, command.clone()),
            CaptureRaftEntry {
                log_id: u17a_id(2).into(),
                payload: openraft::EntryPayload::Membership(stored.membership().clone()),
            },
        ];
        store.append_native(&entries).expect("golden native append");
        for (index, kind, body) in [
            (0, 0, vec![]),
            (1, 1, command.encode().expect("canonical CRTD")),
            (2, 2, empty.encode().expect("canonical CMEM")),
        ] {
            let raw = store.read_entry(index).expect("opaque reader").expect("golden row");
            let mut golden = b"CENR\x01".to_vec();
            golden.push(kind);
            golden.extend(u64::try_from(body.len()).expect("body length").to_be_bytes());
            golden.extend(body);
            assert_eq!(raw.payload, golden, "kind {kind} exact envelope");
            assert_eq!(raw.id, u17a_id(index));
        }
        let original = store.read_entry(0).expect("blank row").expect("blank exists");
        for bytes in [
            b"CENR\x01\x00\0\0\0\0\0\0\0\0x".to_vec(),
            b"CENR\x02\x00\0\0\0\0\0\0\0\0".to_vec(),
            b"CENR\x01\x03\0\0\0\0\0\0\0\0".to_vec(),
            b"CENR\x01\x00\xff\xff\xff\xff\xff\xff\xff\xff".to_vec(),
            vec![],
        ] {
            let tx = store.write_transaction().expect("corrupt envelope transaction");
            tx.open_table(LOGS)
                .expect("logs")
                .insert(
                    0,
                    encode_log(&LogEntry {
                        id: original.id,
                        payload: bytes,
                    })
                    .expect("outer record")
                    .as_slice(),
                )
                .expect("replace envelope");
            store.commit(tx).expect("persist corruption");
            assert_eq!(store.read_native_entry(0).expect_err("strict decode").code(), "corrupt_record");
        }
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity()).err().expect("corrupt reopen").code(),
            "corrupt_record"
        );
    }
    #[test]
    fn u17a_native_schema_and_legacy_gates() {
        let (_dir, _backend, mut store) = media_store();
        assert_eq!(store.enable_native_empty().expect_err("schema1 refused").code(), "unsupported_schema");
        assert_eq!(store.append_native(&[]).expect_err("schema1 append refused").code(), "unsupported_schema");
        store.append(&batch()).expect("legacy append");
        store.save_committed(batch()[0].id).expect("schema2 commit");
        assert_eq!(store.enable_native_empty().expect_err("schema2 refused").code(), "unsupported_schema");
        assert_eq!(committed_schema(&store), 2);
        let (_dir, _backend, mut store) = u17a_store();
        assert_eq!(store.append(&[]).expect_err("schema4 raw append refused").code(), "unsupported_schema");
        assert_eq!(
            store
                .apply_next_created(u17a_id(0))
                .expect_err("legacy application refused")
                .code(),
            "unsupported_schema"
        );
        store.pin_capture_binding(u17a_binding()).expect("schema4 same binding");
        assert_eq!(store.capture_binding().expect("binding projection"), Some(u17a_binding()));
        store.append_native(&[u17a_blank(0)]).expect("native append");
        store.save_committed(u17a_id(0)).expect("native commit");
        assert_eq!(committed_schema(&store), 4, "save committed cannot downgrade");
    }
    #[test]
    fn u17a_native_full_membership_pair() {
        let (_dir, backend, mut store) = u17a_store();
        let (entry, membership) = u17a_membership(0);
        store.append_native(&[entry]).expect("persist full membership");
        store.save_committed(u17a_id(0)).expect("commit membership");
        let response = store.apply_next_native(u17a_id(0)).expect("apply membership");
        assert_eq!(response.kind(), CaptureResponseKind::Membership);
        assert_eq!(response.created_result(), None);
        let pair = store.native_application_state().expect("full pair");
        assert_eq!(pair, (Some(u17a_id(0)), membership.clone()));
        assert_eq!(
            pair.1.nodes().get(&10).expect("learner retained").rpc_uri(),
            "https://learner.example:7010"
        );
        assert_eq!(
            store
                .created_head(&u17a_command().object)
                .expect("control leaves business empty"),
            None
        );
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity())
                .expect("membership reopen")
                .native_application_state()
                .expect("reopened pair"),
            (Some(u17a_id(0)), membership)
        );
    }
    #[test]
    fn u17a_native_apply_sequence_commit_identity() {
        let (_dir, _backend, mut store) = u17a_store();
        store
            .append_native(&[u17a_blank(0), u17a_blank(1), u17a_blank(2)])
            .expect("sequence fixtures");
        assert_eq!(
            store.apply_next_native(u17a_id(0)).expect_err("uncommitted refused").code(),
            "invalid_committed_log"
        );
        store.save_committed(u17a_id(1)).expect("commit first two");
        assert_eq!(
            store.apply_next_native(u17a_id(1)).expect_err("gap refused").code(),
            "invalid_apply_sequence"
        );
        for id in [
            LogId { term: 12, ..u17a_id(0) },
            LogId {
                leader_node: 8,
                ..u17a_id(0)
            },
        ] {
            assert_eq!(
                store.apply_next_native(id).expect_err("exact identity refused").code(),
                "invalid_committed_log"
            );
            assert_eq!(store.last_applied().expect("unchanged application"), None);
        }
        let response = store.apply_next_native(u17a_id(0)).expect("exact first");
        assert_eq!(*response.log_id(), u17a_id(0));
        assert_eq!(
            store
                .apply_next_native(LogId { term: 12, ..u17a_id(0) })
                .expect_err("wrong replay identity")
                .code(),
            "invalid_committed_log"
        );
        store.apply_next_native(u17a_id(1)).expect("exact second");
        assert_eq!(
            store.apply_next_native(u17a_id(2)).expect_err("beyond committed").code(),
            "invalid_committed_log"
        );
        assert_eq!(store.last_applied().expect("exact frontier"), Some(u17a_id(1)));
    }
    #[test]
    fn u17a_native_created_duplicates_precedence() {
        let (_dir, _backend, mut store) = u17a_store();
        let command = u17a_command();
        let mut conflict = command.clone();
        conflict.binding_revision = 99;
        let mut binding_mismatch = conflict.clone();
        binding_mismatch.operation_id = [11; 16];
        binding_mismatch.expected_head = Some([12; 16]);
        let mut head_mismatch = command.clone();
        head_mismatch.operation_id = [13; 16];
        let mut version_conflict = command.clone();
        version_conflict.operation_id = [14; 16];
        version_conflict.expected_head = Some(command.prepared.version_id);
        let entries = [
            u17a_created(0, command.clone()),
            u17a_created(1, command.clone()),
            u17a_created(2, conflict),
            u17a_created(3, binding_mismatch),
            u17a_created(4, head_mismatch),
            u17a_created(5, version_conflict),
        ];
        store.append_native(&entries).expect("duplicate and rejection batch");
        store.save_committed(u17a_id(5)).expect("commit batch");
        let first = store.apply_next_native(u17a_id(0)).expect("first Created");
        assert_eq!(
            store.apply_next_native(u17a_id(1)).expect("duplicate").created_result(),
            first.created_result(),
            "duplicate compares canonical CRTD body"
        );
        for (index, expected) in [
            (2, CreatedResult::OperationConflict),
            (3, CreatedResult::BindingMismatch),
            (
                4,
                CreatedResult::HeadMismatch {
                    actual_head: Some(command.prepared.version_id),
                },
            ),
            (5, CreatedResult::VersionConflict),
        ] {
            assert_eq!(
                store
                    .apply_next_native(u17a_id(index))
                    .expect("deterministic rejection")
                    .created_result(),
                Some(&expected)
            );
        }
        assert_eq!(
            store
                .created_operation(command.operation_id)
                .expect("operation")
                .expect("first operation")
                .canonical_command,
            command.encode().expect("canonical bytes")
        );
        assert_eq!(
            store
                .created_event(command.operation_id)
                .expect("event")
                .expect("Created event")
                .decision_log_id,
            u17a_id(0)
        );
    }
    #[test]
    fn u17a_native_created_history_preserved() {
        let (_dir, backend, mut store) = u17a_store();
        let first = u17a_command();
        let mut second = first.clone();
        second.operation_id = [15; 16];
        second.prepared.version_id = [16; 16];
        second.prepared.content_length = 8193;
        second.expected_head = Some(first.prepared.version_id);
        store
            .append_native(&[
                u17a_created(0, first.clone()),
                u17a_blank(1),
                u17a_membership(2).0,
                u17a_created(3, second.clone()),
            ])
            .expect("history batch");
        store.save_committed(u17a_id(3)).expect("commit history");
        for index in 0..=3 {
            store.apply_next_native(u17a_id(index)).expect("history apply");
        }
        assert_eq!(store.created_head(&first.object).expect("latest head"), Some(second.prepared.version_id));
        for (command, index) in [(first, 0), (second, 3)] {
            let version = store
                .created_version(&command.object, command.prepared.version_id)
                .expect("historical version")
                .expect("version exists");
            assert_eq!(version.prepared, command.prepared);
            assert_eq!(version.decision_log_id, u17a_id(index));
            assert_eq!(
                store
                    .created_event(command.operation_id)
                    .expect("historical event")
                    .expect("event exists")
                    .decision_log_id,
                u17a_id(index)
            );
        }
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity())
                .expect("history reopen")
                .last_applied()
                .expect("history frontier"),
            Some(u17a_id(3))
        );
        let (_dir, backend, mut store) = u17a_store();
        let first = u17a_command();
        let mut rejected = first.clone();
        rejected.prepared.version_id = [19; 16];
        rejected.expected_head = Some(first.prepared.version_id);
        let mut latest = rejected.clone();
        latest.operation_id = [20; 16];
        latest.prepared.version_id = [21; 16];
        latest.prepared.preparation_id = [22; 16];
        store
            .append_native(&[
                u17a_created(0, first),
                u17a_created(1, rejected.clone()),
                u17a_created(2, latest),
            ])
            .expect("rejected history batch");
        store.save_committed(u17a_id(2)).expect("commit rejected history");
        for index in 0..=2 {
            store.apply_next_native(u17a_id(index)).expect("rejected history apply");
        }
        assert_eq!(
            store
                .native_apply_response(1)
                .expect("rejected receipt")
                .expect("receipt exists")
                .created_result(),
            Some(&CreatedResult::OperationConflict)
        );
        drop(store);
        let mut store = Store::open(&backend.path, identity()).expect("valid rejected history reopen");
        let tx = store.write_transaction().expect("orphan version transaction");
        let mut key = crate::created::object_bytes(&rejected.object).expect("orphan key");
        key.extend(rejected.prepared.version_id);
        let mut value = Vec::new();
        crate::created::put_prepared(&mut value, &rejected.prepared);
        value.extend(encode_id(u17a_id(1)));
        value.extend(u17a_binding().revision.to_be_bytes());
        tx.open_table(application::VERSIONS)
            .expect("version table")
            .insert(key.as_slice(), value.as_slice())
            .expect("inject syntactically valid rejected version");
        store.commit(tx).expect("persist rejected version");
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity())
                .err()
                .expect("U17A_REJECTED_VERSION_NOT_MATERIALIZED")
                .code(),
            "corrupt_record"
        );
    }
    #[test]
    fn u17a_native_reopen_rejects_corruption() {
        for corruption in 0..7 {
            let (_dir, backend, mut store) = u17a_store();
            store
                .append_native(&[u17a_blank(0), u17a_membership(1).0])
                .expect("corruption prefix");
            store.save_committed(u17a_id(1)).expect("commit prefix");
            for index in 0..=1 {
                store.apply_next_native(u17a_id(index)).expect("apply prefix");
            }
            let tx = store.write_transaction().expect("corruption transaction");
            match corruption {
                0 => {
                    tx.open_table(META)
                        .expect("meta")
                        .insert("tail", encode_id(u17a_id(0)).as_slice())
                        .expect("corrupt tail");
                }
                1 => {
                    tx.open_table(META)
                        .expect("meta")
                        .insert("committed", encode_id(LogId { term: 99, ..u17a_id(1) }).as_slice())
                        .expect("corrupt committed");
                }
                2 => {
                    tx.open_table(application::APPLY_RESULTS)
                        .expect("receipts")
                        .remove(0)
                        .expect("receipt gap");
                }
                3..=5 => {
                    let mut table = tx.open_table(application::APPLY_RESULTS).expect("receipts");
                    let mut bytes = table.get(0).expect("receipt read").expect("receipt exists").value().to_vec();
                    match corruption {
                        3 => bytes.push(0),
                        4 => bytes[5] = 2,
                        _ => bytes[6] ^= 1,
                    }
                    table.insert(0, bytes.as_slice()).expect("corrupt receipt");
                }
                _ => {
                    tx.open_table(META)
                        .expect("meta")
                        .insert("native_membership", b"bad".as_slice())
                        .expect("corrupt membership");
                }
            }
            store.commit(tx).expect("persist corruption");
            drop(store);
            assert_eq!(
                Store::open(&backend.path, identity())
                    .err()
                    .expect("strict reopen refusal")
                    .code(),
                "corrupt_record",
                "corruption {corruption}"
            );
        }
    }
    fn u17a_replace_decision(bytes: &mut [u8], from: LogId, to: LogId) {
        let old = encode_id(from);
        let new = encode_id(to);
        let mut offset = 0;
        while offset + old.len() <= bytes.len() {
            if bytes[offset..offset + old.len()] == old {
                bytes[offset..offset + old.len()].copy_from_slice(&new);
                offset += old.len();
            } else {
                offset += 1;
            }
        }
    }
    #[test]
    fn u17a_native_first_decision_provenance() {
        let (_dir, backend, mut store) = u17a_store();
        let command = u17a_command();
        store
            .append_native(&[u17a_created(0, command.clone()), u17a_created(1, command)])
            .expect("duplicate provenance prefix");
        store.save_committed(u17a_id(1)).expect("commit duplicate");
        for index in 0..=1 {
            store.apply_next_native(u17a_id(index)).expect("duplicate apply");
        }
        let tx = store.write_transaction().expect("coherent forgery transaction");
        for definition in [application::OPERATIONS, application::EVENTS, application::VERSIONS] {
            let mut table = tx.open_table(definition).expect("business table");
            let rows = table
                .iter()
                .expect("business rows")
                .map(|row| {
                    let (k, v) = row.expect("business row");
                    (k.value().to_vec(), v.value().to_vec())
                })
                .collect::<Vec<_>>();
            for (key, mut value) in rows {
                u17a_replace_decision(&mut value, u17a_id(0), u17a_id(1));
                table.insert(key.as_slice(), value.as_slice()).expect("forge decision");
            }
        }
        {
            let mut table = tx.open_table(application::APPLY_RESULTS).expect("receipts");
            for index in 0..=1 {
                let mut value = table
                    .get(index)
                    .expect("receipt read")
                    .expect("receipt exists")
                    .value()
                    .to_vec();
                u17a_replace_decision(&mut value[38..], u17a_id(0), u17a_id(1));
                table.insert(index, value.as_slice()).expect("forge receipt result only");
            }
        }
        store.commit(tx).expect("persist coherent forgery");
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity())
                .err()
                .expect("later duplicate cannot become first decision")
                .code(),
            "corrupt_record"
        );
    }
    fn u17a_cut_recovery(backend: &CrashBackend) -> Option<Store> {
        match Store::open(&backend.path, identity()) {
            Ok(store) => Some(store),
            Err(StoreError::Database(_) | StoreError::Storage(_) | StoreError::CorruptRecord) => None,
            Err(error) => panic!("unexpected cut recovery error: {error:?}"),
        }
    }
    #[test]
    fn u17a_native_append_ack_and_cut() {
        let (_dir, backend, mut store) = u17a_store();
        let entries = vec![u17a_blank(0), u17a_created(1, u17a_command()), u17a_membership(2).0];
        backend.media.lock().expect("reset sync count").syncs = 0;
        store.append_native(&entries).expect("ack full native append");
        let reached = backend.media.lock().expect("reached sync count").syncs;
        assert!(reached > 0);
        backend.cut();
        drop(store);
        assert_eq!(
            Store::open(&backend.path, identity())
                .expect("ACK recovery")
                .read_native_entries(0, 3)
                .expect("durable ACK entries"),
            entries
        );
        for sync in 1..=reached {
            for after in [false, true] {
                let (_dir, backend, mut store) = u17a_store();
                let expected = entries.clone();
                let (entered, resume) = backend.pause_sync(sync, after);
                let writer = std::thread::spawn(move || {
                    let result = store.append_native(&expected);
                    (store, result)
                });
                entered.wait();
                backend.cut();
                resume.wait();
                let (store, result) = writer.join().expect("append cut writer");
                drop(store);
                if let Some(store) = u17a_cut_recovery(&backend) {
                    let actual = store.read_native_entries(0, 3).expect("cut prefix");
                    assert!(actual.is_empty() || actual == entries, "no partial native batch");
                    assert_eq!(store.log_state().expect("cut tail").last, actual.last().map(|entry| entry.log_id.into()));
                    if result.is_ok() {
                        assert_eq!(actual, entries, "ACK must be complete");
                    }
                } else {
                    assert!(result.is_err(), "ACK cannot reopen corrupt");
                }
            }
        }
    }
    #[test]
    fn u17a_native_control_apply_ack_and_cut() {
        for membership_kind in [false, true] {
            let (_dir, backend, mut store) = u17a_store();
            let (entry, membership) = if membership_kind {
                u17a_membership(0)
            } else {
                (
                    u17a_blank(0),
                    CaptureMembership::new(None, vec![], std::collections::BTreeMap::new()).expect("empty membership"),
                )
            };
            store.append_native(&[entry.clone()]).expect("control append");
            store.save_committed(u17a_id(0)).expect("control commit");
            backend.media.lock().expect("reset sync count").syncs = 0;
            let response = store.apply_next_native(u17a_id(0)).expect("control ACK");
            let reached = backend.media.lock().expect("reached sync count").syncs;
            assert!(reached > 0);
            backend.cut();
            drop(store);
            let reopened = Store::open(&backend.path, identity()).expect("control ACK recovery");
            assert_eq!(
                reopened.native_application_state().expect("ACK pair"),
                (Some(u17a_id(0)), membership.clone())
            );
            assert_eq!(reopened.native_apply_response(0).expect("ACK receipt"), Some(response));
            drop(reopened);
            for sync in 1..=reached {
                for after in [false, true] {
                    let (_dir, backend, mut store) = u17a_store();
                    let old = store.native_application_state().expect("old pair");
                    store.append_native(&[entry.clone()]).expect("cut control append");
                    store.save_committed(u17a_id(0)).expect("cut control commit");
                    let (entered, resume) = backend.pause_sync(sync, after);
                    let writer = std::thread::spawn(move || {
                        let result = store.apply_next_native(u17a_id(0));
                        (store, result)
                    });
                    entered.wait();
                    backend.cut();
                    resume.wait();
                    let (store, result) = writer.join().expect("apply cut writer");
                    drop(store);
                    if let Some(store) = u17a_cut_recovery(&backend) {
                        let pair = store.native_application_state().expect("cut control pair");
                        let new = (Some(u17a_id(0)), membership.clone());
                        assert!(pair == old || pair == new, "control transaction whole old/new");
                        assert_eq!(store.native_apply_response(0).expect("cut receipt").is_some(), pair.0.is_some());
                        if result.is_ok() {
                            assert_eq!(pair, new);
                        }
                    } else {
                        assert!(result.is_err(), "control ACK cannot reopen corrupt");
                    }
                }
            }
        }
    }
    #[test]
    fn u17a_native_commit_error_disables_writes() {
        for mutation in 0..3 {
            let (_dir, backend, mut store) = u17a_bound();
            if mutation != 0 {
                store.enable_native_empty().expect("enable before failure");
            }
            if mutation == 2 {
                store.append_native(&[u17a_blank(0)]).expect("append before apply failure");
                store.save_committed(u17a_id(0)).expect("commit before apply failure");
            }
            backend.arm(1, false, false);
            let result = match mutation {
                0 => store.enable_native_empty(),
                1 => store.append_native(&[u17a_blank(0)]),
                _ => store.apply_next_native(u17a_id(0)).map(|_| ()),
            };
            assert_eq!(result.expect_err("uncertain commit refused").code(), "commit_indeterminate");
            assert_eq!(store.enable_native_empty().expect_err("enable disabled").code(), "write_disabled");
            assert_eq!(store.append_native(&[]).expect_err("empty append disabled").code(), "write_disabled");
            assert_eq!(store.append(&[]).expect_err("raw append disabled").code(), "write_disabled");
            assert_eq!(
                store
                    .apply_next_native(u17a_id(0))
                    .expect_err("apply and replay disabled")
                    .code(),
                "write_disabled"
            );
            assert_eq!(
                store
                    .apply_next_created(u17a_id(0))
                    .expect_err("legacy apply disabled")
                    .code(),
                "write_disabled"
            );
            assert_eq!(
                store
                    .pin_capture_binding(u17a_binding())
                    .expect_err("binding disabled")
                    .code(),
                "write_disabled"
            );
            assert_eq!(store.save_vote(vote()).expect_err("vote disabled").code(), "write_disabled");
            assert_eq!(store.save_committed(u17a_id(0)).expect_err("commit disabled").code(), "write_disabled");
            assert_eq!(store.truncate_suffix(0).expect_err("truncate disabled").code(), "write_disabled");
            backend.cut();
            drop(store);
            let store = Store::open(&backend.path, identity()).expect("uncertain commit reopened");
            assert!(matches!(committed_schema(&store), 3 | 4));
        }
    }
    #[test]
    fn u17a_native_application_pair_transaction() {
        let (_dir, backend, mut store) = u17a_store();
        let (entry, membership) = u17a_membership(0);
        let old = store.native_application_state().expect("old pair");
        store.append_native(&[entry]).expect("pair append");
        store.save_committed(u17a_id(0)).expect("pair commit");
        let read_tx = store.database.begin_read().expect("real reader transaction");
        let (entered, resume) = backend.pause_sync(1, false);
        let writer = std::thread::spawn(move || {
            let result = store.apply_next_native(u17a_id(0));
            (store, result)
        });
        entered.wait();
        assert_eq!(application::native_application_state(&read_tx).expect("pair during commit"), old);
        resume.wait();
        let (store, result) = writer.join().expect("pair writer");
        result.expect("pair apply ACK");
        assert_eq!(application::native_application_state(&read_tx).expect("pinned reader stays old"), old);
        assert_eq!(
            store.native_application_state().expect("pair after commit"),
            (Some(u17a_id(0)), membership)
        );
        drop(read_tx);
        drop(store);
    }
    fn vote() -> VoteRecord {
        VoteRecord {
            term: 8,
            voted_node: 0,
            committed: true,
        }
    }
    fn batch() -> Vec<LogEntry> {
        (0..3)
            .map(|index| LogEntry {
                id: LogId {
                    term: 8,
                    leader_node: 0,
                    index,
                },
                payload: vec![1, 2, 3],
            })
            .collect()
    }
    #[derive(Debug)]
    struct Media {
        sync_threads: Vec<std::thread::ThreadId>,
        volatile: Vec<u8>,
        dead: bool,
        syncs: usize,
        fault: Option<(usize, bool, bool)>,
        pause: Option<(usize, bool, Arc<Gate>, Arc<Gate>)>,
    }
    #[derive(Debug, Clone)]
    pub(super) struct CrashBackend {
        path: std::path::PathBuf,
        media: Arc<Mutex<Media>>,
    }
    impl CrashBackend {
        pub(super) fn new(path: &Path) -> Self {
            let mut bytes = Vec::new();
            File::open(path)
                .expect("stable exists")
                .read_to_end(&mut bytes)
                .expect("read stable");
            Self {
                path: path.to_owned(),
                media: Arc::new(Mutex::new(Media {
                    sync_threads: Vec::new(),
                    volatile: bytes,
                    dead: false,
                    syncs: 0,
                    fault: None,
                    pause: None,
                })),
            }
        }
        pub(super) fn cut(&self) {
            let mut m = self.media.lock().expect("media");
            m.dead = true;
            m.volatile.clear();
        }
        fn arm(&self, sync: usize, after: bool, cut: bool) {
            let mut m = self.media.lock().expect("media");
            m.syncs = 0;
            m.fault = Some((sync, after, cut));
        }
        pub(super) fn pause_sync(&self, sync: usize, after: bool) -> (Arc<Gate>, Arc<Gate>) {
            let entered = Arc::new(Gate::default());
            let resume = Arc::new(Gate::default());
            let mut m = self.media.lock().expect("media");
            m.syncs = 0;
            m.pause = Some((sync, after, entered.clone(), resume.clone()));
            (entered, resume)
        }
        pub(super) fn database(&self) -> Database {
            Database::builder()
                .create_with_backend(self.clone())
                .expect("backend database")
        }
    }
    impl redb::StorageBackend for CrashBackend {
        fn len(&self) -> io::Result<u64> {
            let m = self.media.lock().expect("media");
            Ok(u64::try_from(m.volatile.len()).expect("length"))
        }
        fn read(&self, offset: u64, data: &mut [u8]) -> io::Result<()> {
            let m = self.media.lock().expect("media");
            if m.dead {
                return Err(io::Error::other("power lost"));
            }
            let offset = usize::try_from(offset).map_err(io::Error::other)?;
            data.copy_from_slice(
                m.volatile
                    .get(offset..offset.checked_add(data.len()).ok_or_else(|| io::Error::other("overflow"))?)
                    .ok_or_else(|| io::Error::other("read bounds"))?,
            );
            Ok(())
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            let mut m = self.media.lock().expect("media");
            if m.dead {
                return Err(io::Error::other("power lost"));
            }
            let offset = usize::try_from(offset).map_err(io::Error::other)?;
            let end = offset.checked_add(data.len()).ok_or_else(|| io::Error::other("overflow"))?;
            if end > m.volatile.len() {
                return Err(io::Error::other("write bounds"));
            }
            m.volatile[offset..end].copy_from_slice(data);
            Ok(())
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            let mut m = self.media.lock().expect("media");
            if m.dead {
                return Err(io::Error::other("power lost"));
            }
            m.volatile.resize(usize::try_from(len).map_err(io::Error::other)?, 0);
            Ok(())
        }
        fn sync_data(&self) -> io::Result<()> {
            let mut m = self.media.lock().expect("media");
            if m.dead {
                return Err(io::Error::other("power lost"));
            }
            m.syncs += 1;
            m.sync_threads.push(std::thread::current().id());
            let fault = m.fault.filter(|(n, _, _)| *n == m.syncs);
            let pause = if m.pause.as_ref().is_some_and(|(n, _, _, _)| *n == m.syncs) {
                m.pause.take()
            } else {
                None
            };
            if let Some((_, false, entered, resume)) = &pause {
                drop(m);
                entered.wait();
                resume.wait();
                m = self.media.lock().expect("media");
                if m.dead {
                    return Err(io::Error::other("power lost during sync"));
                }
            }
            if fault.is_none_or(|(_, after, _)| after) {
                let mut file = OpenOptions::new().write(true).truncate(true).open(&self.path)?;
                file.write_all(&m.volatile)?;
                file.sync_all()?;
            }
            if let Some((_, true, entered, resume)) = pause {
                drop(m);
                entered.wait();
                resume.wait();
                m = self.media.lock().expect("media");
                if m.dead {
                    return Err(io::Error::other("power lost after sync"));
                }
            }
            if let Some((_, _, cut)) = fault {
                m.fault = None;
                if cut {
                    m.dead = true;
                    m.volatile.clear();
                }
                return Err(io::Error::other("injected sync failure"));
            }
            Ok(())
        }
    }
    fn media_store() -> (tempfile::TempDir, CrashBackend, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db");
        drop(Store::initialize(&path, identity()).expect("initialize"));
        let backend = CrashBackend::new(&path);
        let store = Store::from_database(backend.database(), identity()).expect("open initialized backend");
        (dir, backend, store)
    }
    fn assert_complete(store: &Store, new_required: bool) {
        let entries = store.read_entries(0, 4).expect("read entries");
        let state = store.log_state().expect("state");
        assert!(entries.is_empty() || entries == batch(), "U08_INVARIANT partial batch");
        assert_eq!(state.last, entries.last().map(|e| e.id), "U08_INVARIANT tail split");
        if new_required {
            assert_eq!(entries, batch(), "U08_INVARIANT acknowledged batch lost");
        }
    }
    #[test]
    fn power_cut_after_success_preserves_vote_and_entire_batch() {
        let (_dir, backend, mut store) = media_store();
        store.save_vote(vote()).expect("vote ACK");
        store.append(&batch()).expect("batch ACK");
        backend.cut();
        drop(store);
        let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen");
        assert_eq!(reopened.read_vote().expect("vote"), Some(vote()), "U08_INVARIANT acknowledged vote lost");
        assert_complete(&reopened, true);
    }
    #[test]
    fn crash_backend_discards_non_durable_control_write() {
        let (_dir, backend, store) = media_store();
        let mut tx = store.database.begin_write().expect("write");
        tx.set_durability(Durability::None).expect("legal non-durable control");
        tx.set_two_phase_commit(true);
        {
            tx.open_table(META)
                .expect("meta")
                .insert("control", &[1][..])
                .expect("insert");
        }
        tx.commit().expect("none commit");
        backend.cut();
        drop(store);
        let db = CrashBackend::new(&backend.path).database();
        let tx = db.begin_read().expect("read");
        assert!(
            tx.open_table(META).expect("meta").get("control").expect("get").is_none(),
            "U08_INVARIANT non-durable control persisted"
        );
    }
    #[test]
    fn power_cut_during_commit_never_exposes_partial_batch() {
        for sync in 1..=2 {
            for after in [false, true] {
                let (_dir, backend, mut store) = media_store();
                let (entered, resume) = backend.pause_sync(sync, after);
                let writer = std::thread::spawn(move || {
                    let result = store.append(&batch());
                    (store, result)
                });
                entered.wait();
                backend.cut();
                resume.wait();
                let (store, result) = writer.join().expect("join writer");
                drop(store);
                let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity());
                assert!(
                    !matches!(&reopened, Err(StoreError::InvalidLogSequence)),
                    "U08_INVARIANT logical batch split"
                );
                match reopened {
                    Ok(reopened) => assert_complete(&reopened, result.is_ok()),
                    Err(StoreError::Database(_) | StoreError::Storage(_) | StoreError::CorruptRecord) => {
                        assert!(result.is_err(), "U08_INVARIANT ACK became corruption")
                    }
                    Err(e) => panic!("unexpected reopen failure: {e:?}"),
                }
            }
        }
    }
    #[test]
    fn sync_error_fails_closed_and_reopen_resolves() {
        let (_dir, backend, mut store) = media_store();
        backend.arm(1, false, false);
        assert!(
            matches!(store.append(&batch()), Err(StoreError::CommitIndeterminate(_))),
            "U08_INVARIANT sync error gave ACK"
        );
        assert!(
            matches!(store.save_vote(vote()), Err(StoreError::WriteDisabled)),
            "U08_INVARIANT write continued after unknown commit"
        );
        backend.cut();
        drop(store);
        let reopened = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen");
        assert_complete(&reopened, false);
    }
    #[test]
    fn identity_initialize_reopen_and_mismatch() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        drop(Store::initialize(&path, identity()).expect("initialize"));
        assert!(Store::open(&path, identity()).is_ok());
        for wrong in [
            NodeIdentity {
                cluster_id: [8; 16],
                ..identity()
            },
            NodeIdentity {
                node_id: 1,
                ..identity()
            },
        ] {
            assert!(matches!(Store::open(&path, wrong), Err(StoreError::IdentityMismatch)));
        }
        assert!(matches!(Store::initialize(&path, identity()), Err(StoreError::AlreadyInitialized)));
        let empty = dir.path().join("empty");
        File::create(&empty).expect("empty");
        assert!(Store::open(&empty, identity()).is_err());
        let db = Database::open(&path).expect("db");
        let tx = db.begin_write().expect("tx");
        tx.open_table(META).expect("meta").remove("identity").expect("remove");
        tx.commit().expect("commit");
        drop(db);
        assert!(Store::open(&path, identity()).is_err(), "U08_INVARIANT missing identity accepted");
        for missing in 0..4 {
            let path = dir.path().join(format!("missing-{missing}"));
            drop(Store::initialize(&path, identity()).expect("initialize fixture"));
            let db = Database::open(&path).expect("fixture db");
            let tx = db.begin_write().expect("fixture tx");
            match missing {
                0 => {
                    tx.open_table(META).expect("meta").remove("schema").expect("remove schema");
                }
                1 => {
                    tx.delete_table(META).expect("delete meta");
                }
                2 => {
                    tx.delete_table(VOTE).expect("delete vote");
                }
                _ => {
                    tx.delete_table(LOGS).expect("delete logs");
                }
            }
            tx.commit().expect("fixture commit");
            drop(db);
            assert!(
                Store::open(&path, identity()).is_err(),
                "U08_INVARIANT missing required state accepted {missing}"
            );
        }
    }
    #[test]
    fn append_rejects_gaps_overwrite_and_index_overflow() {
        let dir = tempfile::tempdir().expect("dir");
        let mut store = Store::initialize(dir.path().join("db"), identity()).expect("init");
        let mut gap = batch();
        gap[0].id.index = 1;
        assert!(matches!(store.append(&gap), Err(StoreError::InvalidLogSequence)));
        store.append(&batch()).expect("append");
        assert!(matches!(store.append(&batch()), Err(StoreError::InvalidLogSequence)));
        store.append(&[]).expect("empty");
        assert_complete(&store, true);
        assert_eq!(next_index(Some(u64::MAX - 1)).expect("last index"), u64::MAX);
        assert!(
            matches!(next_index(Some(u64::MAX)), Err(StoreError::IndexExhausted)),
            "U08_INVARIANT index overflow"
        );
    }
    #[test]
    fn strict_record_decode_rejects_corruption() {
        for target in 0..6 {
            let dir = tempfile::tempdir().expect("dir");
            let path = dir.path().join("db");
            let mut store = Store::initialize(&path, identity()).expect("init");
            store.save_vote(vote()).expect("vote");
            store.append(&batch()).expect("append");
            let tx = store.database.begin_write().expect("tx");
            match target {
                0 => {
                    tx.open_table(META).expect("meta").insert("schema", &[1][..]).expect("insert");
                }
                1 => {
                    tx.open_table(VOTE).expect("vote").insert(0, &[1][..]).expect("insert");
                }
                2 => {
                    let mut b = encode_vote(vote());
                    b[16] = 2;
                    tx.open_table(VOTE).expect("vote").insert(0, b.as_slice()).expect("insert");
                }
                3 => {
                    tx.open_table(LOGS).expect("logs").insert(0, &[1][..]).expect("insert");
                }
                4 => {
                    let mut e = batch()[0].clone();
                    e.id.index = 9;
                    tx.open_table(LOGS)
                        .expect("logs")
                        .insert(0, encode_log(&e).expect("encode").as_slice())
                        .expect("insert");
                }
                _ => {
                    tx.open_table(META)
                        .expect("meta")
                        .insert("tail", encode_id(batch()[0].id).as_slice())
                        .expect("insert");
                }
            }
            tx.commit().expect("commit");
            drop(store);
            assert!(Store::open(&path, identity()).is_err(), "U08_INVARIANT corrupt record accepted {target}");
        }
    }
    fn truncate_batch() -> Vec<LogEntry> {
        (0..5)
            .map(|index| LogEntry {
                id: LogId {
                    term: 8,
                    leader_node: 0,
                    index,
                },
                payload: vec![u8::try_from(index).expect("small index"), 2, 3],
            })
            .collect()
    }
    fn assert_prefix(store: &Store, expected: &[LogEntry]) {
        assert_eq!(
            store.read_entries(0, u64::MAX).expect("entries"),
            expected,
            "U09_INVARIANT acknowledged truncate lost"
        );
        assert_eq!(
            store.log_state().expect("state"),
            LogState {
                first: expected.first().map(|e| e.id),
                last: expected.last().map(|e| e.id),
            },
            "U09_INVARIANT tail split"
        );
        assert_eq!(store.read_vote().expect("vote"), Some(vote()));
    }
    #[test]
    fn truncate_suffix_reopen_then_replace() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        let mut store = Store::initialize(&path, identity()).expect("init");
        store.save_vote(vote()).expect("vote");
        let old = truncate_batch();
        store.append(&old).expect("append");
        store.truncate_suffix(2).expect("truncate");
        drop(store);
        let reopened = Store::open(&path, identity());
        assert!(reopened.is_ok(), "U09_INVARIANT reopened truncate invalid: {:?}", reopened.as_ref().err());
        let mut store = reopened.expect("asserted reopen");
        assert_prefix(&store, &old[..2]);
        let replacement: Vec<_> = (2..4)
            .map(|index| LogEntry {
                id: LogId {
                    term: 9,
                    leader_node: 17,
                    index,
                },
                payload: vec![9, 0, 7],
            })
            .collect();
        store.append(&replacement).expect("replace suffix");
        drop(store);
        let store = Store::open(&path, identity()).expect("reopen replacement");
        let mut expected = old[..2].to_vec();
        expected.extend(replacement);
        assert_prefix(&store, &expected);
    }
    #[test]
    fn truncate_zero_and_noop_preserve_invariants() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        let mut store = Store::initialize(&path, identity()).expect("init");
        store.save_vote(vote()).expect("vote");
        for from in [0, 1, u64::MAX] {
            store.truncate_suffix(from).expect("empty no-op");
        }
        store.append(&truncate_batch()).expect("append");
        for from in [5, 6, u64::MAX] {
            store.truncate_suffix(from).expect("tail no-op");
        }
        assert_prefix(&store, &truncate_batch());
        store.truncate_suffix(0).expect("clear");
        drop(store);
        let mut store = Store::open(&path, identity()).expect("reopen empty");
        assert_prefix(&store, &[]);
        store.append(&truncate_batch()[..1]).expect("restart at zero");
        drop(store);
        assert_prefix(&Store::open(&path, identity()).expect("reopen zero"), &truncate_batch()[..1]);
    }
    #[test]
    fn truncate_success_survives_power_cut() {
        for from in [2, 0] {
            let (_dir, backend, mut store) = media_store();
            store.save_vote(vote()).expect("vote");
            let old = truncate_batch();
            store.append(&old).expect("stable append");
            store.truncate_suffix(from).expect("truncate ACK");
            backend.cut();
            drop(store);
            let store = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen");
            assert_prefix(&store, &old[..usize::try_from(from).expect("small from")]);
        }
    }
    #[test]
    fn truncate_power_cut_never_splits_tail() {
        for from in [2, 0] {
            for sync in 1..=2 {
                for after in [false, true] {
                    let (_dir, backend, mut store) = media_store();
                    store.save_vote(vote()).expect("vote");
                    let old = truncate_batch();
                    store.append(&old).expect("stable append");
                    backend.arm(sync, after, true);
                    assert!(
                        matches!(store.truncate_suffix(from), Err(StoreError::CommitIndeterminate(_))),
                        "U09_INVARIANT interrupted truncate ACK"
                    );
                    backend.cut();
                    drop(store);
                    let fresh = CrashBackend::new(&backend.path);
                    match Database::builder().create_with_backend(fresh) {
                        Ok(database) => match Store::from_database(database, identity()) {
                            Ok(store) => {
                                let entries = store.read_entries(0, u64::MAX).expect("entries");
                                let prefix = &old[..usize::try_from(from).expect("small from")];
                                assert!(entries == old || entries == prefix, "U09_INVARIANT partial truncate");
                                assert_prefix(&store, &entries);
                            }
                            Err(error) => panic!("U09_INVARIANT logical recovery error: {error:?}"),
                        },
                        Err(redb::DatabaseError::Storage(redb::StorageError::Corrupted(_))) => {}
                        Err(error) => panic!("unexpected media recovery error: {error:?}"),
                    }
                }
            }
        }
    }
    #[test]
    fn truncate_sync_error_fails_closed() {
        let (_dir, backend, mut store) = media_store();
        store.save_vote(vote()).expect("vote");
        let old = truncate_batch();
        store.append(&old).expect("append");
        backend.arm(1, false, false);
        assert!(
            matches!(store.truncate_suffix(2), Err(StoreError::CommitIndeterminate(_))),
            "U09_INVARIANT sync error ACK"
        );
        assert!(matches!(store.save_vote(vote()), Err(StoreError::WriteDisabled)));
        assert!(matches!(store.append(&[]), Err(StoreError::WriteDisabled)));
        for from in [0, 2, 99, u64::MAX] {
            assert!(
                matches!(store.truncate_suffix(from), Err(StoreError::WriteDisabled)),
                "U09_INVARIANT truncate continued after unknown commit"
            );
        }
        backend.cut();
        drop(store);
        let store = Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen");
        assert_prefix(&store, &old);
    }
    #[test]
    fn truncate_survives_child_kill() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        let mut store = Store::initialize(&path, identity()).expect("init");
        store.save_vote(vote()).expect("vote");
        store.append(&truncate_batch()).expect("append");
        drop(store);
        let mut child = child(&path, "truncate");
        ack(&mut child);
        child.kill().expect("kill");
        child.wait().expect("wait");
        assert_prefix(&Store::open(&path, identity()).expect("reopen"), &truncate_batch()[..2]);
    }
    fn committed_schema(store: &Store) -> u64 {
        let tx = store.database.begin_read().expect("read metadata transaction");
        number(
            tx.open_table(META)
                .expect("open committed metadata")
                .get("schema")
                .expect("read committed schema")
                .expect("committed fixture value")
                .value(),
        )
        .expect("committed fixture value")
    }
    fn committed_fixture() -> (tempfile::TempDir, CrashBackend, Store) {
        let (dir, backend, mut store) = media_store();
        store.save_vote(vote()).expect("save fixture vote");
        store.append(&truncate_batch()).expect("append fixture logs");
        (dir, backend, store)
    }
    #[test]
    fn committed_schema1_migration_reopen() {
        let (_dir, backend, mut store) = committed_fixture();
        assert_eq!(store.read_committed().expect("read exact committed"), None);
        assert_eq!(committed_schema(&store), 1);
        let id = truncate_batch()[2].id;
        store.save_committed(id).expect("save committed ACK");
        drop(store);
        let store =
            Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen committed fixture");
        assert_eq!(store.read_committed().expect("read exact committed"), Some(id));
        assert_eq!(committed_schema(&store), 2);
        assert_prefix(&store, &truncate_batch());
        drop(store);
        for (schema, marker) in [
            (1u64, Some(encode_id(id))),
            (2, None),
            (2, Some(vec![1])),
            (2, Some(encode_id(LogId { term: 99, ..id }))),
            (2, Some(encode_id(LogId { leader_node: 99, ..id }))),
        ] {
            let db = CrashBackend::new(&backend.path).database();
            let tx = db.begin_write().expect("write corrupt fixture transaction");
            {
                let mut meta = tx.open_table(META).expect("open metadata");
                meta.insert("schema", schema.to_be_bytes().as_slice())
                    .expect("insert corrupt fixture schema");
                meta.remove("committed").expect("remove corrupt fixture marker");
                if let Some(bytes) = marker {
                    meta.insert("committed", bytes.as_slice())
                        .expect("insert corrupt fixture marker");
                }
            }
            tx.commit().expect("commit corrupt fixture");
            let corrupt = Store {
                database: db,
                writable: true,
            };
            assert!(
                matches!(corrupt.read_committed(), Err(StoreError::CorruptRecord)),
                "U10_INVARIANT corrupt committed read accepted"
            );
            assert!(
                matches!(Store::from_database(corrupt.database, identity()), Err(StoreError::CorruptRecord)),
                "U10_INVARIANT corrupt committed accepted"
            );
        }
    }
    #[test]
    fn committed_rejects_missing_mismatch_and_regression() {
        let (_dir, _backend, mut store) = committed_fixture();
        let id = truncate_batch()[2].id;
        for bad in [
            LogId { index: 99, ..id },
            LogId { term: 99, ..id },
            LogId { leader_node: 99, ..id },
        ] {
            assert!(matches!(store.save_committed(bad), Err(StoreError::InvalidCommittedLog)));
            assert_eq!(store.read_committed().expect("read exact committed"), None);
            assert_eq!(committed_schema(&store), 1);
            assert_prefix(&store, &truncate_batch());
        }
        store.save_committed(id).expect("save committed ACK");
        store.save_committed(id).expect("save committed ACK");
        assert!(matches!(
            store.save_committed(truncate_batch()[1].id),
            Err(StoreError::CommittedRegression)
        ));
        assert_eq!(store.read_committed().expect("read exact committed"), Some(id));
        store.save_committed(truncate_batch()[3].id).expect("committed fixture value");
        assert_eq!(store.read_committed().expect("read exact committed"), Some(truncate_batch()[3].id));
        assert_eq!(committed_schema(&store), 2);
        assert_prefix(&store, &truncate_batch());
    }
    #[test]
    fn committed_protects_truncate_and_allows_suffix_replacement() {
        let (_dir, backend, mut store) = committed_fixture();
        let id = truncate_batch()[2].id;
        store.save_committed(id).expect("save committed ACK");
        for from in [0, 2] {
            assert!(
                matches!(store.truncate_suffix(from), Err(StoreError::CommittedLogProtected)),
                "U10_INVARIANT committed prefix deleted"
            );
            assert_prefix(&store, &truncate_batch());
        }
        store.truncate_suffix(3).expect("truncate uncommitted suffix");
        let mut expected = truncate_batch()[..3].to_vec();
        let replacement: Vec<_> = (3..5)
            .map(|index| LogEntry {
                id: LogId {
                    term: 12,
                    leader_node: 7,
                    index,
                },
                payload: vec![9],
            })
            .collect();
        store.append(&replacement).expect("append replacement suffix");
        expected.extend(replacement);
        drop(store);
        let store =
            Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen committed fixture");
        assert_eq!(store.read_committed().expect("read exact committed"), Some(id));
        assert_prefix(&store, &expected);
    }
    #[test]
    fn committed_success_survives_power_cut() {
        for advance in [false, true] {
            let (_dir, backend, mut store) = committed_fixture();
            store.save_committed(truncate_batch()[1].id).expect("committed fixture value");
            let id = truncate_batch()[if advance { 2 } else { 1 }].id;
            if advance {
                store.save_committed(id).expect("save committed ACK");
            }
            backend.cut();
            drop(store);
            let store =
                Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("committed fixture value");
            assert_eq!(
                store.read_committed().expect("read exact committed"),
                Some(id),
                "U10_INVARIANT acknowledged committed lost"
            );
            assert_eq!(committed_schema(&store), 2);
            assert_prefix(&store, &truncate_batch());
        }
    }
    #[test]
    fn committed_power_cut_never_splits_schema_and_marker() {
        for sync in 1..=2 {
            for after in [false, true] {
                let (_dir, backend, mut store) = committed_fixture();
                let id = truncate_batch()[2].id;
                backend.arm(sync, after, true);
                assert!(matches!(store.save_committed(id), Err(StoreError::CommitIndeterminate(_))));
                backend.cut();
                drop(store);
                match Database::builder().create_with_backend(CrashBackend::new(&backend.path)) {
                    Ok(db) => {
                        let recovered = Store::from_database(db, identity());
                        assert!(recovered.is_ok(), "U10_INVARIANT logical schema marker split: {:?}", recovered.err());
                        let store = recovered.expect("committed fixture value");
                        let pair = (committed_schema(&store), store.read_committed().expect("read exact committed"));
                        assert!(pair == (1, None) || pair == (2, Some(id)), "U10_INVARIANT partial migration");
                        assert_prefix(&store, &truncate_batch());
                    }
                    Err(redb::DatabaseError::Storage(redb::StorageError::Corrupted(_))) => {}
                    Err(error) => panic!("unexpected media recovery error: {error:?}"),
                }
            }
        }
    }
    #[test]
    fn committed_sync_error_disables_all_mutations() {
        for migrated in [false, true] {
            let (_dir, backend, mut store) = committed_fixture();
            let previous = truncate_batch()[1].id;
            if migrated {
                store.save_committed(previous).expect("stable first committed");
            }
            let id = truncate_batch()[2].id;
            backend.arm(1, false, false);
            assert!(matches!(store.save_committed(id), Err(StoreError::CommitIndeterminate(_))));
            for blocked in [previous, id] {
                assert!(matches!(store.save_committed(blocked), Err(StoreError::WriteDisabled)));
            }
            assert!(matches!(store.save_vote(vote()), Err(StoreError::WriteDisabled)));
            assert!(matches!(store.append(&[]), Err(StoreError::WriteDisabled)));
            assert!(matches!(store.append(&truncate_batch()), Err(StoreError::WriteDisabled)));
            for from in [0, 2, 99] {
                assert!(matches!(store.truncate_suffix(from), Err(StoreError::WriteDisabled)));
            }
            backend.cut();
            drop(store);
            let store =
                Store::from_database(CrashBackend::new(&backend.path).database(), identity()).expect("reopen after sync error");
            let pair = (committed_schema(&store), store.read_committed().expect("read recovered committed"));
            let old = if migrated { (2, Some(previous)) } else { (1, None) };
            assert!(pair == old || pair == (2, Some(id)), "U10_INVARIANT recovered wrong committed");
            assert_prefix(&store, &truncate_batch());
        }
    }
    #[test]
    fn committed_survives_child_kill() {
        let dir = tempfile::tempdir().expect("committed fixture value");
        let path = dir.path().join("db");
        let mut store = Store::initialize(&path, identity()).expect("committed fixture value");
        store.save_vote(vote()).expect("save fixture vote");
        store.append(&truncate_batch()).expect("append fixture logs");
        drop(store);
        let mut child = child(&path, "committed");
        ack(&mut child);
        child.kill().expect("committed fixture value");
        child.wait().expect("committed fixture value");
        let mut store = Store::open(&path, identity()).expect("committed fixture value");
        assert_eq!(store.read_committed().expect("read exact committed"), Some(truncate_batch()[2].id));
        assert_eq!(committed_schema(&store), 2);
        assert!(matches!(store.truncate_suffix(2), Err(StoreError::CommittedLogProtected)));
        assert_prefix(&store, &truncate_batch());
    }
    fn child(path: &Path, mode: &str) -> std::process::Child {
        Command::new(std::env::current_exe().expect("exe"))
            .args(["--exact", "tests::process_child", "--nocapture"])
            .env("U08_CHILD_PATH", path)
            .env("U08_CHILD_MODE", mode)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn child")
    }
    fn ack(child: &mut std::process::Child) {
        let stdout = child.stdout.take().expect("stdout");
        let (sender, receiver) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if line.trim() == "U08_ACK" => {
                        let _ = sender.send(());
                        break;
                    }
                    Ok(_) => {}
                }
            }
        });
        let result = receiver.recv_timeout(Duration::from_secs(20));
        if result.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        reader.join().expect("join ACK reader");
        assert!(result.is_ok(), "child exited or timed out before ACK");
    }
    #[test]
    fn exclusive_open_rejects_second_process() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        let store = Store::initialize(&path, identity()).expect("init");
        assert!(Store::open(&path, identity()).is_err(), "U08_INVARIANT second handle accepted");
        let mut child = child(&path, "exclusive");
        assert!(child.wait().expect("wait").success(), "U08_INVARIANT second process accepted");
        drop(store);
    }
    #[test]
    fn vote_and_log_batch_survive_child_kill() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("db");
        drop(Store::initialize(&path, identity()).expect("init"));
        let mut child = child(&path, "write");
        ack(&mut child);
        child.kill().expect("kill");
        child.wait().expect("wait");
        let store = Store::open(&path, identity()).expect("reopen");
        assert_eq!(store.read_vote().expect("vote"), Some(vote()), "U08_INVARIANT child vote lost");
        assert_complete(&store, true);
    }
    #[test]
    fn process_child() {
        if let Some(path) = std::env::var_os("U08_CHILD_PATH") {
            if std::env::var("U08_CHILD_MODE").expect("mode") == "exclusive" {
                assert!(Store::open(path, identity()).is_err(), "U08_INVARIANT exclusive lock absent");
            } else {
                let mut store = Store::open(path, identity()).expect("child open");
                let mode = std::env::var("U08_CHILD_MODE").expect("mode");
                if matches!(mode.as_str(), "provenance-native" | "provenance-legacy") {
                    store.save_vote(vote()).expect("child provenance vote ACK");
                    let native = mode == "provenance-native";
                    if native {
                        store.enable_native_empty().expect("child native enable");
                    }
                    provenance_promote(&mut store);
                    let commands = provenance_commands();
                    if native {
                        store
                            .append_native(
                                &commands
                                    .iter()
                                    .enumerate()
                                    .map(|(i, command)| u17a_created(u64::try_from(i).expect("child index"), command.clone()))
                                    .collect::<Vec<_>>(),
                            )
                            .expect("child native append");
                    } else {
                        store
                            .append(
                                &commands
                                    .iter()
                                    .enumerate()
                                    .map(|(i, command)| LogEntry {
                                        id: u17a_id(u64::try_from(i).expect("child index")),
                                        payload: command.encode().expect("child canonical"),
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .expect("child legacy append");
                    }
                    store.save_committed(u17a_id(8)).expect("child committed ACK");
                    for index in 0..9 {
                        if native {
                            store.apply_next_native(u17a_id(index)).expect("child native apply ACK");
                        } else {
                            store.apply_next_created(u17a_id(index)).expect("child legacy apply ACK");
                        }
                    }
                } else if mode == "committed" {
                    store.save_committed(truncate_batch()[2].id).expect("child committed ACK");
                } else if std::env::var("U08_CHILD_MODE").expect("mode") == "truncate" {
                    store.truncate_suffix(2).expect("truncate ACK");
                } else {
                    store.save_vote(vote()).expect("vote");
                    store.append(&batch()).expect("append");
                }
                println!("U08_ACK");
                io::stdout().flush().expect("flush");
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        } else {
            let dir = tempfile::tempdir().expect("dir");
            let path = dir.path().join("db");
            drop(Store::initialize(&path, identity()).expect("init"));
            let store = Store::open(&path, identity()).expect("open");
            assert_eq!(store.read_vote().expect("vote"), None);
            assert_eq!(store.log_state().expect("state"), LogState { first: None, last: None });
        }
    }
}
