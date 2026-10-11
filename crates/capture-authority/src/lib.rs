//! Synchronous, exclusive local authority storage. Run from a blocking worker.
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

mod application;
mod committed;
mod created;
mod native_snapshot;
mod node;
pub use native_snapshot::{NativeSnapshotError, NativeSnapshotView};
mod raft_types;
pub mod snapshot;
pub use created::{
    CaptureBinding, CreatedApplyResult, CreatedEvent, CreatedOperation, CreatedResult, CreatedVersion, DecideCreated,
    MAX_OBJECT_KEY_LENGTH, ObjectIdentity, PreparedIdentity,
};
pub use node::{CaptureNode, CaptureNodeError};
pub use raft_types::{
    CaptureCommand, CaptureMembership, CaptureRaftEntry, CaptureResponse, CaptureResponseKind, CaptureTypeConfig, MembershipError,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            Self::UnsupportedSchema => "unsupported_schema",
            Self::CorruptRecord => "corrupt_record",
            Self::InvalidMembership(_) => "invalid_membership",
            Self::InvalidLogSequence => "invalid_log_sequence",
            Self::InvalidCommittedLog => "invalid_committed_log",
            Self::CommittedRegression => "committed_regression",
            Self::CommittedLogProtected => "committed_log_protected",
            Self::BindingNotPinned => "binding_not_pinned",
            Self::BindingConflict => "binding_conflict",
            Self::InvalidApplySequence => "invalid_apply_sequence",
            Self::IndexExhausted => "index_exhausted",
            Self::AlreadyInitialized => "already_initialized",
            Self::MissingStore => "missing_store",
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
    fn from_database(database: Database, expected: NodeIdentity) -> Result<Self, StoreError> {
        let store = Self {
            database,
            writable: true,
        };
        let tx = store.database.begin_read()?;
        let meta = tx.open_table(META)?;
        let schema = meta.get("schema")?.ok_or(StoreError::CorruptRecord)?;
        if !matches!(number(schema.value())?, 1..=4) {
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
        let mut next = Some(0);
        let mut last = None;
        for row in logs.iter()? {
            let (key, value) = row?;
            let entry = decode_log(value.value())?;
            if next != Some(key.value()) || entry.id.index != key.value() {
                return Err(if number(schema.value())? == 4 {
                    StoreError::CorruptRecord
                } else {
                    StoreError::InvalidLogSequence
                });
            }
            if number(schema.value())? == 4 {
                application::decode_native_entry(entry.clone())?;
            }
            next = key.value().checked_add(1);
            last = Some(entry.id);
        }
        committed::validate_committed(&meta, &logs)?;
        application::validate_application(&tx)?;
        let tail = meta.get("tail")?.map(|v| decode_id(v.value())).transpose()?;
        if tail != last {
            return Err(if number(schema.value())? == 4 {
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
        if number(tx.open_table(META)?.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())? == 4 {
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
        let first = logs.get(0)?.map(|v| decode_log(v.value()).map(|e| e.id)).transpose()?;
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

    fn u17b_snapshot(store: &Store) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        use crate::application::{APPLY_RESULTS, EVENTS, HEADS, OPERATIONS, VERSIONS};
        let tx = store.database.begin_read().expect("snapshot transaction");
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
        assert!(matches!(done, Ok(true)), "U24_STREAM_RELEASES_WRITER");
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
                if std::env::var("U08_CHILD_MODE").expect("mode") == "committed" {
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
