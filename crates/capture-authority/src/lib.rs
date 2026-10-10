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
mod node;
mod raft_types;
pub use created::{
    CaptureBinding, CreatedApplyResult, CreatedEvent, CreatedOperation, CreatedResult, CreatedVersion, DecideCreated,
    MAX_OBJECT_KEY_LENGTH, ObjectIdentity, PreparedIdentity,
};
pub use node::{CaptureNode, CaptureNodeError};
pub use raft_types::{CaptureMembership, MembershipError};
#[cfg(test)]
mod application_tests;
mod truncate;

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
        if !matches!(number(schema.value())?, 1..=3) {
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
                return Err(StoreError::InvalidLogSequence);
            }
            next = key.value().checked_add(1);
            last = Some(entry.id);
        }
        committed::validate_committed(&meta, &logs)?;
        application::validate_application(&tx)?;
        let tail = meta.get("tail")?.map(|v| decode_id(v.value())).transpose()?;
        if tail != last {
            return Err(StoreError::InvalidLogSequence);
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
