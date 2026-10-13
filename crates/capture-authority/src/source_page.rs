//! Disposable event ordering index. EVENTS remains the sole event authority.
use crate::{
    CaptureBinding, CreatedEvent, CreatedResult, LogId, NativeLogWriter, NativeWriteError, Store, StoreError, application,
    decode_id, encode_id,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction};

pub const MAX_EVENT_PAGE: usize = 256;
pub(crate) const EVENT_SEQUENCE: TableDefinition<u64, &[u8]> = TableDefinition::new("event_sequence_cache_v1");

/// Persist only after every event preceding this cursor has a durable consumer ACK.
/// Deserialization does not grant authority: each use checks the stored event.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    cluster: [u8; 16],
    term: u64,
    leader: u64,
    index: u64,
    event: [u8; 16],
    binding_id: [u8; 16],
    target_id: [u8; 16],
    revision: u64,
}
impl EventCursor {
    pub(crate) fn new(cluster: [u8; 16], event: [u8; 16], value: &CreatedEvent) -> Self {
        Self {
            cluster,
            term: value.decision_log_id.term,
            leader: value.decision_log_id.leader_node,
            index: value.decision_log_id.index,
            event,
            binding_id: value.binding.binding_id,
            target_id: value.binding.target_id,
            revision: value.binding.revision,
        }
    }
    fn matches(&self, cluster: [u8; 16], event: [u8; 16], value: &CreatedEvent) -> bool {
        self == &Self::new(cluster, event, value)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEvent {
    pub event_id: [u8; 16],
    pub captured: CreatedEvent,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPage {
    pub events: Vec<SourceEvent>,
    pub next: Option<EventCursor>,
}
fn encoded_index(id: LogId, event: [u8; 16]) -> Vec<u8> {
    let mut bytes = encode_id(id).to_vec();
    bytes.extend(event);
    bytes
}
fn decode_index(bytes: &[u8]) -> Result<(LogId, [u8; 16]), StoreError> {
    if bytes.len() != 40 {
        return Err(StoreError::CorruptRecord);
    }
    let id = decode_id(&bytes[..24])?;
    let event: [u8; 16] = bytes[24..].try_into().map_err(|_| StoreError::CorruptRecord)?;
    if event == [0; 16] {
        return Err(StoreError::CorruptRecord);
    }
    Ok((id, event))
}
pub(crate) fn insert_event_index(tx: &WriteTransaction, id: LogId, event: [u8; 16]) -> Result<(), StoreError> {
    let bytes = encoded_index(id, event);
    let mut index = tx.open_table(EVENT_SEQUENCE)?;
    if index.get(id.index)?.is_some() {
        return Err(StoreError::CorruptRecord);
    }
    index.insert(id.index, bytes.as_slice())?;
    Ok(())
}

/// Check the event semantics required to derive ordering from checkpoint authority.
/// This deliberately does not grant snapshot installation or source authority.
fn validated_event(
    key: &[u8],
    bytes: &[u8],
    operations: &impl ReadableTable<&'static [u8], &'static [u8]>,
    versions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    schema: u64,
    binding: &CaptureBinding,
    applied: Option<LogId>,
) -> Result<([u8; 16], CreatedEvent), StoreError> {
    let event_id: [u8; 16] = key.try_into().map_err(|_| StoreError::CorruptRecord)?;
    let event = application::decode_event(bytes)?;
    let cut = applied.ok_or(StoreError::CorruptRecord)?;
    let operation = operations.get(key)?.ok_or(StoreError::CorruptRecord)?;
    let first_decision = if matches!(schema, 5 | 6 | 7) {
        Some(application::decode_provenance(operation.value())?.first_decision_log_id)
    } else {
        None
    };
    let operation = application::decode_stored_operation(operation.value(), schema)?;
    let command = crate::DecideCreated::decode(&operation.canonical_command)?;
    let version_key = application::version_key(&event.object, event.prepared.version_id)?;
    let version = versions.get(version_key.as_slice())?.ok_or(StoreError::CorruptRecord)?;
    let version = application::decode_version(version.value())?;
    if first_decision.is_some_and(|first| first != event.decision_log_id)
        || event_id == [0; 16]
        || event.decision_log_id.index > cut.index
        || (event.decision_log_id.index == cut.index && event.decision_log_id != cut)
        || event.binding != *binding
        || command.operation_id != event_id
        || command.object != event.object
        || command.prepared != event.prepared
        || command.binding_revision != event.binding.revision
        || operation.result
            != (CreatedResult::Created {
                version_id: event.prepared.version_id,
                event_id,
                decision_log_id: event.decision_log_id,
            })
        || version
            != (crate::CreatedVersion {
                prepared: event.prepared.clone(),
                decision_log_id: event.decision_log_id,
                binding_revision: event.binding.revision,
            })
    {
        return Err(StoreError::CorruptRecord);
    }
    Ok((event_id, event))
}

/// Rebuild from the same authority rows emitted by the existing four-section checkpoint.
/// Callers must independently establish authority and validate all other state before promotion.
pub(crate) fn rebuild_from_authority(
    tx: &WriteTransaction,
    schema: u64,
    binding: &CaptureBinding,
    applied: Option<LogId>,
) -> Result<(), StoreError> {
    let events = tx.open_table(application::EVENTS)?;
    let operations = tx.open_table(application::OPERATIONS)?;
    let versions = tx.open_table(application::VERSIONS)?;
    {
        let mut index = tx.open_table(EVENT_SEQUENCE)?;
        index.retain(|_, _| false)?;
    }
    for row in events.iter()? {
        let (key, bytes) = row?;
        let (event, value) = validated_event(key.value(), bytes.value(), &operations, &versions, schema, binding, applied)?;
        insert_event_index(tx, value.decision_log_id, event)?;
    }
    Ok(())
}
impl Store {
    pub(crate) fn ensure_event_index(&mut self) -> Result<(), StoreError> {
        let read = self.database.begin_read()?;
        let meta = read.open_table(crate::META)?;
        let schema = crate::number(meta.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())?;
        if !matches!(schema, 3..=7) {
            return Ok(());
        }
        let binding = application::read_binding(&meta)?;
        let applied = application::read_applied(&meta)?;
        let events = read.open_table(application::EVENTS)?;
        let complete = match read.open_table(EVENT_SEQUENCE) {
            Ok(index) => {
                let mut complete = index.len()? == events.len()?;
                if complete {
                    for row in events.iter()? {
                        let (key, bytes) = row?;
                        let value = application::decode_event(bytes.value())?;
                        let expected =
                            encoded_index(value.decision_log_id, key.value().try_into().map_err(|_| StoreError::CorruptRecord)?);
                        if index
                            .get(value.decision_log_id.index)?
                            .is_none_or(|stored| stored.value() != expected.as_slice())
                        {
                            complete = false;
                            break;
                        }
                    }
                }
                complete
            }
            Err(redb::TableError::TableDoesNotExist(_)) => false,
            Err(error) => return Err(error.into()),
        };
        drop(events);
        drop(meta);
        drop(read);
        if !complete {
            let tx = self.write_transaction()?;
            rebuild_from_authority(&tx, schema, &binding, applied)?;
            self.commit(tx)?;
        }
        Ok(())
    }
    /// Read a bounded page without changing any delivery ACK or cursor state.
    pub fn created_events_page(&self, cursor: Option<&EventCursor>, limit: usize) -> Result<EventPage, StoreError> {
        if !self.writable {
            return Err(StoreError::WriteDisabled);
        }
        if limit == 0 || limit > MAX_EVENT_PAGE {
            return Err(StoreError::InvalidEventPage);
        }
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(crate::META)?;
        let schema = crate::number(meta.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())?;
        if !matches!(schema, 3..=7) {
            return Err(StoreError::UnsupportedSchema);
        }
        let identity = crate::decode_identity(meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())?;
        let events = tx.open_table(application::EVENTS)?;
        let index = tx.open_table(EVENT_SEQUENCE)?;
        if index.len()? != events.len()? {
            return Err(StoreError::CorruptRecord);
        }
        let read_event = |id: u64, bytes: &[u8]| -> Result<SourceEvent, StoreError> {
            let (log, event_id) = decode_index(bytes)?;
            let value = events.get(event_id.as_slice())?.ok_or(StoreError::CorruptRecord)?;
            let captured = application::decode_event(value.value())?;
            if log.index != id || captured.decision_log_id != log {
                return Err(StoreError::CorruptRecord);
            }
            Ok(SourceEvent { event_id, captured })
        };
        if let Some(cursor) = cursor {
            let stored = index.get(cursor.index)?.ok_or(StoreError::IdentityMismatch)?;
            let event = read_event(cursor.index, stored.value())?;
            if !cursor.matches(identity.cluster_id, event.event_id, &event.captured) {
                return Err(StoreError::IdentityMismatch);
            }
        }
        let start = cursor.map_or(0, |c| c.index);
        let range = if cursor.is_some() {
            (std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded)
        } else {
            (std::ops::Bound::Included(start), std::ops::Bound::Unbounded)
        };
        let mut page = EventPage {
            events: Vec::with_capacity(limit),
            next: cursor.cloned(),
        };
        for row in index.range(range)?.take(limit) {
            let (id, bytes) = row?;
            let event = read_event(id.value(), bytes.value())?;
            page.next = Some(EventCursor::new(identity.cluster_id, event.event_id, &event.captured));
            page.events.push(event);
        }
        Ok(page)
    }
}
impl NativeLogWriter {
    pub async fn created_events_page(&self, cursor: Option<EventCursor>, limit: usize) -> Result<EventPage, NativeWriteError> {
        let owner = self.store.clone();
        tokio::task::spawn_blocking(move || {
            owner
                .lock()
                .map_err(|_| NativeWriteError::Poisoned)?
                .created_events_page(cursor.as_ref(), limit)
                .map_err(NativeWriteError::Store)
        })
        .await
        .map_err(NativeWriteError::Worker)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CaptureCommand, CaptureRaftEntry, DecideCreated, NodeIdentity, ObjectIdentity, PreparedIdentity, snapshot};
    use std::io;
    fn identity() -> NodeIdentity {
        NodeIdentity {
            cluster_id: [7; 16],
            node_id: 0,
        }
    }
    fn binding() -> CaptureBinding {
        CaptureBinding {
            revision: 9,
            binding_id: [8; 16],
            target_id: [9; 16],
        }
    }
    fn id(index: u64) -> LogId {
        LogId {
            term: 8,
            leader_node: 0,
            index,
        }
    }
    fn command(event: u8) -> DecideCreated {
        DecideCreated {
            operation_id: [event; 16],
            object: ObjectIdentity {
                bucket_incarnation: [2; 16],
                key: vec![event],
            },
            expected_head: None,
            prepared: PreparedIdentity {
                version_id: [event; 16],
                preparation_id: [4; 16],
                content_digest: [5; 32],
                content_length: 123,
            },
            binding_revision: 9,
        }
    }
    fn apply(store: &mut Store, index: u64, command: DecideCreated) -> CreatedResult {
        store
            .append_native(&[CaptureRaftEntry {
                log_id: id(index).into(),
                payload: openraft::EntryPayload::Normal(CaptureCommand::try_created(command).expect("command")),
            }])
            .expect("append native");
        store.save_committed(id(index)).expect("commit native");
        store
            .apply_next_native(id(index))
            .expect("apply native")
            .created_result()
            .expect("created response")
            .clone()
    }
    fn fixture() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("directory");
        let mut store = Store::initialize(dir.path().join("source.redb"), identity()).expect("initialize");
        store.pin_capture_binding(binding()).expect("pin");
        store.enable_native_empty().expect("native");
        apply(&mut store, 0, command(9));
        apply(&mut store, 1, command(7));
        (dir, store)
    }
    #[test]
    fn committed_events_page_in_decision_order() {
        let (_dir, store) = fixture();
        let first = store.created_events_page(None, 1).expect("first");
        assert_eq!(
            first.events.iter().map(|e| e.event_id).collect::<Vec<_>>(),
            vec![[9; 16]],
            "SOURCE_PAGE_ORDER_BOUND"
        );
        let next = store.created_events_page(first.next.as_ref(), 1).expect("second");
        assert_eq!(next.events[0].event_id, [7; 16]);
        assert_eq!(next.events[0].captured.binding, binding());
        assert!(
            store
                .created_events_page(next.next.as_ref(), 1)
                .expect("end")
                .events
                .is_empty()
        );
    }
    #[test]
    fn replay_and_rejections_do_not_duplicate_events() {
        let (_dir, mut store) = fixture();
        apply(&mut store, 2, command(9));
        let mut rejected = command(3);
        rejected.object = command(9).object;
        assert!(matches!(apply(&mut store, 3, rejected), CreatedResult::HeadMismatch { .. }));
        let page = store.created_events_page(None, 256).expect("page");
        assert_eq!(page.events.len(), 2, "SOURCE_PAGE_REPLAY_BOUND");
    }
    #[test]
    fn reopen_rebuilds_missing_or_stale_index() {
        let (dir, mut store) = fixture();
        apply(&mut store, 2, command(1));
        let expected = store.created_events_page(None, 256).expect("before");
        let tx = store.write_transaction().expect("remove cache");
        tx.delete_table(EVENT_SEQUENCE).expect("delete cache");
        store.commit(tx).expect("cache deletion");
        drop(store);
        let mut store = Store::open(dir.path().join("source.redb"), identity()).expect("rebuild missing");
        assert_eq!(store.created_events_page(None, 256).expect("rebuilt"), expected);
        let tx = store.write_transaction().expect("stale cache");
        {
            let mut index = tx.open_table(EVENT_SEQUENCE).expect("cache");
            index.remove(1).expect("omit middle item");
            index.insert(200, b"bad".as_slice()).expect("same-count stale item");
        }
        store.commit(tx).expect("cache damage");
        drop(store);
        let store = Store::open(dir.path().join("source.redb"), identity()).expect("rebuild stale");
        assert_eq!(store.created_events_page(None, 256).expect("rebuilt stale"), expected);
    }
    #[test]
    fn cursor_binds_cluster_term_leader_and_event() {
        let (_dir, store) = fixture();
        let cursor = store.created_events_page(None, 1).expect("page").next.expect("cursor");
        let mut bad = Vec::new();
        let mut c = cursor.clone();
        c.cluster = [6; 16];
        bad.push(c);
        let mut c = cursor.clone();
        c.term += 1;
        bad.push(c);
        let mut c = cursor.clone();
        c.leader += 1;
        bad.push(c);
        let mut c = cursor.clone();
        c.event = [7; 16];
        bad.push(c);
        let mut c = cursor.clone();
        c.revision += 1;
        bad.push(c);
        let mut c = cursor.clone();
        c.binding_id = [6; 16];
        bad.push(c);
        let mut c = cursor.clone();
        c.target_id = [6; 16];
        bad.push(c);
        for cursor in bad {
            assert_eq!(
                store.created_events_page(Some(&cursor), 1).err().map(|e| e.code()),
                Some("identity_mismatch"),
                "SOURCE_PAGE_CURSOR_BOUND"
            );
        }
        let encoded = serde_json::to_vec(&cursor).expect("cursor bytes");
        let decoded: EventCursor = serde_json::from_slice(&encoded).expect("cursor decode");
        assert_eq!(store.created_events_page(Some(&decoded), 1).expect("resume").events[0].event_id, [7; 16]);
    }
    #[test]
    fn page_is_bounded_and_new_lower_uuid_is_not_lost() {
        let (_dir, mut store) = fixture();
        let page = store.created_events_page(None, 1).expect("one");
        assert_eq!(page.events.len(), 1);
        apply(&mut store, 2, command(1));
        let next = store.created_events_page(page.next.as_ref(), 256).expect("later");
        assert_eq!(next.events.iter().map(|e| e.event_id).collect::<Vec<_>>(), vec![[7; 16], [1; 16]]);
        for limit in [0, MAX_EVENT_PAGE + 1, usize::MAX] {
            assert_eq!(store.created_events_page(None, limit).err().map(|e| e.code()), Some("invalid_event_page"));
        }
        store.writable = false;
        assert_eq!(store.created_events_page(None, 1).err().map(|e| e.code()), Some("write_disabled"));
        store.writable = true;
        let mut max = page.next.expect("cursor");
        max.index = u64::MAX;
        assert_eq!(
            store.created_events_page(Some(&max), 1).err().map(|e| e.code()),
            Some("identity_mismatch")
        );
    }
    struct Staging<'a>(&'a WriteTransaction);
    impl snapshot::StagingSink for Staging<'_> {
        fn row(&mut self, section: snapshot::Section, key: &[u8], value: &[u8]) -> io::Result<()> {
            let table = match section {
                snapshot::Section::Heads => application::HEADS,
                snapshot::Section::Versions => application::VERSIONS,
                snapshot::Section::Events => application::EVENTS,
                snapshot::Section::Operations => application::OPERATIONS,
            };
            self.0
                .open_table(table)
                .map_err(io::Error::other)?
                .insert(key, value)
                .map_err(io::Error::other)?;
            Ok(())
        }
    }
    fn limits() -> snapshot::Limits {
        snapshot::Limits {
            total_bytes: 1_048_576,
            membership_bytes: 4096,
            key_bytes: 4096,
            value_bytes: 8192,
            records: 1024,
        }
    }
    async fn artifact(store: Store) -> (snapshot::CapabilityArtifact<Vec<u8>>, EventPage) {
        let writer = NativeLogWriter::new(store);
        let before = writer.created_events_page(None, 256).await.expect("async page");
        let artifact = writer
            .snapshot_view()
            .await
            .expect("snapshot view")
            .stream_capability_artifact(Vec::new(), limits())
            .expect("capability export");
        (artifact, before)
    }
    fn stage(artifact: snapshot::CapabilityArtifact<Vec<u8>>, damage: bool) -> Result<(tempfile::TempDir, Store), StoreError> {
        let dir = tempfile::tempdir().expect("stage directory");
        let database = redb::Database::create(dir.path().join("stage.redb")).expect("isolated database");
        let tx = database.begin_write()?;
        let header = snapshot::decode_capability_artifact(
            snapshot::CapabilityArtifact {
                manifest: artifact.manifest,
                payload: std::io::Cursor::new(artifact.payload),
            },
            &mut Staging(&tx),
            limits(),
        )
        .expect("real capability decoder");
        if damage {
            let mut events = tx.open_table(application::EVENTS)?;
            let value = events.get([9; 16].as_slice())?.expect("event").value().to_vec();
            events.insert([6; 16].as_slice(), value.as_slice())?;
            events.remove([9; 16].as_slice())?;
        }
        rebuild_from_authority(&tx, 6, &header.binding, header.applied)?;
        {
            let mut meta = tx.open_table(crate::META)?;
            meta.insert("schema", 6u64.to_be_bytes().as_slice())?;
            meta.insert(
                "identity",
                crate::encode_identity(NodeIdentity {
                    cluster_id: header.cluster_id,
                    ..identity()
                })
                .as_slice(),
            )?;
            meta.insert("capture_binding", crate::created::binding_bytes(&header.binding)?.as_slice())?;
            let mut applied = vec![1];
            applied.extend(encode_id(header.applied.expect("cut")));
            meta.insert("last_applied", applied.as_slice())?;
        }
        tx.commit().expect("stage commit");
        // A test-only staging handle, not a production installer or authority grant.
        Ok((
            dir,
            Store {
                database,
                writable: true,
            },
        ))
    }
    #[tokio::test]
    async fn snapshot_authority_rebuild_matches_source() {
        let (_dir, mut store) = fixture();
        apply(&mut store, 2, command(9));
        let mut rejected = command(3);
        rejected.object = command(9).object;
        apply(&mut store, 3, rejected);
        store.promote_created_provenance().expect("schema6");
        let (artifact, before) = artifact(store).await;
        let (_stage, restored) = stage(artifact, false).expect("staged authority index");
        assert_eq!(
            restored.created_events_page(None, 256).expect("restored page"),
            before,
            "SOURCE_PAGE_SNAPSHOT_BOUND"
        );
        let first = restored.created_events_page(None, 1).expect("first");
        assert_eq!(
            restored.created_events_page(first.next.as_ref(), 1).expect("resume").events[0],
            before.events[1]
        );
    }
    #[tokio::test]
    async fn snapshot_event_operation_mismatch_rejected() {
        let (_dir, mut store) = fixture();
        store.promote_created_provenance().expect("schema6");
        let (artifact, _) = artifact(store).await;
        assert_eq!(
            stage(artifact, true).err().map(|e| e.code()),
            Some("corrupt_record"),
            "SOURCE_PAGE_SNAPSHOT_BOUND"
        );
    }
}
