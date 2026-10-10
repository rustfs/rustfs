use crate::{
    LOGS, LogId, META, Store, StoreError, committed::validate_committed, created::*, decode_log, encode_id, encode_log,
    next_index, number,
};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::BTreeMap;

pub(crate) const HEADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("heads");
pub(crate) const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
pub(crate) const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("events");
pub(crate) const OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operations");
pub(crate) const APPLY_RESULTS: TableDefinition<u64, &[u8]> = TableDefinition::new("apply_results");

fn version_key(object: &ObjectIdentity, version: [u8; 16]) -> Result<Vec<u8>, StoreError> {
    let mut b = object_bytes(object)?;
    b.extend(version);
    Ok(b)
}
fn decode_version(b: &[u8]) -> Result<CreatedVersion, StoreError> {
    let mut d = Decoder::new(b);
    let v = CreatedVersion {
        prepared: d.prepared()?,
        decision_log_id: d.id()?,
        binding_revision: d.u64()?,
    };
    d.finish()?;
    Ok(v)
}
fn decode_event(b: &[u8]) -> Result<CreatedEvent, StoreError> {
    let mut d = Decoder::new(b);
    let e = CreatedEvent {
        object: d.object()?,
        prepared: d.prepared()?,
        decision_log_id: d.id()?,
        binding: d.binding()?,
    };
    d.finish()?;
    Ok(e)
}
fn decode_operation(b: &[u8]) -> Result<CreatedOperation, StoreError> {
    let mut d = Decoder::new(b);
    let len = usize::try_from(d.u64()?).map_err(|_| StoreError::CorruptRecord)?;
    let command = b
        .get(8..8usize.checked_add(len).ok_or(StoreError::CorruptRecord)?)
        .ok_or(StoreError::CorruptRecord)?;
    DecideCreated::decode(command)?;
    let mut d = Decoder::new(&b[8 + len..]);
    let result = d.result()?;
    d.finish()?;
    Ok(CreatedOperation {
        canonical_command: command.to_vec(),
        result,
    })
}
fn decode_apply(b: &[u8]) -> Result<CreatedApplyResult, StoreError> {
    let mut d = Decoder::new(b);
    let r = CreatedApplyResult {
        log_id: d.id()?,
        result: d.result()?,
    };
    d.finish()?;
    Ok(r)
}
fn read_binding(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<CaptureBinding, StoreError> {
    let b = meta.get("capture_binding")?.ok_or(StoreError::BindingNotPinned)?;
    let mut d = Decoder::new(b.value());
    let binding = d.binding()?;
    d.finish()?;
    Ok(binding)
}
fn read_applied(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<Option<LogId>, StoreError> {
    let b = meta.get("last_applied")?.ok_or(StoreError::CorruptRecord)?;
    let mut d = Decoder::new(b.value());
    let id = match d.take::<1>()?[0] {
        0 => None,
        1 => Some(d.id()?),
        _ => return Err(StoreError::CorruptRecord),
    };
    d.finish()?;
    Ok(id)
}
fn required<T>(value: Option<T>) -> Result<T, StoreError> {
    value.ok_or(StoreError::CorruptRecord)
}
fn encode_native_entry(entry: &crate::CaptureRaftEntry) -> Result<Vec<u8>, StoreError> {
    let id: LogId = entry.log_id.into();
    let (kind, body) = match &entry.payload {
        openraft::EntryPayload::Blank => (0, vec![]),
        openraft::EntryPayload::Normal(command) => (1, command.created().encode()?),
        openraft::EntryPayload::Membership(membership) => {
            let stored = openraft::StoredMembership::new(Some(entry.log_id), membership.clone());
            let checked = crate::CaptureMembership::try_from(&stored)?;
            (2, checked.encode()?)
        }
    };
    let mut payload = b"CENR\x01".to_vec();
    payload.push(kind);
    payload.extend(
        u64::try_from(body.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes(),
    );
    payload.extend(body);
    encode_log(&crate::LogEntry { id, payload })
}

pub(crate) fn decode_native_entry(entry: crate::LogEntry) -> Result<crate::CaptureRaftEntry, StoreError> {
    let bytes = &entry.payload;
    if bytes.len() < 14 || &bytes[..5] != b"CENR\x01" {
        return Err(StoreError::CorruptRecord);
    }
    let length = usize::try_from(number(&bytes[6..14])?).map_err(|_| StoreError::CorruptRecord)?;
    let body = &bytes[14..];
    if length != body.len() {
        return Err(StoreError::CorruptRecord);
    }
    let payload = match bytes[5] {
        0 if body.is_empty() => openraft::EntryPayload::Blank,
        1 => openraft::EntryPayload::Normal(crate::CaptureCommand::try_created(DecideCreated::decode(body)?)?),
        2 => {
            let membership = crate::CaptureMembership::decode(body).map_err(|_| StoreError::CorruptRecord)?;
            if *membership.log_id() != Some(entry.id) {
                return Err(StoreError::CorruptRecord);
            }
            let stored = openraft::StoredMembership::try_from(&membership).map_err(|_| StoreError::CorruptRecord)?;
            openraft::EntryPayload::Membership(stored.membership().clone())
        }
        _ => return Err(StoreError::CorruptRecord),
    };
    Ok(crate::CaptureRaftEntry {
        log_id: entry.id.into(),
        payload,
    })
}

pub(crate) fn native_application_state(tx: &ReadTransaction) -> Result<(Option<LogId>, crate::CaptureMembership), StoreError> {
    let meta = tx.open_table(META)?;
    if number(required(meta.get("schema")?)?.value())? != 4 {
        return Err(StoreError::UnsupportedSchema);
    }
    let last = read_applied(&meta)?;
    let membership = crate::CaptureMembership::decode(required(meta.get("native_membership")?)?.value())?;
    if last.is_some() || membership.log_id().is_some() || !membership.configs().is_empty() || !membership.nodes().is_empty() {
        return Err(StoreError::CorruptRecord);
    }
    Ok((last, membership))
}

impl Store {
    /// Explicitly opt an empty, bound store into native entry persistence.
    pub fn enable_native_empty(&mut self) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 3 | 4) {
                return Err(StoreError::UnsupportedSchema);
            }
            read_binding(&meta)?;
            if schema == 4 {
                let membership = crate::CaptureMembership::decode(required(meta.get("native_membership")?)?.value())?;
                if read_applied(&meta)?.is_some()
                    || membership.log_id().is_some()
                    || !membership.configs().is_empty()
                    || !membership.nodes().is_empty()
                {
                    return Err(StoreError::CorruptRecord);
                }
                return Ok(());
            }
            if read_applied(&meta)?.is_some() || !tx.open_table(LOGS)?.is_empty()? {
                return Err(StoreError::UnsupportedSchema);
            }
            for table in [HEADS, VERSIONS, EVENTS, OPERATIONS] {
                if !tx.open_table(table)?.is_empty()? {
                    return Err(StoreError::UnsupportedSchema);
                }
            }
            if !tx.open_table(APPLY_RESULTS)?.is_empty()? || meta.get("committed")?.is_some() || meta.get("tail")?.is_some() {
                return Err(StoreError::UnsupportedSchema);
            }
            if meta.get("native_membership")?.is_some() {
                return Err(StoreError::CorruptRecord);
            }
            let membership = crate::CaptureMembership::new(None, vec![], BTreeMap::new())?;
            meta.insert("native_membership", membership.encode()?.as_slice())?;
            meta.insert("schema", 4u64.to_be_bytes().as_slice())?;
        }
        self.commit(tx)
    }
    pub fn append_native(&mut self, entries: &[crate::CaptureRaftEntry]) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            if number(required(meta.get("schema")?)?.value())? != 4 {
                return Err(StoreError::UnsupportedSchema);
            }
            if entries.is_empty() {
                return Ok(());
            }
            let tail = meta.get("tail")?.map(|value| crate::decode_id(value.value())).transpose()?;
            let mut next = next_index(tail.map(|id| id.index))?;
            let mut encoded = Vec::with_capacity(entries.len());
            for (position, entry) in entries.iter().enumerate() {
                if entry.log_id.index != next {
                    return Err(StoreError::InvalidLogSequence);
                }
                encoded.push(encode_native_entry(entry)?);
                if position + 1 < entries.len() {
                    next = next_index(Some(next))?;
                }
            }
            let mut logs = tx.open_table(LOGS)?;
            for (entry, bytes) in entries.iter().zip(&encoded) {
                logs.insert(entry.log_id.index, bytes.as_slice())?;
            }
            let last = entries.last().ok_or(StoreError::InvalidLogSequence)?;
            meta.insert("tail", encode_id(last.log_id.into()).as_slice())?;
        }
        self.commit(tx)
    }
    pub fn read_native_entry(&self, index: u64) -> Result<Option<crate::CaptureRaftEntry>, StoreError> {
        let tx = self.database.begin_read()?;
        if number(required(tx.open_table(META)?.get("schema")?)?.value())? != 4 {
            return Err(StoreError::UnsupportedSchema);
        }
        tx.open_table(LOGS)?
            .get(index)?
            .map(|value| {
                let entry = decode_log(value.value())?;
                if entry.id.index != index {
                    return Err(StoreError::CorruptRecord);
                }
                decode_native_entry(entry)
            })
            .transpose()
    }
    pub fn read_native_entries(&self, start: u64, end: u64) -> Result<Vec<crate::CaptureRaftEntry>, StoreError> {
        let tx = self.database.begin_read()?;
        if number(required(tx.open_table(META)?.get("schema")?)?.value())? != 4 {
            return Err(StoreError::UnsupportedSchema);
        }
        if start > end {
            return Err(StoreError::InvalidLogSequence);
        }
        let logs = tx.open_table(LOGS)?;
        let mut entries = Vec::new();
        for row in logs.range(start..end)? {
            let (key, value) = row?;
            let entry = decode_log(value.value())?;
            if entry.id.index != key.value() {
                return Err(StoreError::CorruptRecord);
            }
            entries.push(decode_native_entry(entry)?);
        }
        Ok(entries)
    }
    pub fn apply_next_native(&mut self, _id: LogId) -> Result<crate::CaptureResponse, StoreError> {
        let _tx = self.write_transaction()?;
        Err(StoreError::UnsupportedSchema)
    }
    pub fn native_apply_response(&self, _index: u64) -> Result<Option<crate::CaptureResponse>, StoreError> {
        Err(StoreError::UnsupportedSchema)
    }
    pub fn native_application_state(&self) -> Result<(Option<LogId>, crate::CaptureMembership), StoreError> {
        native_application_state(&self.database.begin_read()?)
    }
    /// Bootstrap the immutable capture policy. The caller must be the trusted
    /// deployment provisioning owner. This crate does not authenticate callers;
    /// do not expose this public method through an ordinary object request API.
    /// Prepared identities assert identity only, not physical data durability.
    pub fn pin_capture_binding(&mut self, binding: CaptureBinding) -> Result<(), StoreError> {
        let encoded = binding_bytes(&binding)?;
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            if matches!(number(required(meta.get("schema")?)?.value())?, 3 | 4) {
                if read_binding(&meta)? == binding {
                    return Ok(());
                }
                return Err(StoreError::BindingConflict);
            }
            if meta.get("capture_binding")?.is_some() || meta.get("last_applied")?.is_some() {
                return Err(StoreError::CorruptRecord);
            }
            for table in [HEADS, VERSIONS, EVENTS, OPERATIONS] {
                if !tx.open_table(table)?.is_empty()? {
                    return Err(StoreError::CorruptRecord);
                }
            }
            if !tx.open_table(APPLY_RESULTS)?.is_empty()? {
                return Err(StoreError::CorruptRecord);
            }
            meta.insert("capture_binding", encoded.as_slice())?;
            meta.insert("last_applied", &[0][..])?;
            meta.insert("schema", 3u64.to_be_bytes().as_slice())?;
        }
        self.commit(tx)
    }
    /// Apply the next exact persisted committed Created command atomically.
    pub fn apply_next_created(&mut self, id: LogId) -> Result<CreatedResult, StoreError> {
        let tx = self.write_transaction()?;
        let result;
        {
            let mut meta = tx.open_table(META)?;
            if number(required(meta.get("schema")?)?.value())? != 3 {
                return Err(StoreError::UnsupportedSchema);
            }
            let binding = read_binding(&meta)?;
            let logs = tx.open_table(LOGS)?;
            let committed = validate_committed(&meta, &logs)?.ok_or(StoreError::InvalidCommittedLog)?;
            let entry = decode_log(required(logs.get(id.index)?)?.value())?;
            if entry.id != id || id.index > committed.index {
                return Err(StoreError::InvalidCommittedLog);
            }
            let last = read_applied(&meta)?;
            let mut results = tx.open_table(APPLY_RESULTS)?;
            if last.is_some_and(|last| id.index <= last.index) {
                let saved = decode_apply(required(results.get(id.index)?)?.value())?;
                if saved.log_id != id {
                    return Err(StoreError::InvalidApplySequence);
                }
                return Ok(saved.result);
            }
            if id.index != next_index(last.map(|last| last.index))? {
                return Err(StoreError::InvalidApplySequence);
            }
            let command = DecideCreated::decode(&entry.payload)?;
            let mut operations = tx.open_table(OPERATIONS)?;
            let existing = operations
                .get(command.operation_id.as_slice())?
                .map(|v| decode_operation(v.value()))
                .transpose()?;
            result = if let Some(existing) = existing {
                if existing.canonical_command == entry.payload {
                    existing.result
                } else {
                    CreatedResult::OperationConflict
                }
            } else {
                let mut heads = tx.open_table(HEADS)?;
                let mut versions = tx.open_table(VERSIONS)?;
                let object = object_bytes(&command.object)?;
                let key = version_key(&command.object, command.prepared.version_id)?;
                let actual = heads
                    .get(object.as_slice())?
                    .map(|v| {
                        let mut d = Decoder::new(v.value());
                        let id = d.uuid()?;
                        d.finish()?;
                        Ok::<_, StoreError>(id)
                    })
                    .transpose()?;
                let outcome = if command.binding_revision != binding.revision {
                    CreatedResult::BindingMismatch
                } else if actual != command.expected_head {
                    CreatedResult::HeadMismatch { actual_head: actual }
                } else if versions.get(key.as_slice())?.is_some() {
                    CreatedResult::VersionConflict
                } else {
                    let mut version = Vec::new();
                    put_prepared(&mut version, &command.prepared);
                    version.extend(encode_id(id));
                    version.extend(binding.revision.to_be_bytes());
                    versions.insert(key.as_slice(), version.as_slice())?;
                    let mut event = object;
                    put_prepared(&mut event, &command.prepared);
                    event.extend(encode_id(id));
                    event.extend(binding_bytes(&binding)?);
                    tx.open_table(EVENTS)?
                        .insert(command.operation_id.as_slice(), event.as_slice())?;
                    heads.insert(object_bytes(&command.object)?.as_slice(), command.prepared.version_id.as_slice())?;
                    CreatedResult::Created {
                        version_id: command.prepared.version_id,
                        event_id: command.operation_id,
                        decision_log_id: id,
                    }
                };
                let mut op = u64::try_from(entry.payload.len())
                    .map_err(|_| StoreError::CorruptRecord)?
                    .to_be_bytes()
                    .to_vec();
                op.extend(&entry.payload);
                op.extend(result_bytes(&outcome));
                operations.insert(command.operation_id.as_slice(), op.as_slice())?;
                outcome
            };
            let mut applied = encode_id(id);
            applied.extend(result_bytes(&result));
            results.insert(id.index, applied.as_slice())?;
            let mut last = vec![1];
            last.extend(encode_id(id));
            meta.insert("last_applied", last.as_slice())?;
        }
        self.commit(tx)?;
        Ok(result)
    }
    pub fn capture_binding(&self) -> Result<Option<CaptureBinding>, StoreError> {
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(META)?;
        if !matches!(number(required(meta.get("schema")?)?.value())?, 3 | 4) {
            return Ok(None);
        }
        Ok(Some(read_binding(&meta)?))
    }
    pub fn last_applied(&self) -> Result<Option<LogId>, StoreError> {
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(META)?;
        if !matches!(number(required(meta.get("schema")?)?.value())?, 3 | 4) {
            return Ok(None);
        }
        read_applied(&meta)
    }
    pub fn created_head(&self, object: &ObjectIdentity) -> Result<Option<[u8; 16]>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(HEADS)?;
        t.get(object_bytes(object)?.as_slice())?
            .map(|v| {
                let mut d = Decoder::new(v.value());
                let id = d.uuid()?;
                d.finish()?;
                Ok(id)
            })
            .transpose()
    }
    pub fn created_version(&self, object: &ObjectIdentity, version: [u8; 16]) -> Result<Option<CreatedVersion>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(VERSIONS)?;
        t.get(version_key(object, version)?.as_slice())?
            .map(|v| decode_version(v.value()))
            .transpose()
    }
    pub fn created_event(&self, operation: [u8; 16]) -> Result<Option<CreatedEvent>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(EVENTS)?;
        t.get(operation.as_slice())?.map(|v| decode_event(v.value())).transpose()
    }
    pub fn created_operation(&self, operation: [u8; 16]) -> Result<Option<CreatedOperation>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(OPERATIONS)?;
        t.get(operation.as_slice())?.map(|v| decode_operation(v.value())).transpose()
    }
    pub fn created_apply_result(&self, index: u64) -> Result<Option<CreatedApplyResult>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(APPLY_RESULTS)?;
        t.get(index)?.map(|v| decode_apply(v.value())).transpose()
    }
}

pub(crate) fn validate_application(tx: &ReadTransaction) -> Result<(), StoreError> {
    let meta = tx.open_table(META)?;
    let schema = number(required(meta.get("schema")?)?.value())?;
    if schema == 4 {
        read_binding(&meta)?;
        native_application_state(tx)?;
        for table in [HEADS, VERSIONS, EVENTS, OPERATIONS] {
            if !tx.open_table(table)?.is_empty()? {
                return Err(StoreError::CorruptRecord);
            }
        }
        if !tx.open_table(APPLY_RESULTS)?.is_empty()? {
            return Err(StoreError::CorruptRecord);
        }
        return Ok(());
    }
    if schema != 3 {
        return Ok(());
    }
    let binding = read_binding(&meta).map_err(|error| match error {
        StoreError::BindingNotPinned => StoreError::CorruptRecord,
        other => other,
    })?;
    let last = read_applied(&meta)?;
    let logs = tx.open_table(LOGS)?;
    let committed = validate_committed(&meta, &logs)?;
    let heads = tx.open_table(HEADS)?;
    let versions = tx.open_table(VERSIONS)?;
    let events = tx.open_table(EVENTS)?;
    let operations = tx.open_table(OPERATIONS)?;
    let results = tx.open_table(APPLY_RESULTS)?;
    if last.is_some_and(|last| committed.is_none_or(|c| last.index > c.index)) {
        return Err(StoreError::CorruptRecord);
    }
    let mut next = Some(0);
    let mut observed = None;
    let mut first_operations = BTreeMap::new();
    for row in results.iter()? {
        let (k, v) = row?;
        let r = decode_apply(v.value())?;
        if next != Some(k.value()) || r.log_id.index != k.value() {
            return Err(StoreError::CorruptRecord);
        }
        let entry = decode_log(required(logs.get(k.value())?)?.value())?;
        if entry.id != r.log_id {
            return Err(StoreError::CorruptRecord);
        }
        let c = DecideCreated::decode(&entry.payload)?;
        let op = decode_operation(required(operations.get(c.operation_id.as_slice())?)?.value())?;
        let canonical_matches = op.canonical_command == entry.payload;
        let expected = if canonical_matches {
            op.result
        } else {
            CreatedResult::OperationConflict
        };
        if expected != r.result {
            return Err(StoreError::CorruptRecord);
        }
        if canonical_matches {
            first_operations.entry(c.operation_id).or_insert(r.log_id);
        }
        observed = Some(r.log_id);
        next = k.value().checked_add(1)
    }
    if observed != last {
        return Err(StoreError::CorruptRecord);
    }
    for row in operations.iter()? {
        let (k, v) = row?;
        let op = decode_operation(v.value())?;
        let c = DecideCreated::decode(&op.canonical_command)?;
        if k.value() != c.operation_id {
            return Err(StoreError::CorruptRecord);
        }
        let first = required(first_operations.get(&c.operation_id))?;
        if let CreatedResult::Created {
            version_id,
            event_id,
            decision_log_id,
        } = op.result
        {
            if version_id != c.prepared.version_id
                || event_id != c.operation_id
                || c.binding_revision != binding.revision
                || decision_log_id != *first
            {
                return Err(StoreError::CorruptRecord);
            }
            let event = decode_event(required(events.get(event_id.as_slice())?)?.value())?;
            if event
                != (CreatedEvent {
                    object: c.object.clone(),
                    prepared: c.prepared.clone(),
                    decision_log_id,
                    binding: binding.clone(),
                })
            {
                return Err(StoreError::CorruptRecord);
            }
        } else if events.get(c.operation_id.as_slice())?.is_some() {
            return Err(StoreError::CorruptRecord);
        }
    }
    for row in events.iter()? {
        let (k, v) = row?;
        let e = decode_event(v.value())?;
        if e.binding != binding {
            return Err(StoreError::CorruptRecord);
        }
        let op = decode_operation(required(operations.get(k.value())?)?.value())?;
        let c = DecideCreated::decode(&op.canonical_command)?;
        if c.object != e.object
            || c.prepared != e.prepared
            || op.result
                != (CreatedResult::Created {
                    version_id: e.prepared.version_id,
                    event_id: c.operation_id,
                    decision_log_id: e.decision_log_id,
                })
        {
            return Err(StoreError::CorruptRecord);
        }
        let version =
            decode_version(required(versions.get(version_key(&e.object, e.prepared.version_id)?.as_slice())?)?.value())?;
        if version
            != (CreatedVersion {
                prepared: e.prepared.clone(),
                decision_log_id: e.decision_log_id,
                binding_revision: binding.revision,
            })
        {
            return Err(StoreError::CorruptRecord);
        }
    }
    for row in versions.iter()? {
        let (k, v) = row?;
        let version = decode_version(v.value())?;
        let mut d = Decoder::new(k.value());
        let object = d.object()?;
        let id = d.uuid()?;
        d.finish()?;
        if id != version.prepared.version_id || version.binding_revision != binding.revision {
            return Err(StoreError::CorruptRecord);
        }
        let entry = decode_log(required(logs.get(version.decision_log_id.index)?)?.value())?;
        let c = DecideCreated::decode(&entry.payload)?;
        if entry.id != version.decision_log_id
            || c.object != object
            || c.prepared != version.prepared
            || events.get(c.operation_id.as_slice())?.is_none()
        {
            return Err(StoreError::CorruptRecord);
        }
        let head = required(heads.get(object_bytes(&object)?.as_slice())?)?;
        let mut d = Decoder::new(head.value());
        let head_id = d.uuid()?;
        d.finish()?;
        let current = decode_version(required(versions.get(version_key(&object, head_id)?.as_slice())?)?.value())?;
        if current.decision_log_id.index < version.decision_log_id.index {
            return Err(StoreError::CorruptRecord);
        }
    }
    for row in heads.iter()? {
        let (k, v) = row?;
        let mut d = Decoder::new(k.value());
        let object = d.object()?;
        d.finish()?;
        let mut d = Decoder::new(v.value());
        let id = d.uuid()?;
        d.finish()?;
        decode_version(required(versions.get(version_key(&object, id)?.as_slice())?)?.value())?;
    }
    Ok(())
}
