fn read_native_range_in<R: std::ops::RangeBounds<u64>>(
    tx: &redb::ReadTransaction,
    range: R,
) -> Result<Vec<crate::CaptureRaftEntry>, StoreError> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let logs = tx.open_table(LOGS)?;
    let start = match range.start_bound() {
        Included(value) => *value,
        Excluded(value) => match value.checked_add(1) {
            Some(value) => value,
            None => return Ok(Vec::new()),
        },
        Unbounded => 0,
    };
    let end = match range.end_bound() {
        Included(value) => *value,
        Excluded(value) => match value.checked_sub(1) {
            Some(value) => value,
            None => return Ok(Vec::new()),
        },
        Unbounded => u64::MAX,
    };
    if start > end {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for row in logs.range(start..=end)? {
        let (key, value) = row?;
        let entry = decode_log(value.value())?;
        if entry.id.index != key.value() {
            return Err(StoreError::CorruptRecord);
        }
        entries.push(decode_native_entry(entry)?);
    }
    Ok(entries)
}

use crate::{
    LOGS, LogId, META, Store, StoreError, committed::validate_committed, created::*, decode_log, encode_id, encode_log,
    next_index, number,
};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle};
use std::collections::BTreeMap;

pub(crate) const HEADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("heads");
pub(crate) const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
pub(crate) const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("events");
pub(crate) const OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operations");
pub(crate) const APPLY_RESULTS: TableDefinition<u64, &[u8]> = TableDefinition::new("apply_results");

pub(crate) fn version_key(object: &ObjectIdentity, version: [u8; 16]) -> Result<Vec<u8>, StoreError> {
    let mut b = object_bytes(object)?;
    b.extend(version);
    Ok(b)
}
pub(crate) fn decode_version(b: &[u8]) -> Result<CreatedVersion, StoreError> {
    let mut d = Decoder::new(b);
    let v = CreatedVersion {
        prepared: d.prepared()?,
        decision_log_id: d.id()?,
        binding_revision: d.u64()?,
    };
    d.finish()?;
    Ok(v)
}
pub(crate) fn decode_event(b: &[u8]) -> Result<CreatedEvent, StoreError> {
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
pub(crate) fn decode_provenance(b: &[u8]) -> Result<CreatedOperationProvenance, StoreError> {
    if b.len() > MAX_OBJECT_KEY_LENGTH + 512 || b.get(..5) != Some(b"COPR\x01") {
        return Err(StoreError::CorruptRecord);
    }
    let encoded_id = b.get(5..29).ok_or(StoreError::CorruptRecord)?;
    if encoded_id == [0; 24] {
        return Err(StoreError::CorruptRecord);
    }
    Ok(CreatedOperationProvenance {
        first_decision_log_id: crate::decode_id(encoded_id)?,
        operation: decode_operation(b.get(29..).ok_or(StoreError::CorruptRecord)?)?,
    })
}

pub(crate) fn validate_provenance_encoding(bytes: &[u8]) -> Result<(), StoreError> {
    decode_provenance(bytes).map(|_| ())
}

pub(crate) fn decode_stored_operation(b: &[u8], schema: u64) -> Result<CreatedOperation, StoreError> {
    if matches!(schema, 5 | 6 | 7) {
        Ok(decode_provenance(b)?.operation)
    } else {
        decode_operation(b)
    }
}

pub(crate) fn encode_provenance(first: LogId, operation: &[u8]) -> Result<Vec<u8>, StoreError> {
    let mut bytes = b"COPR\x01".to_vec();
    bytes.extend(encode_id(first));
    bytes.extend(operation);
    decode_provenance(&bytes)?;
    Ok(bytes)
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
pub(crate) fn read_binding(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<CaptureBinding, StoreError> {
    let b = meta.get("capture_binding")?.ok_or(StoreError::BindingNotPinned)?;
    let mut d = Decoder::new(b.value());
    let binding = d.binding()?;
    d.finish()?;
    Ok(binding)
}
pub(crate) fn read_applied(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<Option<LogId>, StoreError> {
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

pub(crate) fn apply_created(
    tx: &redb::WriteTransaction,
    id: LogId,
    command: &DecideCreated,
    canonical: &[u8],
    binding: &CaptureBinding,
    schema: u64,
) -> Result<CreatedResult, StoreError> {
    let mut operations = tx.open_table(OPERATIONS)?;
    let existing = operations
        .get(command.operation_id.as_slice())?
        .map(|v| decode_stored_operation(v.value(), schema))
        .transpose()?;
    let result = if let Some(existing) = existing {
        if existing.canonical_command == canonical {
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
            crate::source_page::insert_event_index(tx, id, command.operation_id)?;
            heads.insert(object_bytes(&command.object)?.as_slice(), command.prepared.version_id.as_slice())?;
            CreatedResult::Created {
                version_id: command.prepared.version_id,
                event_id: command.operation_id,
                decision_log_id: id,
            }
        };
        let mut op = u64::try_from(canonical.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes()
            .to_vec();
        op.extend(canonical);
        op.extend(result_bytes(&outcome));
        if matches!(schema, 5 | 6 | 7) {
            op = encode_provenance(id, &op)?;
        }
        operations.insert(command.operation_id.as_slice(), op.as_slice())?;
        outcome
    };
    Ok(result)
}

pub(crate) fn validate_response_kind(
    entry: &crate::CaptureRaftEntry,
    response: &crate::CaptureResponse,
) -> Result<(), StoreError> {
    let kind = match entry.payload {
        openraft::EntryPayload::Blank => crate::CaptureResponseKind::Blank,
        openraft::EntryPayload::Normal(_) => crate::CaptureResponseKind::Created,
        openraft::EntryPayload::Membership(_) => crate::CaptureResponseKind::Membership,
    };
    if response.kind() != kind || *response.log_id() != LogId::from(entry.log_id) {
        return Err(StoreError::CorruptRecord);
    }
    Ok(())
}
fn encode_response(response: &crate::CaptureResponse) -> Result<Vec<u8>, StoreError> {
    let (kind, body) = match response.kind() {
        crate::CaptureResponseKind::Blank => (0, vec![]),
        crate::CaptureResponseKind::Created => (1, result_bytes(response.created_result().ok_or(StoreError::CorruptRecord)?)),
        crate::CaptureResponseKind::Membership => (2, vec![]),
    };
    let mut bytes = b"CRSP\x01".to_vec();
    bytes.push(kind);
    bytes.extend(encode_id(*response.log_id()));
    bytes.extend(
        u64::try_from(body.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes(),
    );
    bytes.extend(body);
    Ok(bytes)
}
pub(crate) fn decode_response(bytes: &[u8]) -> Result<crate::CaptureResponse, StoreError> {
    if bytes.len() < 38
        || &bytes[..5] != b"CRSP\x01"
        || number(&bytes[30..38])? != u64::try_from(bytes.len() - 38).map_err(|_| StoreError::CorruptRecord)?
    {
        return Err(StoreError::CorruptRecord);
    }
    let id = crate::decode_id(&bytes[6..30])?;
    match bytes[5] {
        0 if bytes.len() == 38 => crate::CaptureResponse::blank(id),
        1 => {
            let mut body = encode_id(id);
            body.extend(&bytes[38..]);
            crate::CaptureResponse::created(decode_apply(&body)?)
        }
        2 if bytes.len() == 38 => crate::CaptureResponse::membership(id),
        _ => Err(StoreError::CorruptRecord),
    }
}
fn entry_command(entry: &crate::LogEntry, native: bool) -> Result<DecideCreated, StoreError> {
    if native {
        match decode_native_entry(entry.clone())?.payload {
            openraft::EntryPayload::Normal(command) => Ok(command.created().clone()),
            _ => Err(StoreError::CorruptRecord),
        }
    } else {
        DecideCreated::decode(&entry.payload)
    }
}

pub(crate) fn native_application_state(tx: &ReadTransaction) -> Result<(Option<LogId>, crate::CaptureMembership), StoreError> {
    native_application_metadata(&tx.open_table(META)?)
}

fn native_application_metadata(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(Option<LogId>, crate::CaptureMembership), StoreError> {
    let schema = number(required(meta.get("schema")?)?.value())?;
    if !matches!(schema, 4 | 6 | 7) {
        return Err(StoreError::UnsupportedSchema);
    }
    let last = read_applied(meta)?;
    let membership_bytes = required(meta.get("native_membership")?)?;
    if schema == 7 && membership_bytes.value().len() > 1024 * 1024 {
        return Err(StoreError::CorruptRecord);
    }
    let membership = crate::CaptureMembership::decode(membership_bytes.value()).map_err(|_| StoreError::CorruptRecord)?;
    if membership
        .log_id()
        .is_some_and(|id| last.is_none_or(|last| id.index > last.index))
    {
        return Err(StoreError::CorruptRecord);
    }
    Ok((last, membership))
}

impl Store {
    pub fn migrate_legacy_created_to_native(&mut self) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 3 | 4) {
                return Err(StoreError::UnsupportedSchema);
            }
            let mut logs = tx.open_table(LOGS)?;
            let heads = tx.open_table(HEADS)?;
            let versions = tx.open_table(VERSIONS)?;
            let events = tx.open_table(EVENTS)?;
            let operations = tx.open_table(OPERATIONS)?;
            let mut results = tx.open_table(APPLY_RESULTS)?;
            validate_application_tables(&meta, &logs, &heads, &versions, &events, &operations, &results, false)?;
            if schema == 4 {
                return Ok(());
            }
            if meta.get("native_membership")?.is_some() {
                return Err(StoreError::CorruptRecord);
            }
            // Validate the entire retained suffix before replacing any record.
            let mut native_logs = Vec::new();
            for row in logs.iter()? {
                let (index, bytes) = row?;
                let entry = decode_log(bytes.value())?;
                let command = DecideCreated::decode(&entry.payload)?;
                if entry.id.index != index.value() || command.encode()? != entry.payload {
                    return Err(StoreError::CorruptRecord);
                }
                let native = crate::CaptureRaftEntry {
                    log_id: entry.id.into(),
                    payload: openraft::EntryPayload::Normal(crate::CaptureCommand::try_created(command)?),
                };
                native_logs.push((index.value(), encode_native_entry(&native)?));
            }
            let mut native_results = Vec::new();
            for row in results.iter()? {
                let (index, bytes) = row?;
                let response = crate::CaptureResponse::created(decode_apply(bytes.value())?)?;
                native_results.push((index.value(), encode_response(&response)?));
            }
            for (index, bytes) in native_logs {
                logs.insert(index, bytes.as_slice())?;
            }
            for (index, bytes) in native_results {
                results.insert(index, bytes.as_slice())?;
            }
            let membership = crate::CaptureMembership::new(None, vec![], BTreeMap::new())?;
            meta.insert("native_membership", membership.encode()?.as_slice())?;
            meta.insert("schema", 4u64.to_be_bytes().as_slice())?;
            // Replay native decisions inside the same durable writer boundary.
            validate_application_tables(&meta, &logs, &heads, &versions, &events, &operations, &results, false)?;
        }
        self.commit(tx)
    }

    /// Explicitly opt an empty, bound store into native entry persistence.
    pub fn enable_native_empty(&mut self) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 3..=7) {
                return Err(StoreError::UnsupportedSchema);
            }
            read_binding(&meta)?;
            if matches!(schema, 4 | 6 | 7) {
                let membership = crate::CaptureMembership::decode(required(meta.get("native_membership")?)?.value())
                    .map_err(|_| StoreError::CorruptRecord)?;
                let last = read_applied(&meta)?;
                if membership
                    .log_id()
                    .is_some_and(|id| last.is_none_or(|last| id.index > last.index))
                    || (last.is_none() && (!membership.configs().is_empty() || !membership.nodes().is_empty()))
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
            meta.insert("schema", (if schema == 5 { 6u64 } else { 4u64 }).to_be_bytes().as_slice())?;
        }
        self.commit(tx)
    }
    pub fn append_native(&mut self, entries: &[crate::CaptureRaftEntry]) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 4 | 6 | 7) {
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
                if schema == 7 {
                    if let openraft::EntryPayload::Membership(membership) = &entry.payload {
                        // Reuse framing's size preflight before allocating the membership encoding.
                        let stored = openraft::StoredMembership::new(Some(entry.log_id), membership.clone());
                        let header = crate::snapshot::Header {
                            cluster_id: crate::decode_identity(required(meta.get("identity")?)?.value())?.cluster_id,
                            applied: Some(entry.log_id.into()),
                            membership: crate::CaptureMembership::try_from(&stored)?,
                            binding: read_binding(&meta)?,
                        };
                        crate::snapshot::Encoder::new(
                            std::io::sink(),
                            &header,
                            [0; 4],
                            crate::snapshot::Limits {
                                total_bytes: 1024 * 1024,
                                membership_bytes: 1024 * 1024 - 256,
                                key_bytes: 0,
                                value_bytes: 0,
                                records: 0,
                            },
                        )
                        .map_err(|_| StoreError::ResourceLimit)?;
                    }
                }
                let bytes = encode_native_entry(entry)?;
                if schema == 7 && bytes.len() > 1024 * 1024 {
                    return Err(StoreError::ResourceLimit);
                }
                encoded.push(bytes);
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
        if !matches!(number(required(tx.open_table(META)?.get("schema")?)?.value())?, 4 | 6 | 7) {
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
    /// Read typed native entries within the requested bounds.
    pub fn read_native_range<R: std::ops::RangeBounds<u64>>(&self, range: R) -> Result<Vec<crate::CaptureRaftEntry>, StoreError> {
        let tx = self.database.begin_read()?;
        if !matches!(number(required(tx.open_table(META)?.get("schema")?)?.value())?, 4 | 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        read_native_range_in(&tx, range)
    }
    pub fn read_native_entries(&self, start: u64, end: u64) -> Result<Vec<crate::CaptureRaftEntry>, StoreError> {
        let tx = self.database.begin_read()?;
        if !matches!(number(required(tx.open_table(META)?.get("schema")?)?.value())?, 4 | 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        if start > end {
            return Err(StoreError::InvalidLogSequence);
        }
        read_native_range_in(&tx, start..end)
    }

    pub fn apply_next_native(&mut self, id: LogId) -> Result<crate::CaptureResponse, StoreError> {
        let tx = self.write_transaction()?;
        let response;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 4 | 6 | 7) {
                return Err(StoreError::UnsupportedSchema);
            }
            let binding = read_binding(&meta)?;
            let logs = tx.open_table(LOGS)?;
            let committed = validate_committed(&meta, &logs)?.ok_or(StoreError::InvalidCommittedLog)?;
            let entry = decode_log(required(logs.get(id.index)?)?.value())?;
            if entry.id != id || id.index > committed.index {
                return Err(StoreError::InvalidCommittedLog);
            }
            let native = decode_native_entry(entry)?;
            let last = read_applied(&meta)?;
            let mut results = tx.open_table(APPLY_RESULTS)?;
            if last.is_some_and(|last| id.index <= last.index) {
                let saved = decode_response(required(results.get(id.index)?)?.value())?;
                if *saved.log_id() != id {
                    return Err(StoreError::InvalidApplySequence);
                }
                validate_response_kind(&native, &saved)?;
                return Ok(saved);
            }
            if id.index != next_index(last.map(|last| last.index))? {
                return Err(StoreError::InvalidApplySequence);
            }
            response = match native.payload {
                openraft::EntryPayload::Blank => crate::CaptureResponse::blank(id)?,
                openraft::EntryPayload::Normal(command) => {
                    let canonical = command.created().encode()?;
                    let result = apply_created(&tx, id, command.created(), &canonical, &binding, schema)?;
                    crate::CaptureResponse::created(CreatedApplyResult { log_id: id, result })?
                }
                openraft::EntryPayload::Membership(membership) => {
                    let stored = openraft::StoredMembership::new(Some(id.into()), membership);
                    let checked = crate::CaptureMembership::try_from(&stored)?;
                    meta.insert("native_membership", checked.encode()?.as_slice())?;
                    crate::CaptureResponse::membership(id)?
                }
            };
            results.insert(id.index, encode_response(&response)?.as_slice())?;
            let mut last = vec![1];
            last.extend(encode_id(id));
            meta.insert("last_applied", last.as_slice())?;
        }
        self.commit(tx)?;
        Ok(response)
    }
    pub fn native_apply_response(&self, index: u64) -> Result<Option<crate::CaptureResponse>, StoreError> {
        let tx = self.database.begin_read()?;
        if !matches!(number(required(tx.open_table(META)?.get("schema")?)?.value())?, 4 | 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        let results = tx.open_table(APPLY_RESULTS)?;
        results.get(index)?.map(|v| decode_response(v.value())).transpose()
    }
    pub fn native_application_state(&self) -> Result<(Option<LogId>, crate::CaptureMembership), StoreError> {
        native_application_state(&self.database.begin_read()?)
    }

    /// Check that a recovered native state supports provenance-backed snapshots.
    /// This does not promote legacy records or change durable metadata.
    pub fn validate_native_provenance_state(&self) -> Result<(), StoreError> {
        let tx = self.database.begin_read()?;
        let schema = number(required(tx.open_table(META)?.get("schema")?)?.value())?;
        if !matches!(schema, 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        native_application_state(&tx)?;
        read_binding(&tx.open_table(META)?)?;
        Ok(())
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
            if matches!(number(required(meta.get("schema")?)?.value())?, 3..=7) {
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
            tx.open_table(crate::source_page::EVENT_SEQUENCE)?.retain(|_, _| false)?;
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
            let schema = number(required(meta.get("schema")?)?.value())?;
            if !matches!(schema, 3 | 5) {
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
            result = apply_created(&tx, id, &command, &entry.payload, &binding, schema)?;
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
        if !matches!(number(required(meta.get("schema")?)?.value())?, 3..=7) {
            return Ok(None);
        }
        Ok(Some(read_binding(&meta)?))
    }
    pub fn last_applied(&self) -> Result<Option<LogId>, StoreError> {
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(META)?;
        if !matches!(number(required(meta.get("schema")?)?.value())?, 3..=7) {
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
    /// Read the persisted first decision, without fabricating legacy provenance.
    pub fn created_operation_provenance(&self, operation: [u8; 16]) -> Result<Option<CreatedOperationProvenance>, StoreError> {
        let tx = self.database.begin_read()?;
        let schema = number(required(tx.open_table(META)?.get("schema")?)?.value())?;
        if !matches!(schema, 5 | 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        tx.open_table(OPERATIONS)?
            .get(operation.as_slice())?
            .map(|value| decode_provenance(value.value()))
            .transpose()
    }

    /// Explicitly promote a fully verified legacy or native Created history.
    pub fn promote_created_provenance(&mut self) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let schema = number(required(meta.get("schema")?)?.value())?;
            if schema == 7 {
                return Ok(());
            }
            if !matches!(schema, 3..=6) {
                return Err(StoreError::UnsupportedSchema);
            }
            let tables = tx
                .list_tables()?
                .map(|table| table.name().to_owned())
                .collect::<std::collections::BTreeSet<_>>();
            if [
                "meta",
                "vote",
                "logs",
                "heads",
                "versions",
                "events",
                "operations",
                "apply_results",
            ]
            .iter()
            .any(|name| !tables.contains(*name))
            {
                return Err(StoreError::CorruptRecord);
            }
            crate::validate_identity(crate::decode_identity(required(meta.get("identity")?)?.value())?)?;
            for row in tx.open_table(crate::VOTE)?.iter()? {
                let (key, value) = row?;
                if key.value() != 0 {
                    return Err(StoreError::CorruptRecord);
                }
                crate::decode_vote(value.value())?;
            }
            let logs = tx.open_table(LOGS)?;
            let heads = tx.open_table(HEADS)?;
            let versions = tx.open_table(VERSIONS)?;
            let events = tx.open_table(EVENTS)?;
            let mut operations = tx.open_table(OPERATIONS)?;
            let results = tx.open_table(APPLY_RESULTS)?;
            let first = validate_application_tables(&meta, &logs, &heads, &versions, &events, &operations, &results, true)?;
            if matches!(schema, 5 | 6 | 7) {
                return Ok(());
            }
            // Keep one encoded row at a time; the replay map contains only first LogIds.
            for (operation, id) in first {
                let original = required(operations.get(operation.as_slice())?)?.value().to_vec();
                operations.insert(operation.as_slice(), encode_provenance(id, &original)?.as_slice())?;
            }
            meta.insert("schema", (if schema == 3 { 5u64 } else { 6u64 }).to_be_bytes().as_slice())?;
        }
        self.commit(tx)
    }

    pub fn created_operation(&self, operation: [u8; 16]) -> Result<Option<CreatedOperation>, StoreError> {
        let tx = self.database.begin_read()?;
        let schema = number(required(tx.open_table(META)?.get("schema")?)?.value())?;
        let t = tx.open_table(OPERATIONS)?;
        t.get(operation.as_slice())?
            .map(|v| decode_stored_operation(v.value(), schema))
            .transpose()
    }
    pub fn created_apply_result(&self, index: u64) -> Result<Option<CreatedApplyResult>, StoreError> {
        let tx = self.database.begin_read()?;
        let t = tx.open_table(APPLY_RESULTS)?;
        if matches!(number(required(tx.open_table(META)?.get("schema")?)?.value())?, 4 | 6 | 7) {
            t.get(index)?
                .map(|v| {
                    decode_response(v.value()).map(|r| {
                        r.created_result().cloned().map(|result| CreatedApplyResult {
                            log_id: *r.log_id(),
                            result,
                        })
                    })
                })
                .transpose()
                .map(Option::flatten)
        } else {
            t.get(index)?.map(|v| decode_apply(v.value())).transpose()
        }
    }
}

pub(crate) fn validate_application(tx: &ReadTransaction) -> Result<(), StoreError> {
    let meta = tx.open_table(META)?;
    if number(required(meta.get("schema")?)?.value())? == 7 {
        return crate::checkpoint_baseline::validate_recovery(tx);
    }
    if !matches!(number(required(meta.get("schema")?)?.value())?, 3..=6) {
        return Ok(());
    }
    validate_application_tables(
        &tx.open_table(META)?,
        &tx.open_table(LOGS)?,
        &tx.open_table(HEADS)?,
        &tx.open_table(VERSIONS)?,
        &tx.open_table(EVENTS)?,
        &tx.open_table(OPERATIONS)?,
        &tx.open_table(APPLY_RESULTS)?,
        false,
    )
    .map(|_| ())
}

fn validate_application_tables(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    logs: &impl ReadableTable<u64, &'static [u8]>,
    heads: &impl ReadableTable<&'static [u8], &'static [u8]>,
    versions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    events: &impl ReadableTable<&'static [u8], &'static [u8]>,
    operations: &impl ReadableTable<&'static [u8], &'static [u8]>,
    results: &impl ReadableTable<u64, &'static [u8]>,
    promoting_provenance: bool,
) -> Result<BTreeMap<[u8; 16], LogId>, StoreError> {
    let schema = number(required(meta.get("schema")?)?.value())?;
    let native = matches!(schema, 4 | 6);
    let full_replay = native || schema == 5 || promoting_provenance;
    if !matches!(schema, 3..=6) {
        return Ok(BTreeMap::new());
    }
    let binding = read_binding(meta).map_err(|error| match error {
        StoreError::BindingNotPinned => StoreError::CorruptRecord,
        other => other,
    })?;
    let last = read_applied(meta)?;
    let committed = validate_committed(meta, logs)?;
    if last.is_some_and(|last| committed.is_none_or(|c| last.index > c.index)) {
        return Err(StoreError::CorruptRecord);
    }
    if !native && meta.get("native_membership")?.is_some() {
        return Err(StoreError::CorruptRecord);
    }
    let mut log_next = Some(0);
    let mut tail = None;
    for row in logs.iter()? {
        let (index, bytes) = row?;
        let entry = decode_log(bytes.value())?;
        if log_next != Some(index.value()) || entry.id.index != index.value() {
            return Err(StoreError::CorruptRecord);
        }
        if native {
            decode_native_entry(entry.clone())?;
        } else if promoting_provenance || schema == 5 {
            let command = DecideCreated::decode(&entry.payload)?;
            if command.encode()? != entry.payload {
                return Err(StoreError::CorruptRecord);
            }
        }
        log_next = index.value().checked_add(1);
        tail = Some(entry.id);
    }
    if meta.get("tail")?.map(|value| crate::decode_id(value.value())).transpose()? != tail {
        return Err(StoreError::CorruptRecord);
    }
    let mut next = Some(0);
    let mut observed = None;
    let mut first_operations = BTreeMap::new();
    let mut expected_membership = crate::CaptureMembership::new(None, vec![], BTreeMap::new())?;
    let mut replay_operations = BTreeMap::<[u8; 16], (Vec<u8>, CreatedResult)>::new();
    let mut replay_heads = BTreeMap::<Vec<u8>, [u8; 16]>::new();
    let mut replay_versions = std::collections::BTreeSet::new();
    for row in results.iter()? {
        let (k, v) = row?;
        let entry = decode_log(required(logs.get(k.value())?)?.value())?;
        let r = if native {
            let response = decode_response(v.value())?;
            let native_entry = decode_native_entry(entry.clone())?;
            validate_response_kind(&native_entry, &response)?;
            if next != Some(k.value()) || response.log_id().index != k.value() {
                return Err(StoreError::CorruptRecord);
            }
            observed = Some(*response.log_id());
            next = k.value().checked_add(1);
            match native_entry.payload {
                openraft::EntryPayload::Membership(membership) => {
                    expected_membership =
                        crate::CaptureMembership::try_from(&openraft::StoredMembership::new(Some(entry.id.into()), membership))?;
                    continue;
                }
                openraft::EntryPayload::Blank => continue,
                openraft::EntryPayload::Normal(_) => CreatedApplyResult {
                    log_id: *response.log_id(),
                    result: response.created_result().cloned().ok_or(StoreError::CorruptRecord)?,
                },
            }
        } else {
            decode_apply(v.value())?
        };
        if !native && (next != Some(k.value()) || r.log_id.index != k.value()) {
            return Err(StoreError::CorruptRecord);
        }
        if entry.id != r.log_id {
            return Err(StoreError::CorruptRecord);
        }
        let c = entry_command(&entry, native)?;
        let canonical = c.encode()?;
        let op = decode_stored_operation(required(operations.get(c.operation_id.as_slice())?)?.value(), schema)?;
        let canonical_matches = op.canonical_command == canonical;
        let expected = if canonical_matches {
            op.result
        } else {
            CreatedResult::OperationConflict
        };
        if full_replay {
            let replay = if let Some((previous, result)) = replay_operations.get(&c.operation_id) {
                if *previous == canonical {
                    result.clone()
                } else {
                    CreatedResult::OperationConflict
                }
            } else {
                let object = object_bytes(&c.object)?;
                let key = version_key(&c.object, c.prepared.version_id)?;
                let actual = replay_heads.get(&object).copied();
                let result = if c.binding_revision != binding.revision {
                    CreatedResult::BindingMismatch
                } else if actual != c.expected_head {
                    CreatedResult::HeadMismatch { actual_head: actual }
                } else if replay_versions.contains(&key) {
                    CreatedResult::VersionConflict
                } else {
                    replay_heads.insert(object, c.prepared.version_id);
                    replay_versions.insert(key);
                    CreatedResult::Created {
                        version_id: c.prepared.version_id,
                        event_id: c.operation_id,
                        decision_log_id: r.log_id,
                    }
                };
                first_operations.insert(c.operation_id, r.log_id);
                replay_operations.insert(c.operation_id, (canonical.clone(), result.clone()));
                result
            };
            if replay != r.result {
                return Err(StoreError::CorruptRecord);
            }
        }
        if expected != r.result {
            return Err(StoreError::CorruptRecord);
        }
        if !full_replay && canonical_matches {
            first_operations.entry(c.operation_id).or_insert(r.log_id);
        }
        observed = Some(r.log_id);
        next = k.value().checked_add(1)
    }
    if observed != last {
        return Err(StoreError::CorruptRecord);
    }
    if full_replay {
        if (native && native_application_metadata(meta)?.1 != expected_membership)
            || heads.len()? != u64::try_from(replay_heads.len()).map_err(|_| StoreError::CorruptRecord)?
            || versions.len()? != u64::try_from(replay_versions.len()).map_err(|_| StoreError::CorruptRecord)?
        {
            return Err(StoreError::CorruptRecord);
        }
        for (object, id) in replay_heads {
            if required(heads.get(object.as_slice())?)?.value() != id {
                return Err(StoreError::CorruptRecord);
            }
        }
    }
    for row in operations.iter()? {
        let (k, v) = row?;
        let op = decode_stored_operation(v.value(), schema)?;
        let c = DecideCreated::decode(&op.canonical_command)?;
        if k.value() != c.operation_id {
            return Err(StoreError::CorruptRecord);
        }
        let first = required(first_operations.get(&c.operation_id))?;
        if matches!(schema, 5 | 6) && decode_provenance(v.value())?.first_decision_log_id != *first {
            return Err(StoreError::CorruptRecord);
        }
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
        let op = decode_stored_operation(required(operations.get(k.value())?)?.value(), schema)?;
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
        let c = entry_command(&entry, native)?;
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
    Ok(first_operations)
}

impl Store {
    pub(crate) fn authorize_created(
        &self,
        expected: &DecideCreated,
        decision: LogId,
    ) -> Result<crate::CommittedCreatedAuthorization, StoreError> {
        if !self.writable {
            return Err(StoreError::WriteDisabled);
        }
        expected.encode()?;
        let tx = self.database.begin_read()?;
        let meta = tx.open_table(META)?;
        let schema = number(required(meta.get("schema")?)?.value())?;
        if !matches!(schema, 6 | 7) {
            return Err(StoreError::UnsupportedSchema);
        }
        let (applied, _) = native_application_state(&tx)?;
        let committed = crate::committed::validate_committed(&meta, &tx.open_table(LOGS)?)?;
        let identity = crate::decode_identity(required(meta.get("identity")?)?.value())?;
        let binding = read_binding(&meta)?;
        let operations = tx.open_table(OPERATIONS)?;
        let record = operations
            .get(expected.operation_id.as_slice())?
            .ok_or(StoreError::CreatedAuthorizationMismatch)?;
        let provenance = decode_provenance(record.value())?;
        let command = DecideCreated::decode(&provenance.operation.canonical_command)?;
        if command.encode()? != provenance.operation.canonical_command {
            return Err(StoreError::CorruptRecord);
        }
        if command.operation_id != expected.operation_id
            || command.object != expected.object
            || command.expected_head != expected.expected_head
            || command.binding_revision != expected.binding_revision
        {
            return Err(StoreError::CreatedAuthorizationMismatch);
        }
        if command.prepared != expected.prepared {
            return Err(StoreError::CreatedAuthorizationMismatch);
        }
        let CreatedResult::Created {
            version_id,
            event_id,
            decision_log_id,
        } = provenance.operation.result
        else {
            return Err(StoreError::CreatedAuthorizationMismatch);
        };
        if decision_log_id != decision
            || provenance.first_decision_log_id != decision
            || version_id != command.prepared.version_id
            || event_id != command.operation_id
            || binding.revision != command.binding_revision
        {
            return Err(StoreError::CreatedAuthorizationMismatch);
        }
        for boundary in [applied, committed] {
            if boundary.is_none_or(|boundary| {
                boundary.index < decision.index || (boundary.index == decision.index && boundary != decision)
            }) {
                return Err(StoreError::CreatedAuthorizationMismatch);
            }
        }
        let events = tx.open_table(EVENTS)?;
        let event = decode_event(required(events.get(command.operation_id.as_slice())?)?.value())?;
        let versions = tx.open_table(VERSIONS)?;
        let version = decode_version(
            required(versions.get(version_key(&command.object, command.prepared.version_id)?.as_slice())?)?.value(),
        )?;
        if event.object != command.object
            || event.prepared != command.prepared
            || event.decision_log_id != decision
            || event.binding != binding
            || version.prepared != command.prepared
            || version.decision_log_id != decision
            || version.binding_revision != binding.revision
        {
            return Err(StoreError::CorruptRecord);
        }
        Ok(crate::CommittedCreatedAuthorization::verified(identity, command, binding, decision))
    }
}
