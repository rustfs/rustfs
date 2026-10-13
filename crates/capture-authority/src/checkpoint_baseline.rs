//! Immutable checkpoint state and disk-backed validation of the retained suffix.
use crate::{
    CaptureMembership, CreatedApplyResult, CreatedEvent, CreatedResult, CreatedVersion, LogId, StoreError, application,
    created::{DecideCreated, Decoder, object_bytes, result_bytes},
    native_checkpoint::{CheckpointLimits, recovery_scratch},
    snapshot,
};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

pub(crate) const META_KEY: &str = "checkpoint_baseline";
pub(crate) const PURGED_KEY: &str = "checkpoint_purged";
pub(crate) const HEADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("checkpoint_heads_v1");
pub(crate) const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("checkpoint_versions_v1");
pub(crate) const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("checkpoint_events_v1");
pub(crate) const OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("checkpoint_operations_v1");
const FIRST_DECISIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("checkpoint_validation_decisions");
const VERSION_OPERATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("checkpoint_validation_versions");
const METADATA_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct BaselineMetadata {
    pub(crate) generation: u64,
    pub(crate) header: snapshot::Header,
    pub(crate) limits: CheckpointLimits,
}

struct EmptySink;
impl snapshot::StagingSink for EmptySink {
    fn row(&mut self, _: snapshot::Section, _: &[u8], _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("checkpoint metadata contains rows"))
    }
}
fn metadata_limits() -> snapshot::Limits {
    snapshot::Limits {
        total_bytes: 1024 * 1024,
        membership_bytes: 512 * 1024,
        key_bytes: 0,
        value_bytes: 0,
        records: 0,
    }
}
pub(crate) fn encode_metadata(metadata: &BaselineMetadata) -> Result<Vec<u8>, StoreError> {
    metadata.limits.validate()?;
    if metadata.generation == 0 {
        return Err(StoreError::CorruptRecord);
    }
    let mut bytes = b"CBAS\x01".to_vec();
    for value in [
        metadata.generation,
        metadata.limits.snapshot.total_bytes,
        metadata.limits.snapshot.membership_bytes,
        metadata.limits.snapshot.key_bytes,
        metadata.limits.snapshot.value_bytes,
        metadata.limits.snapshot.records,
        metadata.limits.staging_bytes,
    ] {
        bytes.extend(value.to_be_bytes());
    }
    let frame = snapshot::Encoder::new(Vec::new(), &metadata.header, [0; 4], metadata_limits())
        .and_then(snapshot::Encoder::finish)
        .map_err(|_| StoreError::CorruptRecord)?;
    bytes.extend(frame);
    if bytes.len() > METADATA_BYTES {
        return Err(StoreError::CorruptRecord);
    }
    Ok(bytes)
}
pub(crate) fn read_metadata(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<Option<BaselineMetadata>, StoreError> {
    let schema = crate::number(required(meta.get("schema")?)?.value())?;
    let stored = meta.get(META_KEY)?;
    if schema != 7 {
        if stored.is_some() || meta.get(PURGED_KEY)?.is_some() {
            return Err(StoreError::CorruptRecord);
        }
        return Ok(None);
    }
    let value = required(stored)?;
    let bytes = value.value();
    if bytes.len() > METADATA_BYTES || bytes.get(..5) != Some(b"CBAS\x01") {
        return Err(StoreError::CorruptRecord);
    }
    let mut numbers = [0u64; 7];
    for (index, number) in numbers.iter_mut().enumerate() {
        let start = 5 + index * 8;
        *number = crate::number(bytes.get(start..start + 8).ok_or(StoreError::CorruptRecord)?)?;
    }
    let limits = CheckpointLimits {
        snapshot: snapshot::Limits {
            total_bytes: numbers[1],
            membership_bytes: numbers[2],
            key_bytes: numbers[3],
            value_bytes: numbers[4],
            records: numbers[5],
        },
        staging_bytes: numbers[6],
    };
    limits.validate()?;
    let header = snapshot::decode(bytes.get(61..).ok_or(StoreError::CorruptRecord)?, &mut EmptySink, metadata_limits())
        .map_err(|_| StoreError::CorruptRecord)?;
    let metadata = BaselineMetadata {
        generation: numbers[0],
        header,
        limits,
    };
    if encode_metadata(&metadata)?.as_slice() != bytes {
        return Err(StoreError::CorruptRecord);
    }
    Ok(Some(metadata))
}
pub(crate) fn read_baseline_cut(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<Option<LogId>, StoreError> {
    Ok(read_metadata(meta)?.and_then(|metadata| metadata.header.applied))
}
pub(crate) fn encode_purged(id: LogId) -> Vec<u8> {
    let mut bytes = b"CPUR\x01".to_vec();
    bytes.extend(crate::encode_id(id));
    bytes
}
pub(crate) fn read_purged(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<Option<LogId>, StoreError> {
    let cut = read_baseline_cut(meta)?;
    let Some(value) = meta.get(PURGED_KEY)? else {
        return Ok(None);
    };
    let bytes = value.value();
    if bytes.len() != 29 || bytes.get(..5) != Some(b"CPUR\x01") {
        return Err(StoreError::CorruptRecord);
    }
    let id = crate::decode_id(&bytes[5..])?;
    at_or_before(id, required(cut)?)?;
    Ok(Some(id))
}
fn at_or_before(id: LogId, cut: LogId) -> Result<(), StoreError> {
    if id.index > cut.index || (id.index == cut.index && id != cut) {
        return Err(StoreError::CorruptRecord);
    }
    Ok(())
}
fn required<T>(value: Option<T>) -> Result<T, StoreError> {
    value.ok_or(StoreError::CorruptRecord)
}
struct Budget {
    limits: snapshot::Limits,
    bytes: u64,
    records: u64,
}
impl Budget {
    fn new(limits: snapshot::Limits) -> Self {
        Self {
            limits,
            bytes: 0,
            records: 0,
        }
    }
    fn row(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let key = u64::try_from(key.len()).map_err(|_| StoreError::CorruptRecord)?;
        let value = u64::try_from(value.len()).map_err(|_| StoreError::CorruptRecord)?;
        if key > 1024 * 1024 || value > 1024 * 1024 {
            return Err(StoreError::CorruptRecord);
        }
        self.bytes = self
            .bytes
            .checked_add(key)
            .and_then(|n| n.checked_add(value))
            .ok_or(StoreError::ResourceLimit)?;
        self.records = self.records.checked_add(1).ok_or(StoreError::ResourceLimit)?;
        if key > self.limits.key_bytes
            || value > self.limits.value_bytes
            || self.bytes > self.limits.total_bytes
            || self.records > self.limits.records
        {
            return Err(StoreError::ResourceLimit);
        }
        Ok(())
    }
}

pub(crate) fn resource_error(error: StoreError) -> StoreError {
    use std::error::Error;
    let mut cause: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(current) = cause {
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::StorageFull)
        {
            return StoreError::ResourceLimit;
        }
        cause = current.source();
    }
    error
}

/// Validate relationships, preserving negative history rather than re-evaluating it.
/// The caller separately establishes local capture or authenticated installation authority.
pub(crate) fn validate_baseline_with_budget(
    tx: &ReadTransaction,
    metadata: &BaselineMetadata,
    index_max: u64,
) -> Result<(), StoreError> {
    validate_baseline_inner(tx, metadata, index_max).map_err(resource_error)
}

fn validate_baseline_inner(tx: &ReadTransaction, metadata: &BaselineMetadata, index_max: u64) -> Result<(), StoreError> {
    encode_metadata(metadata)?;
    let Some(cut) = metadata.header.applied else {
        if metadata.header.membership != CaptureMembership::new(None, vec![], std::collections::BTreeMap::new())? {
            return Err(StoreError::CorruptRecord);
        }
        for definition in [HEADS, VERSIONS, EVENTS, OPERATIONS] {
            if tx.open_table(definition)?.len()? != 0 {
                return Err(StoreError::CorruptRecord);
            }
        }
        return Ok(());
    };
    let heads = tx.open_table(HEADS)?;
    let versions = tx.open_table(VERSIONS)?;
    let events = tx.open_table(EVENTS)?;
    let operations = tx.open_table(OPERATIONS)?;
    let (_directory, index) = recovery_scratch(index_max)?;
    let mut index_write = index.begin_write()?;
    index_write.open_table(FIRST_DECISIONS)?;
    index_write.open_table(VERSION_OPERATIONS)?;
    let mut budget = Budget::new(metadata.limits.snapshot);
    let mut pending = 0;
    for row in operations.iter()? {
        let (key, value) = row?;
        budget.row(key.value(), value.value())?;
        let provenance = application::decode_provenance(value.value())?;
        let command = DecideCreated::decode(&provenance.operation.canonical_command)?;
        let canonical = command.encode()?;
        if key.value() != command.operation_id || canonical != provenance.operation.canonical_command {
            return Err(StoreError::CorruptRecord);
        }
        at_or_before(provenance.first_decision_log_id, cut)?;
        let mut original = u64::try_from(canonical.len())
            .map_err(|_| StoreError::CorruptRecord)?
            .to_be_bytes()
            .to_vec();
        original.extend(&canonical);
        original.extend(result_bytes(&provenance.operation.result));
        if application::encode_provenance(provenance.first_decision_log_id, &original)?.as_slice() != value.value() {
            return Err(StoreError::CorruptRecord);
        }
        if index_write
            .open_table(FIRST_DECISIONS)?
            .insert(provenance.first_decision_log_id.index, key.value())?
            .is_some()
        {
            return Err(StoreError::CorruptRecord);
        }
        if provenance.operation.result == CreatedResult::OperationConflict {
            return Err(StoreError::CorruptRecord);
        }
        if let CreatedResult::Created {
            version_id,
            event_id,
            decision_log_id,
        } = provenance.operation.result
        {
            if version_id != command.prepared.version_id
                || event_id != command.operation_id
                || decision_log_id != provenance.first_decision_log_id
                || command.binding_revision != metadata.header.binding.revision
            {
                return Err(StoreError::CorruptRecord);
            }
            let event = application::decode_event(required(events.get(key.value())?)?.value())?;
            if event
                != (CreatedEvent {
                    object: command.object.clone(),
                    prepared: command.prepared.clone(),
                    decision_log_id,
                    binding: metadata.header.binding.clone(),
                })
            {
                return Err(StoreError::CorruptRecord);
            }
            let version_key = application::version_key(&command.object, version_id)?;
            let version = application::decode_version(required(versions.get(version_key.as_slice())?)?.value())?;
            if version
                != (CreatedVersion {
                    prepared: command.prepared.clone(),
                    decision_log_id,
                    binding_revision: metadata.header.binding.revision,
                })
            {
                return Err(StoreError::CorruptRecord);
            }
            if index_write
                .open_table(VERSION_OPERATIONS)?
                .insert(version_key.as_slice(), key.value())?
                .is_some()
            {
                return Err(StoreError::CorruptRecord);
            }
        } else if events.get(key.value())?.is_some() {
            return Err(StoreError::CorruptRecord);
        }
        pending += 1;
        if pending == 128 {
            index_write.commit().map_err(StoreError::StagingCommit)?;
            index_write = index.begin_write()?;
            pending = 0;
        }
    }
    index_write.commit().map_err(StoreError::StagingCommit)?;
    let index_read = index.begin_read()?;
    let version_operations = index_read.open_table(VERSION_OPERATIONS)?;
    if version_operations.len()? != versions.len()? {
        return Err(StoreError::CorruptRecord);
    }
    for row in versions.iter()? {
        let (key, value) = row?;
        budget.row(key.value(), value.value())?;
        required(version_operations.get(key.value())?)?;
        let mut decoder = Decoder::new(key.value());
        let object = decoder.object()?;
        let id = decoder.uuid()?;
        decoder.finish()?;
        let version = application::decode_version(value.value())?;
        let head_bytes = required(heads.get(object_bytes(&object)?.as_slice())?)?;
        let mut decoder = Decoder::new(head_bytes.value());
        let head = decoder.uuid()?;
        decoder.finish()?;
        let current =
            application::decode_version(required(versions.get(application::version_key(&object, head)?.as_slice())?)?.value())?;
        if id != version.prepared.version_id
            || current.decision_log_id.index < version.decision_log_id.index
            || (current.decision_log_id.index == version.decision_log_id.index
                && current.decision_log_id != version.decision_log_id)
        {
            return Err(StoreError::CorruptRecord);
        }
    }
    for row in events.iter()? {
        let (key, value) = row?;
        budget.row(key.value(), value.value())?;
        let event = application::decode_event(value.value())?;
        let operation = application::decode_provenance(required(operations.get(key.value())?)?.value())?;
        if operation.operation.result
            != (CreatedResult::Created {
                version_id: event.prepared.version_id,
                event_id: key.value().try_into().map_err(|_| StoreError::CorruptRecord)?,
                decision_log_id: event.decision_log_id,
            })
        {
            return Err(StoreError::CorruptRecord);
        }
    }
    for row in heads.iter()? {
        let (key, value) = row?;
        budget.row(key.value(), value.value())?;
        let mut decoder = Decoder::new(key.value());
        let object = decoder.object()?;
        decoder.finish()?;
        let mut decoder = Decoder::new(value.value());
        let id = decoder.uuid()?;
        decoder.finish()?;
        required(versions.get(application::version_key(&object, id)?.as_slice())?)?;
    }
    Ok(())
}

pub(crate) fn validate_recovery(tx: &ReadTransaction) -> Result<(), StoreError> {
    validate_recovery_with_limits(tx, None)
}

pub(crate) fn validate_recovery_with_limits(
    tx: &ReadTransaction,
    override_limits: Option<CheckpointLimits>,
) -> Result<(), StoreError> {
    validate_recovery_inner(tx, override_limits).map_err(resource_error)
}

fn validate_recovery_inner(tx: &ReadTransaction, override_limits: Option<CheckpointLimits>) -> Result<(), StoreError> {
    let meta = tx.open_table(crate::META)?;
    let metadata = required(read_metadata(&meta)?)?;
    crate::raft_state_machine::selected_meta(&meta, &metadata)?;
    let cut = metadata.header.applied;
    let recovery_limits = override_limits.unwrap_or(metadata.limits);
    recovery_limits.validate()?;
    let identity = crate::decode_identity(required(meta.get("identity")?)?.value())?;
    let binding = application::read_binding(&meta)?;
    if identity.cluster_id != metadata.header.cluster_id || binding != metadata.header.binding {
        return Err(StoreError::CorruptRecord);
    }
    let purged = read_purged(&meta)?;
    validate_baseline_with_budget(tx, &metadata, recovery_limits.staging_bytes)?;
    let last = application::read_applied(&meta)?;
    optional_at_or_before(cut, last)?;
    let logs = tx.open_table(crate::LOGS)?;
    let committed = crate::committed::validate_committed(&meta, &logs)?;
    optional_at_or_before(last, committed)?;
    let declared_tail = meta.get("tail")?.map(|v| crate::decode_id(v.value())).transpose()?;
    optional_at_or_before(committed, declared_tail)?;
    let (_directory, scratch) = recovery_scratch(recovery_limits.staging_bytes)?;
    for (source, target) in [
        (HEADS, application::HEADS),
        (VERSIONS, application::VERSIONS),
        (EVENTS, application::EVENTS),
        (OPERATIONS, application::OPERATIONS),
    ] {
        let table = tx.open_table(source)?;
        let mut write = scratch.begin_write()?;
        write.open_table(target)?;
        let mut pending = 0;
        for row in table.iter()? {
            let (key, value) = row?;
            write.open_table(target)?.insert(key.value(), value.value())?;
            pending += 1;
            if pending == 128 {
                write.commit().map_err(StoreError::StagingCommit)?;
                write = scratch.begin_write()?;
                pending = 0;
            }
        }
        write.commit().map_err(StoreError::StagingCommit)?;
    }
    let mut membership: CaptureMembership = metadata.header.membership.clone();
    if let Some(cut) = cut
        && let Some(retained_cut) = logs.get(cut.index)?
        && crate::decode_log(retained_cut.value())?.id != cut
    {
        return Err(StoreError::CorruptRecord);
    }
    let results = tx.open_table(application::APPLY_RESULTS)?;
    // Covered entries may remain until purge. Check their exact typed responses
    // without rebuilding the historical state or allocating a prefix-sized map.
    let operations = tx.open_table(OPERATIONS)?;
    let mut actual_tail = purged;
    let mut prefix_next = purged.map_or(Some(0), |id| id.index.checked_add(1));
    if let Some(cut) = cut {
        for row in logs.range(..=cut.index)? {
            let (key, bytes) = row?;
            if purged.is_some_and(|purged| key.value() <= purged.index) {
                return Err(StoreError::CorruptRecord);
            }
            if bytes.value().len() > 1024 * 1024 {
                return Err(StoreError::CorruptRecord);
            }
            let entry = crate::decode_log(bytes.value())?;
            if entry.id.index != key.value() || prefix_next != Some(key.value()) {
                return Err(StoreError::CorruptRecord);
            }
            actual_tail = Some(entry.id);
            prefix_next = key.value().checked_add(1);
            let native = application::decode_native_entry(entry)?;
            let bytes = required(results.get(key.value())?)?;
            if bytes.value().len() > 1024 * 1024 {
                return Err(StoreError::CorruptRecord);
            }
            let response = application::decode_response(bytes.value())?;
            application::validate_response_kind(&native, &response)?;
            if let openraft::EntryPayload::Normal(command) = native.payload {
                let operation = application::decode_provenance(
                    required(operations.get(command.created().operation_id.as_slice())?)?.value(),
                )?;
                let canonical = command.created().encode()?;
                let expected = if operation.operation.canonical_command == canonical {
                    operation.operation.result
                } else {
                    CreatedResult::OperationConflict
                };
                if response.created_result() != Some(&expected) {
                    return Err(StoreError::CorruptRecord);
                }
                if operation.first_decision_log_id.index == key.value()
                    && operation.first_decision_log_id != LogId::from(native.log_id)
                {
                    return Err(StoreError::CorruptRecord);
                }
            }
        }
        for row in results.range(..=cut.index)? {
            let (key, _) = row?;
            if purged.is_some_and(|purged| key.value() <= purged.index) || logs.get(key.value())?.is_none() {
                return Err(StoreError::CorruptRecord);
            }
        }
        if purged.is_none_or(|purged| purged.index < cut.index) && logs.get(cut.index)?.is_none() {
            return Err(StoreError::CorruptRecord);
        }
    }
    if actual_tail != cut {
        return Err(StoreError::CorruptRecord);
    }

    let mut observed = cut;
    let mut next = cut.map_or(Some(0), |cut| cut.index.checked_add(1));
    let mut budget = Budget::new(recovery_limits.snapshot);
    if let Some(start) = next {
        for row in logs.range(start..)? {
            let (key, bytes) = row?;
            budget.row(&key.value().to_be_bytes(), bytes.value())?;
            let entry = crate::decode_log(bytes.value())?;
            if next != Some(key.value()) || entry.id.index != key.value() {
                return Err(StoreError::CorruptRecord);
            }
            next = key.value().checked_add(1);
            actual_tail = Some(entry.id);
            let native = application::decode_native_entry(entry.clone())?;
            if last.is_none_or(|last| key.value() > last.index) {
                continue;
            }
            let response_bytes = required(results.get(key.value())?)?;
            if response_bytes.value().len() > 1024 * 1024 {
                return Err(StoreError::CorruptRecord);
            }
            if u64::try_from(response_bytes.value().len()).map_err(|_| StoreError::CorruptRecord)?
                > recovery_limits.snapshot.value_bytes
            {
                return Err(StoreError::ResourceLimit);
            }
            let response = application::decode_response(response_bytes.value())?;
            application::validate_response_kind(&native, &response)?;
            let write = scratch.begin_write()?;
            let expected = match native.payload {
                openraft::EntryPayload::Blank => crate::CaptureResponse::blank(entry.id)?,
                openraft::EntryPayload::Membership(value) => {
                    membership = CaptureMembership::try_from(&openraft::StoredMembership::new(Some(entry.id.into()), value))?;
                    crate::CaptureResponse::membership(entry.id)?
                }
                openraft::EntryPayload::Normal(command) => {
                    let canonical = command.created().encode()?;
                    let result = application::apply_created(&write, entry.id, command.created(), &canonical, &binding, 6)?;
                    crate::CaptureResponse::created(CreatedApplyResult {
                        log_id: entry.id,
                        result,
                    })?
                }
            };
            if response != expected {
                return Err(StoreError::CorruptRecord);
            }
            write.commit().map_err(StoreError::StagingCommit)?;
            observed = Some(entry.id);
        }
        for row in results.range(start..)? {
            let (key, _) = row?;
            if last.is_none_or(|last| key.value() > last.index) || logs.get(key.value())?.is_none() {
                return Err(StoreError::CorruptRecord);
            }
        }
    }
    if actual_tail != declared_tail || observed != last || application::native_application_state(tx)?.1 != membership {
        return Err(StoreError::CorruptRecord);
    }
    let recovered = scratch.begin_read()?;
    let mut live_budget = Budget::new(recovery_limits.snapshot);
    for definition in [
        application::HEADS,
        application::VERSIONS,
        application::EVENTS,
        application::OPERATIONS,
    ] {
        let expected = recovered.open_table(definition)?;
        let actual = tx.open_table(definition)?;
        if expected.len()? != actual.len()? {
            return Err(StoreError::CorruptRecord);
        }
        for row in actual.iter()? {
            let (key, value) = row?;
            live_budget.row(key.value(), value.value())?;
            if required(expected.get(key.value())?)?.value() != value.value() {
                return Err(StoreError::CorruptRecord);
            }
        }
    }
    Ok(())
}

fn optional_at_or_before(id: Option<LogId>, cut: Option<LogId>) -> Result<(), StoreError> {
    match (id, cut) {
        (None, _) => Ok(()),
        (Some(id), Some(cut)) => at_or_before(id, cut),
        _ => Err(StoreError::CorruptRecord),
    }
}
