use crate::{LOGS, LogId, META, Store, StoreError, decode_id, decode_log, encode_id, number};
use redb::{ReadableDatabase, ReadableTable};

pub(crate) fn validate_committed(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    logs: &impl ReadableTable<u64, &'static [u8]>,
) -> Result<Option<LogId>, StoreError> {
    let schema = number(meta.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())?;
    let marker = meta.get("committed")?;
    match (schema, marker) {
        (1 | 3, None) => {
            if schema == 3 && meta.get("last_applied")?.is_none_or(|v| v.value() != [0]) {
                return Err(StoreError::CorruptRecord);
            }
            Ok(None)
        }
        (2 | 3, Some(marker)) => {
            let id = decode_id(marker.value())?;
            let record = logs.get(id.index)?.ok_or(StoreError::CorruptRecord)?;
            if decode_log(record.value())?.id != id {
                return Err(StoreError::CorruptRecord);
            }
            Ok(Some(id))
        }
        (1 | 2, _) => Err(StoreError::CorruptRecord),
        _ => Err(StoreError::UnsupportedSchema),
    }
}

impl Store {
    /// Read the exact committed boundary, validating it against the stored log.
    pub fn read_committed(&self) -> Result<Option<LogId>, StoreError> {
        let tx = self.database.begin_read()?;
        validate_committed(&tx.open_table(META)?, &tx.open_table(LOGS)?)
    }

    /// Persist a monotonic committed boundary supplied by the authority owner.
    pub fn save_committed(&mut self, id: LogId) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let logs = tx.open_table(LOGS)?;
            let previous = validate_committed(&meta, &logs)?;
            let record = logs.get(id.index)?.ok_or(StoreError::InvalidCommittedLog)?;
            if decode_log(record.value())?.id != id {
                return Err(StoreError::InvalidCommittedLog);
            }
            if let Some(previous) = previous {
                if id.index < previous.index {
                    return Err(StoreError::CommittedRegression);
                }
                if id == previous {
                    return Ok(());
                }
            }
            meta.insert("committed", encode_id(id).as_slice())?;
            if previous.is_none() && number(meta.get("schema")?.ok_or(StoreError::CorruptRecord)?.value())? != 3 {
                meta.insert("schema", 2u64.to_be_bytes().as_slice())?;
            }
        }
        self.commit(tx)
    }
}
