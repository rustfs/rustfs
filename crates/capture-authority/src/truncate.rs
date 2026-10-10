use crate::{LOGS, META, Store, StoreError, decode_id, decode_log, encode_id};
use redb::ReadableTable;

impl Store {
    /// Atomically remove every log entry at or after `from`.
    pub fn truncate_suffix(&mut self, from: u64) -> Result<(), StoreError> {
        let tx = self.write_transaction()?;
        {
            let mut meta = tx.open_table(META)?;
            let logs = tx.open_table(LOGS)?;
            if crate::committed::validate_committed(&meta, &logs)?.is_some_and(|id| from <= id.index) {
                return Err(StoreError::CommittedLogProtected);
            }
            drop(logs);
            let tail = meta.get("tail")?.map(|v| decode_id(v.value())).transpose()?;
            if tail.is_none_or(|id| from > id.index) {
                return Ok(());
            }
            let mut logs = tx.open_table(LOGS)?;
            let new_tail = if from == 0 {
                None
            } else {
                let index = from - 1;
                let record = logs.get(index)?.ok_or(StoreError::InvalidLogSequence)?;
                let id = decode_log(record.value())?.id;
                if id.index != index {
                    return Err(StoreError::CorruptRecord);
                }
                Some(id)
            };
            logs.retain(|index, _| index < from)?;
            match new_tail {
                Some(id) => {
                    meta.insert("tail", encode_id(id).as_slice())?;
                }
                None => {
                    meta.remove("tail")?;
                }
            }
        }
        self.commit(tx)
    }
}
