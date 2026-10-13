//! OpenRaft log storage backed by the existing native writer and local durable baseline.
use crate::{CaptureRaftEntry, CaptureTypeConfig, NativeLogReader, NativeLogWriter, NativeWriteError, StoreError};
use openraft::{
    LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use redb::{ReadableDatabase, ReadableTable};

/// Clones share one native Store, write ordering and checkpoint authority.
#[derive(Clone)]
pub struct NativeRaftLogStore {
    writer: NativeLogWriter,
}
impl NativeRaftLogStore {
    pub fn new(writer: NativeLogWriter) -> Self {
        Self { writer }
    }
}
fn error(error: NativeWriteError, operation: fn(openraft::AnyError) -> StorageIOError<u64>) -> StorageError<u64> {
    StorageError::IO {
        source: operation(openraft::AnyError::new(&error)),
    }
}
impl RaftLogReader<CaptureTypeConfig> for NativeRaftLogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<CaptureRaftEntry>, StorageError<u64>> {
        self.writer.reader().try_get_log_entries(range).await
    }
}
impl RaftLogStorage<CaptureTypeConfig> for NativeRaftLogStore {
    type LogReader = NativeLogReader;
    async fn get_log_state(&mut self) -> Result<openraft::LogState<CaptureTypeConfig>, StorageError<u64>> {
        self.writer
            .write(|store| {
                if !store.writable {
                    return Err(StoreError::WriteDisabled);
                }
                let read = store.database.begin_read()?;
                let meta = read.open_table(crate::META)?;
                let purged = crate::checkpoint_baseline::read_purged(&meta)?;
                let logs = read.open_table(crate::LOGS)?;
                let last = logs
                    .last()?
                    .map(|(index, value)| {
                        let id = crate::decode_log(value.value())?.id;
                        if id.index != index.value() {
                            return Err(StoreError::CorruptRecord);
                        }
                        Ok(id)
                    })
                    .transpose()?
                    .or(purged);
                let tail = meta.get("tail")?.map(|value| crate::decode_id(value.value())).transpose()?;
                if tail != last {
                    return Err(StoreError::CorruptRecord);
                }
                Ok(openraft::LogState {
                    last_purged_log_id: purged.map(Into::into),
                    last_log_id: last.map(Into::into),
                })
            })
            .await
            .map_err(|e| error(e, StorageIOError::read_logs))
    }
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.writer.reader()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.writer
            .save_vote(*vote)
            .await
            .map_err(|e| error(e, StorageIOError::write_vote))
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.writer.read_vote().await.map_err(|e| error(e, StorageIOError::read_vote))
    }
    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.writer
            .write(move |store| match committed {
                Some(id) => store.save_committed(id.into()),
                None if store.read_committed()?.is_none() => Ok(()),
                None => Err(StoreError::CommittedRegression),
            })
            .await
            .map_err(|e| error(e, StorageIOError::write_logs))
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.writer
            .write(|store| store.read_committed().map(|id| id.map(Into::into)))
            .await
            .map_err(|e| error(e, StorageIOError::read_logs))
    }
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<CaptureTypeConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = CaptureRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        // The original blocking worker owns ordering and callback through durable commit,
        // even if OpenRaft cancels its awaiting append future.
        self.writer
            .write(move |store| {
                let result = store.append_native(&entries);
                callback.log_io_completed(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| std::io::Error::other(error.code())),
                );
                result
            })
            .await
            .map_err(|e| error(e, StorageIOError::write_logs))
    }
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.writer
            .truncate_suffix(log_id.index)
            .await
            .map_err(|e| error(e, StorageIOError::write_logs))
    }
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(error(NativeWriteError::Store(StoreError::StaleCheckpoint), StorageIOError::write_logs));
            }
            let notified = self.writer.snapshot_receives.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let receives = self.writer.snapshot_receives.clone();
            // Observe coverage and receive ownership while install is excluded by ordering.
            let (result, pending) = self
                .writer
                .write(move |store| Ok((store.purge_native_prefix(log_id.into(), None), receives.pending())))
                .await
                .map_err(|e| error(e, StorageIOError::write_logs))?;
            if matches!(&result, Err(StoreError::StaleCheckpoint)) {
                #[cfg(test)]
                {
                    let hook = self
                        .writer
                        .snapshot_receives
                        .purge_recheck
                        .lock()
                        .expect("test hook lock")
                        .take();
                    if let Some((entered, resume)) = hook {
                        entered.notify_one();
                        resume.notified().await;
                    }
                }
                if pending {
                    #[cfg(test)]
                    let _waiter = crate::raft_state_machine::Waiter::new(self.writer.snapshot_receives.clone());
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return Err(error(NativeWriteError::Store(StoreError::StaleCheckpoint), StorageIOError::write_logs));
                    }
                    continue;
                }
            }
            return result.map_err(|e| error(e.into(), StorageIOError::write_logs));
        }
    }
}
