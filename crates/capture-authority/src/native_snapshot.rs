//! Owned consistent native application snapshot view.
use crate::{Store, StoreError, application, snapshot};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};

#[cfg(test)]
std::thread_local! {
    static AFTER_CAPTURE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct CaptureHook;
#[cfg(test)]
impl Drop for CaptureHook {
    fn drop(&mut self) {
        AFTER_CAPTURE.with(|hook| {
            hook.borrow_mut().take();
        });
    }
}
#[cfg(test)]
pub(crate) fn after_capture(hook: impl FnOnce() + 'static) -> CaptureHook {
    AFTER_CAPTURE.with(|slot| {
        assert!(slot.borrow().is_none(), "one capture hook per test thread");
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    CaptureHook
}

#[derive(Debug, thiserror::Error)]
pub enum NativeSnapshotError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] snapshot::SnapshotError),
    #[error("native snapshot Store lock poisoned")]
    Poisoned,
    #[error("native snapshot worker failed")]
    Worker(#[source] tokio::task::JoinError),
}

pub struct NativeSnapshotView {
    // Drop the transaction before the owner so the Database stays open throughout the view.
    transaction: ReadTransaction,
    header: snapshot::Header,
    counts: [u64; 4],
    schema: u64,
    _owner: Arc<Mutex<Store>>,
}
impl NativeSnapshotView {
    pub(crate) fn acquire(owner: Arc<Mutex<Store>>) -> Result<Self, NativeSnapshotError> {
        let transaction = {
            let store = owner.lock().map_err(|_| NativeSnapshotError::Poisoned)?;
            if !store.writable {
                return Err(StoreError::WriteDisabled.into());
            }
            store.database.begin_read().map_err(StoreError::from)?
        };
        #[cfg(test)]
        if let Some(hook) = AFTER_CAPTURE.with(|slot| slot.borrow_mut().take()) {
            hook();
        }
        let (applied, membership) = application::native_application_state(&transaction)?;
        let (header, schema) = {
            let meta = transaction.open_table(crate::META).map_err(StoreError::from)?;
            let logs = transaction.open_table(crate::LOGS).map_err(StoreError::from)?;
            let committed = crate::committed::validate_committed(&meta, &logs)?;
            if let Some(applied) = applied {
                let committed = committed.ok_or(StoreError::CorruptRecord)?;
                if applied.index > committed.index || (applied.index == committed.index && applied != committed) {
                    return Err(StoreError::CorruptRecord.into());
                }
            }
            let identity = meta
                .get("identity")
                .map_err(StoreError::from)?
                .ok_or(StoreError::CorruptRecord)?;
            let schema = crate::number(
                meta.get("schema")
                    .map_err(StoreError::from)?
                    .ok_or(StoreError::CorruptRecord)?
                    .value(),
            )?;
            (
                snapshot::Header {
                    cluster_id: crate::decode_identity(identity.value())?.cluster_id,
                    applied,
                    membership,
                    binding: application::read_binding(&meta)?,
                },
                schema,
            )
        };
        let mut counts = [0; 4];
        for (index, definition) in [
            application::HEADS,
            application::VERSIONS,
            application::EVENTS,
            application::OPERATIONS,
        ]
        .into_iter()
        .enumerate()
        {
            counts[index] = transaction
                .open_table(definition)
                .map_err(StoreError::from)?
                .len()
                .map_err(StoreError::from)?;
        }
        Ok(Self {
            transaction,
            header,
            counts,
            schema,
            _owner: owner,
        })
    }

    pub fn stream_capability_artifact<W: Write>(
        self,
        writer: W,
        limits: snapshot::Limits,
    ) -> Result<snapshot::CapabilityArtifact<W>, NativeSnapshotError> {
        if !matches!(self.schema, 6 | 7) {
            return Err(StoreError::UnsupportedSchema.into());
        }
        let output = snapshot::CapabilityOutput::new(writer, limits.total_bytes);
        Ok(self.stream(output, limits)?.finish())
    }

    pub(crate) fn transaction(&self) -> &ReadTransaction {
        &self.transaction
    }

    pub(crate) fn header(&self) -> &snapshot::Header {
        &self.header
    }

    pub(crate) fn owner(&self) -> &Arc<Mutex<Store>> {
        &self._owner
    }

    pub(crate) fn stream_local_capability<W: Write>(
        &self,
        writer: W,
        limits: snapshot::Limits,
    ) -> Result<snapshot::CapabilityArtifact<W>, NativeSnapshotError> {
        if !matches!(self.schema, 6 | 7) {
            return Err(StoreError::UnsupportedSchema.into());
        }
        let output = snapshot::CapabilityOutput::new(writer, limits.total_bytes);
        Ok(self.stream_borrowed(output, limits)?.finish())
    }

    pub fn stream<W: Write>(self, writer: W, limits: snapshot::Limits) -> Result<W, NativeSnapshotError> {
        self.stream_borrowed(writer, limits)
    }

    fn stream_borrowed<W: Write>(&self, writer: W, limits: snapshot::Limits) -> Result<W, NativeSnapshotError> {
        let mut encoder = snapshot::Encoder::new(writer, &self.header, self.counts, limits)?;
        for (definition, section) in [
            (application::HEADS, snapshot::Section::Heads),
            (application::VERSIONS, snapshot::Section::Versions),
            (application::EVENTS, snapshot::Section::Events),
            (application::OPERATIONS, snapshot::Section::Operations),
        ] {
            let table = self.transaction.open_table(definition).map_err(StoreError::from)?;
            for row in table.iter().map_err(StoreError::from)? {
                let (key, value) = row.map_err(StoreError::from)?;
                encoder.row(snapshot::Row {
                    section,
                    key: key.value(),
                    value: value.value(),
                })?;
            }
        }
        Ok(encoder.finish()?)
    }
}
