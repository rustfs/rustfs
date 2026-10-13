//! Durable OpenRaft application state and selected baseline snapshots.
use crate::{
    CaptureRaftEntry, CaptureResponse, CaptureTypeConfig, CheckpointLimits, NativeLogWriter, NativeSnapshotData,
    NativeSnapshotError, StoreError, checkpoint_baseline, snapshot,
};
use openraft::{
    OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
    storage::RaftStateMachine,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::{
    io::{Seek, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(crate) const SNAPSHOT_META: &str = "native_snapshot_meta_v1";
type Meta = SnapshotMeta<u64, crate::CaptureNode>;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedMeta {
    generation: u64,
    meta: Meta,
}

pub(crate) fn publish_meta(
    meta: &mut redb::Table<&str, &[u8]>,
    baseline: &checkpoint_baseline::BaselineMetadata,
    value: Meta,
) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec(&SelectedMeta {
        generation: baseline.generation,
        meta: value,
    })
    .map_err(|_| StoreError::CorruptRecord)?;
    if bytes.len() > 1024 * 1024 {
        return Err(StoreError::ResourceLimit);
    }
    meta.insert(SNAPSHOT_META, bytes.as_slice())?;
    meta.insert("native_snapshot_meta_required", &[1u8][..])?;
    Ok(())
}
pub(crate) fn local_meta(
    baseline: &checkpoint_baseline::BaselineMetadata,
    identity: crate::NodeIdentity,
) -> Result<Meta, StoreError> {
    Ok(Meta {
        last_log_id: baseline.header.applied.map(Into::into),
        last_membership: StoredMembership::try_from(&baseline.header.membership)?,
        snapshot_id: format!("native-local-{:x?}-{}-{}", identity.cluster_id, identity.node_id, baseline.generation),
    })
}
pub(crate) fn publish_local_meta(
    meta: &mut redb::Table<&str, &[u8]>,
    baseline: &checkpoint_baseline::BaselineMetadata,
) -> Result<(), StoreError> {
    let identity = crate::decode_identity(meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())?;
    publish_meta(meta, baseline, local_meta(baseline, identity)?)
}
pub(crate) fn selected_meta(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    baseline: &checkpoint_baseline::BaselineMetadata,
) -> Result<Meta, StoreError> {
    let Some(bytes) = meta.get(SNAPSHOT_META)? else {
        if meta.get("native_snapshot_meta_required")?.is_some() {
            return Err(StoreError::CorruptRecord);
        }
        return local_meta(
            baseline,
            crate::decode_identity(meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())?,
        );
    };
    if bytes.value().len() > 1024 * 1024 {
        return Err(StoreError::CorruptRecord);
    }
    let selected: SelectedMeta = serde_json::from_slice(bytes.value()).map_err(|_| StoreError::CorruptRecord)?;
    if selected.generation != baseline.generation
        || selected.meta.last_log_id.map(crate::LogId::from) != baseline.header.applied
        || crate::CaptureMembership::try_from(&selected.meta.last_membership)? != baseline.header.membership
    {
        return Err(StoreError::CorruptRecord);
    }
    Ok(selected.meta)
}

#[cfg(test)]
pub(crate) type TestPause = std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>;

#[derive(Default)]
pub(crate) struct ReceiveState {
    count: AtomicUsize,
    #[cfg(test)]
    pub(crate) purge_recheck: TestPause,
    pub(crate) changed: tokio::sync::Notify,
    #[cfg(test)]
    pub(crate) waiters: AtomicUsize,
}
pub(crate) struct ReceiveGuard {
    state: Arc<ReceiveState>,
}
impl ReceiveGuard {
    pub(crate) fn belongs_to(&self, state: &Arc<ReceiveState>) -> bool {
        Arc::ptr_eq(&self.state, state)
    }
    fn new(state: Arc<ReceiveState>) -> Self {
        state.count.fetch_add(1, Ordering::AcqRel);
        Self { state }
    }
}
impl Drop for ReceiveGuard {
    fn drop(&mut self) {
        self.state.count.fetch_sub(1, Ordering::AcqRel);
        self.state.changed.notify_waiters();
    }
}
impl ReceiveState {
    pub(crate) fn pending(&self) -> bool {
        self.count.load(Ordering::Acquire) > 0
    }
}

/// Both adapters must use clones of the same writer. Limits apply while receiving.
#[derive(Clone)]
pub struct NativeRaftStateMachine {
    pub(crate) writer: NativeLogWriter,
    pub(crate) limits: CheckpointLimits,
    #[cfg(test)]
    pub(crate) build_before_ordering: Arc<TestPause>,
}
impl NativeRaftStateMachine {
    pub fn new(writer: NativeLogWriter, limits: CheckpointLimits) -> Result<Self, NativeSnapshotError> {
        limits.validate()?;
        limits.snapshot.total_bytes.checked_add(56).ok_or(StoreError::ResourceLimit)?;
        Ok(Self {
            writer,
            limits,
            #[cfg(test)]
            build_before_ordering: Arc::new(TestPause::default()),
        })
    }
    fn maximum(&self) -> u64 {
        self.limits.snapshot.total_bytes + 56
    }
    async fn current(&self) -> Result<Option<Snapshot<CaptureTypeConfig>>, NativeSnapshotError> {
        let owner = self.writer.store.clone();
        let limits = self.limits;
        tokio::task::spawn_blocking(move || {
            let store = owner.lock().map_err(|_| StoreError::CorruptRecord)?;
            if !store.writable {
                return Err(StoreError::WriteDisabled);
            }
            let tx = store.database.begin_read()?;
            drop(store);
            let table = tx.open_table(crate::META)?;
            let Some(baseline) = checkpoint_baseline::read_metadata(&table)? else {
                return Ok(None);
            };
            let meta = selected_meta(&table, &baseline)?;
            let definitions = [
                checkpoint_baseline::HEADS,
                checkpoint_baseline::VERSIONS,
                checkpoint_baseline::EVENTS,
                checkpoint_baseline::OPERATIONS,
            ];
            let mut counts = [0; 4];
            for (i, definition) in definitions.iter().enumerate() {
                counts[i] = tx.open_table(*definition)?.len()?;
            }
            let out = snapshot::CapabilityOutput::new(Vec::new(), limits.snapshot.total_bytes);
            let mut encoder =
                snapshot::Encoder::new(out, &baseline.header, counts, limits.snapshot).map_err(|_| StoreError::CorruptRecord)?;
            for (definition, section) in definitions.into_iter().zip([
                snapshot::Section::Heads,
                snapshot::Section::Versions,
                snapshot::Section::Events,
                snapshot::Section::Operations,
            ]) {
                for row in tx.open_table(definition)?.iter()? {
                    let (key, value) = row?;
                    encoder
                        .row(snapshot::Row {
                            section,
                            key: key.value(),
                            value: value.value(),
                        })
                        .map_err(|_| StoreError::CorruptRecord)?;
                }
            }
            let artifact = encoder.finish().map_err(|_| StoreError::CorruptRecord)?.finish();
            let mut data = NativeSnapshotData::new(limits.snapshot.total_bytes + 56).map_err(|_| StoreError::ResourceLimit)?;
            data.write_all(&artifact.manifest.bytes)
                .map_err(|_| StoreError::ResourceLimit)?;
            data.write_all(&artifact.payload).map_err(|_| StoreError::ResourceLimit)?;
            data.rewind().map_err(|_| StoreError::CorruptRecord)?;
            Ok::<_, StoreError>(Some(Snapshot {
                meta,
                snapshot: Box::new(data),
            }))
        })
        .await
        .map_err(NativeSnapshotError::Worker)?
        .map_err(NativeSnapshotError::Store)
    }
}
fn failure(error: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::read_state_machine(openraft::AnyError::new(&error)).into()
}
impl RaftStateMachine<CaptureTypeConfig> for NativeRaftStateMachine {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<openraft::LogId<u64>>, StoredMembership<u64, crate::CaptureNode>), StorageError<u64>> {
        let (id, membership) = self.writer.applied_state().await.map_err(failure)?;
        Ok((id.map(Into::into), StoredMembership::try_from(&membership).map_err(failure)?))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CaptureResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = CaptureRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.writer
            .apply_committed(entries.into_iter().collect())
            .await
            .map_err(failure)
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<NativeSnapshotData>, StorageError<u64>> {
        let mut data = NativeSnapshotData::new(self.maximum()).map_err(failure)?;
        data.attach_receive_guard(ReceiveGuard::new(self.writer.snapshot_receives.clone()));
        Ok(Box::new(data))
    }
    async fn install_snapshot(&mut self, meta: &Meta, data: Box<NativeSnapshotData>) -> Result<(), StorageError<u64>> {
        crate::native_snapshot_install::install(self.writer.clone(), self.limits, meta.clone(), data)
            .await
            .map_err(failure)
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<CaptureTypeConfig>>, StorageError<u64>> {
        self.current().await.map_err(failure)
    }
}
impl RaftSnapshotBuilder<CaptureTypeConfig> for NativeRaftStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<CaptureTypeConfig>, StorageError<u64>> {
        #[cfg(test)]
        {
            let hook = self.build_before_ordering.lock().expect("build test hook").take();
            if let Some((entered, resume)) = hook {
                entered.notify_one();
                resume.notified().await;
            }
        }
        let limits = self.limits;
        let published_empty = self
            .writer
            .write(move |store| {
                let tx = store.write_transaction()?;
                let mut meta = tx.open_table(crate::META)?;
                // Decide the empty/nonempty path under the original writer ordering.
                if crate::application::read_applied(&meta)?.is_some() {
                    return Ok(false);
                }
                let identity = crate::decode_identity(meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())?;
                let membership =
                    crate::CaptureMembership::decode(meta.get("native_membership")?.ok_or(StoreError::CorruptRecord)?.value())?;
                let previous = checkpoint_baseline::read_metadata(&meta)?;
                let generation = previous.map_or(Ok(1), |v| v.generation.checked_add(1).ok_or(StoreError::ResourceLimit))?;
                let baseline = checkpoint_baseline::BaselineMetadata {
                    generation,
                    header: snapshot::Header {
                        cluster_id: identity.cluster_id,
                        applied: None,
                        membership,
                        binding: crate::application::read_binding(&meta)?,
                    },
                    limits,
                };
                for (live, base) in [
                    (crate::application::HEADS, checkpoint_baseline::HEADS),
                    (crate::application::VERSIONS, checkpoint_baseline::VERSIONS),
                    (crate::application::EVENTS, checkpoint_baseline::EVENTS),
                    (crate::application::OPERATIONS, checkpoint_baseline::OPERATIONS),
                ] {
                    if tx.open_table(live)?.len()? != 0 {
                        return Err(StoreError::CorruptRecord);
                    }
                    tx.open_table(base)?.retain(|_, _| false)?;
                }
                meta.insert("schema", 7u64.to_be_bytes().as_slice())?;
                meta.insert(checkpoint_baseline::META_KEY, checkpoint_baseline::encode_metadata(&baseline)?.as_slice())?;
                publish_local_meta(&mut meta, &baseline)?;
                drop(meta);
                store.commit(tx)?;
                Ok(true)
            })
            .await
            .map_err(failure)?;
        if !published_empty {
            self.writer.checkpoint_local(self.limits).await.map_err(failure)?;
        }
        self.current()
            .await
            .map_err(failure)?
            .ok_or_else(|| failure(StoreError::CorruptRecord))
    }
}

#[cfg(test)]
pub(crate) struct Waiter(Arc<ReceiveState>);
#[cfg(test)]
impl Waiter {
    pub(crate) fn new(state: Arc<ReceiveState>) -> Self {
        state.waiters.fetch_add(1, Ordering::AcqRel);
        Self(state)
    }
}
#[cfg(test)]
impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}
