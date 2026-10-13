//! Validate isolated received bytes before one durable shared-owner publication.
use crate::{
    CaptureMembership, CheckpointLimits, NativeLogWriter, NativeSnapshotData, StoreError, application, checkpoint_baseline,
    native_checkpoint, snapshot,
};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Stage {
    database: redb::Database,
    _directory: tempfile::TempDir,
    baseline: checkpoint_baseline::BaselineMetadata,
}
fn stage(data: &NativeSnapshotData, limits: CheckpointLimits) -> Result<Stage, StoreError> {
    let bytes = data.as_slice();
    if u64::try_from(bytes.len()).map_err(|_| StoreError::ResourceLimit)?
        > limits.snapshot.total_bytes.checked_add(56).ok_or(StoreError::ResourceLimit)?
    {
        return Err(StoreError::ResourceLimit);
    }
    let manifest = snapshot::CapabilityManifest::decode(bytes.get(..56).ok_or(StoreError::CorruptRecord)?)
        .map_err(|_| StoreError::CorruptRecord)?;
    let directory = tempfile::tempdir()?;
    let database = native_checkpoint::stage_database(directory.path().join("received.redb"), limits.staging_bytes)?;
    let tx = database.begin_write()?;
    for definition in [
        checkpoint_baseline::HEADS,
        checkpoint_baseline::VERSIONS,
        checkpoint_baseline::EVENTS,
        checkpoint_baseline::OPERATIONS,
    ] {
        tx.open_table(definition)?;
    }
    let header = snapshot::decode_capability_artifact(
        snapshot::CapabilityArtifact {
            manifest,
            payload: &bytes[56..],
        },
        &mut native_checkpoint::StageSink(&tx),
        limits.snapshot,
    )
    .map_err(|_| StoreError::CorruptRecord)?;
    let baseline = checkpoint_baseline::BaselineMetadata {
        generation: 1,
        header,
        limits,
    };
    let encoded = checkpoint_baseline::encode_metadata(&baseline)?;
    {
        let mut meta = tx.open_table(crate::META)?;
        meta.insert("schema", 7u64.to_be_bytes().as_slice())?;
        meta.insert(checkpoint_baseline::META_KEY, encoded.as_slice())?;
    }
    tx.commit().map_err(StoreError::StagingCommit)?;
    checkpoint_baseline::validate_baseline_with_budget(&database.begin_read()?, &baseline, limits.staging_bytes)?;
    Ok(Stage {
        database,
        _directory: directory,
        baseline,
    })
}
pub(crate) async fn install(
    writer: NativeLogWriter,
    limits: CheckpointLimits,
    meta: openraft::SnapshotMeta<u64, crate::CaptureNode>,
    data: Box<NativeSnapshotData>,
) -> Result<(), StoreError> {
    if data
        .receive_guard
        .as_ref()
        .is_none_or(|guard| !guard.belongs_to(&writer.snapshot_receives))
    {
        return Err(StoreError::CorruptRecord);
    }
    let (mut stage, data) = tokio::task::spawn_blocking(move || Ok::<_, StoreError>((stage(&data, limits)?, data)))
        .await
        .map_err(|_| StoreError::CorruptRecord)??;
    if meta.last_log_id.map(crate::LogId::from) != stage.baseline.header.applied {
        return Err(StoreError::CorruptRecord);
    }
    if CaptureMembership::try_from(&meta.last_membership)? != stage.baseline.header.membership {
        return Err(StoreError::CorruptRecord);
    }
    let ordering = writer.ordering.clone().lock_owned().await;
    let cancel = Cancel(Arc::new(AtomicBool::new(false)));
    let cancelled = cancel.0.clone();
    tokio::task::spawn_blocking(move || {
        let _ordering = ordering;
        let _data = data;
        if cancelled.load(Ordering::Acquire) {
            return Err(StoreError::WriteDisabled);
        }
        let mut store = writer.store.lock().map_err(|_| StoreError::CorruptRecord)?;
        let read = store.database.begin_read()?;
        let old_meta = read.open_table(crate::META)?;
        let identity = crate::decode_identity(old_meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())?;
        if identity.cluster_id != stage.baseline.header.cluster_id
            || application::read_binding(&old_meta)? != stage.baseline.header.binding
        {
            return Err(StoreError::CorruptRecord);
        }
        let old_applied = application::read_applied(&old_meta)?;
        let old_logs = read.open_table(crate::LOGS)?;
        let committed = crate::committed::validate_committed(&old_meta, &old_logs)?;
        let cut = stage.baseline.header.applied;
        if old_applied.is_some_and(|id| cut.is_none_or(|cut| id.index > cut.index || (id.index == cut.index && id != cut))) {
            return Err(StoreError::CommittedRegression);
        }
        let compatible = match cut {
            None => true,
            Some(cut) => {
                let at_cut = old_logs
                    .get(cut.index)?
                    .map(|v| crate::decode_log(v.value()).map(|v| v.id))
                    .transpose()?
                    .or(checkpoint_baseline::read_baseline_cut(&old_meta)?.filter(|id| id.index == cut.index));
                if committed
                    .is_some_and(|id| id.index >= cut.index && (at_cut != Some(cut) || (id.index == cut.index && id != cut)))
                {
                    return Err(StoreError::CommittedRegression);
                }
                at_cut == Some(cut)
            }
        };
        let previous = checkpoint_baseline::read_metadata(&old_meta)?;
        stage.baseline.generation =
            previous.map_or(Ok(1), |old| old.generation.checked_add(1).ok_or(StoreError::ResourceLimit))?;
        drop(old_logs);
        drop(old_meta);
        drop(read);
        let source = stage.database.begin_read()?;
        let tx = store.write_transaction()?;
        for (baseline, live) in [
            (checkpoint_baseline::HEADS, application::HEADS),
            (checkpoint_baseline::VERSIONS, application::VERSIONS),
            (checkpoint_baseline::EVENTS, application::EVENTS),
            (checkpoint_baseline::OPERATIONS, application::OPERATIONS),
        ] {
            let table = source.open_table(baseline)?;
            for target in [baseline, live] {
                let mut target = tx.open_table(target)?;
                target.retain(|_, _| false)?;
                for row in table.iter()? {
                    let (key, value) = row?;
                    target.insert(key.value(), value.value())?;
                }
            }
        }
        {
            let mut index = tx.open_table(crate::source_page::EVENT_SEQUENCE)?;
            index.retain(|_, _| false)?;
        }
        for row in source.open_table(checkpoint_baseline::EVENTS)?.iter()? {
            let (key, value) = row?;
            let event = application::decode_event(value.value())?;
            crate::source_page::insert_event_index(
                &tx,
                event.decision_log_id,
                key.value().try_into().map_err(|_| StoreError::CorruptRecord)?,
            )?;
        }
        let tail = {
            let mut logs = tx.open_table(crate::LOGS)?;
            logs.retain(|index, _| compatible && cut.is_none_or(|cut| index > cut.index))?;
            logs.last()?
                .map(|(_, v)| crate::decode_log(v.value()).map(|v| v.id))
                .transpose()?
                .or(cut)
        };
        tx.open_table(application::APPLY_RESULTS)?.retain(|_, _| false)?;
        {
            let mut target = tx.open_table(crate::META)?;
            target.insert("schema", 7u64.to_be_bytes().as_slice())?;
            target.insert(
                checkpoint_baseline::META_KEY,
                checkpoint_baseline::encode_metadata(&stage.baseline)?.as_slice(),
            )?;
            let mut applied = vec![0];
            if let Some(cut) = cut {
                target.insert(checkpoint_baseline::PURGED_KEY, checkpoint_baseline::encode_purged(cut).as_slice())?;
                applied = vec![1];
                applied.extend(crate::encode_id(cut));
            } else {
                target.remove(checkpoint_baseline::PURGED_KEY)?;
            }
            target.insert("last_applied", applied.as_slice())?;
            target.insert("native_membership", stage.baseline.header.membership.encode()?.as_slice())?;
            if let Some(tail) = tail {
                target.insert("tail", crate::encode_id(tail).as_slice())?;
            } else {
                target.remove("tail")?;
            }
            let committed = committed.filter(|id| cut.is_none_or(|cut| id.index > cut.index)).or(cut);
            if let Some(committed) = committed {
                target.insert("committed", crate::encode_id(committed).as_slice())?;
            } else {
                target.remove("committed")?;
            }
            crate::raft_state_machine::publish_meta(&mut target, &stage.baseline, meta)?;
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(StoreError::WriteDisabled);
        }
        store.commit(tx)
    })
    .await
    .map_err(|_| StoreError::CorruptRecord)?
}
