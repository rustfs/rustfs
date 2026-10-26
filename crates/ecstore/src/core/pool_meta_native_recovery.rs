// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::pool_meta_recovery_codec as codec;
use super::*;
use crate::disk::{
    DiskAPI, DiskStore, PoolMetaNativeInspection, PoolMetaNativeObject, PoolMetaNativeRecoveryRequest as Request,
    PoolMetaNativeRecoveryResponse as Response,
};
use bytes::Bytes;

#[cfg(test)]
#[derive(Default)]
pub(super) struct NativeApplyPause {
    pub(super) reached: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static AFTER_NATIVE_APPLY: Arc<NativeApplyPause>;
}

struct InspectedPool {
    disks: Vec<DiskStore>,
    snapshots: Vec<PoolMetaNativeInspection>,
}

fn invalid() -> Error {
    Error::other("native pool metadata recovery requires one proven V3 transaction and complete matching disk evidence")
}

fn known_validation(source: Error, phase: &'static str) -> Error {
    Error::other(pool_metadata_error(
        crate::error::PoolMetadataFailure::RecoveryRequired,
        phase,
        Some(Arc::new(source)),
    ))
}

fn codec_validation(source: codec::PoolMetaCodecError, phase: &'static str) -> Error {
    // A supported transport may carry a valid layout outside this narrow
    // recovery codec. That is not evidence of corruption or a transaction fork.
    if matches!(source, codec::PoolMetaCodecError::UnsupportedLayout) {
        return Error::other(source);
    }
    known_validation(Error::other(source), phase)
}

fn latch_validation(state: &mut PoolMetaWriteState, error: Error) -> Error {
    if let Some(context) = error.pool_metadata_failure()
        && context.kind == crate::error::PoolMetadataFailure::RecoveryRequired
    {
        state.block_with_context(context.clone());
    }
    error
}

fn current(pool: &InspectedPool) -> Result<Vec<Bytes>> {
    pool.snapshots
        .iter()
        .map(|snapshot| snapshot.current.clone().ok_or_else(invalid))
        .collect()
}

async fn inspect(pools: &[Arc<Sets>], fence: &PoolMetaPersistenceFence<'_>) -> Result<Vec<InspectedPool>> {
    let mut inspected = Vec::with_capacity(pools.len());
    // Pool -> disk order is stable; all capability probes precede every mutation.
    for pool in pools {
        fence.ensure_held()?;
        let set = pool.get_disks_by_key(POOL_META_NAME);
        let disks = set
            .disks
            .read()
            .await
            .iter()
            .cloned()
            .map(|disk| disk.ok_or_else(invalid))
            .collect::<Result<Vec<_>>>()?;
        let probes = futures::stream::iter(disks.iter().cloned().map(|disk| async move {
            match disk.pool_meta_native_recovery(Request::Probe {}).await? {
                Response::Probed => Ok(()),
                _ => Err(invalid()),
            }
        }))
        .buffered(4)
        .collect::<Vec<Result<()>>>()
        .await;
        for result in probes {
            result?;
        }
        inspected.push(InspectedPool {
            disks,
            snapshots: Vec::new(),
        });
    }
    for pool in &mut inspected {
        fence.ensure_held()?;
        let results = futures::stream::iter(pool.disks.iter().cloned().map(|disk| async move {
            match disk
                .pool_meta_native_recovery(Request::Inspect {
                    object: PoolMetaNativeObject::PoolBin,
                })
                .await?
            {
                Response::Inspected(snapshot) if !snapshot.disk_id.is_nil() => Ok(snapshot),
                _ => Err(invalid()),
            }
        }))
        .buffered(4)
        .collect::<Vec<Result<PoolMetaNativeInspection>>>()
        .await;
        for result in results {
            pool.snapshots.push(result?);
        }
    }
    fence.ensure_held()?;
    Ok(inspected)
}

async fn validate_identities(pools: &[Arc<Sets>], canonical: &[u8], fence: &PoolMetaPersistenceFence<'_>) -> Result<()> {
    for pool in pools {
        fence.ensure_held()?;
        // Identity and pool.bin have independent placement hashes. Read every
        // member of the identity's actual set instead of assuming co-location.
        let set = pool.get_disks_by_key(POOL_META_IDENTITY_NAME);
        let disks = set
            .disks
            .read()
            .await
            .iter()
            .cloned()
            .map(|disk| disk.ok_or_else(invalid))
            .collect::<Result<Vec<_>>>()?;
        let results = futures::stream::iter(disks.into_iter().enumerate().map(|(index, disk)| async move {
            match disk.pool_meta_native_recovery(Request::InspectIdentity {}).await? {
                Response::IdentityInspected { disk_id, raw } if !disk_id.is_nil() => {
                    codec::validate_inline_pool_meta_shard(&raw, index, canonical)
                        .await
                        .map_err(|error| codec_validation(error, "native_identity_validation"))
                }
                _ => Err(invalid()),
            }
        }))
        .buffered(4)
        .collect::<Vec<Result<()>>>()
        .await;
        for result in results {
            result?;
        }
    }
    fence.ensure_held()
}

async fn plan(
    inspected: &[InspectedPool],
    canonical: &[u8],
    revision: PoolMetaRevision,
    identity: &[u8],
) -> Result<Vec<Vec<Bytes>>> {
    let mut pending = Vec::<Vec<u8>>::new();
    let mut predecessors = Vec::<Vec<u8>>::new();
    let mut decoded = Vec::<Vec<u8>>::new();
    let mut newest = OffsetDateTime::now_utc();
    for pool in inspected {
        let current = current(pool)?;
        for (index, snapshot) in pool.snapshots.iter().enumerate() {
            if let Some(local_identity) = snapshot.identity.as_ref() {
                codec::validate_inline_pool_meta_shard(local_identity, index, identity)
                    .await
                    .map_err(|error| codec_validation(error, "native_metadata_validation"))?;
            }
            for raw in std::iter::once(current[index].clone()).chain(snapshot.backups.iter().map(|backup| backup.data.clone())) {
                let info =
                    codec::parse_inline_info(&raw).map_err(|error| codec_validation(error, "native_metadata_validation"))?;
                newest = newest.max(info.mod_time.ok_or_else(invalid)?);
            }
        }
        let mut inventories = vec![current.clone()];
        let directories = pool
            .snapshots
            .iter()
            .flat_map(|snapshot| snapshot.backups.iter().map(|backup| backup.directory))
            .collect::<HashSet<_>>();
        for directory in directories {
            // A missing backup contributes its actual current shard only to
            // integrity reconstruction. It never invents backup evidence.
            inventories.push(
                pool.snapshots
                    .iter()
                    .enumerate()
                    .map(|(index, snapshot)| {
                        snapshot
                            .backups
                            .iter()
                            .find(|backup| backup.directory == directory)
                            .map(|backup| backup.data.clone())
                            .unwrap_or_else(|| current[index].clone())
                    })
                    .collect(),
            );
        }
        for raw in inventories {
            for generation in codec::decode_inline_pool_meta_replicas(&raw)
                .await
                .map_err(|error| codec_validation(error, "native_metadata_validation"))?
            {
                let payload = generation.to_vec();
                if !decoded.contains(&payload) {
                    decoded.push(payload);
                }
            }
        }
    }
    let mut obsolete = Vec::<Vec<u8>>::new();
    for payload in &decoded {
        match decode_pool_meta_replica(payload.clone()) {
            PoolMetaReplica::Valid {
                canonical: candidate,
                revision: candidate_revision,
                committed: false,
                previous: Some(previous),
                ..
            } if candidate_revision == revision && candidate == canonical => {
                if !pending.contains(payload) {
                    pending.push(payload.clone());
                }
                if !predecessors.contains(&previous.canonical) {
                    predecessors.push(previous.canonical);
                }
            }
            PoolMetaReplica::Valid { revision: older, .. }
                if older.is_generation_protocol()
                    && older.cluster_id == revision.cluster_id
                    && older.epoch == revision.epoch
                    && older.generation < revision.generation =>
            {
                // Retained evidence from an earlier successful recovery is
                // obsolete, not a competing commit authority. It remains
                // fully reconstructed and bound into the native CAS.
                obsolete.push(payload.clone());
            }
            PoolMetaReplica::Valid {
                canonical: candidate,
                revision: candidate_revision,
                committed: true,
                ..
            } if candidate_revision == revision && candidate == canonical => {}
            PoolMetaReplica::Valid { revision: conflict, .. } if conflict.is_generation_protocol() => {
                return Err(known_validation(
                    Error::other("native V3 transaction conflicts with committed authority"),
                    "native_transaction_validation",
                ));
            }
            PoolMetaReplica::Corrupt(_) | PoolMetaReplica::Incompatible(_) => {
                return Err(known_validation(
                    Error::other("native V3 payload is corrupt or incompatible"),
                    "native_transaction_validation",
                ));
            }
            _ => {}
        }
    }
    if pending.len() > 1 {
        return Err(known_validation(
            Error::other("native V3 prepare records disagree on the predecessor"),
            "native_transaction_validation",
        ));
    }
    if pending.is_empty() {
        return Err(invalid());
    }
    for payload in &decoded {
        if payload.as_slice() != canonical
            && !pending.contains(payload)
            && !predecessors.contains(payload)
            && !obsolete.contains(payload)
        {
            return Err(invalid());
        }
    }
    for pool in inspected {
        for (index, snapshot) in pool.snapshots.iter().enumerate() {
            let raw = snapshot.current.as_ref().ok_or_else(invalid)?;
            let mut matched = false;
            for payload in std::iter::once(canonical).chain(pending.iter().map(Vec::as_slice)) {
                matched |= codec::validate_inline_pool_meta_shard(raw, index, payload).await.is_ok();
            }
            if !matched {
                return Err(invalid());
            }
            for backup in &snapshot.backups {
                let mut matched = false;
                for payload in std::iter::once(canonical).chain(
                    pending
                        .iter()
                        .chain(predecessors.iter())
                        .chain(obsolete.iter())
                        .map(Vec::as_slice),
                ) {
                    matched |= codec::validate_inline_pool_meta_shard(&backup.data, index, payload)
                        .await
                        .is_ok();
                }
                if !matched {
                    return Err(invalid());
                }
            }
        }
    }
    let modified = newest.checked_add(Duration::nanoseconds(1)).ok_or_else(invalid)?;
    let mut replacements = Vec::with_capacity(inspected.len());
    for pool in inspected {
        replacements.push(
            codec::encode_inline_pool_meta_replicas(&current(pool)?, canonical, modified)
                .await
                .map_err(|error| codec_validation(error, "native_metadata_validation"))?,
        );
    }
    Ok(replacements)
}

pub(super) async fn repair(
    store: &ECStore,
    state: &mut PoolMetaWriteState,
    fence: &PoolMetaPersistenceFence<'_>,
) -> Result<bool> {
    fence.ensure_held()?;
    let reads = read_pool_meta_replicas(store.pools.clone(), true).await;
    if !reads
        .iter()
        .any(|read| matches!(read.replica, PoolMetaReplica::Unreadable(_)))
    {
        return Ok(false);
    }
    state.ensure_write_safe("native pool metadata recovery")?;
    if reads
        .iter()
        .any(|read| matches!(read.replica, PoolMetaReplica::Corrupt(_) | PoolMetaReplica::Incompatible(_)))
    {
        return Err(latch_validation(
            state,
            known_validation(Error::other("pool metadata is corrupt or incompatible"), "native_metadata_validation"),
        ));
    }
    if reads
        .iter()
        .any(|read| !matches!(read.replica, PoolMetaReplica::Valid { .. } | PoolMetaReplica::Unreadable(_)))
    {
        return Err(invalid());
    }
    // Only an ordinary, successfully read COMMIT grants authority. Rebuilding
    // two erasure shards below proves integrity, never a commit decision.
    let ordinary_commits = reads
        .iter()
        .filter_map(|read| match &read.replica {
            PoolMetaReplica::Valid {
                raw,
                canonical,
                revision,
                committed: true,
                ..
            } if revision.is_generation_protocol() && raw == canonical => Some((canonical.clone(), *revision)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if ordinary_commits.is_empty() {
        return Err(invalid());
    }
    let selection = select_pool_meta_replica_reads(reads)
        .map_err(|error| latch_validation(state, known_validation(error, "native_metadata_validation")))?;
    state
        .validate_selection(&selection)
        .map_err(|error| latch_validation(state, known_validation(error, "native_metadata_validation")))?;
    if selection.meta.pools.len() != store.pools.len()
        || selection
            .meta
            .pools
            .iter()
            .zip(&store.pools)
            .enumerate()
            .any(|(index, (meta, pool))| meta.id != index || meta.cmd_line != pool.endpoints.cmd_line)
    {
        return Err(invalid());
    }
    let canonical = selection.canonical.as_ref().ok_or_else(invalid)?;
    if !selection.revision.is_generation_protocol()
        || !ordinary_commits
            .iter()
            .any(|(raw, revision)| raw == canonical && *revision == selection.revision)
    {
        return Err(invalid());
    }
    let cluster_id = selection.revision.cluster_id.ok_or_else(invalid)?;
    if state.expected_cluster_id != Some(cluster_id) {
        return Err(invalid());
    }
    let identities = join_all(store.pools.iter().cloned().map(read_pool_meta_identity_replica)).await;
    if identities.iter().any(|read| {
        matches!(
            read.replica,
            PoolMetaIdentityReplica::Corrupt(_) | PoolMetaIdentityReplica::Incompatible(_)
        )
    }) {
        return Err(latch_validation(
            state,
            known_validation(Error::other("pool identity is corrupt or incompatible"), "native_identity_validation"),
        ));
    }
    if identities
        .iter()
        .any(|read| !matches!(read.replica, PoolMetaIdentityReplica::Valid(_)))
    {
        return Err(invalid());
    }
    let identity = select_pool_meta_identity(identities, cluster_id)
        .map_err(|error| latch_validation(state, known_validation(error, "native_identity_validation")))?;
    state
        .validate_identity(&identity)
        .map_err(|error| latch_validation(state, known_validation(error, "native_identity_validation")))?;
    let identity = identity.identity.ok_or_else(invalid)?;
    if !identity.initialized {
        return Err(invalid());
    }
    if identity.epoch != selection.revision.epoch {
        return Err(latch_validation(
            state,
            known_validation(Error::other("pool identity epoch conflicts with metadata"), "native_identity_validation"),
        ));
    }
    let identity_bytes = encode_pool_meta_identity(identity)?;
    let repair_started = std::time::Instant::now();
    validate_identities(&store.pools, &identity_bytes, fence)
        .await
        .map_err(|error| latch_validation(state, error))?;
    let inspected = inspect(&store.pools, fence).await?;
    let replacements = plan(&inspected, canonical, selection.revision, &identity_bytes)
        .await
        .map_err(|error| latch_validation(state, error))?;
    fence.ensure_held()?;
    let mut arm = state.arm_transaction();
    arm.phase = Some("native_repair_apply");
    for (pool, replacements) in inspected.iter().zip(replacements) {
        for ((disk, snapshot), replacement) in pool.disks.iter().zip(&pool.snapshots).zip(replacements) {
            fence.ensure_held()?;
            let request = Request::Apply {
                object: PoolMetaNativeObject::PoolBin,
                expected_disk_id: snapshot.disk_id,
                expected_current_sha256: snapshot.current.as_ref().map(|raw| Sha256::digest(raw).into()),
                expected_backup_inventory_sha256: snapshot.inventory_sha256()?,
                replacement,
            };
            match disk.pool_meta_native_recovery(request).await? {
                Response::Applied => {}
                _ => return Err(invalid()),
            }
        }
    }
    #[cfg(test)]
    if let Ok(pause) = AFTER_NATIVE_APPLY.try_with(Arc::clone) {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
    fence.ensure_held()?;
    let confirmed = load_pool_meta_replicas(store.pools.clone(), true).await?;
    if confirmed.canonical.as_ref() != Some(canonical)
        || confirmed.revision != selection.revision
        || !confirmed.replica_state.repair_write_safe
        || confirmed.replica_state.needs_repair
    {
        return Err(invalid());
    }
    // Re-enter the normal V3 conditional-write path, including native CAS,
    // full tail completion, and the final ordinary quorum readback.
    for (pool, token) in store.pools.iter().cloned().zip(&confirmed.cas_tokens) {
        save_pool_meta_object_cas(pool, POOL_META_NAME, canonical.clone(), token, fence, "native_repair_confirm", &mut arm)
            .await?;
    }
    let confirmed = load_pool_meta_replicas(store.pools.clone(), true).await?;
    if confirmed.canonical.as_ref() != Some(canonical)
        || confirmed.revision != selection.revision
        || confirmed.replica_state.needs_repair
        || !confirmed.replica_state.repair_write_safe
    {
        return Err(invalid());
    }
    validate_identities(&store.pools, &identity_bytes, fence)
        .await
        .map_err(|error| latch_validation(state, error))?;
    let identities = join_all(store.pools.iter().cloned().map(read_pool_meta_identity_replica)).await;
    if identities.iter().any(|read| {
        matches!(
            read.replica,
            PoolMetaIdentityReplica::Corrupt(_) | PoolMetaIdentityReplica::Incompatible(_)
        )
    }) {
        return Err(latch_validation(
            state,
            known_validation(Error::other("pool identity is corrupt or incompatible"), "native_identity_validation"),
        ));
    }
    if identities
        .iter()
        .any(|read| !matches!(read.replica, PoolMetaIdentityReplica::Valid(_)))
    {
        return Err(invalid());
    }
    let identity = select_pool_meta_identity(identities, cluster_id)
        .map_err(|error| latch_validation(state, known_validation(error, "native_identity_validation")))?;
    state
        .validate_identity(&identity)
        .map_err(|error| latch_validation(state, known_validation(error, "native_identity_validation")))?;
    if identity.identity.map(encode_pool_meta_identity).transpose()?.as_deref() != Some(identity_bytes.as_slice()) {
        return Err(latch_validation(
            state,
            known_validation(Error::other("native identity changed after repair"), "native_identity_validation"),
        ));
    }
    fence.ensure_held()?;
    arm.disarm();
    info!(
        event = EVENT_DECOMMISSION_STATE,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_POOLS,
        state = "pool_metadata_native_repaired",
        generation = selection.revision.generation,
        pools = inspected.len(),
        disks = inspected.iter().map(|pool| pool.disks.len()).sum::<usize>(),
        elapsed_ms = repair_started.elapsed().as_secs_f64() * 1000.0,
        "Pool metadata native replicas repaired"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use codec::{fixture_inline_pool_meta_replicas as replicas, fixture_split_inline_pool_meta_replicas as split};

    struct Fixture {
        pools: Vec<InspectedPool>,
        commit: Vec<u8>,
        pending: Vec<u8>,
        identity: Vec<u8>,
        revision: PoolMetaRevision,
        modified: OffsetDateTime,
    }

    async fn fixture() -> Fixture {
        let cluster_id = uuid::Uuid::new_v4();
        let revision = PoolMetaRevision {
            version: POOL_META_GENERATION_VERSION,
            cluster_id: Some(cluster_id),
            epoch: 1,
            generation: 1,
            transaction_id: Some(uuid::Uuid::new_v4()),
        };
        let meta = PoolMeta {
            version: POOL_META_GENERATION_VERSION,
            ..Default::default()
        };
        let previous_meta = PoolMeta {
            version: POOL_META_V1_VERSION,
            ..Default::default()
        };
        let previous = PoolMetaCommittedCandidate {
            canonical: previous_meta.encode_config_data_for_v2_gate(true).unwrap(),
            meta: previous_meta,
            revision: PoolMetaRevision::legacy(POOL_META_V1_VERSION),
        };
        let pending = encode_pool_meta_v3_envelope(&meta, revision, false, Some(&previous)).unwrap();
        let commit = encode_pool_meta_v3_envelope(&meta, revision, true, None).unwrap();
        let identity = encode_pool_meta_identity(PersistedPoolMetaIdentity {
            version: POOL_META_IDENTITY_VERSION,
            cluster_id,
            epoch: 1,
            initialized: true,
            fresh_bootstrap_nonce: None,
        })
        .unwrap();
        let modified = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let current_split = split(&pending, &commit, modified).await;
        let current_commit = replicas(&commit, modified + Duration::seconds(1)).await;
        let backups = replicas(&pending, modified).await;
        let identities = replicas(&identity, modified).await;
        let directory = uuid::Uuid::new_v4();
        let pools = [current_split.clone(), current_split, current_commit.clone(), current_commit]
            .into_iter()
            .map(|current| InspectedPool {
                disks: Vec::new(),
                snapshots: current
                    .into_iter()
                    .enumerate()
                    .map(|(index, current)| PoolMetaNativeInspection {
                        disk_id: uuid::Uuid::new_v4(),
                        identity: Some(identities[index].clone()),
                        current: Some(current),
                        backups: vec![crate::disk::PoolMetaNativeBackup {
                            directory,
                            data: backups[index].clone(),
                        }],
                    })
                    .collect(),
            })
            .collect();
        Fixture {
            pools,
            commit,
            pending,
            identity,
            revision,
            modified,
        }
    }

    #[tokio::test]
    async fn pool_meta_native_plan_restores_physical_split_with_fresh_common_identity() {
        let f = fixture().await;
        let repaired = plan(&f.pools, &f.commit, f.revision, &f.identity).await.unwrap();
        assert_eq!(repaired.len(), 4);
        for pool in repaired {
            assert_eq!(
                codec::decode_inline_pool_meta_replicas(&pool).await.unwrap(),
                vec![Bytes::copy_from_slice(&f.commit)]
            );
            let times = pool
                .iter()
                .map(|raw| codec::parse_inline_info(raw).unwrap().mod_time.unwrap())
                .collect::<HashSet<_>>();
            assert_eq!(times.len(), 1);
            assert!(*times.iter().next().unwrap() > f.modified + Duration::seconds(1));
        }
    }

    #[tokio::test]
    async fn pool_meta_native_plan_rejects_single_hidden_identity_fork() {
        let mut f = fixture().await;
        let wrong = replicas(b"different identity", f.modified).await;
        f.pools[0].snapshots[0].identity = Some(wrong[0].clone());
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_err());
    }

    #[tokio::test]
    async fn pool_meta_native_plan_rejects_hidden_higher_generation_and_unknown_backup() {
        let mut f = fixture().await;
        let higher_revision = PoolMetaRevision {
            generation: 2,
            transaction_id: Some(uuid::Uuid::new_v4()),
            ..f.revision
        };
        let higher = encode_pool_meta_v3_envelope(
            &PoolMeta {
                version: POOL_META_GENERATION_VERSION,
                ..Default::default()
            },
            higher_revision,
            true,
            None,
        )
        .unwrap();
        let wrong = replicas(&higher, f.modified + Duration::seconds(2)).await;
        f.pools[0].snapshots[0].current = Some(wrong[0].clone());
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_err());
        f.pools[0].snapshots[0].current = Some(replicas(&f.pending, f.modified).await[0].clone());
        f.pools[0].snapshots[0].backups[0].data = wrong[0].clone();
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_err());
    }

    #[tokio::test]
    async fn pool_meta_native_plan_accepts_retained_older_recovery_backups() {
        let mut f = fixture().await;
        let older_commit = f.commit.clone();
        let older_pending = f.pending.clone();
        let next = PoolMetaRevision {
            generation: 2,
            transaction_id: Some(uuid::Uuid::new_v4()),
            ..f.revision
        };
        let PoolMetaReplica::Valid { meta, .. } = decode_pool_meta_replica(older_commit.clone()) else {
            panic!("valid fixture")
        };
        let previous = PoolMetaCommittedCandidate {
            canonical: older_commit.clone(),
            meta: meta.clone(),
            revision: f.revision,
        };
        f.commit = encode_pool_meta_v3_envelope(&meta, next, true, None).unwrap();
        f.pending = encode_pool_meta_v3_envelope(&meta, next, false, Some(&previous)).unwrap();
        f.revision = next;
        let current = split(&f.pending, &f.commit, f.modified + Duration::seconds(2)).await;
        let old_commit = replicas(&older_commit, f.modified + Duration::seconds(1)).await;
        let current_pending = replicas(&f.pending, f.modified + Duration::seconds(2)).await;
        let new_directory = uuid::Uuid::new_v4();
        let old_directory = uuid::Uuid::new_v4();
        for pool in &mut f.pools {
            for (index, snapshot) in pool.snapshots.iter_mut().enumerate() {
                snapshot.current = Some(current[index].clone());
                snapshot.backups.push(crate::disk::PoolMetaNativeBackup {
                    directory: new_directory,
                    data: current_pending[index].clone(),
                });
                snapshot.backups.push(crate::disk::PoolMetaNativeBackup {
                    directory: old_directory,
                    data: old_commit[index].clone(),
                });
            }
        }
        assert!(older_pending != f.pending);
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_ok());
    }

    #[tokio::test]
    async fn pool_meta_native_plan_rejects_missing_or_corrupt_evidence() {
        let mut f = fixture().await;
        let before = f.pools[1].snapshots[2].current.take();
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_err());
        f.pools[1].snapshots[2].current = before;
        f.pools[1].snapshots[2].backups[0].data = Bytes::from_static(b"invalid metadata");
        assert!(plan(&f.pools, &f.commit, f.revision, &f.identity).await.is_err());
    }
}

#[cfg(all(test, feature = "test-util"))]
#[path = "pool_meta_native_integration_tests.rs"]
mod integration_tests;
