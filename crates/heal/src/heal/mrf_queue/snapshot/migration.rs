// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Pending, owner-local legacy import. These paths are deliberately invisible
//! to the active snapshot reader and legacy consumer. Source revalidation is
//! not a writer freeze: no result here grants activation or reclamation rights.
//! All source bytes and inherited responsibilities survive admission/replay.

use super::{MANIFEST_LEN, Manifest, SnapshotError, read_bounded};
use crate::heal::RUSTFS_META_BUCKET;
use crate::heal::mrf_queue::{MRF_JOURNAL_PATH, MRF_SCOPED_JOURNAL_PATH, decode_one};
use crate::heal::storage_api::owner::{
    EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskStore,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

const PAYLOADS: [&str; 2] = [".heal-mrf-import-pending.0.bin", ".heal-mrf-import-pending.1.bin"];
const COMMITS: [&str; 2] = [".heal-mrf-import-commit.0.bin", ".heal-mrf-import-commit.1.bin"];
const MAX_DISKS: usize = 64;
const CLAIM: &str = ".heal-mrf-import-claim.bin";

/// Limits apply to the complete encoded candidate and all distinct raw records,
/// including inherited sources. Exceeding either preserves previous anchors.
#[derive(Clone, Copy, Debug)]
pub struct MigrationLimits {
    pub max_bytes: usize,
    pub max_records: usize,
    pub max_sources: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("MRF migration requires every configured, formatted local disk")]
    CoverageGap,
    #[error("MRF migration source changed; all recovery anchors are retained")]
    SourceChanged,
    #[error("MRF migration candidate is invalid")]
    Invalid,
    #[error("MRF migration has no responsibility evidence")]
    Empty,
    #[error("MRF migration conditional publication conflicted")]
    Conflict,
    #[error("MRF migration staging is claimed; interrupted claims require separately fenced recovery")]
    Claimed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum LegacyPath {
    Scoped,
    Mirror,
}

impl LegacyPath {
    fn path(self) -> &'static str {
        match self {
            Self::Scoped => MRF_SCOPED_JOURNAL_PATH,
            Self::Mirror => MRF_JOURNAL_PATH,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    disk_id: Uuid,
    path: LegacyPath,
    digest: [u8; 32],
    // None proves an observed absent path, distinct from a present empty file.
    bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingMigration {
    version: u8,
    sources: Vec<Source>,
    inherited: Vec<Source>,
}

impl PendingMigration {
    /// Raw, complete records. No scope/version normalization, attempts pruning,
    /// incarnation inference or task-success interpretation is performed.
    pub fn replay_records(&self, limits: MigrationLimits) -> Result<Vec<Vec<u8>>, MigrationError> {
        self.validate_limits(limits)?;
        let mut records = BTreeSet::new();
        for source in self.sources.iter().chain(&self.inherited) {
            if source.disk_id.is_nil() {
                return Err(MigrationError::Invalid);
            }
            let bytes = source.bytes.as_deref().unwrap_or_default();
            if <[u8; 32]>::from(Sha256::digest(bytes)) != source.digest {
                return Err(MigrationError::Invalid);
            }
            let mut offset = 0;
            while offset < bytes.len() {
                let (_, consumed) = decode_one(&bytes[offset..]).ok_or(MigrationError::Invalid)?;
                let end = offset.checked_add(consumed).ok_or(MigrationError::Invalid)?;
                records.insert(bytes[offset..end].to_vec());
                if records.len() > limits.max_records {
                    return Err(SnapshotError::TooLarge.into());
                }
                offset = end;
            }
        }
        Ok(records.into_iter().collect())
    }

    fn validate_limits(&self, limits: MigrationLimits) -> Result<(), MigrationError> {
        if self.version != 1 {
            return Err(SnapshotError::Unsupported.into());
        }
        if self.sources.is_empty() || self.sources.len() > MAX_DISKS * 2 {
            return Err(MigrationError::Invalid);
        }
        if self
            .sources
            .len()
            .checked_add(self.inherited.len())
            .is_none_or(|count| count > limits.max_sources)
        {
            return Err(SnapshotError::TooLarge.into());
        }
        // Bound the raw input before allocating the JSON representation.
        let total = self
            .sources
            .iter()
            .chain(&self.inherited)
            .try_fold(0usize, |total, source| {
                total
                    .checked_add(source.bytes.as_ref().map_or(0, Vec::len))
                    .and_then(|n| n.checked_add(128))
            })
            .ok_or(SnapshotError::TooLarge)?;
        if total > limits.max_bytes {
            return Err(SnapshotError::TooLarge.into());
        }
        Ok(())
    }

    fn encode(&self, limits: MigrationLimits) -> Result<Vec<u8>, MigrationError> {
        self.replay_records(limits)?;
        let bytes = serde_json::to_vec(self).map_err(|_| MigrationError::Invalid)?;
        if bytes.len() > limits.max_bytes {
            return Err(SnapshotError::TooLarge.into());
        }
        Ok(bytes)
    }

    async fn revalidate(&self, disks: &[EcstoreDiskStore], limits: MigrationLimits) -> Result<(), MigrationError> {
        let current = capture(disks, limits).await?;
        if current.sources != self.sources {
            return Err(MigrationError::SourceChanged);
        }
        Ok(())
    }
}

async fn configured_disks(disks: &[Option<EcstoreDiskStore>]) -> Result<Vec<EcstoreDiskStore>, MigrationError> {
    if disks.is_empty() || disks.len() > MAX_DISKS {
        return Err(MigrationError::CoverageGap);
    }
    let mut ordered = std::collections::BTreeMap::new();
    for disk in disks {
        let disk = disk.as_ref().ok_or(MigrationError::CoverageGap)?;
        if !EcstoreDiskAPI::is_local(disk.as_ref()) {
            return Err(MigrationError::CoverageGap);
        }
        let id = EcstoreDiskAPI::get_disk_id(disk.as_ref())
            .await
            .map_err(SnapshotError::Disk)?
            .filter(|id| !id.is_nil())
            .ok_or(MigrationError::CoverageGap)?;
        if ordered.insert(id, disk.clone()).is_some() {
            return Err(MigrationError::CoverageGap);
        }
    }
    Ok(ordered.into_values().collect())
}

async fn capture(disks: &[EcstoreDiskStore], limits: MigrationLimits) -> Result<PendingMigration, MigrationError> {
    let mut sources = Vec::with_capacity(disks.len() * 2);
    let mut identities = BTreeSet::new();
    let mut remaining = limits.max_bytes;
    for disk in disks {
        let id = EcstoreDiskAPI::get_disk_id(disk.as_ref())
            .await
            .map_err(SnapshotError::Disk)?
            .filter(|id| !id.is_nil())
            .ok_or(MigrationError::CoverageGap)?;
        if !identities.insert(id) {
            return Err(MigrationError::CoverageGap);
        }
        for path in [LegacyPath::Scoped, LegacyPath::Mirror] {
            // A missing metadata volume is a coverage gap, not an absent journal.
            let bytes = match EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, path.path()).await {
                Ok(reader) => {
                    let maximum = u64::try_from(remaining.checked_add(1).ok_or(SnapshotError::TooLarge)?)
                        .map_err(|_| SnapshotError::TooLarge)?;
                    let mut bytes = Vec::new();
                    reader
                        .take(maximum)
                        .read_to_end(&mut bytes)
                        .await
                        .map_err(SnapshotError::Read)?;
                    if bytes.len() > remaining {
                        return Err(SnapshotError::TooLarge.into());
                    }
                    Some(bytes)
                }
                Err(EcstoreDiskError::FileNotFound) => None,
                Err(error) => return Err(SnapshotError::Disk(error).into()),
            };
            remaining = remaining
                .checked_sub(bytes.as_ref().map_or(0, Vec::len))
                .ok_or(SnapshotError::TooLarge)?;
            let digest = Sha256::digest(bytes.as_deref().unwrap_or_default()).into();
            sources.push(Source {
                disk_id: id,
                path,
                digest,
                bytes,
            });
        }
    }
    sources.sort_by_key(|source| (source.disk_id, matches!(source.path, LegacyPath::Mirror)));
    let candidate = PendingMigration {
        version: 1,
        sources,
        inherited: Vec::new(),
    };
    candidate.encode(limits)?;
    Ok(candidate)
}

/// Inspect every configured source without merging v1 mirrors into a claimed
/// latest snapshot. A complete subset/superset is retained as pending evidence.
pub async fn capture_legacy_migration(
    disks: &[Option<EcstoreDiskStore>],
    limits: MigrationLimits,
) -> Result<PendingMigration, MigrationError> {
    capture(&configured_disks(disks).await?, limits).await
}

struct Staged {
    manifest: Manifest,
    candidate: PendingMigration,
    slot: usize,
}

type PayloadIdentity = (usize, [u8; 32]);

struct StagingLineage {
    latest: Option<Staged>,
    // Each collection has at most two identities per configured disk.
    committed_payloads: BTreeSet<PayloadIdentity>,
    orphaned_payloads: Vec<PayloadIdentity>,
}

fn payload_identity(payload: &[u8]) -> PayloadIdentity {
    (payload.len(), Sha256::digest(payload).into())
}

impl StagingLineage {
    fn validate_orphans(&self, retry_payload: Option<&[u8]>) -> Result<(), MigrationError> {
        let retry_identity = retry_payload.map(payload_identity);
        if self
            .orphaned_payloads
            .iter()
            .any(|identity| Some(*identity) != retry_identity && !self.committed_payloads.contains(identity))
        {
            return Err(MigrationError::Conflict);
        }
        Ok(())
    }
}

async fn read_staging_lineage(disks: &[EcstoreDiskStore], limits: MigrationLimits) -> Result<StagingLineage, MigrationError> {
    let mut selected: Option<Staged> = None;
    let mut identities = std::collections::BTreeMap::new();
    let mut orphaned_payloads = Vec::new();
    let mut committed_payloads = BTreeSet::new();
    for disk in disks {
        for slot in 0..2 {
            let result = async {
                let payload = read_bounded(disk, PAYLOADS[slot], limits.max_bytes).await?;
                let Some(bytes) = read_bounded(disk, COMMITS[slot], MANIFEST_LEN).await? else {
                    if let Some(payload) = payload.as_deref() {
                        orphaned_payloads.push(payload_identity(payload));
                    }
                    return Ok(None);
                };
                let manifest = match Manifest::decode(&bytes, limits.max_bytes) {
                    Ok(manifest) => manifest,
                    Err(SnapshotError::Unsupported) => return Err(SnapshotError::Unsupported.into()),
                    Err(error) => {
                        if let Some(payload) = payload.as_deref() {
                            orphaned_payloads.push(payload_identity(payload));
                            return Ok(None);
                        }
                        return Err(error.into());
                    }
                };
                let payload = payload.ok_or(MigrationError::Invalid)?;
                if payload.len() != manifest.payload_len || payload_identity(&payload).1 != manifest.payload_digest {
                    // A reused slot can hold the next retry payload while the
                    // old manifest still names the previous generation.
                    orphaned_payloads.push(payload_identity(&payload));
                    return Ok(None);
                }
                let candidate: PendingMigration = serde_json::from_slice(&payload).map_err(|_| MigrationError::Invalid)?;
                if candidate.encode(limits)? != payload || candidate.replay_records(limits)?.is_empty() {
                    return Err(MigrationError::Invalid);
                }
                Ok(Some(Staged {
                    manifest,
                    candidate,
                    slot,
                }))
            }
            .await;
            match result {
                Ok(Some(next)) => {
                    committed_payloads.insert((next.manifest.payload_len, next.manifest.payload_digest));
                    let identity = (next.manifest.owner, next.manifest.payload_digest);
                    if identities
                        .insert(next.manifest.sequence, identity)
                        .is_some_and(|old| old != identity)
                    {
                        return Err(MigrationError::Conflict);
                    }
                    if selected
                        .as_ref()
                        .is_none_or(|old| old.manifest.sequence < next.manifest.sequence)
                    {
                        selected = Some(next);
                    }
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(StagingLineage {
        latest: selected,
        committed_payloads,
        orphaned_payloads,
    })
}

async fn install(disk: &EcstoreDiskStore, path: &str, bytes: &[u8], limit: usize) -> Result<(), MigrationError> {
    let expected = read_bounded(disk, path, limit).await?.map(EcstoreDiskBytes::from);
    let result = EcstoreDiskAPI::compare_and_update_file(
        disk.as_ref(),
        RUSTFS_META_BUCKET,
        path,
        expected,
        Some(EcstoreDiskBytes::copy_from_slice(bytes)),
    )
    .await
    .map_err(SnapshotError::Disk)?;
    if result != EcstoreConditionalFileUpdate::Updated {
        return Err(MigrationError::Conflict);
    }
    Ok(())
}

/// Persist an explicitly requested pending import using the storage owner's CAS
/// (and its configured metadata durability). This does not freeze legacy ingress
/// or grant a durable-acceptance/GC receipt. Activation requires W14/W21 evidence.
pub async fn stage_legacy_migration(
    disks: &[Option<EcstoreDiskStore>],
    candidate: &PendingMigration,
    owner: Uuid,
    limits: MigrationLimits,
) -> Result<u64, MigrationError> {
    let disks = configured_disks(disks).await?;
    // Claim every configured disk in identity order. A crash/cancellation leaves
    // claims intact; a new process cannot guess that the old writer is fenced.
    let claim = EcstoreDiskBytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let mut claimed = Vec::new();
    let result = async {
        for disk in &disks {
            match EcstoreDiskAPI::compare_and_update_file(disk.as_ref(), RUSTFS_META_BUCKET, CLAIM, None, Some(claim.clone()))
                .await
                .map_err(SnapshotError::Disk)?
            {
                EcstoreConditionalFileUpdate::Updated => claimed.push(disk.clone()),
                _ => return Err(MigrationError::Claimed),
            }
        }
        stage_claimed(&disks, candidate, owner, limits).await
    }
    .await;
    let mut release_error = None;
    for disk in claimed {
        let release = async {
            #[cfg(test)]
            tests::interrupt_at(owner, tests::Boundary::Release).await?;
            let released =
                EcstoreDiskAPI::compare_and_update_file(disk.as_ref(), RUSTFS_META_BUCKET, CLAIM, Some(claim.clone()), None)
                    .await
                    .map_err(SnapshotError::Disk)?;
            if released != EcstoreConditionalFileUpdate::Updated {
                return Err(MigrationError::Claimed);
            }
            Ok(())
        }
        .await;
        if let Err(error) = release
            && release_error.is_none()
        {
            release_error = Some(error);
        }
    }
    if let Some(error) = release_error {
        return Err(error);
    }
    result
}

async fn stage_claimed(
    disks: &[EcstoreDiskStore],
    candidate: &PendingMigration,
    owner: Uuid,
    limits: MigrationLimits,
) -> Result<u64, MigrationError> {
    candidate.revalidate(disks, limits).await?;
    candidate.validate_limits(limits)?;
    let lineage = read_staging_lineage(disks, limits).await?;
    let previous = lineage.latest.as_ref();
    if previous.is_some_and(|old| old.manifest.owner != owner) {
        return Err(MigrationError::Conflict);
    }
    let mut candidate = candidate.clone();
    let key = |source: &Source| (source.disk_id, source.path, source.digest, source.bytes.is_some());
    let mut source_index = std::collections::HashMap::new();
    for (index, source) in candidate.sources.iter().chain(&candidate.inherited).enumerate() {
        if source_index.insert(key(source), index).is_some() {
            return Err(MigrationError::Invalid);
        }
    }
    if let Some(old) = previous {
        for source in old.candidate.sources.iter().chain(&old.candidate.inherited) {
            if let Some(index) = source_index.get(&key(source)).copied() {
                let existing = if index < candidate.sources.len() {
                    &candidate.sources[index]
                } else {
                    &candidate.inherited[index - candidate.sources.len()]
                };
                if existing != source {
                    return Err(MigrationError::Conflict);
                }
            } else {
                if source_index.len() >= limits.max_sources {
                    return Err(SnapshotError::TooLarge.into());
                }
                source_index.insert(key(source), candidate.sources.len() + candidate.inherited.len());
                candidate.inherited.push(source.clone());
            }
        }
    }
    if candidate.replay_records(limits)?.is_empty() {
        return Err(MigrationError::Empty);
    }
    let payload = candidate.encode(limits)?;
    // Exact retry comparison must include all inherited responsibilities.
    // Validating only the freshly captured sources would reject our own
    // interrupted successor payload before it can be completed.
    lineage.validate_orphans(Some(&payload))?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    let (sequence, slot) = previous.map_or((1, 0), |old| {
        if old.manifest.payload_digest == digest {
            (old.manifest.sequence, old.slot)
        } else {
            (old.manifest.sequence.saturating_add(1), 1 - old.slot)
        }
    });
    let manifest = Manifest::encode(owner, sequence, &payload)?;
    for disk in disks {
        install(disk, PAYLOADS[slot], &payload, limits.max_bytes).await?;
    }
    #[cfg(test)]
    tests::interrupt_at(owner, tests::Boundary::AfterPayload).await?;
    candidate.revalidate(disks, limits).await?;
    #[cfg(test)]
    tests::interrupt_at(owner, tests::Boundary::BeforeManifest).await?;
    for disk in disks {
        install(disk, COMMITS[slot], &manifest, MANIFEST_LEN).await?;
    }
    #[cfg(test)]
    tests::interrupt_at(owner, tests::Boundary::AfterManifest).await?;
    let readback = read_staging_lineage(disks, limits).await?;
    readback.validate_orphans(None)?;
    let recovered = readback.latest.ok_or(MigrationError::Invalid)?;
    if recovered.manifest.sequence != sequence || recovered.manifest.payload_digest != digest {
        return Err(MigrationError::Conflict);
    }
    recovered.candidate.revalidate(disks, limits).await?;
    #[cfg(test)]
    tests::interrupt_at(owner, tests::Boundary::AfterReadback).await?;
    Ok(sequence)
}

/// Reload pending obligations after process restart. Manager admission never
/// removes them. Missing/changed sources block migration, preserving all files.
pub async fn recover_pending_migration(
    disks: &[Option<EcstoreDiskStore>],
    limits: MigrationLimits,
) -> Result<Option<PendingMigration>, MigrationError> {
    let disks = configured_disks(disks).await?;
    let lineage = read_staging_lineage(&disks, limits).await?;
    lineage.validate_orphans(None)?;
    let Some(staged) = lineage.latest else {
        return Ok(None);
    };
    staged.candidate.revalidate(&disks, limits).await?;
    Ok(Some(staged.candidate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::mrf_queue::encode_intent;
    use crate::heal::{DiskOption, Endpoint, new_disk};
    use rustfs_common::mrf_channel::{MrfIntent, MrfKind, MrfScope};
    use std::sync::Arc;
    use tempfile::TempDir;

    const LIMITS: MigrationLimits = MigrationLimits {
        max_bytes: 64 * 1024,
        max_records: 100,
        max_sources: 64,
    };

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum Boundary {
        AfterPayload,
        BeforeManifest,
        AfterManifest,
        AfterReadback,
        Release,
    }

    static INTERRUPTIONS: std::sync::LazyLock<std::sync::Mutex<std::collections::BTreeMap<Uuid, Boundary>>> =
        std::sync::LazyLock::new(Default::default);

    type SourceChange = (EcstoreDiskStore, Vec<u8>);
    type SourceChangeMap = std::collections::BTreeMap<Uuid, SourceChange>;

    static SOURCE_CHANGES: std::sync::LazyLock<std::sync::Mutex<SourceChangeMap>> = std::sync::LazyLock::new(Default::default);

    pub(super) async fn interrupt_at(owner: Uuid, boundary: Boundary) -> Result<(), MigrationError> {
        if boundary == Boundary::AfterPayload {
            let change = SOURCE_CHANGES.lock().expect("source fault map").remove(&owner);
            if let Some((disk, bytes)) = change {
                source(&disk, &bytes).await;
            }
        }
        let mut interruptions = INTERRUPTIONS.lock().expect("fault map");
        if interruptions.get(&owner) == Some(&boundary) {
            interruptions.remove(&owner);
            return Err(SnapshotError::Read(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected migration boundary failure",
            ))
            .into());
        }
        Ok(())
    }

    fn record(object: &str, kind: MrfKind, scope: Option<MrfScope>) -> Vec<u8> {
        let mut bytes = Vec::new();
        assert!(encode_intent(
            &MrfIntent {
                bucket: Arc::from("bucket"),
                object: Arc::from(object),
                version_id: None,
                kind,
                scope,
                lease: None,
                enqueued_at_ms: 1,
                attempts: 255,
            },
            &mut bytes
        ));
        bytes
    }

    async fn disk(root: &TempDir, name: &str) -> EcstoreDiskStore {
        let path = root.path().join(name);
        std::fs::create_dir_all(&path).expect("create test disk");
        let mut endpoint = Endpoint::try_from(path.to_string_lossy().as_ref()).expect("disk endpoint");
        endpoint.set_idx = 0;
        endpoint.disk_idx = 0;
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("open disk");
        let created = EcstoreDiskAPI::make_volume(disk.as_ref(), RUSTFS_META_BUCKET).await;
        assert!(
            matches!(created, Ok(()) | Err(EcstoreDiskError::VolumeExists)),
            "metadata volume: {created:?}"
        );
        let id = Uuid::new_v4();
        let format = serde_json::json!({
            "version": "1", "format": "xl-single", "id": Uuid::new_v4(),
            "xl": { "version": "3", "this": id, "sets": [[id]], "distributionAlgo": "SIPMOD+PARITY" }
        });
        EcstoreDiskAPI::write_all(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            "format.json",
            serde_json::to_vec(&format).expect("format").into(),
        )
        .await
        .expect("format disk");
        assert_eq!(EcstoreDiskAPI::get_disk_id(disk.as_ref()).await.expect("formatted identity"), Some(id));
        disk
    }

    async fn source(disk: &EcstoreDiskStore, bytes: &[u8]) {
        // Legacy source paths predate the root-level COW control files.
        EcstoreDiskAPI::write_all(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            MRF_SCOPED_JOURNAL_PATH,
            EcstoreDiskBytes::copy_from_slice(bytes),
        )
        .await
        .expect("write legacy source");
    }

    #[tokio::test]
    async fn migration_subset_union_preserves_sources_across_reopen() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let a = record("a", MrfKind::PartialWrite, None);
        let b = record(
            "b",
            MrfKind::DecodeFailure,
            Some(MrfScope {
                pool_index: 0,
                set_index: 1,
            }),
        );
        source(&first, &a).await;
        source(&second, &[a.clone(), b.clone()].concat()).await;
        let disks = [Some(first.clone()), Some(second.clone())];
        let candidate = capture_legacy_migration(&disks, LIMITS)
            .await
            .expect("capture both complete sources");
        let reverse = capture_legacy_migration(&[Some(second.clone()), Some(first.clone())], LIMITS)
            .await
            .expect("reverse disk order");
        assert_eq!(
            candidate.encode(LIMITS).expect("candidate"),
            reverse.encode(LIMITS).expect("reversed candidate")
        );
        assert_eq!(candidate.replay_records(LIMITS).expect("raw records").len(), 2);
        let owner = Uuid::new_v4();
        assert_eq!(
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("stage candidate"),
            1
        );
        drop(candidate);
        let recovered = recover_pending_migration(&disks, LIMITS)
            .await
            .expect("restart read")
            .expect("pending import");
        // Reading or discarding a replay batch must not consume its stored anchor.
        let mut admitted = recovered.replay_records(LIMITS).expect("replay batch");
        admitted.pop();
        drop(admitted);
        assert_eq!(
            recover_pending_migration(&disks, LIMITS)
                .await
                .expect("second restart")
                .expect("anchor")
                .replay_records(LIMITS)
                .expect("records")
                .len(),
            2
        );
        assert_eq!(
            stage_legacy_migration(&disks, &recovered, owner, LIMITS)
                .await
                .expect("idempotent retry"),
            1
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(first.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                .await
                .expect("first source"),
            a
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(second.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                .await
                .expect("second source"),
            [a, b].concat()
        );
        assert!(
            super::super::read_committed(&[first, second], LIMITS.max_bytes)
                .await
                .expect("active reader")
                .is_none(),
            "pending import must not activate the production snapshot"
        );
    }

    #[tokio::test]
    async fn migration_source_change_and_capacity_failure_keep_old_commit() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        let a = record("a", MrfKind::PartialWrite, None);
        let b = record("b", MrfKind::PartialWrite, None);
        source(&disk, &a).await;
        let original = capture_legacy_migration(&disks, LIMITS).await.expect("initial capture");
        stage_legacy_migration(&disks, &original, owner, LIMITS)
            .await
            .expect("initial commit");
        let before = EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, COMMITS[0])
            .await
            .expect("old commit");
        source(&disk, &b).await;
        assert!(matches!(
            stage_legacy_migration(&disks, &original, owner, LIMITS).await,
            Err(MigrationError::SourceChanged)
        ));
        let next = capture_legacy_migration(&disks, LIMITS).await.expect("new source capture");
        assert!(matches!(
            stage_legacy_migration(
                &disks,
                &next,
                owner,
                MigrationLimits {
                    max_records: 1,
                    ..LIMITS
                }
            )
            .await,
            Err(MigrationError::Snapshot(SnapshotError::TooLarge))
        ));
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, COMMITS[0])
                .await
                .expect("retained commit"),
            before
        );
        assert_eq!(
            stage_legacy_migration(&disks, &next, owner, LIMITS)
                .await
                .expect("COW successor"),
            2
        );
        let records = recover_pending_migration(&disks, LIMITS)
            .await
            .expect("successor restart")
            .expect("successor")
            .replay_records(LIMITS)
            .expect("responsibilities");
        assert!(
            records.contains(&a) && records.contains(&b),
            "successor must inherit old source responsibilities"
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, COMMITS[0])
                .await
                .expect("previous slot retained"),
            before
        );
    }

    #[tokio::test]
    async fn migration_torn_inactive_payload_and_manifest_keep_previous_anchor() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        source(&disk, &record("a", MrfKind::PartialWrite, None)).await;
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
        stage_legacy_migration(&disks, &candidate, Uuid::new_v4(), LIMITS)
            .await
            .expect("initial commit");
        let previous = read_bounded(&disk, PAYLOADS[0], LIMITS.max_bytes).await.expect("old payload");
        for (path, bytes) in [
            (PAYLOADS[1], b"torn payload".as_slice()),
            (COMMITS[1], b"torn manifest".as_slice()),
        ] {
            install(&disk, path, bytes, LIMITS.max_bytes)
                .await
                .expect("interrupted inactive write");
            assert!(
                recover_pending_migration(&disks, LIMITS).await.is_err(),
                "unknown successor responsibility must block recovery"
            );
            assert_eq!(read_bounded(&disk, PAYLOADS[0], LIMITS.max_bytes).await.expect("old anchor"), previous);
        }
    }

    #[tokio::test]
    async fn migration_missing_corrupt_and_empty_sources_fail_closed() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        assert!(matches!(
            capture_legacy_migration(&[Some(disk.clone()), None], LIMITS).await,
            Err(MigrationError::CoverageGap)
        ));
        let empty = capture_legacy_migration(&[Some(disk.clone())], LIMITS)
            .await
            .expect("observed empty sources");
        assert!(matches!(
            stage_legacy_migration(&[Some(disk.clone())], &empty, Uuid::new_v4(), LIMITS).await,
            Err(MigrationError::Empty)
        ));
        source(&disk, b"corrupt").await;
        assert!(matches!(
            capture_legacy_migration(&[Some(disk)], LIMITS).await,
            Err(MigrationError::Invalid)
        ));
    }

    #[tokio::test]
    async fn migration_commit_boundaries_and_lost_response_recover_idempotently() {
        for boundary in [
            Boundary::AfterPayload,
            Boundary::BeforeManifest,
            Boundary::AfterManifest,
            Boundary::AfterReadback,
        ] {
            let root = TempDir::new().expect("test directory");
            let disk = disk(&root, "disk").await;
            let disks = [Some(disk.clone())];
            let bytes = record("a", MrfKind::PartialWrite, None);
            source(&disk, &bytes).await;
            let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
            let owner = Uuid::new_v4();
            INTERRUPTIONS.lock().expect("fault map").insert(owner, boundary);
            assert!(stage_legacy_migration(&disks, &candidate, owner, LIMITS).await.is_err());
            assert_eq!(
                EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                    .await
                    .expect("source survives interruption"),
                bytes
            );
            let recovery = recover_pending_migration(&disks, LIMITS).await;
            if matches!(boundary, Boundary::AfterManifest | Boundary::AfterReadback) {
                assert!(recovery.expect("committed restart").is_some());
            } else {
                assert!(
                    matches!(recovery, Err(MigrationError::Conflict)),
                    "uncommitted candidate is not a committed recovery result"
                );
            }
            assert_eq!(
                stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                    .await
                    .expect("retry interrupted stage"),
                1
            );
            assert_eq!(
                recover_pending_migration(&disks, LIMITS)
                    .await
                    .expect("restart after retry")
                    .expect("anchor")
                    .replay_records(LIMITS)
                    .expect("records"),
                vec![bytes]
            );
        }
    }

    #[tokio::test]
    async fn migration_interrupted_claim_does_not_authorize_takeover() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        source(&disk, &record("a", MrfKind::PartialWrite, None)).await;
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
        let owner = Uuid::new_v4();
        stage_legacy_migration(&disks, &candidate, owner, LIMITS)
            .await
            .expect("committed anchor");
        install(&disk, CLAIM, b"interrupted writer", 64)
            .await
            .expect("interrupted claim");
        assert!(matches!(
            stage_legacy_migration(&disks, &candidate, owner, LIMITS).await,
            Err(MigrationError::Claimed)
        ));
        assert_eq!(
            recover_pending_migration(&disks, LIMITS)
                .await
                .expect("recovery remains read-only")
                .expect("anchor")
                .replay_records(LIMITS)
                .expect("record")
                .len(),
            1
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, CLAIM)
                .await
                .expect("claim retained"),
            b"interrupted writer".as_slice()
        );
    }

    #[tokio::test]
    async fn migration_source_change_after_payload_prevents_manifest_publication() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        source(&disk, &record("a", MrfKind::PartialWrite, None)).await;
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
        let owner = Uuid::new_v4();
        let changed = record("b", MrfKind::PartialWrite, None);
        SOURCE_CHANGES
            .lock()
            .expect("source fault map")
            .insert(owner, (disk.clone(), changed.clone()));
        assert!(matches!(
            stage_legacy_migration(&disks, &candidate, owner, LIMITS).await,
            Err(MigrationError::SourceChanged)
        ));
        assert!(matches!(recover_pending_migration(&disks, LIMITS).await, Err(MigrationError::Conflict)));
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                .await
                .expect("changed source survives"),
            changed
        );
        assert!(
            read_bounded(&disk, PAYLOADS[0], LIMITS.max_bytes)
                .await
                .expect("candidate retained")
                .is_some()
        );
    }

    #[test]
    fn migration_raw_identity_preserves_kind_scope_and_nil_version() {
        let id = Uuid::new_v4();
        let mut variants = Vec::new();
        for kind in [MrfKind::PartialWrite, MrfKind::DecodeFailure] {
            for scope in [
                None,
                Some(MrfScope {
                    pool_index: 0,
                    set_index: 0,
                }),
                Some(MrfScope {
                    pool_index: 0,
                    set_index: 1,
                }),
            ] {
                variants.push(record("same", kind, scope));
            }
        }
        let mut nil = record("same", MrfKind::PartialWrite, None);
        nil[12] = 1;
        nil.splice(13..13, [0; 16]);
        let end = nil.len() - 4;
        let mut crc = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        crc.update(&nil[..end]);
        nil[end..].copy_from_slice(&u32::try_from(crc.finalize()).expect("CRC").to_le_bytes());
        variants.push(nil);
        let bytes = variants.concat();
        let candidate = PendingMigration {
            version: 1,
            sources: vec![Source {
                disk_id: id,
                path: LegacyPath::Scoped,
                digest: Sha256::digest(&bytes).into(),
                bytes: Some(bytes),
            }],
            inherited: Vec::new(),
        };
        let records = candidate.replay_records(LIMITS).expect("raw identities");
        assert_eq!(records.len(), variants.len());
        for variant in variants {
            assert!(records.contains(&variant));
        }
    }

    async fn slot_bytes(disk: &EcstoreDiskStore) -> Vec<Option<Vec<u8>>> {
        let mut bytes = Vec::new();
        for path in PAYLOADS.into_iter().chain(COMMITS) {
            bytes.push(read_bounded(disk, path, LIMITS.max_bytes).await.expect("slot bytes"));
        }
        bytes
    }

    #[tokio::test]
    async fn migration_damaged_newer_commit_never_overwrites_successor_responsibility() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        for name in ["a", "b"] {
            source(&disk, &record(name, MrfKind::PartialWrite, None)).await;
            let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("committed generation");
        }
        install(&disk, COMMITS[1], b"torn higher manifest", MANIFEST_LEN)
            .await
            .expect("manifest fault");
        source(&disk, &record("c", MrfKind::PartialWrite, None)).await;
        let before = slot_bytes(&disk).await;
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("latest legacy source");
        assert!(stage_legacy_migration(&disks, &candidate, owner, LIMITS).await.is_err());
        assert_eq!(slot_bytes(&disk).await, before, "the only payload containing b must not be overwritten");
    }

    #[tokio::test]
    async fn migration_lower_limits_never_fall_back_to_a_smaller_old_generation() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        for name in ["a", "b"] {
            source(&disk, &record(name, MrfKind::PartialWrite, None)).await;
            let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("committed generation");
        }
        source(&disk, &record("a", MrfKind::PartialWrite, None)).await;
        let before = slot_bytes(&disk).await;
        let first_len = before[0].as_ref().expect("first payload").len();
        for limits in [
            MigrationLimits {
                max_bytes: first_len,
                ..LIMITS
            },
            MigrationLimits {
                max_records: 1,
                ..LIMITS
            },
            MigrationLimits {
                max_sources: 2,
                ..LIMITS
            },
        ] {
            assert!(matches!(
                recover_pending_migration(&disks, limits).await,
                Err(MigrationError::Snapshot(SnapshotError::TooLarge))
            ));
            assert_eq!(slot_bytes(&disk).await, before);
        }
    }

    #[tokio::test]
    async fn migration_source_history_count_is_bounded_before_successor_write() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        let limits = MigrationLimits {
            max_sources: 3,
            ..LIMITS
        };
        let mut records = (0..10)
            .map(|n| record(&n.to_string(), MrfKind::PartialWrite, None))
            .collect::<Vec<_>>();
        for round in 0..3 {
            records.rotate_left(1);
            source(&disk, &records.concat()).await;
            let candidate = capture_legacy_migration(&disks, limits)
                .await
                .expect("bounded current sources");
            assert_eq!(candidate.replay_records(limits).expect("same responsibilities").len(), 10);
            let before = slot_bytes(&disk).await;
            let result = stage_legacy_migration(&disks, &candidate, owner, limits).await;
            if round < 2 {
                assert_eq!(result.expect("within source count"), round + 1);
            } else {
                assert!(matches!(result, Err(MigrationError::Snapshot(SnapshotError::TooLarge))));
                assert_eq!(slot_bytes(&disk).await, before);
            }
        }
    }

    #[tokio::test]
    async fn migration_empty_legacy_sources_still_inherit_prior_responsibilities() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        let bytes = record("a", MrfKind::PartialWrite, None);
        source(&disk, &bytes).await;
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("initial source");
        stage_legacy_migration(&disks, &candidate, owner, LIMITS)
            .await
            .expect("initial stage");
        EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            MRF_SCOPED_JOURNAL_PATH,
            Some(bytes.clone().into()),
            None,
        )
        .await
        .expect("legacy removes source");
        let empty = capture_legacy_migration(&disks, LIMITS)
            .await
            .expect("valid absent-source observation");
        assert!(empty.replay_records(LIMITS).expect("empty observation").is_empty());
        assert_eq!(
            stage_legacy_migration(&disks, &empty, owner, LIMITS)
                .await
                .expect("inherit earlier obligation"),
            2
        );
        assert_eq!(
            recover_pending_migration(&disks, LIMITS)
                .await
                .expect("restart")
                .expect("pending")
                .replay_records(LIMITS)
                .expect("inherited responsibility"),
            vec![bytes]
        );
    }

    #[tokio::test]
    async fn migration_release_failure_still_releases_other_owned_claims() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        source(&first, &record("a", MrfKind::PartialWrite, None)).await;
        source(&second, &record("a", MrfKind::PartialWrite, None)).await;
        let disks = [Some(first), Some(second)];
        let ordered = configured_disks(&disks).await.expect("disk order");
        let owner = Uuid::new_v4();
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
        INTERRUPTIONS.lock().expect("fault map").insert(owner, Boundary::Release);
        assert!(stage_legacy_migration(&disks, &candidate, owner, LIMITS).await.is_err());
        assert!(
            read_bounded(&ordered[0], CLAIM, 64)
                .await
                .expect("failed release remains claimed")
                .is_some()
        );
        assert!(
            read_bounded(&ordered[1], CLAIM, 64)
                .await
                .expect("later release attempted")
                .is_none()
        );
    }

    #[tokio::test]
    async fn migration_retry_repairs_missing_manifest_replica() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let disks = [Some(first.clone()), Some(second.clone())];
        let owner = Uuid::new_v4();
        for name in ["a", "b"] {
            let bytes = record(name, MrfKind::PartialWrite, None);
            source(&first, &bytes).await;
            source(&second, &bytes).await;
            let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture generation");
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("stage generation");
        }
        let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture");
        let committed = read_bounded(&second, COMMITS[1], MANIFEST_LEN)
            .await
            .expect("manifest")
            .expect("committed");
        EcstoreDiskAPI::compare_and_update_file(
            second.as_ref(),
            RUSTFS_META_BUCKET,
            COMMITS[1],
            Some(committed.clone().into()),
            None,
        )
        .await
        .expect("lost replica");
        assert_eq!(
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("retry missing replica"),
            2
        );
        assert_eq!(
            read_bounded(&second, COMMITS[1], MANIFEST_LEN)
                .await
                .expect("repaired manifest"),
            Some(committed)
        );
    }

    #[tokio::test]
    async fn migration_old_payload_orphan_matches_any_validated_replica() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let disks = [Some(first.clone()), Some(second)];
        let owner = Uuid::new_v4();
        for name in ["a", "b"] {
            let bytes = record(name, MrfKind::PartialWrite, None);
            for disk in disks.iter().flatten() {
                source(disk, &bytes).await;
            }
            let candidate = capture_legacy_migration(&disks, LIMITS).await.expect("capture generation");
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("stage generation");
        }
        let old_manifest = read_bounded(&first, COMMITS[0], MANIFEST_LEN)
            .await
            .expect("old manifest")
            .expect("gen1");
        assert_eq!(
            EcstoreDiskAPI::compare_and_update_file(
                first.as_ref(),
                RUSTFS_META_BUCKET,
                COMMITS[0],
                Some(old_manifest.into()),
                None
            )
            .await
            .expect("remove one old manifest"),
            EcstoreConditionalFileUpdate::Updated
        );
        let before = slot_bytes(&first).await;
        for ordered in [disks.clone(), [disks[1].clone(), disks[0].clone()]] {
            let recovered = recover_pending_migration(&ordered, LIMITS)
                .await
                .expect("older orphan has independent proof")
                .expect("gen2");
            assert_eq!(recovered.replay_records(LIMITS).expect("A and B obligations").len(), 2);
            let candidate = capture_legacy_migration(&ordered, LIMITS).await.expect("current B source");
            assert_eq!(
                stage_legacy_migration(&ordered, &candidate, owner, LIMITS)
                    .await
                    .expect("gen2 retry"),
                2
            );
            assert_eq!(
                slot_bytes(&first).await,
                before,
                "known older orphan must not cause fallback or overwrite"
            );
        }
    }

    #[tokio::test]
    async fn migration_successor_retry_validates_orphan_after_inheriting_previous_records() {
        for boundary in [Boundary::AfterPayload, Boundary::BeforeManifest] {
            let root = TempDir::new().expect("test directory");
            let disk = disk(&root, "disk").await;
            let disks = [Some(disk.clone())];
            let owner = Uuid::new_v4();
            let a = record("a", MrfKind::PartialWrite, None);
            let b = record("b", MrfKind::PartialWrite, None);
            source(&disk, &a).await;
            let original = capture_legacy_migration(&disks, LIMITS).await.expect("source A");
            stage_legacy_migration(&disks, &original, owner, LIMITS).await.expect("gen1");
            let old_slot = slot_bytes(&disk).await;
            source(&disk, &b).await;
            let next = capture_legacy_migration(&disks, LIMITS).await.expect("source B");
            INTERRUPTIONS.lock().expect("fault map").insert(owner, boundary);
            assert!(stage_legacy_migration(&disks, &next, owner, LIMITS).await.is_err());
            assert!(
                matches!(recover_pending_migration(&disks, LIMITS).await, Err(MigrationError::Conflict)),
                "uncommitted AB is not silently accepted as A"
            );
            let captured_again = capture_legacy_migration(&disks, LIMITS)
                .await
                .expect("restart source capture contains only B");
            assert_eq!(captured_again.replay_records(LIMITS).expect("current source"), vec![b.clone()]);
            assert_eq!(
                stage_legacy_migration(&disks, &captured_again, owner, LIMITS)
                    .await
                    .expect("retry must compare inherited AB"),
                2
            );
            let recovered = recover_pending_migration(&disks, LIMITS)
                .await
                .expect("committed restart")
                .expect("gen2");
            let records = recovered.replay_records(LIMITS).expect("retained A and B");
            assert_eq!(records.len(), 2);
            assert!(records.contains(&a) && records.contains(&b));
            let after = slot_bytes(&disk).await;
            assert_eq!(after[0], old_slot[0]);
            assert_eq!(after[2], old_slot[2]);
        }
    }

    #[tokio::test]
    async fn migration_third_generation_retry_keeps_reused_slot_recoverable() {
        for boundary in [Boundary::AfterPayload, Boundary::BeforeManifest] {
            let root = TempDir::new().expect("test directory");
            let disk = disk(&root, "disk").await;
            let disks = [Some(disk.clone())];
            let owner = Uuid::new_v4();
            let records = ["a", "b", "c"]
                .into_iter()
                .map(|name| record(name, MrfKind::PartialWrite, None))
                .collect::<Vec<_>>();

            for bytes in &records[..2] {
                source(&disk, bytes).await;
                let candidate = capture_legacy_migration(&disks, LIMITS)
                    .await
                    .expect("capture committed generation");
                stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                    .await
                    .expect("stage committed generation");
            }

            source(&disk, &records[2]).await;
            let third = capture_legacy_migration(&disks, LIMITS)
                .await
                .expect("capture third generation");
            INTERRUPTIONS.lock().expect("fault map").insert(owner, boundary);
            assert!(stage_legacy_migration(&disks, &third, owner, LIMITS).await.is_err());
            assert!(matches!(recover_pending_migration(&disks, LIMITS).await, Err(MigrationError::Conflict)));

            let retry = capture_legacy_migration(&disks, LIMITS)
                .await
                .expect("recapture third generation");
            assert_eq!(
                stage_legacy_migration(&disks, &retry, owner, LIMITS)
                    .await
                    .expect("retry third generation"),
                3
            );
            let recovered = recover_pending_migration(&disks, LIMITS)
                .await
                .expect("recover after third retry")
                .expect("third generation");
            let replayed = recovered.replay_records(LIMITS).expect("all staged responsibilities");
            assert_eq!(replayed.len(), 3);
            for record in &records {
                assert!(replayed.contains(record));
            }
        }
    }

    #[tokio::test]
    async fn migration_third_generation_source_change_retry_keeps_reused_slot_recoverable() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let disks = [Some(disk.clone())];
        let owner = Uuid::new_v4();
        let records = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| record(name, MrfKind::PartialWrite, None))
            .collect::<Vec<_>>();

        for bytes in &records[..2] {
            source(&disk, bytes).await;
            let candidate = capture_legacy_migration(&disks, LIMITS)
                .await
                .expect("capture committed generation");
            stage_legacy_migration(&disks, &candidate, owner, LIMITS)
                .await
                .expect("stage committed generation");
        }

        source(&disk, &records[2]).await;
        let third = capture_legacy_migration(&disks, LIMITS)
            .await
            .expect("capture third generation");
        SOURCE_CHANGES
            .lock()
            .expect("source fault map")
            .insert(owner, (disk.clone(), records[3].clone()));
        assert!(matches!(
            stage_legacy_migration(&disks, &third, owner, LIMITS).await,
            Err(MigrationError::SourceChanged)
        ));
        assert!(matches!(recover_pending_migration(&disks, LIMITS).await, Err(MigrationError::Conflict)));

        source(&disk, &records[2]).await;
        let retry = capture_legacy_migration(&disks, LIMITS)
            .await
            .expect("recapture restored third generation");
        assert_eq!(
            stage_legacy_migration(&disks, &retry, owner, LIMITS)
                .await
                .expect("retry restored third generation"),
            3
        );
        let recovered = recover_pending_migration(&disks, LIMITS)
            .await
            .expect("recover after restored third retry")
            .expect("third generation");
        let replayed = recovered.replay_records(LIMITS).expect("all staged responsibilities");
        assert_eq!(replayed.len(), 3);
        for record in &records[..3] {
            assert!(replayed.contains(record));
        }
        assert!(!replayed.contains(&records[3]));
    }
}
