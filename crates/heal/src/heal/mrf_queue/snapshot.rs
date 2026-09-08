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

//! Reader-first support for owner-local MRF checkpoints.
//!
//! Each of two slots has a payload and a commit manifest. The manifest binds
//! the writer identity, persistent sequence, length and whole-payload digest.
//! Replacing the inactive slot must leave the previous committed slot intact.
//! Production publication and reclamation are deliberately not enabled here.
//! An unreadable commit path cannot prove that only legacy data exists. This
//! explicit inspection API fails closed and never mutates recovery anchors.
//! It is not wired into the legacy consumer: that transition requires the
//! ownership-aware replay and producer handoff before writer activation.
//! One surviving committed replica supports process restart recovery only;
//! this reader does not establish a replication quorum or a power-loss policy.

use super::{MRF_JOURNAL_PATH, MRF_SCOPED_JOURNAL_PATH, decode_journal};
use crate::heal::RUSTFS_META_BUCKET;
use crate::heal::storage_api::owner::{EcstoreDiskAPI, EcstoreDiskError, EcstoreDiskStore};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

/// Explicit pending migration; never activates the production writer or GC.
pub mod migration;

// Root-level control files avoid requiring a new directory before the first
// atomic commit. They remain inside the storage owner's metadata volume.
const PAYLOAD_PATHS: [&str; 2] = [".heal-mrf-snapshot.0.bin", ".heal-mrf-snapshot.1.bin"];
const MANIFEST_PATHS: [&str; 2] = [".heal-mrf-commit.0.bin", ".heal-mrf-commit.1.bin"];
const MAGIC: &[u8; 8] = b"RFMRFC01";
const MANIFEST_LEN: usize = 8 + 1 + 16 + 8 + 8 + 32 + 32;
const VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("MRF checkpoint has an invalid or incomplete commit record")]
    Corrupt,
    #[error("MRF checkpoint format is unsupported")]
    Unsupported,
    #[error("MRF checkpoint exceeds the configured byte limit")]
    TooLarge,
    #[error("MRF checkpoint replicas disagree at the same sequence")]
    Conflict,
    #[error("MRF checkpoint storage is unavailable")]
    Disk(#[source] EcstoreDiskError),
    #[error("MRF checkpoint body could not be read")]
    Read(#[source] std::io::Error),
}

#[derive(Debug, PartialEq, Eq)]
struct Manifest {
    owner: Uuid,
    sequence: u64,
    payload_len: usize,
    payload_digest: [u8; 32],
}

impl Manifest {
    fn encode(owner: Uuid, sequence: u64, payload: &[u8]) -> Result<Vec<u8>, SnapshotError> {
        let mut bytes = Vec::with_capacity(MANIFEST_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(owner.as_bytes());
        bytes.extend_from_slice(&sequence.to_le_bytes());
        bytes.extend_from_slice(
            &u64::try_from(payload.len())
                .map_err(|_| SnapshotError::TooLarge)?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&Sha256::digest(payload));
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        Self::decode(&bytes, payload.len())?;
        Ok(bytes)
    }

    fn decode(bytes: &[u8], limit: usize) -> Result<Self, SnapshotError> {
        if bytes.len() != MANIFEST_LEN || &bytes[..8] != MAGIC {
            return Err(SnapshotError::Corrupt);
        }
        if bytes[8] != VERSION {
            return Err(SnapshotError::Unsupported);
        }
        let signed = MANIFEST_LEN - 32;
        let checksum: [u8; 32] = Sha256::digest(&bytes[..signed]).into();
        if checksum != bytes[signed..] {
            return Err(SnapshotError::Corrupt);
        }
        let owner = Uuid::from_slice(&bytes[9..25]).map_err(|_| SnapshotError::Corrupt)?;
        let sequence = u64::from_le_bytes(bytes[25..33].try_into().map_err(|_| SnapshotError::Corrupt)?);
        let payload_len = u64::from_le_bytes(bytes[33..41].try_into().map_err(|_| SnapshotError::Corrupt)?);
        let payload_len = usize::try_from(payload_len).map_err(|_| SnapshotError::TooLarge)?;
        if owner.is_nil() || sequence == 0 || sequence == u64::MAX {
            return Err(SnapshotError::Corrupt);
        }
        if payload_len > limit {
            return Err(SnapshotError::TooLarge);
        }
        Ok(Self {
            owner,
            sequence,
            payload_len,
            payload_digest: bytes[41..73].try_into().map_err(|_| SnapshotError::Corrupt)?,
        })
    }
}

#[derive(Debug)]
pub struct CommittedSnapshot {
    manifest: Manifest,
    payload: Vec<u8>,
}

#[derive(Default)]
struct SnapshotReadStats {
    file_reads: usize,
    bytes_read: usize,
    peak_file_bytes: usize,
}

impl CommittedSnapshot {
    /// Persistent single-writer sequence, not a process UUID ordering.
    pub fn sequence(&self) -> u64 {
        self.manifest.sequence
    }

    /// Identity recorded by the committed checkpoint's writer.
    pub fn owner(&self) -> Uuid {
        self.manifest.owner
    }

    /// Complete, checksum-validated record bytes. Inspection does not consume
    /// these records or acknowledge completion to any producer.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn decode(manifest: &[u8], payload: Vec<u8>, limit: usize) -> Result<Self, SnapshotError> {
        let manifest = Manifest::decode(manifest, limit)?;
        let checksum: [u8; 32] = Sha256::digest(&payload).into();
        if payload.len() != manifest.payload_len || checksum != manifest.payload_digest {
            return Err(SnapshotError::Corrupt);
        }
        if decode_journal(&payload).1 != 0 {
            return Err(SnapshotError::Corrupt);
        }
        Ok(Self { manifest, payload })
    }
}

#[derive(Debug)]
pub enum RecoverySnapshot {
    /// An intact legacy snapshot, without a comparable commit sequence.
    Legacy(Vec<u8>),
    /// A committed checkpoint requiring ownership-aware replay before use.
    Committed(CommittedSnapshot),
}

async fn read_bounded(disk: &EcstoreDiskStore, path: &str, limit: usize) -> Result<Option<Vec<u8>>, SnapshotError> {
    read_bounded_with_stats(disk, path, limit, None).await
}

async fn read_bounded_with_stats(
    disk: &EcstoreDiskStore,
    path: &str,
    limit: usize,
    mut stats: Option<&mut SnapshotReadStats>,
) -> Result<Option<Vec<u8>>, SnapshotError> {
    let reader = match EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, path).await {
        Ok(reader) => reader,
        Err(EcstoreDiskError::FileNotFound | EcstoreDiskError::VolumeNotFound) => return Ok(None),
        Err(error) => return Err(SnapshotError::Disk(error)),
    };
    let maximum = limit.checked_add(1).ok_or(SnapshotError::TooLarge)?;
    let maximum = u64::try_from(maximum).map_err(|_| SnapshotError::TooLarge)?;
    let mut bytes = Vec::new();
    reader
        .take(maximum)
        .read_to_end(&mut bytes)
        .await
        .map_err(SnapshotError::Read)?;
    if bytes.len() > limit {
        return Err(SnapshotError::TooLarge);
    }
    if let Some(stats) = stats.as_mut() {
        stats.file_reads += 1;
        stats.bytes_read = stats.bytes_read.checked_add(bytes.len()).ok_or(SnapshotError::TooLarge)?;
        stats.peak_file_bytes = stats.peak_file_bytes.max(bytes.len());
    }
    Ok(Some(bytes))
}

fn select_snapshot(selected: &mut Option<CommittedSnapshot>, candidate: CommittedSnapshot) -> Result<(), SnapshotError> {
    if let Some(current) = selected {
        if current.manifest.sequence == candidate.manifest.sequence
            && (current.manifest != candidate.manifest || current.payload != candidate.payload)
        {
            return Err(SnapshotError::Conflict);
        }
        if current.manifest.sequence >= candidate.manifest.sequence {
            return Ok(());
        }
    }
    *selected = Some(candidate);
    Ok(())
}

async fn read_committed(disks: &[EcstoreDiskStore], limit: usize) -> Result<Option<CommittedSnapshot>, SnapshotError> {
    read_committed_with_stats(disks, limit, None).await
}

async fn read_committed_with_stats(
    disks: &[EcstoreDiskStore],
    limit: usize,
    mut stats: Option<&mut SnapshotReadStats>,
) -> Result<Option<CommittedSnapshot>, SnapshotError> {
    let mut selected = None;
    let mut damaged = None;
    let mut identities = HashMap::new();
    for disk in disks {
        for (manifest_path, payload_path) in MANIFEST_PATHS.into_iter().zip(PAYLOAD_PATHS) {
            let candidate = async {
                let Some(manifest) = read_bounded_with_stats(disk, manifest_path, MANIFEST_LEN, stats.as_deref_mut()).await?
                else {
                    return Ok(None);
                };
                let header = Manifest::decode(&manifest, limit)?;
                let payload = read_bounded_with_stats(disk, payload_path, header.payload_len, stats.as_deref_mut())
                    .await?
                    .ok_or(SnapshotError::Corrupt)?;
                CommittedSnapshot::decode(&manifest, payload, limit).map(Some)
            }
            .await;
            match candidate {
                Ok(Some(candidate)) => {
                    let identity = (
                        candidate.manifest.owner,
                        candidate.manifest.payload_len,
                        candidate.manifest.payload_digest,
                    );
                    if identities
                        .insert(candidate.manifest.sequence, identity)
                        .is_some_and(|previous| previous != identity)
                    {
                        return Err(SnapshotError::Conflict);
                    }
                    select_snapshot(&mut selected, candidate)?;
                }
                Ok(None) => {}
                // A future committed format may supersede all readable slots.
                Err(SnapshotError::Unsupported) => return Err(SnapshotError::Unsupported),
                Err(error) => damaged = Some(error),
            }
        }
    }
    match (selected, damaged) {
        (Some(snapshot), _) => Ok(Some(snapshot)),
        (None, Some(error)) => Err(error),
        (None, None) => Ok(None),
    }
}

async fn read_legacy(disks: &[EcstoreDiskStore], path: &str, limit: usize) -> Result<Option<Vec<u8>>, SnapshotError> {
    let mut selected = None;
    let mut incomplete: Option<Vec<u8>> = None;
    for disk in disks {
        match read_bounded(disk, path, limit).await {
            Ok(Some(payload)) if decode_journal(&payload).1 == 0 => {
                if selected.as_ref().is_some_and(|current| *current != payload) {
                    // Legacy snapshots have no sequence. There is no evidence
                    // that the first, longest or nonempty replica is newest.
                    return Err(SnapshotError::Conflict);
                }
                selected = Some(payload);
            }
            Ok(Some(payload)) => {
                if let Some(previous) = &incomplete {
                    if previous.starts_with(&payload) {
                        continue;
                    }
                    if !payload.starts_with(previous) {
                        return Err(SnapshotError::Corrupt);
                    }
                }
                incomplete = Some(payload);
            }
            Ok(None) => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(prefix) = incomplete
        && !selected.as_ref().is_some_and(|payload| payload.starts_with(&prefix))
    {
        // In particular, an empty O_TRUNC replica cannot supersede another
        // replica containing intact records followed by a torn tail.
        return Err(SnapshotError::Corrupt);
    }
    Ok(selected)
}

/// Inspect local MRF checkpoints without replaying, acknowledging or deleting.
///
/// `max_bytes` bounds each payload read. Every local replica is examined and
/// ambiguous identities, unavailable proof or unsupported formats return a
/// typed error. This API must not authorize a writer without the separate
/// ownership and mixed-version activation checks.
pub async fn inspect_local_recovery_snapshot(max_bytes: usize) -> Result<Option<RecoverySnapshot>, SnapshotError> {
    read_recovery_snapshot(&super::journal_disks().await, max_bytes).await
}

async fn read_recovery_snapshot(disks: &[EcstoreDiskStore], limit: usize) -> Result<Option<RecoverySnapshot>, SnapshotError> {
    if let Some(snapshot) = read_committed(disks, limit).await? {
        return Ok(Some(RecoverySnapshot::Committed(snapshot)));
    }
    // RUSTFS_COMPAT_TODO(backlog-2263): inspect retained legacy MRF journals. Remove after all supported upgrade and rollback readers understand committed snapshots and retained journals have migrated.
    if let Some(payload) = read_legacy(disks, MRF_SCOPED_JOURNAL_PATH, limit).await? {
        return Ok(Some(RecoverySnapshot::Legacy(payload)));
    }
    Ok(read_legacy(disks, MRF_JOURNAL_PATH, limit)
        .await?
        .map(RecoverySnapshot::Legacy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::mrf_queue::encode_intent;
    use crate::heal::storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskBytes};
    use crate::heal::{DiskOption, Endpoint, new_disk};
    use rustfs_common::mrf_channel::{MrfIntent, MrfKind, MrfScope};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn payload(object: &str) -> Vec<u8> {
        let intent = MrfIntent {
            bucket: Arc::from("bucket"),
            object: Arc::from(object),
            version_id: None,
            kind: MrfKind::PartialWrite,
            scope: None,
            lease: None,
            enqueued_at_ms: 1234,
            attempts: 0,
        };
        let mut bytes = Vec::new();
        assert!(encode_intent(&intent, &mut bytes), "fixture must encode a full record");
        bytes
    }

    fn manifest(owner: Uuid, sequence: u64, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MANIFEST_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(owner.as_bytes());
        bytes.extend_from_slice(&sequence.to_le_bytes());
        bytes.extend_from_slice(&u64::try_from(payload.len()).expect("fixture length fits").to_le_bytes());
        bytes.extend_from_slice(&Sha256::digest(payload));
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        bytes
    }

    async fn disk(root: &TempDir, name: &str) -> EcstoreDiskStore {
        let path = root.path().join(name);
        std::fs::create_dir_all(&path).expect("create disk directory");
        let endpoint = Endpoint::try_from(path.to_string_lossy().as_ref()).expect("valid disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("open disk");
        let result = EcstoreDiskAPI::make_volume(disk.as_ref(), RUSTFS_META_BUCKET).await;
        assert!(
            matches!(result, Ok(()) | Err(EcstoreDiskError::VolumeExists)),
            "metadata volume: {result:?}"
        );
        disk
    }

    // Exercise the existing storage owner's atomic CAS primitive. No production
    // caller publishes this format until ownership-aware replay is available.
    async fn install(disk: &EcstoreDiskStore, path: &str, bytes: &[u8]) {
        let expected = EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, path).await.ok();
        let result = EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            path,
            expected,
            Some(EcstoreDiskBytes::copy_from_slice(bytes)),
        )
        .await
        .expect("atomic snapshot slot write");
        assert_eq!(result, EcstoreConditionalFileUpdate::Updated);
    }

    async fn commit(disk: &EcstoreDiskStore, slot: usize, owner: Uuid, sequence: u64, bytes: &[u8]) {
        install(disk, PAYLOAD_PATHS[slot], bytes).await;
        install(disk, MANIFEST_PATHS[slot], &manifest(owner, sequence, bytes)).await;
    }

    #[test]
    fn manifest_validates_identity_sequence_length_and_digest() {
        let bytes = payload("object");
        let owner = Uuid::new_v4();
        assert!(CommittedSnapshot::decode(&manifest(owner, 1, &bytes), bytes.clone(), bytes.len()).is_ok());
        for (owner, sequence) in [(Uuid::nil(), 1), (owner, 0), (owner, u64::MAX)] {
            assert!(matches!(
                Manifest::decode(&manifest(owner, sequence, &bytes), bytes.len()),
                Err(SnapshotError::Corrupt)
            ));
        }
        assert!(matches!(
            Manifest::decode(&manifest(owner, 1, &bytes), bytes.len() - 1),
            Err(SnapshotError::TooLarge)
        ));
        let mut corrupt = manifest(owner, 1, &bytes);
        corrupt[25] ^= 1;
        assert!(matches!(Manifest::decode(&corrupt, bytes.len()), Err(SnapshotError::Corrupt)));
        let mut unsupported = manifest(owner, 1, &bytes);
        unsupported[8] = 2;
        assert!(matches!(Manifest::decode(&unsupported, bytes.len()), Err(SnapshotError::Unsupported)));
    }

    #[test]
    fn whole_payload_integrity_is_required_even_with_a_valid_manifest() {
        let bytes = payload("object");
        let owner = Uuid::new_v4();
        let header = manifest(owner, 1, &bytes);
        assert!(matches!(
            CommittedSnapshot::decode(&header, bytes[..bytes.len() - 1].to_vec(), bytes.len()),
            Err(SnapshotError::Corrupt)
        ));
        let invalid = b"not an MRF record".to_vec();
        assert!(matches!(
            CommittedSnapshot::decode(&manifest(owner, 2, &invalid), invalid, bytes.len()),
            Err(SnapshotError::Corrupt)
        ));
    }

    #[tokio::test]
    async fn newest_complete_replica_wins_in_both_disk_orders() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let owner = Uuid::new_v4();
        commit(&first, 0, owner, 1, &payload("old")).await;
        commit(&second, 1, owner, 2, &payload("new")).await;
        for disks in [vec![first.clone(), second.clone()], vec![second.clone(), first.clone()]] {
            let recovered = read_committed(&disks, 4096)
                .await
                .expect("read replicas")
                .expect("committed snapshot");
            assert_eq!(recovered.manifest.sequence, 2);
            assert_eq!(recovered.payload, payload("new"));
        }
    }

    #[tokio::test]
    async fn divergent_commits_at_same_sequence_fail_closed() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let owner = Uuid::new_v4();
        commit(&first, 0, owner, 7, &payload("a")).await;
        commit(&second, 1, owner, 7, &payload("b")).await;
        assert!(matches!(read_committed(&[first, second], 4096).await, Err(SnapshotError::Conflict)));
    }

    #[tokio::test]
    async fn newer_slot_does_not_hide_a_conflicting_commit_history() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let owner = Uuid::new_v4();
        commit(&first, 0, owner, 8, &payload("newest")).await;
        commit(&first, 1, owner, 7, &payload("a")).await;
        commit(&second, 1, owner, 7, &payload("b")).await;
        assert!(matches!(read_committed(&[first, second], 4096).await, Err(SnapshotError::Conflict)));
    }

    #[tokio::test]
    async fn uncommitted_or_torn_successor_preserves_previous_slot() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let owner = Uuid::new_v4();
        let old = payload("old");
        let next = payload("next");
        commit(&disk, 0, owner, 1, &old).await;
        install(&disk, PAYLOAD_PATHS[1], &next).await;
        let recovered = read_committed(std::slice::from_ref(&disk), 4096)
            .await
            .expect("staged payload is not a commit")
            .expect("old snapshot");
        assert_eq!(recovered.payload, old);
        install(&disk, MANIFEST_PATHS[1], &manifest(owner, 2, &next)[..20]).await;
        let recovered = read_committed(std::slice::from_ref(&disk), 4096)
            .await
            .expect("torn manifest preserves old slot")
            .expect("old snapshot");
        assert_eq!(recovered.manifest.sequence, 1);
        install(&disk, MANIFEST_PATHS[1], &manifest(owner, 2, &next)).await;
        install(&disk, PAYLOAD_PATHS[1], b"torn").await;
        let recovered = read_committed(&[disk], 4096)
            .await
            .expect("torn payload preserves old slot")
            .expect("old snapshot");
        assert_eq!(recovered.manifest.sequence, 1);
    }

    #[tokio::test]
    async fn committed_reader_reopens_previous_anchor_across_publication_boundaries() {
        let owner = Uuid::new_v4();
        let old = payload("old");
        let next = payload("next");
        let boundaries = [
            ("payload-only", next.clone(), None),
            ("torn-manifest", next.clone(), Some(manifest(owner, 2, &next)[..20].to_vec())),
            ("stale-payload", old.clone(), Some(manifest(owner, 2, &next))),
        ];
        for (case, successor_payload, successor_manifest) in boundaries {
            let root = TempDir::new().expect("test directory");
            let store = disk(&root, "disk").await;
            commit(&store, 0, owner, 1, &old).await;
            install(&store, PAYLOAD_PATHS[1], &successor_payload).await;
            if let Some(manifest) = &successor_manifest {
                install(&store, MANIFEST_PATHS[1], manifest).await;
            }

            let reopened = disk(&root, "disk").await;
            let recovered = read_committed(std::slice::from_ref(&reopened), 4096)
                .await
                .unwrap_or_else(|error| panic!("{case}: old anchor must remain readable after reopen: {error:?}"))
                .unwrap_or_else(|| panic!("{case}: previous committed anchor missing after reopen"));
            assert_eq!(recovered.manifest.sequence, 1, "{case}: successor must not become authoritative");
            assert_eq!(recovered.payload, old, "{case}: previous payload must survive");
            assert_eq!(
                EcstoreDiskAPI::read_all(reopened.as_ref(), RUSTFS_META_BUCKET, PAYLOAD_PATHS[0])
                    .await
                    .expect("old payload retained")
                    .as_ref(),
                old.as_slice(),
                "{case}: previous payload bytes changed"
            );
            assert_eq!(
                EcstoreDiskAPI::read_all(reopened.as_ref(), RUSTFS_META_BUCKET, MANIFEST_PATHS[0])
                    .await
                    .expect("old manifest retained")
                    .as_ref(),
                manifest(owner, 1, &old).as_slice(),
                "{case}: previous manifest bytes changed"
            );
            assert_eq!(
                EcstoreDiskAPI::read_all(reopened.as_ref(), RUSTFS_META_BUCKET, PAYLOAD_PATHS[1])
                    .await
                    .expect("successor payload retained")
                    .as_ref(),
                successor_payload.as_slice(),
                "{case}: successor evidence changed"
            );
            if let Some(manifest) = &successor_manifest {
                assert_eq!(
                    EcstoreDiskAPI::read_all(reopened.as_ref(), RUSTFS_META_BUCKET, MANIFEST_PATHS[1])
                        .await
                        .expect("successor manifest retained")
                        .as_ref(),
                    manifest.as_slice(),
                    "{case}: successor manifest evidence changed"
                );
            }
        }
    }

    #[tokio::test]
    async fn stale_manifest_cas_cannot_replace_committed_anchor() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let owner = Uuid::new_v4();
        let bytes = payload("object");
        commit(&disk, 0, owner, 1, &bytes).await;
        let result = EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            MANIFEST_PATHS[0],
            None,
            Some(manifest(owner, 2, &bytes).into()),
        )
        .await
        .expect("CAS call");
        assert_eq!(result, EcstoreConditionalFileUpdate::Mismatch);
        let recovered = read_committed(&[disk], 4096)
            .await
            .expect("read old anchor")
            .expect("snapshot");
        assert_eq!(recovered.manifest.sequence, 1);
    }

    #[tokio::test]
    async fn legacy_import_requires_complete_consistent_replicas() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let bytes = payload("object");
        for (disk, data) in [(&first, &bytes[..bytes.len() - 1]), (&second, bytes.as_slice())] {
            EcstoreDiskAPI::write_all(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                MRF_SCOPED_JOURNAL_PATH,
                EcstoreDiskBytes::copy_from_slice(data),
            )
            .await
            .expect("legacy fixture");
        }
        let disks = [first.clone(), second];
        assert!(
            matches!(read_recovery_snapshot(&disks, 4096).await.expect("intact legacy replica"), Some(RecoverySnapshot::Legacy(data)) if data == bytes)
        );
        EcstoreDiskAPI::write_all(first.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH, payload("different").into())
            .await
            .expect("divergent fixture");
        assert!(matches!(read_recovery_snapshot(&disks, 4096).await, Err(SnapshotError::Conflict)));
    }

    #[tokio::test]
    async fn committed_inspection_leaves_payload_and_manifest_unchanged() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let owner = Uuid::new_v4();
        let bytes = payload("object");
        commit(&disk, 0, owner, 3, &bytes).await;
        assert!(matches!(
            read_recovery_snapshot(std::slice::from_ref(&disk), 4096)
                .await
                .expect("new snapshot"),
            Some(RecoverySnapshot::Committed(_))
        ));
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, MANIFEST_PATHS[0])
                .await
                .expect("manifest retained")
                .as_ref(),
            manifest(owner, 3, &bytes)
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, PAYLOAD_PATHS[0])
                .await
                .expect("payload retained")
                .as_ref(),
            bytes
        );
    }

    #[tokio::test]
    async fn committed_reader_resource_bounds_are_measured() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let owner = Uuid::new_v4();
        let old = [payload("old-0"), payload("old-1")].concat();
        let new = [payload("new-0"), payload("new-1"), payload("new-2")].concat();
        commit(&first, 0, owner, 1, &old).await;
        commit(&second, 1, owner, 2, &new).await;

        let mut stats = SnapshotReadStats::default();
        let recovered = read_committed_with_stats(&[first, second], 4096, Some(&mut stats))
            .await
            .expect("read committed replicas")
            .expect("committed snapshot");

        assert_eq!(recovered.sequence(), 2);
        assert_eq!(recovered.payload(), new.as_slice());
        assert_eq!(recovered.manifest.payload_len, new.len());
        assert_eq!(stats.file_reads, 4, "only committed manifests and their payloads are materialized");
        assert_eq!(stats.bytes_read, (MANIFEST_LEN * 2) + old.len() + new.len());
        assert_eq!(
            stats.peak_file_bytes,
            new.len().max(MANIFEST_LEN),
            "reader peak allocation remains bounded by one manifest or payload file"
        );
    }

    #[tokio::test]
    async fn legacy_inspection_rejects_complete_subsets_and_scope_ambiguity() {
        let scoped = |set_index| {
            let intent = MrfIntent {
                bucket: Arc::from("bucket"),
                object: Arc::from("a"),
                version_id: None,
                kind: MrfKind::PartialWrite,
                scope: Some(MrfScope {
                    pool_index: 0,
                    set_index,
                }),
                lease: None,
                enqueued_at_ms: 1234,
                attempts: 0,
            };
            let mut bytes = Vec::new();
            assert!(encode_intent(&intent, &mut bytes), "scoped fixture must encode");
            bytes
        };
        let mut superset = payload("a");
        superset.extend_from_slice(&payload("b"));
        for (case, first_bytes, second_bytes) in [
            ("complete-subset", payload("a"), superset),
            ("different-set", scoped(1), scoped(2)),
            ("unknown-scope", payload("a"), scoped(1)),
        ] {
            let root = TempDir::new().expect("test directory");
            let first = disk(&root, "first").await;
            let second = disk(&root, "second").await;
            for (disk, bytes) in [(&first, &first_bytes), (&second, &second_bytes)] {
                assert_eq!(decode_journal(bytes).1, 0, "{case}: complete fixture");
                EcstoreDiskAPI::write_all(
                    disk.as_ref(),
                    RUSTFS_META_BUCKET,
                    MRF_SCOPED_JOURNAL_PATH,
                    EcstoreDiskBytes::copy_from_slice(bytes),
                )
                .await
                .expect("write legacy replica");
            }
            for disks in [vec![first.clone(), second.clone()], vec![second.clone(), first.clone()]] {
                assert!(
                    matches!(read_recovery_snapshot(&disks, 4096).await, Err(SnapshotError::Conflict)),
                    "{case}: neither replica order proves a latest snapshot"
                );
            }
            for (disk, bytes) in [(&first, &first_bytes), (&second, &second_bytes)] {
                assert_eq!(
                    EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                        .await
                        .expect("legacy evidence retained")
                        .as_ref(),
                    bytes.as_slice(),
                    "{case}: inspection must preserve both source replicas"
                );
            }
        }
    }

    #[tokio::test]
    async fn oversized_or_corrupt_scoped_snapshot_never_falls_back_to_legacy() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        EcstoreDiskAPI::write_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH, vec![0; 1025].into())
            .await
            .expect("oversized fixture");
        EcstoreDiskAPI::write_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_JOURNAL_PATH, payload("old").into())
            .await
            .expect("legacy fixture");
        assert!(matches!(
            read_recovery_snapshot(std::slice::from_ref(&disk), 1024).await,
            Err(SnapshotError::TooLarge)
        ));
        assert!(matches!(read_recovery_snapshot(&[disk], 2048).await, Err(SnapshotError::Corrupt)));
    }

    #[tokio::test]
    async fn empty_legacy_replica_cannot_erase_records_in_a_torn_replica() {
        let root = TempDir::new().expect("test directory");
        let first = disk(&root, "first").await;
        let second = disk(&root, "second").await;
        let mut incomplete = payload("durable-object");
        incomplete.extend_from_slice(b"torn");
        EcstoreDiskAPI::write_all(first.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH, Vec::new().into())
            .await
            .expect("empty truncated replica");
        EcstoreDiskAPI::write_all(second.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH, incomplete.clone().into())
            .await
            .expect("records and torn tail");
        for disks in [vec![first.clone(), second.clone()], vec![second.clone(), first.clone()]] {
            assert!(matches!(read_recovery_snapshot(&disks, 4096).await, Err(SnapshotError::Corrupt)));
        }
        assert_eq!(
            EcstoreDiskAPI::read_all(second.as_ref(), RUSTFS_META_BUCKET, MRF_SCOPED_JOURNAL_PATH)
                .await
                .expect("recovery anchor preserved")
                .as_ref(),
            incomplete
        );
    }

    #[tokio::test]
    async fn unreadable_commit_record_never_implies_legacy_only() {
        let root = TempDir::new().expect("test directory");
        let disk = disk(&root, "disk").await;
        let legacy = payload("old");
        EcstoreDiskAPI::write_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_JOURNAL_PATH, legacy.clone().into())
            .await
            .expect("legacy fixture");
        // Opening a directory as a record either fails at open or at read,
        // depending on the platform. Neither outcome proves absence.
        std::fs::create_dir(root.path().join("disk").join(RUSTFS_META_BUCKET).join(MANIFEST_PATHS[0]))
            .expect("unreadable manifest fixture");
        let recovered = read_recovery_snapshot(std::slice::from_ref(&disk), 4096).await;
        assert!(
            matches!(recovered, Err(SnapshotError::Disk(_) | SnapshotError::Read(_))),
            "must preserve unavailable proof: {recovered:?}"
        );
        assert_eq!(
            EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, MRF_JOURNAL_PATH)
                .await
                .expect("legacy remains")
                .as_ref(),
            legacy
        );
    }
}
