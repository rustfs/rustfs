// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use super::{DirectoryScope, DirectoryStamp, DirectoryWork, FolderScanMode, native_io, pages::NamePage};
use crate::data_usage_define::{
    DataUsageCacheInfo, DataUsageCacheSource, DataUsageEntry, DataUsageScanIdentity, DataUsageScanProgress,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Scope {
    bucket: String,
    source: DataUsageCacheSource,
    identity: DataUsageScanIdentity,
    progress: DataUsageScanProgress,
    mode: u8,
    prefixes: Option<[u8; 32]>,
    watch: Option<[u8; 32]>,
}

impl Scope {
    fn may_adopt(&self, current: &Self) -> bool {
        self.bucket == current.bucket
            && self.source == current.source
            && self.identity == current.identity
            && self.mode == current.mode
            && self.prefixes == current.prefixes
            && self.progress.started_plan == current.progress.started_plan
            && ((self.progress == current.progress && self.watch == current.watch)
                || current.progress.started_plan != current.progress.requested_plan)
    }
    pub(super) fn new(owner: &DirectoryScope) -> Self {
        let mode = match owner.mode {
            FolderScanMode::Namespace => 0,
            FolderScanMode::ErasureCandidate {
                reuse_namespace,
                parent_has_metadata,
            } => 1 + u8::from(reuse_namespace) + 2 * u8::from(parent_has_metadata),
        };
        let prefixes = owner.prefix_scope.as_ref().map(|scope| {
            let mut entries: Vec<_> = scope.selected_top_level_entries.iter().collect();
            entries.sort_unstable();
            let mut hash = Sha256::new();
            for entry in entries {
                hash.update(u64::try_from(entry.len()).unwrap_or(u64::MAX).to_le_bytes());
                hash.update(entry.as_bytes());
            }
            hash.finalize().into()
        });
        Self {
            bucket: owner.bucket.to_string(),
            source: owner.source,
            identity: owner.identity,
            progress: owner.progress,
            mode,
            prefixes,
            watch: owner.watch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Physical {
    pub(crate) endpoint: String,
    device: u64,
    inode: u64,
    changed: (i64, i64),
    modified: SystemTime,
}

impl Physical {
    #[cfg(unix)]
    pub(super) fn of(endpoint: &str, stamp: &DirectoryStamp) -> Option<Self> {
        Some(Self {
            endpoint: endpoint.to_owned(),
            device: stamp.device,
            inode: stamp.inode,
            changed: stamp.changed,
            modified: stamp.modified,
        })
    }
    #[cfg(windows)]
    pub(super) fn of(endpoint: &str, stamp: &DirectoryStamp) -> Option<Self> {
        let (device, inode, created) = stamp.physical?;
        Some(Self {
            endpoint: endpoint.to_owned(),
            device,
            inode,
            changed: (created, 0),
            modified: stamp.modified,
        })
    }
    #[cfg(not(any(unix, windows)))]
    pub(super) fn of(_endpoint: &str, _stamp: &DirectoryStamp) -> Option<Self> {
        None
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Aggregate {
    pub(super) through: String,
    pub(super) value: DataUsageEntry,
}

/// Immutable directory membership and the last fully counted compacted child.
/// The active child's provisional contribution is never part of this receipt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectoryCheckpoint {
    version: u8,
    pub(super) parent: String,
    scope: Scope,
    pub(super) physical: Option<Physical>,
    pub(super) pages: Vec<NamePage>,
    pub(super) anchor: Option<native_io::DirectoryAnchor>,
    pub(super) entries_read: u64,
    pub(super) sealed: bool,
    pub(super) part: Option<String>,
    pub(super) metadata: bool,
    pub(super) aggregate: Option<Aggregate>,
    endpoint: String,
    digest: [u8; 32],
}

/// Physical progress orders only callbacks from the same directory generation.
/// The coordinator uses these receipts for fair scheduling, never for coverage.
#[derive(Clone, Debug)]
pub(crate) struct DirectoryReadProgress {
    pub(crate) parent: String,
    pub(crate) physical: Physical,
    position: (u64, bool),
}
impl DirectoryReadProgress {
    pub(crate) fn same_source(&self, other: &Self) -> bool {
        self.parent == other.parent && self.physical == other.physical
    }
    pub(crate) fn extends(&self, older: &Self) -> bool {
        self.same_source(older) && self.position >= older.position
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DirectoryProgress {
    pub(crate) parent: String,
    pub(crate) physical: Physical,
    manifest: [u8; 32],
    through: String,
}
impl DirectoryProgress {
    pub(crate) fn same_source(&self, previous: &Self) -> bool {
        self.parent == previous.parent && self.physical == previous.physical && self.manifest == previous.manifest
    }
    pub(crate) fn extends(&self, previous: &Self) -> bool {
        self.same_source(previous) && self.through >= previous.through
    }
    pub(crate) fn covers(&self, path: &str) -> bool {
        path.strip_prefix(&self.parent)
            .and_then(|tail| tail.strip_prefix('/'))
            .and_then(|tail| tail.split('/').next())
            .is_some_and(|child| child <= self.through.as_str())
    }
}

impl DirectoryCheckpoint {
    /// Deserialize each replica's independent receipt, then intern equal
    /// verified immutable name pages across the bucket's active directories.
    pub(crate) fn verified_work(work: Vec<Self>, info: &DataUsageCacheInfo) -> Vec<Self> {
        let mut shared = HashMap::new();
        work.into_iter()
            .filter(|receipt| receipt.is_bound_to(info))
            .map(|mut receipt| {
                for page in &mut receipt.pages {
                    let canonical = shared.entry(*page.digest()).or_insert_with(|| page.clone());
                    *page = canonical.clone();
                }
                receipt
            })
            .collect()
    }
    pub(crate) fn parent(&self) -> &str {
        &self.parent
    }

    pub(super) fn manifest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for p in &self.pages {
            hash.update(p.digest());
        }
        hash.finalize().into()
    }
    fn checksum(&self) -> Option<[u8; 32]> {
        // Nested usage distributions can contain maps. Canonicalize those
        // map keys before hashing so restart hash randomization is irrelevant.
        let mut aggregate = serde_json::to_value(&self.aggregate).ok()?;
        aggregate.sort_all_objects();
        let header = rmp_serde::to_vec_named(&(
            self.version,
            &self.parent,
            &self.scope,
            &self.physical,
            &self.anchor,
            self.entries_read,
            self.sealed,
            &self.part,
            self.metadata,
            self.manifest(),
            aggregate,
            &self.endpoint,
        ))
        .ok()?;
        Some(Sha256::digest(header).into())
    }
    pub(crate) fn is_bound_to(&self, info: &DataUsageCacheInfo) -> bool {
        self.version == 1
            && self.scope.bucket == info.name
            && self.scope.identity.is_valid()
            && Some(self.scope.identity) == info.scan_identity
            && Some(self.scope.source) == info.source
            && info
                .scan_progress
                .is_some_and(|p| p.started_plan == self.scope.progress.started_plan)
            && (self.parent == info.name
                || self
                    .parent
                    .strip_prefix(&info.name)
                    .and_then(|p| p.strip_prefix('/'))
                    .is_some_and(|tail| tail.split('/').all(super::valid_component)))
            && self.valid_payload()
    }
    pub(crate) fn read_progress(&self, info: &DataUsageCacheInfo) -> Option<DirectoryReadProgress> {
        let physical = self.physical.as_ref()?;
        (self.is_bound_to(info) && (self.sealed || self.anchor.is_some()) && self.entries_read > 0).then(|| {
            DirectoryReadProgress {
                parent: self.parent.clone(),
                physical: physical.clone(),
                position: (self.entries_read, self.sealed),
            }
        })
    }
    pub(crate) fn progress(&self, info: &DataUsageCacheInfo) -> Option<DirectoryProgress> {
        let done = self.aggregate.as_ref()?;
        let physical = self.physical.as_ref()?;
        (self.sealed && self.is_bound_to(info)).then(|| DirectoryProgress {
            parent: self.parent.clone(),
            physical: physical.clone(),
            manifest: self.manifest(),
            through: done.through.clone(),
        })
    }
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    fn valid_for(&self, parent: &str, owner: &Scope) -> bool {
        self.version == 1 && self.parent == parent && self.scope.may_adopt(owner) && self.valid_payload()
    }
    fn valid_payload(&self) -> bool {
        self.physical.as_ref().is_some_and(|source| source.endpoint == self.endpoint)
            && (self.sealed || self.anchor.is_some())
            && self.checksum() == Some(self.digest)
            && self.pages.iter().all(NamePage::valid)
            && self.part.as_deref().is_none_or(super::super::is_erasure_part_name)
            && self.aggregate.as_ref().is_none_or(|done| {
                self.sealed
                    && super::valid_component(&done.through)
                    && done.value.children.is_empty()
                    && done.value.failed_objects == 0
                    && self.pages.iter().any(|page| page.contains(&done.through))
            })
    }
    pub(super) fn restore_allowed(&self, parent: &str, owner: &Scope, physical: &Option<Physical>) -> bool {
        self.valid_for(parent, owner)
            && self.physical.is_some()
            && &self.physical == physical
            && (self.sealed || self.anchor.is_some())
    }
}

pub(in crate::scanner_folder) type Saved = Arc<Mutex<Option<DirectoryCheckpoint>>>;

pub(super) struct Recorder {
    pub(super) slot: Saved,
    scope: Scope,
    parent: String,
    physical: Option<Physical>,
    pub(super) anchor: Option<native_io::DirectoryAnchor>,
    pub(super) entries_read: u64,
    pub(super) part: Option<String>,
    pub(super) metadata: bool,
    pub(super) valid_names: bool,
    endpoint: String,
    pub(super) last_update: Instant,
}

impl Recorder {
    pub(super) fn new(
        slot: Saved,
        parent: &str,
        scope: Scope,
        endpoint: String,
        physical: Option<Physical>,
        previous: Option<&DirectoryCheckpoint>,
    ) -> Self {
        Self {
            slot,
            scope,
            parent: parent.to_owned(),
            physical,
            endpoint,
            anchor: previous.and_then(|p| p.anchor.clone()),
            entries_read: previous.map_or(0, |p| p.entries_read),
            part: previous.and_then(|p| p.part.clone()),
            metadata: previous.is_some_and(|p| p.metadata),
            valid_names: true,
            last_update: Instant::now(),
        }
    }
    pub(super) fn save(&self, work: &DirectoryWork) {
        // An ordinary object directory is cheaper to reopen. Only a reader
        // which can actually resume or a closed directory can be checkpointed.
        let sealed = work.names.ready();
        let aggregate = if work.compacted && !work.gap {
            work.names.through().map(|through| Aggregate {
                through: through.to_owned(),
                value: work.compacted_total.clone(),
            })
        } else {
            None
        };
        let snapshot = if self.valid_names
            && self.physical.as_ref().is_some_and(|source| source.endpoint == self.endpoint)
            && (sealed || self.anchor.is_some())
        {
            let mut checkpoint = DirectoryCheckpoint {
                version: 1,
                parent: self.parent.clone(),
                scope: self.scope.clone(),
                physical: self.physical.clone(),
                anchor: (!sealed).then(|| self.anchor.clone()).flatten(),
                entries_read: self.entries_read,
                pages: work.names.snapshot(),
                sealed,
                part: self.part.clone(),
                metadata: self.metadata,
                aggregate,
                endpoint: self.endpoint.clone(),
                digest: [0; 32],
            };
            checkpoint.checksum().map(|digest| {
                checkpoint.digest = digest;
                checkpoint
            })
        } else {
            None
        };
        *self.slot.lock().unwrap_or_else(|p| p.into_inner()) = snapshot;
    }
}

#[cfg(test)]
mod storage_fixture;
