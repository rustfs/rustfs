// Copyright 2024 RustFS Team
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

//! Per-erasure-set disk-walk UNION enumerator for heal (backlog#920).
//!
//! B5's heal enumeration lists versions through the READ-QUORUM metadata view
//! (`list_object_versions`), so a version present on FEWER than read-quorum disks
//! is never enumerated and therefore never healed. This module walks each disk in
//! the set directly (mirroring MinIO cmd/global-heal.go `healErasureSet`:
//! `listPathRaw` with `objQuorum = 1` feeding `mergeXLV2Versions`) and surfaces
//! every `(object, version)` present on ANY disk, feeding each to the existing
//! per-version `SetDisks::heal_object`.

use super::super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A single `(object, version)` unit surfaced by the disk-walk union enumerator.
///
/// `is_delete_marker` is OBSERVABILITY-ONLY (metrics / logging / e2e assertions);
/// it must not gate healing logic — the delete-marker vs data path is chosen
/// inside `ops/heal.rs` from the resolved latest metadata. `version_id` is
/// normalized (nil/absent UUID => `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealWalkVersion {
    /// object key
    pub name: String,
    /// normalized version id (`None` when the version is nil/absent)
    pub version_id: Option<String>,
    /// whether this version is a delete marker (observability only)
    pub is_delete_marker: bool,
}

/// One collected object (a single sorted key) with all its expanded versions.
#[derive(Debug, Clone)]
struct HealWalkObject {
    name: String,
    versions: Vec<HealWalkVersion>,
}

/// Shared collector fed by the `agreed`/`partial` callbacks of `list_path_raw`.
/// `list_path_raw` emits at most one callback per distinct object key in sorted
/// order, so each successful ingest corresponds to exactly one new object.
struct HealWalkCollector {
    bucket: String,
    batch_objects: usize,
    version_budget: usize,
    objects: Mutex<Vec<HealWalkObject>>,
    version_total: AtomicUsize,
    truncated: AtomicBool,
    cancel: CancellationToken,
}

impl HealWalkCollector {
    /// Expand one resolved entry into its versions and record it. Cancels the
    /// walk once EITHER page bound (distinct object names OR expanded versions)
    /// is met — always at a sorted object-key boundary so a heavily-versioned
    /// object is never split across pages.
    fn ingest(&self, entry: MetaCacheEntry) {
        // Skip pure directory entries; they carry no versions to heal here.
        if entry.is_dir() {
            return;
        }

        // Expand to one HealWalkVersion per FileInfo. KEEP remote/transitioned and
        // free-version records: their LOCAL xl.meta may still need healing.
        let fiv = match entry.file_info_versions_with_free_versions(&self.bucket) {
            Ok(fiv) => fiv,
            Err(err) => {
                debug!(entry = %entry.name, error = ?err, "heal disk-walk skipped entry with unreadable versions");
                return;
            }
        };

        let mut versions = Vec::with_capacity(fiv.versions.len() + fiv.free_versions.len());
        for fi in fiv.versions.iter().chain(fiv.free_versions.iter()) {
            versions.push(HealWalkVersion {
                name: entry.name.clone(),
                // Normalize: nil/absent version id => None.
                version_id: fi.version_id.filter(|u| !u.is_nil()).map(|u| u.to_string()),
                is_delete_marker: fi.deleted,
            });
        }

        if versions.is_empty() {
            return;
        }

        let added = versions.len();
        let (objs_len, ver_total) = {
            let mut objects = self.objects.lock().unwrap();
            objects.push(HealWalkObject {
                name: entry.name,
                versions,
            });
            let objs_len = objects.len();
            let ver_total = self.version_total.fetch_add(added, Ordering::SeqCst) + added;
            (objs_len, ver_total)
        };

        // Bound at an object boundary (this object is fully included).
        if objs_len >= self.batch_objects || ver_total >= self.version_budget {
            self.truncated.store(true, Ordering::SeqCst);
            self.cancel.cancel();
        }
    }
}

/// Finalize a collected disk-walk page: compute the resume cursor and apply
/// inclusive-forward de-overlap.
///
/// `next_forward` is derived from the PRE-de-overlap last collected object name
/// (the walk's `forward_to` is inclusive, so the next page re-reads that exact
/// key and drops it here). Leading objects whose name == `forward_to` are dropped
/// to avoid re-healing the boundary object.
fn finalize_heal_walk_page(
    mut objects: Vec<HealWalkObject>,
    forward_to: Option<&str>,
    truncated: bool,
) -> (Vec<HealWalkVersion>, Option<String>, bool) {
    let next_forward = objects.last().map(|o| o.name.clone());

    if let Some(fw) = forward_to {
        while objects.first().map(|o| o.name.as_str()) == Some(fw) {
            objects.remove(0);
        }
    }

    let versions: Vec<HealWalkVersion> = objects.into_iter().flat_map(|o| o.versions).collect();

    if truncated {
        (versions, next_forward, true)
    } else {
        (versions, None, false)
    }
}

impl SetDisks {
    /// Walk every disk in this set and return one page of the cross-disk UNION of
    /// versions (see module docs). Returns `(versions, next_forward, truncated)`.
    ///
    /// - `batch_objects` (>= 2) and `version_budget` are a DUAL page bound: the
    ///   walk stops at the first sorted object-key boundary where EITHER the
    ///   distinct-object-name count reaches `batch_objects` or the expanded
    ///   version count reaches `version_budget`.
    /// - `forward_to` resumes the walk (inclusive); the boundary object is
    ///   de-overlapped here so no version is healed twice across pages.
    ///
    /// `min_disks: 1` means the page is produced from WHATEVER disks respond, so a
    /// version on a single surviving disk is still surfaced.
    pub(crate) async fn heal_walk_versions_page(
        &self,
        bucket: &str,
        prefix: &str,
        forward_to: Option<&str>,
        batch_objects: usize,
        version_budget: usize,
    ) -> disk::error::Result<(Vec<HealWalkVersion>, Option<String>, bool)> {
        assert!(batch_objects >= 2, "heal_walk_versions_page requires batch_objects >= 2");

        let disks = self.get_disks_internal().await;

        let collector = Arc::new(HealWalkCollector {
            bucket: bucket.to_string(),
            batch_objects,
            version_budget: version_budget.max(1),
            objects: Mutex::new(Vec::new()),
            version_total: AtomicUsize::new(0),
            truncated: AtomicBool::new(false),
            cancel: CancellationToken::new(),
        });

        let agreed_collector = collector.clone();
        let partial_collector = collector.clone();
        let partial_bucket = bucket.to_string();

        let filter_prefix = if prefix.is_empty() { None } else { Some(prefix.to_string()) };

        let opts = ListPathRawOptions {
            disks,
            bucket: bucket.to_string(),
            path: String::new(),
            recursive: true,
            incl_deleted: true,
            filter_prefix,
            forward_to: forward_to.map(str::to_string),
            min_disks: 1,
            report_not_found: false,
            per_disk_limit: 0,
            skip_walkdir_total_timeout: true,
            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                let collector = agreed_collector.clone();
                Box::pin(async move {
                    collector.ingest(entry);
                })
            })),
            partial: Some(Box::new(move |entries: MetaCacheEntries, _errs: &[Option<DiskError>]| {
                let collector = partial_collector.clone();
                let bucket = partial_bucket.clone();
                Box::pin(async move {
                    // objQuorum = 1: take the cross-disk union of every version on
                    // any disk. Fall back to the first present entry if the merge
                    // somehow yields nothing.
                    let entry = entries.resolve_union(&bucket).or_else(|| entries.first_found().0);
                    if let Some(entry) = entry {
                        collector.ingest(entry);
                    }
                })
            })),
            finished: None,
            ..Default::default()
        };

        // Drive the walk. A tolerated missing-path / not-found is treated as an
        // empty page rather than an error (nothing to heal on this prefix).
        match list_path_raw(collector.cancel.clone(), opts).await {
            Ok(()) => {}
            Err(DiskError::FileNotFound) | Err(DiskError::VolumeNotFound) => {
                debug!(bucket, prefix, "heal disk-walk treated missing path as empty page");
            }
            Err(err) => return Err(err),
        }

        let objects = std::mem::take(&mut *collector.objects.lock().unwrap());
        let truncated = collector.truncated.load(Ordering::SeqCst);
        Ok(finalize_heal_walk_page(objects, forward_to, truncated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(name: &str, id: &str, dm: bool) -> HealWalkVersion {
        HealWalkVersion {
            name: name.to_string(),
            version_id: Some(id.to_string()),
            is_delete_marker: dm,
        }
    }

    fn object_with_versions(name: &str, count: usize) -> HealWalkObject {
        HealWalkObject {
            name: name.to_string(),
            versions: (0..count).map(|i| version(name, &format!("{name}-v{i}"), false)).collect(),
        }
    }

    /// A single heavily-versioned object whose version count exceeds the budget
    /// is returned WHOLE in one page (never split), and the page is flagged
    /// truncated with a resume cursor pointing at that object.
    #[test]
    fn union_page_bounded_by_version_budget_on_heavily_versioned_object() {
        let big = object_with_versions("big.bin", 50);
        // The collector cancels AFTER completing the object, so the buffer holds
        // exactly this one object even though 50 >> the version budget of 10.
        let (versions, next_forward, truncated) = finalize_heal_walk_page(vec![big], None, true);

        assert_eq!(versions.len(), 50, "the heavily-versioned object must not be split across pages");
        assert!(truncated, "exceeding the version budget must mark the page truncated");
        assert_eq!(
            next_forward.as_deref(),
            Some("big.bin"),
            "resume cursor must point at the boundary object"
        );
    }

    /// Inclusive-forward de-overlap: the boundary object re-read on the next page
    /// (name == forward_to) is dropped, so its versions are not healed twice.
    #[test]
    fn union_page_de_overlaps_inclusive_forward_boundary() {
        let a = object_with_versions("a.bin", 2);
        let b = object_with_versions("b.bin", 3);
        // forward_to == "a.bin": the walk re-reads a.bin (inclusive) then b.bin.
        let (versions, next_forward, truncated) = finalize_heal_walk_page(vec![a, b], Some("a.bin"), true);

        assert!(
            versions.iter().all(|v| v.name == "b.bin"),
            "the boundary object a.bin must be de-overlapped, got {versions:?}"
        );
        assert_eq!(versions.len(), 3);
        assert_eq!(next_forward.as_deref(), Some("b.bin"));
        assert!(truncated);
    }

    /// A non-truncated final page clears the resume cursor.
    #[test]
    fn union_page_non_truncated_clears_cursor() {
        let a = object_with_versions("a.bin", 1);
        let (versions, next_forward, truncated) = finalize_heal_walk_page(vec![a], None, false);
        assert_eq!(versions.len(), 1);
        assert_eq!(next_forward, None, "a complete final page must not carry a resume cursor");
        assert!(!truncated);
    }
}
