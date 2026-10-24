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

use crate::{
    Error, FileInfo, FileInfoOpts, FileInfoVersions, FileMeta, FileMetaShallowVersion, Result, VersionType, get_file_info,
};
use arc_swap::ArcSwapOption;
use rmp::Marker;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::str::from_utf8;
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::spawn;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

const SLASH_SEPARATOR: &str = "/";
pub const MAX_META_CACHE_HEAL_CANDIDATES: usize = 1024;
/// Keep truncation continuations bounded while still giving the scanner a
/// safe object-level retry for versions that did not fit in the candidate set.
pub const MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS: usize = 64;

#[derive(Clone, Debug, Default)]
pub struct MetadataResolutionParams {
    pub dir_quorum: usize,
    pub obj_quorum: usize,
    pub requested_versions: usize,
    pub bucket: String,
    pub strict: bool,
    /// Number of set drives that were unreachable when the listing snapshot was
    /// taken. Write-quorum enforcement relaxes each version's required quorum by
    /// this amount, never below the version's data quorum or `obj_quorum`. A
    /// version legally committed at write quorum can have metadata on drives no
    /// reader could consult, and must not be dropped for it.
    pub write_quorum_slack: usize,
    pub candidates: Vec<Vec<FileMetaShallowVersion>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MetaCacheEntry {
    /// name is the full name of the object including prefixes
    pub name: String,
    /// Metadata. If none is present it is not an object but only a prefix.
    /// Entries without metadata will only be present in non-recursive scans.
    pub metadata: Vec<u8>,

    /// cached contains the metadata if decoded.
    #[serde(skip)]
    pub cached: Option<FileMeta>,

    /// Indicates the entry can be reused and only one reference to metadata is expected.
    pub reusable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MetaCacheHealCandidateKind {
    Object,
    DeleteMarker,
    UnversionedObject,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetaCacheHealCandidate {
    pub object: String,
    pub version_id: Option<Uuid>,
    pub kind: MetaCacheHealCandidateKind,
    /// Number of raw disk entries that carried this validated version.
    pub replica_count: usize,
}

impl MetaCacheHealCandidate {
    pub fn validated_version(&self) -> Option<Uuid> {
        match self.kind {
            MetaCacheHealCandidateKind::Object | MetaCacheHealCandidateKind::DeleteMarker => self.version_id,
            MetaCacheHealCandidateKind::UnversionedObject => None,
        }
    }

    pub fn is_unversioned(&self) -> bool {
        self.kind == MetaCacheHealCandidateKind::UnversionedObject
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaCacheHealDiscovery {
    pub candidates: Vec<MetaCacheHealCandidate>,
    pub unverified_count: usize,
    pub truncated: bool,
    /// Object names whose validated version set exceeded the candidate cap.
    /// The scanner retries these names without a version and with destructive
    /// healing disabled; this is an explicit bounded continuation, not a
    /// version claim.
    pub truncated_objects: Vec<String>,
    /// Validated candidates beyond the main cap, retained with exact version
    /// identities so callers never fall back to a latest-version request.
    pub truncated_candidates: Vec<MetaCacheHealCandidate>,
}

impl MetaCacheEntry {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        rmp::encode::write_bool(&mut wr, true)?;
        rmp::encode::write_str(&mut wr, &self.name)?;
        rmp::encode::write_bin(&mut wr, &self.metadata)?;
        Ok(wr)
    }

    pub fn is_dir(&self) -> bool {
        self.metadata.is_empty() && self.name.ends_with('/')
    }

    pub fn is_in_dir(&self, dir: &str, separator: &str) -> bool {
        if dir.is_empty() {
            let idx = self.name.find(separator);
            return idx.is_none() || idx.unwrap() == self.name.len() - separator.len();
        }

        let ext = self.name.trim_start_matches(dir);

        if ext.len() != self.name.len() {
            let idx = ext.find(separator);
            return idx.is_none() || idx.unwrap() == ext.len() - separator.len();
        }

        false
    }

    pub fn is_object(&self) -> bool {
        !self.metadata.is_empty()
    }

    pub fn is_object_dir(&self) -> bool {
        !self.metadata.is_empty() && self.name.ends_with(SLASH_SEPARATOR)
    }

    pub fn is_latest_delete_marker(&mut self) -> bool {
        if let Some(cached) = &self.cached {
            return cached
                .versions
                .iter()
                .find(|version| !version.header.free_version())
                .is_none_or(|version| version.header.version_type == VersionType::Delete);
        }

        if !FileMeta::is_xl2_v1_format(&self.metadata) {
            return false;
        }

        match FileMeta::is_indexed_meta(&self.metadata) {
            Ok((meta, _inline_data)) => {
                if !meta.is_empty() {
                    return FileMeta::is_latest_delete_marker(meta);
                }
            }
            Err(_) => return true,
        }

        match self.xl_meta() {
            Ok(res) => res
                .versions
                .iter()
                .find(|version| !version.header.free_version())
                .is_none_or(|version| version.header.version_type == VersionType::Delete),
            Err(_) => true,
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn to_fileinfo(&self, bucket: &str) -> Result<FileInfo> {
        if self.is_dir() {
            return Ok(FileInfo {
                volume: bucket.to_owned(),
                name: self.name.clone(),
                ..Default::default()
            });
        }

        if let Some(fm) = &self.cached {
            if fm.versions.is_empty() {
                return Ok(FileInfo {
                    volume: bucket.to_owned(),
                    name: self.name.clone(),
                    deleted: true,
                    is_latest: true,
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                });
            }

            let fi = fm.into_fileinfo_without_part_checksums(bucket, self.name.as_str(), "", false, false)?;
            return Ok(fi);
        }

        get_file_info(
            &self.metadata,
            bucket,
            self.name.as_str(),
            "",
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: false,
            },
        )
    }

    pub fn file_info_versions(&self, bucket: &str) -> Result<FileInfoVersions> {
        if self.is_dir() {
            return Ok(FileInfoVersions {
                volume: bucket.to_string(),
                name: self.name.clone(),
                versions: vec![FileInfo {
                    volume: bucket.to_string(),
                    name: self.name.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        if let Some(fm) = &self.cached {
            let mut versions = fm.get_file_info_versions(bucket, self.name.as_str(), false)?;
            versions.free_versions.clear();
            return Ok(versions);
        }

        let mut versions = FileMeta::decode_file_info_versions_from_buf(&self.metadata, bucket, self.name.as_str(), false)?;
        versions.free_versions.clear();
        Ok(versions)
    }

    pub fn file_info_versions_with_free_versions(&self, bucket: &str) -> Result<FileInfoVersions> {
        if self.is_dir() {
            return Ok(FileInfoVersions {
                volume: bucket.to_string(),
                name: self.name.clone(),
                versions: vec![FileInfo {
                    volume: bucket.to_string(),
                    name: self.name.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        if let Some(fm) = &self.cached {
            return fm.get_file_info_versions(bucket, self.name.as_str(), false);
        }

        // `decode_file_info_versions_from_buf(..., false)` separates persisted
        // tier free-version records into `free_versions` without allocating an
        // intermediate `FileMeta`.
        FileMeta::decode_file_info_versions_from_buf(&self.metadata, bucket, self.name.as_str(), false)
    }

    fn borrow_metadata(&self) -> Result<crate::filemeta::BorrowedFileMeta<'_>> {
        if self.is_dir() {
            return Err(Error::FileNotFound);
        }
        match &self.cached {
            Some(metadata) => Ok(metadata.borrow_cached()),
            None => FileMeta::decode_borrowed(&self.metadata),
        }
    }

    /// Compare a replica group after validating this history once.
    pub fn headers_agree<'a>(&'a self, replicas: impl IntoIterator<Item = &'a Self>, strict: bool) -> bool {
        if self.is_dir() {
            return replicas.into_iter().all(|other| self.name == other.name && other.is_dir());
        }
        let Ok(metadata) = self.borrow_metadata() else {
            return false;
        };
        replicas.into_iter().all(|other| {
            if self.name != other.name || other.is_dir() {
                return false;
            }
            if std::ptr::eq(self, other) {
                return true;
            }
            if let Some(other) = &other.cached {
                crate::filemeta::reconcile_headers(
                    other.versions.iter().map(|version| &version.header),
                    metadata.versions.as_slice().iter().map(|(header, _, _)| header),
                    true,
                    strict,
                )
                .is_some()
            } else {
                FileMeta::decode_borrowed_with_hint(&other.metadata, Some(&metadata)).is_ok_and(|candidate| {
                    crate::filemeta::reconcile_headers(
                        candidate.versions.as_slice().iter().map(|(header, _, _)| header),
                        metadata.versions.as_slice().iter().map(|(header, _, _)| header),
                        true,
                        strict,
                    )
                    .is_some()
                })
            }
        })
    }

    fn cached_latest_listing_quorum(&self, params: &MetadataResolutionParams) -> Option<usize> {
        if params.requested_versions != 1 {
            return None;
        }
        self.cached
            .as_ref()?
            .versions
            .first()
            .filter(|version| !version.header.free_version())
            .map(|version| version.listing_quorum(params.obj_quorum, params.write_quorum_slack))
    }

    /// Whether the inspected versions need more independent replica votes.
    pub fn needs_quorum_supplement(&self, replicas: usize, params: &MetadataResolutionParams) -> Result<bool> {
        if let Some(required) = self.cached_latest_listing_quorum(params) {
            return Ok(replicas < required);
        }
        let metadata = self.borrow_metadata()?;
        let mut selected = 0;
        for (header, _, meta) in metadata.versions.as_slice() {
            let required =
                crate::filemeta::listing_quorum_from_header_and_meta(header, meta, params.obj_quorum, params.write_quorum_slack);
            if replicas < required && (params.requested_versions == 0 || selected < params.requested_versions) {
                return Ok(true);
            }
            if !header.free_version() {
                selected += 1;
            }
            if params.requested_versions > 0 && selected == params.requested_versions {
                break;
            }
        }
        Ok(false)
    }

    pub fn matches(&self, other: Option<&MetaCacheEntry>, strict: bool) -> (Option<MetaCacheEntry>, bool) {
        if other.is_none() {
            return (None, false);
        }

        let other = other.unwrap();
        if self.name != other.name {
            if self.name < other.name {
                return (Some(self.clone()), false);
            }
            return (Some(other.clone()), false);
        }

        if other.is_dir() || self.is_dir() {
            if self.is_dir() {
                return (Some(self.clone()), other.is_dir() == self.is_dir());
            }
            return (Some(other.clone()), other.is_dir() == self.is_dir());
        }

        let self_vers = match &self.cached {
            Some(file_meta) => std::borrow::Cow::Borrowed(file_meta),
            None => match FileMeta::load(&self.metadata) {
                Ok(meta) => std::borrow::Cow::Owned(meta),
                Err(_) => return (None, false),
            },
        };

        let other_vers = match &other.cached {
            Some(file_meta) => std::borrow::Cow::Borrowed(file_meta),
            None => match FileMeta::load(&other.metadata) {
                Ok(meta) => std::borrow::Cow::Owned(meta),
                Err(_) => return (None, false),
            },
        };

        if self_vers.versions.len() != other_vers.versions.len() {
            match self_vers.latest_mod_time().cmp(&other_vers.latest_mod_time()) {
                Ordering::Greater => return (Some(self.clone()), false),
                Ordering::Less => return (Some(other.clone()), false),
                _ => {}
            }

            if self_vers.versions.len() > other_vers.versions.len() {
                return (Some(self.clone()), false);
            }
            return (Some(other.clone()), false);
        }

        let mut prefer = None;
        for (s_version, o_version) in self_vers.versions.iter().zip(other_vers.versions.iter()) {
            if !FileMeta::headers_match_strict(&s_version.header, &o_version.header) {
                if !strict && s_version.header.matches_not_strict(&o_version.header) {
                    if prefer.is_none() {
                        if s_version.header.sorts_before(&o_version.header) {
                            prefer = Some(self.clone());
                        } else {
                            prefer = Some(other.clone());
                        }
                    }
                    continue;
                }

                if prefer.is_some() {
                    return (prefer, false);
                }

                if s_version.header.sorts_before(&o_version.header) {
                    return (Some(self.clone()), false);
                }

                return (Some(other.clone()), false);
            }
        }

        if prefer.is_none() {
            prefer = Some(self.clone());
        }

        (prefer, true)
    }

    pub fn matches_strict(&mut self, other: &MetaCacheEntry) -> bool {
        if self.name != other.name {
            return false;
        }

        if other.is_dir() || self.is_dir() {
            return other.is_dir() == self.is_dir();
        }

        let Ok(self_vers) = self.xl_meta_ref() else {
            return false;
        };

        if let Some(other_vers) = &other.cached {
            if self_vers.versions.len() != other_vers.versions.len() {
                return false;
            }
            return self_vers
                .versions
                .iter()
                .zip(other_vers.versions.iter())
                .all(|(s, o)| FileMeta::headers_match_strict(&s.header, &o.header));
        }

        self_vers.matches_buf_strict(&other.metadata)
    }

    pub fn xl_meta_ref(&mut self) -> Result<&FileMeta> {
        if self.is_dir() {
            return Err(Error::FileNotFound);
        }

        if self.cached.is_none() {
            if self.metadata.is_empty() {
                return Err(Error::FileNotFound);
            }

            let meta = FileMeta::load(&self.metadata)?;
            self.cached = Some(meta);
        }

        Ok(self.cached.as_ref().expect("cached metadata must be populated"))
    }

    pub fn xl_meta(&mut self) -> Result<FileMeta> {
        self.xl_meta_ref().cloned()
    }
}

/// Outcome of resolving one history observed identically by independent readers.
pub enum MetaCacheEntryResolution {
    Resolved(MetaCacheEntry),
    NeedsSupplement(MetaCacheEntry),
    Rejected,
}

/// Apply per-version listing quorum without materializing unchanged histories.
#[inline]
pub fn resolve_agreed_entry(
    mut entry: MetaCacheEntry,
    replicas: usize,
    params: &MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> MetaCacheEntryResolution {
    use MetaCacheEntryResolution::{NeedsSupplement, Rejected, Resolved};
    if !enforce_write_quorum {
        return Resolved(entry);
    }
    if entry.is_dir() {
        return if replicas >= params.dir_quorum {
            Resolved(entry)
        } else {
            Rejected
        };
    }
    if replicas < params.obj_quorum {
        return Rejected;
    }
    if let Some(required) = entry.cached_latest_listing_quorum(params) {
        return if replicas >= required {
            Resolved(entry)
        } else {
            NeedsSupplement(entry)
        };
    }
    let metadata = match entry.borrow_metadata() {
        Ok(metadata) => metadata,
        Err(_) if params.obj_quorum <= 1 => return Resolved(entry),
        Err(_) => return Rejected,
    };
    let versions = metadata.versions.as_slice();
    let mut selected = Vec::new();
    let mut selected_objects = 0;
    let mut needs_supplement = false;
    for (index, (header, _, meta)) in versions.iter().enumerate() {
        let required =
            crate::filemeta::listing_quorum_from_header_and_meta(header, meta, params.obj_quorum, params.write_quorum_slack);
        if replicas < required {
            needs_supplement |= params.requested_versions == 0 || selected_objects < params.requested_versions;
            continue;
        }
        // The common latest-user-version path keeps the original buffer and cache.
        if index == 0 && params.requested_versions == 1 && !header.free_version() {
            return Resolved(entry);
        }
        selected.push(index);
        if !header.free_version() {
            selected_objects += 1;
        }
        if params.requested_versions > 0 && selected_objects == params.requested_versions {
            break;
        }
    }
    if needs_supplement {
        return NeedsSupplement(entry);
    }
    if selected.is_empty() {
        return Rejected;
    }
    if selected.len() == versions.len() {
        return Resolved(entry);
    }
    if entry.cached.is_some() {
        drop(metadata);
        let Some(cached) = entry.cached.as_mut() else {
            return Rejected;
        };
        let mut selected = selected.into_iter().peekable();
        let mut index = 0;
        cached.versions.retain(|_| {
            let keep = selected.peek() == Some(&index);
            if keep {
                selected.next();
            }
            index += 1;
            keep
        });
        let Ok(metadata) = cached.marshal_msg() else {
            return Rejected;
        };
        entry.metadata = metadata;
    } else {
        let mut filtered = FileMeta {
            meta_ver: metadata.meta_ver,
            ..Default::default()
        };
        if !metadata.inline_buf.is_empty() {
            filtered.data.update(metadata.inline_buf);
        }
        filtered.versions = selected
            .into_iter()
            .map(|index| {
                let (header, _, meta) = &versions[index];
                FileMetaShallowVersion {
                    header: header.clone(),
                    meta: meta.to_vec(),
                }
            })
            .collect();
        let Ok(serialized) = filtered.marshal_msg() else {
            return Rejected;
        };
        entry.metadata = serialized;
        entry.cached = Some(filtered);
    }
    Resolved(entry)
}

#[derive(Debug, Default)]
pub struct MetaCacheEntries(pub Vec<Option<MetaCacheEntry>>);

impl MetaCacheEntries {
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[Option<MetaCacheEntry>] {
        &self.0
    }

    pub fn resolve(&self, params: MetadataResolutionParams) -> Option<MetaCacheEntry> {
        Self::resolve_inner(&self.0, params, false)
    }

    pub fn resolve_with_write_quorum(&self, params: MetadataResolutionParams) -> Option<MetaCacheEntry> {
        Self::resolve_inner(&self.0, params, true)
    }

    /// Resolve independently observed replicas without copying their metadata.
    ///
    /// Each occupied slot represents one distinct reader. Repeated references
    /// are valid only when the caller already established agreement among those
    /// readers; pointer equality alone is not evidence of extra replicas.
    pub fn resolve_borrowed_with_write_quorum(
        entries: &[Option<&MetaCacheEntry>],
        params: MetadataResolutionParams,
    ) -> Option<MetaCacheEntry> {
        Self::resolve_inner(entries, params, true)
    }

    /// Resolve the cross-disk UNION of every version present on ANY disk slot.
    ///
    /// This mirrors MinIO's global heal enumeration (cmd/global-heal.go
    /// `healErasureSet` -> `listPathRaw` with `objQuorum = 1` feeding
    /// `mergeEntries`/`mergeXLV2Versions`): at quorum 1 the merge is forced
    /// strict and returns the union of all per-disk version streams, so a version
    /// that survives on FEWER than read-quorum disks is still surfaced for
    /// healing. Read-quorum resolution (`resolve` with `obj_quorum >= 2`) would
    /// silently drop such a sub-quorum version, which is exactly the durability
    /// gap this enumerator closes (backlog#920).
    ///
    /// `bucket` is only used to tag the merged entry; `strict:false` lets the
    /// non-strict header reconciliation collapse equal-but-differently-signed
    /// replicas of the SAME version id while still retaining genuinely distinct
    /// version ids.
    pub fn resolve_union(&self, bucket: &str) -> Option<MetaCacheEntry> {
        self.resolve(MetadataResolutionParams {
            dir_quorum: 1,
            obj_quorum: 1,
            requested_versions: 0,
            bucket: bucket.to_string(),
            strict: false,
            write_quorum_slack: 0,
            candidates: Vec::new(),
        })
    }

    /// Discover validated object/delete-marker versions and safe unversioned
    /// inspection candidates in the raw entries without applying read quorum.
    /// This is intentionally separate from [`Self::resolve`]: a sub-quorum
    /// version is a valid heal target even though it must not participate in
    /// normal reads or writes.
    ///
    /// The validated list is bounded and deduplicated by object, version id,
    /// and metadata kind; each candidate retains the number of raw disk
    /// entries that carried it so callers can classify sub-quorum versions.
    /// Entries whose xl.meta cannot be decoded are counted separately for
    /// discovery accounting; they never become versionless destructive heal
    /// requests and do not consume the validated quota. An
    /// [`MetaCacheHealCandidateKind::UnversionedObject`] is always consumed by
    /// a non-destructive scanner request.
    pub fn discover_heal_candidates(&self, bucket: &str, max_candidates: usize) -> MetaCacheHealDiscovery {
        let limit = max_candidates.min(MAX_META_CACHE_HEAL_CANDIDATES);
        if limit == 0 || bucket.is_empty() {
            return MetaCacheHealDiscovery::default();
        }

        let mut discovery = MetaCacheHealDiscovery {
            candidates: Vec::<MetaCacheHealCandidate>::with_capacity(limit.min(self.0.len())),
            unverified_count: 0,
            truncated: false,
            truncated_objects: Vec::with_capacity(MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS.min(limit)),
            truncated_candidates: Vec::new(),
        };
        let mut seen: HashMap<(String, Option<Uuid>, MetaCacheHealCandidateKind), usize> =
            HashMap::with_capacity(limit.min(self.0.len()));

        for entry in self.0.iter().flatten() {
            if !valid_heal_candidate_name(bucket, entry) {
                continue;
            }

            let meta = match FileMeta::load(&entry.metadata) {
                Ok(meta) => meta,
                Err(_) => {
                    discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                    continue;
                }
            };
            let mut entry_seen = HashSet::new();

            for shallow in meta.versions {
                let version = match shallow.parse_version_meta() {
                    Ok(version) if version.valid() => version,
                    Ok(_) | Err(_) => {
                        discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                        continue;
                    }
                };
                if version.free_version() {
                    continue;
                }

                let payload_header = version.header();
                if normalize_version_id(shallow.header.version_id) != normalize_version_id(payload_header.version_id)
                    || shallow.header.version_type != payload_header.version_type
                {
                    discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                    continue;
                }

                let (kind, version_id) = match version.version_type {
                    VersionType::Object
                        if version.object.is_some() && version.delete_marker.is_none() && version.legacy_object.is_none() =>
                    {
                        match version.object.as_ref().and_then(|object| object.version_id) {
                            Some(id) if !id.is_nil() => (MetaCacheHealCandidateKind::Object, Some(id)),
                            Some(_) | None => (MetaCacheHealCandidateKind::UnversionedObject, None),
                        }
                    }
                    VersionType::Delete
                        if version.delete_marker.is_some() && version.object.is_none() && version.legacy_object.is_none() =>
                    {
                        let Some(id) = version.delete_marker.as_ref().and_then(|marker| marker.version_id) else {
                            discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                            continue;
                        };
                        if id.is_nil() {
                            discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                            continue;
                        }
                        (MetaCacheHealCandidateKind::DeleteMarker, Some(id))
                    }
                    VersionType::Legacy
                        if version.legacy_object.is_some() && version.object.is_none() && version.delete_marker.is_none() =>
                    {
                        let Some(legacy) = version.legacy_object.as_ref() else {
                            continue;
                        };
                        if legacy.version_id.is_empty() {
                            (MetaCacheHealCandidateKind::UnversionedObject, None)
                        } else {
                            let Ok(id) = Uuid::parse_str(&legacy.version_id) else {
                                discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                                continue;
                            };
                            if id.is_nil() {
                                discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                                continue;
                            }
                            (MetaCacheHealCandidateKind::Object, Some(id))
                        }
                    }
                    _ => {
                        discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                        continue;
                    }
                };

                if normalize_version_id(payload_header.version_id) != version_id {
                    discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                    continue;
                }

                // `all_parts=true` is the trust-boundary check for versioned
                // candidates. A null/legacy object may still need the old
                // non-destructive inspection fallback when its part arrays
                // are parseable but incomplete; never use that fallback for
                // a candidate carrying a real version id.
                let file_info = match version.clone().into_fileinfo(bucket, &entry.name, true) {
                    Ok(file_info) => file_info,
                    Err(_) if version_id.is_none() && matches!(kind, MetaCacheHealCandidateKind::UnversionedObject) => {
                        discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                        match version.into_fileinfo(bucket, &entry.name, false) {
                            Ok(file_info) => file_info,
                            Err(_) => continue,
                        }
                    }
                    Err(_) => {
                        discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                        continue;
                    }
                };
                if file_info.volume != bucket || file_info.name != entry.name {
                    discovery.unverified_count = discovery.unverified_count.saturating_add(1);
                    continue;
                }

                let candidate = MetaCacheHealCandidate {
                    object: entry.name.clone(),
                    version_id,
                    kind,
                    replica_count: 1,
                };
                let key = (candidate.object.clone(), candidate.version_id, candidate.kind.clone());
                if entry_seen.contains(&key) {
                    continue;
                }
                if let Some(index) = seen.get(&key).copied() {
                    entry_seen.insert(key);
                    discovery.candidates[index].replica_count = discovery.candidates[index].replica_count.saturating_add(1);
                } else if discovery.candidates.len() >= limit {
                    // Keep the validated candidate list bounded, but retain a
                    // bounded object-level continuation so the scanner cannot
                    // silently lose every version of a busy object.
                    discovery.truncated = true;
                    if discovery.truncated_objects.len() < MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS
                        && !discovery.truncated_objects.iter().any(|object| object == &candidate.object)
                    {
                        discovery.truncated_objects.push(candidate.object.clone());
                    }
                    if discovery.truncated_objects.iter().any(|object| object == &candidate.object) {
                        discovery.truncated_candidates.push(candidate);
                    }
                    continue;
                } else {
                    entry_seen.insert(key.clone());
                    seen.insert(key, discovery.candidates.len());
                    discovery.candidates.push(candidate);
                }
            }
        }

        discovery
    }

    fn resolve_inner<T: std::borrow::Borrow<MetaCacheEntry>>(
        entries: &[Option<T>],
        mut params: MetadataResolutionParams,
        enforce_write_quorum: bool,
    ) -> Option<MetaCacheEntry> {
        if entries.is_empty() {
            debug!(
                bucket = %params.bucket,
                dir_quorum = params.dir_quorum,
                obj_quorum = params.obj_quorum,
                "metacache resolve skipped because there were no entries to reconcile"
            );
            return None;
        }

        let mut dir_exists = 0;
        let mut directory: Option<&MetaCacheEntry> = None;
        let mut selected = None;
        let mut invalid_entries: Vec<usize> = Vec::new();
        let mut fallback_borrowed = Vec::new();

        params.candidates.clear();
        let mut objs_agree = 0;
        let mut objs_valid = 0;

        for (entry_idx, entry) in entries.iter().flatten().map(std::borrow::Borrow::borrow).enumerate() {
            debug!(entry = %entry.name, "metacache resolve examining candidate entry");
            if entry.name.is_empty() {
                continue;
            }
            if entry.is_dir() {
                dir_exists += 1;
                directory = Some(entry);
                debug!(entry = %entry.name, "metacache resolve observed directory candidate");
                continue;
            }

            let Some((current_selected_idx, current_selected, selected_borrowed)) = &mut selected else {
                let initial_borrowed = if let Some(fm) = &entry.cached {
                    fm.borrow_cached()
                } else if entry.metadata.is_empty() {
                    debug!(entry = %entry.name, "metacache resolve skipped candidate with empty xl.meta");
                    continue;
                } else {
                    match FileMeta::decode_borrowed(&entry.metadata) {
                        Ok(borrowed) => borrowed,
                        Err(e) => {
                            invalid_entries.push(entry_idx);
                            debug!(entry = %entry.name, error = ?e, "metacache resolve skipped candidate with unreadable xl.meta");
                            continue;
                        }
                    }
                };
                selected = Some((entry_idx, entry, initial_borrowed));
                objs_valid = 1;
                objs_agree = 1;
                debug!(entry = %entry.name, "metacache resolve selected initial candidate");
                continue;
            };

            let same_name = entry.name == current_selected.name;
            let outcome = if std::ptr::eq(entry, *current_selected) {
                Some(true)
            } else if let Some(fm) = &entry.cached {
                let (res, maybe_prev) = selected_borrowed.compare_cached(fm, same_name, params.strict);
                if let Some(prev) = maybe_prev {
                    fallback_borrowed.push((*current_selected_idx, prev));
                }
                res
            } else if entry.metadata.is_empty() {
                debug!(entry = %entry.name, "metacache resolve skipped candidate with empty xl.meta");
                continue;
            } else if current_selected.cached.is_none() && entry.metadata == current_selected.metadata {
                same_name.then_some(true)
            } else {
                let hint = fallback_borrowed.last().map(|(_, b)| b);
                match FileMeta::validate_and_compare_borrowed(&entry.metadata, selected_borrowed, hint, same_name, params.strict)
                {
                    Ok((res, maybe_borrowed)) => {
                        if let Some(borrowed) = maybe_borrowed {
                            let saved_idx = if res == Some(true) { *current_selected_idx } else { entry_idx };
                            fallback_borrowed.push((saved_idx, borrowed));
                        }
                        res
                    }
                    Err(e) => {
                        invalid_entries.push(entry_idx);
                        debug!(entry = %entry.name, error = ?e, "metacache resolve skipped candidate with unreadable xl.meta");
                        continue;
                    }
                }
            };

            objs_valid += 1;
            if let Some(prefer_self) = outcome {
                if prefer_self {
                    *current_selected_idx = entry_idx;
                    *current_selected = entry;
                }
                objs_agree += 1;
                debug!(entry = %entry.name, "metacache resolve preferred candidate during reconciliation");
            }
        }

        // A prefix and an explicit directory-marker object can share a name.
        // Keep directory evidence separate so its arrival cannot displace an
        // object that meets the metadata quorum.
        let directory = directory.filter(|_| dir_exists >= params.dir_quorum);
        let Some((_selected_idx, selected_ref, selected_borrowed)) = selected else {
            debug!(
                bucket = %params.bucket,
                dir_quorum = params.dir_quorum,
                obj_quorum = params.obj_quorum,
                "metacache resolve could not select any candidate entry"
            );
            return directory.cloned();
        };

        // If we would never be able to reach the required object quorum.
        if objs_valid < params.obj_quorum {
            debug!(
                objs_valid,
                obj_quorum = params.obj_quorum,
                "metacache resolve did not have enough valid object candidates to satisfy quorum"
            );
            return directory.cloned();
        }

        if objs_agree == objs_valid
            && (!enforce_write_quorum
                || (params.requested_versions == 1
                    && selected_borrowed
                        .versions
                        .as_slice()
                        .first()
                        .is_some_and(|(header, _, _)| !header.free_version())))
        {
            let required_quorum = if enforce_write_quorum {
                selected_borrowed
                    .versions
                    .as_slice()
                    .first()
                    .map(|(hdr, _, meta)| {
                        crate::filemeta::listing_quorum_from_header_and_meta(
                            hdr,
                            meta,
                            params.obj_quorum,
                            params.write_quorum_slack,
                        )
                    })
                    .unwrap_or(params.obj_quorum)
            } else {
                params.obj_quorum
            };

            if objs_agree >= required_quorum {
                debug!(
                    selected = %selected_ref.name,
                    objs_agree,
                    objs_valid,
                    "metacache resolve reused selected candidate because all valid object entries agreed"
                );
                let selected_fm = selected_ref
                    .cached
                    .clone()
                    .unwrap_or_else(|| selected_borrowed.into_file_meta());
                return Some(MetaCacheEntry {
                    name: selected_ref.name.clone(),
                    metadata: selected_ref.metadata.clone(),
                    cached: Some(selected_fm),
                    reusable: selected_ref.reusable,
                });
            }
        }

        let selected_meta_ver = selected_borrowed.meta_ver;
        let mut streams: Vec<Vec<(&crate::FileMetaVersionHeader, &[u8])>> = Vec::with_capacity(objs_valid);
        for (entry_idx, entry) in entries.iter().flatten().map(std::borrow::Borrow::borrow).enumerate() {
            if entry.name.is_empty() || entry.is_dir() || invalid_entries.contains(&entry_idx) {
                continue;
            }
            if let Some(fm) = &entry.cached {
                streams.push(fm.versions.iter().map(|v| (&v.header, v.meta.as_slice())).collect());
            } else if std::ptr::eq(entry, selected_ref)
                || (selected_ref.cached.is_none() && entry.metadata == selected_ref.metadata)
            {
                streams.push(selected_borrowed.as_borrowed_stream());
            } else if let Some((_, borrowed)) = fallback_borrowed.iter().find(|(idx, _)| *idx == entry_idx) {
                streams.push(borrowed.as_borrowed_stream());
            } else if let Some(stream) = selected_borrowed
                .stream_from_prevalidated_matching_buf(&entry.metadata)
                .or_else(|| {
                    fallback_borrowed
                        .iter()
                        .find_map(|(_, b)| b.stream_from_prevalidated_matching_buf(&entry.metadata))
                })
            {
                streams.push(stream);
            }
        }
        debug_assert_eq!(streams.len(), objs_valid);

        let had_user_versions = enforce_write_quorum
            && streams
                .iter()
                .any(|stream| stream.iter().any(|(header, _)| !header.free_version()));
        let versions = crate::filemeta::merge_file_meta_borrowed_streams(
            params.obj_quorum,
            params.strict,
            params.requested_versions,
            enforce_write_quorum,
            params.write_quorum_slack,
            streams,
        );
        if versions.is_empty() || (had_user_versions && versions.iter().all(|version| version.header.free_version())) {
            debug!(
                selected = %selected_ref.name,
                requested_versions = params.requested_versions,
                "metacache resolve produced no merged versions after reconciliation"
            );
            return directory.cloned();
        }

        let merged_cached = FileMeta {
            meta_ver: selected_meta_ver,
            versions,
            ..Default::default()
        };

        let metadata = match merged_cached.marshal_msg() {
            Ok(meta) => meta,
            Err(e) => {
                warn!(
                    selected = %selected_ref.name,
                    error = ?e,
                    "metacache resolve failed to marshal merged metadata entry"
                );
                return directory.cloned();
            }
        };

        // Merge if we have disagreement.
        // Create a new merged result.
        let new_selected = MetaCacheEntry {
            name: selected_ref.name.clone(),
            cached: Some(merged_cached),
            reusable: true,
            metadata,
        };

        debug!(
            selected = %new_selected.name,
            candidate_count = params.candidates.len(),
            obj_quorum = params.obj_quorum,
            "metacache resolve produced merged metadata entry after reconciling disagreeing candidates"
        );
        Some(new_selected)
    }

    pub fn first_found(&self) -> (Option<MetaCacheEntry>, usize) {
        (self.0.iter().find(|x| x.is_some()).cloned().unwrap_or_default(), self.0.len())
    }
}

fn valid_heal_candidate_name(bucket: &str, entry: &MetaCacheEntry) -> bool {
    if bucket.is_empty()
        || entry.name.is_empty()
        || entry.is_dir()
        || (cfg!(windows) && entry.name.contains('\\'))
        || entry.name.chars().any(char::is_control)
    {
        return false;
    }

    // Validate raw key components without normalizing them. The scanner maps
    // accepted keys to filesystem paths later, so dot components and empty
    // internal components must be rejected before that boundary. A final
    // empty component is retained for valid keys ending in '/'.
    let mut components = entry.name.split('/').peekable();
    while let Some(component) = components.next() {
        if component == "." || component == ".." || (component.is_empty() && components.peek().is_some()) {
            return false;
        }
    }
    true
}

fn normalize_version_id(version_id: Option<Uuid>) -> Option<Uuid> {
    version_id.filter(|id| !id.is_nil())
}

#[derive(Debug, Default)]
pub struct MetaCacheEntriesSortedResult {
    pub entries: Option<MetaCacheEntriesSorted>,
    pub err: Option<Error>,
}

#[derive(Debug, Default)]
pub struct MetaCacheEntriesSorted {
    pub o: MetaCacheEntries,
    pub list_id: Option<String>,
    pub reuse: bool,
    pub last_skipped_entry: Option<String>,
}

impl MetaCacheEntriesSorted {
    pub fn entries(&self) -> Vec<&MetaCacheEntry> {
        let entries: Vec<&MetaCacheEntry> = self.o.0.iter().flatten().collect();
        entries
    }

    pub fn forward_past(&mut self, marker: Option<String>) {
        if let Some(val) = marker
            && let Some(idx) = self.o.0.iter().flatten().position(|v| v.name > val)
        {
            self.o.0 = self.o.0.split_off(idx);
        }
    }
}

const METACACHE_STREAM_VERSION: u8 = 2;
const METACACHE_READ_CHUNK_SIZE: usize = 64 << 10;

#[derive(Debug)]
pub struct MetacacheWriter<W> {
    wr: W,
    created: bool,
    buf: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> MetacacheWriter<W> {
    pub fn new(wr: W) -> Self {
        Self {
            wr,
            created: false,
            buf: Vec::new(),
        }
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.wr.write_all(&self.buf).await?;
        self.buf.clear();
        Ok(())
    }

    pub async fn init(&mut self) -> Result<()> {
        if !self.created {
            rmp::encode::write_u8(&mut self.buf, METACACHE_STREAM_VERSION).map_err(|e| Error::other(format!("{e:?}")))?;
            self.flush().await?;
            self.created = true;
        }
        Ok(())
    }

    pub async fn write(&mut self, objs: &[MetaCacheEntry]) -> Result<()> {
        if objs.is_empty() {
            return Ok(());
        }

        self.init().await?;

        for obj in objs.iter() {
            if obj.name.is_empty() {
                return Err(Error::other("metacacheWriter: no name"));
            }

            Self::encode_obj(&mut self.buf, obj)?;
            if self.buf.len() >= METACACHE_READ_CHUNK_SIZE {
                self.flush().await?;
            }
        }

        if !self.buf.is_empty() {
            self.flush().await?;
        }

        Ok(())
    }

    fn encode_obj(buf: &mut Vec<u8>, obj: &MetaCacheEntry) -> Result<()> {
        rmp::encode::write_bool(buf, true).map_err(|e| Error::other(format!("{e:?}")))?;
        rmp::encode::write_str(buf, &obj.name).map_err(|e| Error::other(format!("{e:?}")))?;
        rmp::encode::write_bin(buf, &obj.metadata).map_err(|e| Error::other(format!("{e:?}")))?;
        Ok(())
    }

    pub async fn write_obj(&mut self, obj: &MetaCacheEntry) -> Result<()> {
        self.init().await?;

        Self::encode_obj(&mut self.buf, obj)?;
        self.flush().await?;

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.init().await?;
        rmp::encode::write_bool(&mut self.buf, false).map_err(|e| Error::other(format!("{e:?}")))?;
        self.flush().await?;
        Ok(())
    }
}

pub struct MetacacheReader<R> {
    rd: R,
    init: bool,
    in_entry: bool,
    err: Option<Error>,
    buf: Vec<u8>,
    pos: usize,
    has_current: bool,
}

impl<R: AsyncRead + Unpin> MetacacheReader<R> {
    pub fn new(rd: R) -> Self {
        Self {
            rd,
            init: false,
            in_entry: false,
            err: None,
            buf: Vec::new(),
            pos: 0,
            has_current: false,
        }
    }

    pub async fn read_more(&mut self, read_size: usize) -> Result<&[u8]> {
        self.read_more_inner(read_size, true).await
    }

    async fn read_more_inner(&mut self, read_size: usize, lookahead: bool) -> Result<&[u8]> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        if self.buf.len() - self.pos >= read_size {
            let start = self.pos;
            self.pos += read_size;
            return Ok(&self.buf[start..self.pos]);
        }

        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }

        // A whole xl.meta can legitimately exceed the limit on its individual
        // msgpack values. Grow only as payload arrives, so a corrupt length
        // prefix cannot cause a speculative allocation of the advertised size.
        while self.buf.len() < read_size {
            let start = self.buf.len();
            let extra = if lookahead {
                let min_chunk = METACACHE_READ_CHUNK_SIZE.saturating_sub(start);
                (read_size - start).clamp(min_chunk, METACACHE_READ_CHUNK_SIZE)
            } else {
                (read_size - start).min(METACACHE_READ_CHUNK_SIZE)
            };
            if let Err(e) = self.buf.try_reserve(extra) {
                let err = Error::other(format!("metacache stream: buffer allocation of {extra} bytes failed: {e}"));
                self.err = Some(err.clone());
                return Err(err);
            }
            let mut dst = bytes::BufMut::limit(&mut self.buf, extra);
            match self.rd.read_buf(&mut dst).await {
                Ok(0) => {
                    // Listings treat Unexpected/UnexpectedEof as clean exhaustion.
                    // An incomplete entry must not hide remaining keys. Preserve
                    // the existing empty/between-entry EOF handling.
                    let err = if self.in_entry {
                        Error::FileCorrupt
                    } else {
                        Error::Unexpected
                    };
                    self.err = Some(err.clone());
                    return Err(err);
                }
                Ok(_) => {}
                Err(e) => {
                    let err = match e.kind() {
                        std::io::ErrorKind::UnexpectedEof if self.in_entry => Error::FileCorrupt,
                        _ => Error::from(e),
                    };
                    self.err = Some(err.clone());
                    return Err(err);
                }
            }
        }

        self.pos = read_size;
        Ok(&self.buf[..read_size])
    }

    async fn discard(&mut self, mut size: usize) -> Result<()> {
        let avail = self.buf.len() - self.pos;
        if size <= avail {
            self.pos += size;
            self.reset();
            return Ok(());
        }
        size -= avail;
        self.buf.clear();
        self.pos = 0;
        while size > 0 {
            let chunk = size.min(METACACHE_READ_CHUNK_SIZE);
            self.read_more_inner(chunk, false).await?;
            size -= chunk;
            self.buf.clear();
            self.pos = 0;
        }
        Ok(())
    }

    fn reset(&mut self) {
        if self.pos >= self.buf.len() {
            self.buf.clear();
            self.pos = 0;
            if self.buf.capacity() > METACACHE_READ_CHUNK_SIZE {
                self.buf.shrink_to(METACACHE_READ_CHUNK_SIZE);
            }
        } else if self.pos >= METACACHE_READ_CHUNK_SIZE {
            self.buf.drain(..self.pos);
            self.pos = 0;
            if self.buf.capacity() > METACACHE_READ_CHUNK_SIZE {
                self.buf.shrink_to(METACACHE_READ_CHUNK_SIZE);
            }
        }
    }

    async fn check_init(&mut self) -> Result<()> {
        if !self.init {
            let ver = match rmp::decode::read_u8(&mut self.read_more(2).await?) {
                Ok(res) => res,
                Err(err) => {
                    self.err = Some(Error::other(format!("{err:?}")));
                    0
                }
            };
            match ver {
                1 | 2 => (),
                _ => {
                    self.err = Some(Error::other("invalid version"));
                }
            }

            self.init = true;
        }
        Ok(())
    }

    async fn read_str_len(&mut self) -> Result<u32> {
        let mark = match rmp::decode::read_marker(&mut self.read_more(1).await?) {
            Ok(res) => res,
            Err(err) => {
                let err: Error = err.into();
                self.err = Some(err.clone());
                return Err(err);
            }
        };

        match mark {
            Marker::FixStr(size) => Ok(u32::from(size)),
            Marker::Str8 => Ok(u32::from(self.read_u8().await?)),
            Marker::Str16 => Ok(u32::from(self.read_u16().await?)),
            Marker::Str32 => Ok(self.read_u32().await?),
            _marker => Err(Error::other("str marker err")),
        }
    }

    async fn read_bin_len(&mut self) -> Result<u32> {
        let mark = match rmp::decode::read_marker(&mut self.read_more(1).await?) {
            Ok(res) => res,
            Err(err) => {
                let err: Error = err.into();
                self.err = Some(err.clone());
                return Err(err);
            }
        };

        match mark {
            Marker::Bin8 => Ok(u32::from(self.read_u8().await?)),
            Marker::Bin16 => Ok(u32::from(self.read_u16().await?)),
            Marker::Bin32 => Ok(self.read_u32().await?),
            _ => Err(Error::other("bin marker err")),
        }
    }

    async fn read_u8(&mut self) -> Result<u8> {
        let buf = self.read_more(1).await?;
        Ok(u8::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    async fn read_u16(&mut self) -> Result<u16> {
        let buf = self.read_more(2).await?;
        Ok(u16::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    async fn read_u32(&mut self) -> Result<u32> {
        let buf = self.read_more(4).await?;
        Ok(u32::from_be_bytes(buf.try_into().expect("Slice with incorrect length")))
    }

    pub async fn skip(&mut self, size: usize) -> Result<()> {
        self.check_init().await?;

        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        if size == 0 {
            return Ok(());
        }

        let mut n = size;

        if self.has_current {
            n -= 1;
            self.has_current = false;
        }

        while n > 0 {
            match rmp::decode::read_bool(&mut self.read_more(1).await?) {
                Ok(res) => {
                    if !res {
                        return Ok(());
                    }
                }
                Err(err) => {
                    let err: Error = err.into();
                    self.err = Some(err.clone());
                    return Err(err);
                }
            };

            self.in_entry = true;
            let l = self.read_str_len().await?;
            self.discard(usize::try_from(l).map_err(Error::other)?).await?;
            let l = self.read_bin_len().await?;
            self.discard(usize::try_from(l).map_err(Error::other)?).await?;
            self.in_entry = false;

            n -= 1;
        }

        Ok(())
    }

    pub async fn peek(&mut self) -> Result<Option<MetaCacheEntry>> {
        self.check_init().await?;

        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        match rmp::decode::read_bool(&mut self.read_more(1).await?) {
            Ok(res) => {
                if !res {
                    return Ok(None);
                }
            }
            Err(err) => {
                let err: Error = err.into();
                self.err = Some(err.clone());
                return Err(err);
            }
        };

        self.in_entry = true;
        let l = self.read_str_len().await?;

        let buf = self.read_more(l as usize).await?;
        let name = match from_utf8(buf) {
            Ok(decoded) => decoded.to_owned(),
            Err(err) => {
                self.err = Some(Error::other(err.to_string()));
                return Err(Error::other(err.to_string()));
            }
        };

        let l = self.read_bin_len().await?;

        let metadata = if l == 0 {
            Vec::new()
        } else {
            self.read_more(l as usize).await?.to_vec()
        };

        self.in_entry = false;
        self.reset();

        self.has_current = true;
        Ok(Some(MetaCacheEntry {
            name,
            metadata,
            cached: None,
            reusable: false,
        }))
    }

    pub async fn read_all(&mut self) -> Result<Vec<MetaCacheEntry>> {
        let mut ret = Vec::new();

        loop {
            if let Some(entry) = self.peek().await? {
                ret.push(entry);
                continue;
            }
            break;
        }

        Ok(ret)
    }
}

pub type UpdateFn<T> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = std::io::Result<T>> + Send>> + Send + Sync + 'static>;

#[derive(Clone, Debug, Default)]
pub struct Opts {
    pub return_last_good: bool,
    pub no_wait: bool,
}

pub struct Cache<T: Clone + Debug + Send> {
    update_fn: UpdateFn<T>,
    ttl: Duration,
    opts: Opts,
    val: ArcSwapOption<T>,
    last_update_secs: AtomicU64,
    updating: Arc<Mutex<()>>,
}

impl<T: Clone + Debug + Send + Sync + 'static> Cache<T> {
    pub fn new(update_fn: UpdateFn<T>, ttl: Duration, opts: Opts) -> Self {
        Self {
            update_fn,
            ttl,
            opts,
            val: ArcSwapOption::from(None),
            last_update_secs: AtomicU64::new(0),
            updating: Arc::new(Mutex::new(())),
        }
    }

    pub async fn get(self: Arc<Self>) -> std::io::Result<T> {
        let value = self.get_shared().await?;
        Ok(value.as_ref().clone())
    }

    pub async fn get_shared(self: Arc<Self>) -> std::io::Result<Arc<T>> {
        let now = Self::current_unix_secs();
        let current = self.cached_value();
        if self.age_since_last_update(now) < self.ttl.as_secs()
            && let Some(value) = current.clone()
        {
            return Ok(value);
        }

        if self.opts.no_wait
            && self.age_since_last_update(now) < self.ttl.as_secs().saturating_mul(2)
            && let Some(value) = current
        {
            if let Ok(update_guard) = Arc::clone(&self.updating).try_lock_owned() {
                let this = Arc::clone(&self);
                spawn(async move {
                    let _guard = update_guard;
                    let _ = this.update().await;
                });
            }
            return Ok(value);
        }

        let _guard = self.updating.lock().await;

        let now = Self::current_unix_secs();
        if self.age_since_last_update(now) < self.ttl.as_secs()
            && let Some(value) = self.cached_value()
        {
            return Ok(value);
        }

        self.update().await?;
        self.cached_value()
            .ok_or_else(|| std::io::Error::other("cache update completed without a value"))
    }

    async fn update(&self) -> std::io::Result<()> {
        match (self.update_fn)().await {
            Ok(val) => {
                self.val.store(Some(Arc::new(val)));
                self.last_update_secs.store(Self::current_unix_secs(), AtomicOrdering::SeqCst);
                Ok(())
            }
            Err(err) => {
                if self.opts.return_last_good && self.cached_value().is_some() {
                    return Ok(());
                }

                Err(err)
            }
        }
    }

    fn current_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    fn age_since_last_update(&self, now_secs: u64) -> u64 {
        now_secs
            .checked_sub(self.last_update_secs.load(AtomicOrdering::SeqCst))
            .unwrap_or(u64::MAX)
    }

    fn cached_value(&self) -> Option<Arc<T>> {
        self.val.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_data::create_real_xlmeta;
    use crate::{FileMetaVersion, MetaDeleteMarker, MetaObjectV1, MetaObjectV1Erasure, MetaObjectV1Stat, TRANSITION_COMPLETE};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::{Notify, oneshot};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_writer() {
        let mut f = Cursor::new(Vec::new());
        let mut w = MetacacheWriter::new(&mut f);

        let mut objs = Vec::new();
        for i in 0..10 {
            let info = MetaCacheEntry {
                name: format!("item{i}"),
                metadata: vec![0u8, 10],
                cached: None,
                reusable: false,
            };
            objs.push(info);
        }

        w.write(&objs).await.unwrap();
        w.close().await.unwrap();

        let data = f.into_inner();
        let nf = Cursor::new(data);

        let mut r = MetacacheReader::new(nf);
        let nobjs = r.read_all().await.unwrap();

        assert_eq!(objs, nobjs);
    }

    #[tokio::test]
    async fn test_reader_consecutive_peeks_and_mixed_skips_preserve_entries() {
        let entries: Vec<_> = [vec![], vec![1, 2, 3], vec![], vec![4, 5], vec![]]
            .into_iter()
            .enumerate()
            .map(|(i, metadata)| MetaCacheEntry {
                name: format!("object-{i}"),
                metadata,
                ..Default::default()
            })
            .collect();
        let mut output = Cursor::new(Vec::new());
        let mut writer = MetacacheWriter::new(&mut output);
        writer.write(&entries).await.expect("mixed metadata entries must encode");
        writer.close().await.expect("stream must close");
        let mut reader = MetacacheReader::new(Cursor::new(output.into_inner()));
        assert_eq!(reader.peek().await.expect("first entry must decode"), Some(entries[0].clone()));
        assert_eq!(reader.peek().await.expect("peek must advance without skip"), Some(entries[1].clone()));
        reader.skip(0).await.expect("zero skip must preserve the current entry");
        reader
            .skip(2)
            .await
            .expect("skip must consume the current and following entry");
        assert_eq!(reader.read_all().await.expect("remaining entries must decode"), entries[3..]);
    }

    struct FragmentedReader<'a> {
        data: &'a [u8],
        chunk: usize,
        reads: usize,
    }

    impl AsyncRead for FragmentedReader<'_> {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let count = this.data.len().min(this.chunk).min(buf.remaining());
            buf.put_slice(&this.data[..count]);
            this.data = &this.data[count..];
            this.reads += 1;
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_reader_buffered_wire_frames_survive_fragmented_reads_and_skips() {
        for version in [1, 2] {
            let mut bytes = vec![0xcc, version];
            let payloads = [
                vec![1; METACACHE_READ_CHUNK_SIZE - 1],
                vec![],
                vec![3; METACACHE_READ_CHUNK_SIZE + 1],
            ];
            for (i, payload) in payloads.iter().enumerate() {
                rmp::encode::write_bool(&mut bytes, true).expect("entry marker");
                rmp::encode::write_str(&mut bytes, &format!("key-{i}")).expect("entry name");
                rmp::encode::write_bin(&mut bytes, payload).expect("entry metadata");
            }
            rmp::encode::write_bool(&mut bytes, false).expect("stream end");
            for chunk in [1, 2, 7, 4096, METACACHE_READ_CHUNK_SIZE] {
                let mut reader = MetacacheReader::new(FragmentedReader {
                    data: &bytes,
                    chunk,
                    reads: 0,
                });
                assert_eq!(reader.peek().await.expect("first entry").expect("present").metadata, payloads[0]);
                reader.skip(2).await.expect("consume first and empty second entries");
                let last = reader.peek().await.expect("last entry").expect("present");
                assert_eq!(last.name, "key-2");
                assert_eq!(last.metadata, payloads[2]);
                assert!(reader.peek().await.expect("valid terminator").is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_reader_coalesces_small_entries_without_per_field_reads() {
        let mut bytes = vec![0xcc, 2];
        for _ in 0..1000 {
            bytes.extend_from_slice(&[0xc3, 0xa1, b'x', 0xc4, 1, 0x42]);
        }
        bytes.push(0xc2);
        let mut reader = MetacacheReader::new(FragmentedReader {
            data: &bytes,
            chunk: METACACHE_READ_CHUNK_SIZE,
            reads: 0,
        });
        assert_eq!(reader.read_all().await.expect("decode page").len(), 1000);
        assert_eq!(reader.rd.reads, 1, "a ready page smaller than 64 KiB needs one stream read");
    }

    #[tokio::test]
    async fn test_reader_invalid_utf8_name_remains_a_sticky_error() {
        let stream = vec![0xcc, 2, 0xc3, 0xa1, 0xff, 0xc4, 0, 0xc2];
        let mut reader = MetacacheReader::new(Cursor::new(stream));
        let err = reader.peek().await.expect_err("invalid UTF-8 name must be rejected");
        assert!(err.to_string().contains("utf-8"));
        assert_eq!(reader.peek().await.expect_err("invalid name must leave a sticky error"), err);
    }

    #[tokio::test]
    async fn empty_writer_emits_a_valid_stream() {
        let mut output = Cursor::new(Vec::new());
        let mut writer = MetacacheWriter::new(&mut output);
        writer.close().await.expect("empty stream should close");

        let mut reader = MetacacheReader::new(Cursor::new(output.into_inner()));
        assert!(reader.read_all().await.expect("empty stream should decode").is_empty());
    }

    #[tokio::test]
    async fn test_reader_preserves_empty_and_between_entry_eof() {
        let mut empty = MetacacheReader::new(Cursor::new(Vec::<u8>::new()));
        assert_eq!(empty.peek().await.expect_err("legacy empty producer must remain EOF"), Error::Unexpected);

        // A complete entry without a final false marker is a supported legacy EOF.
        let stream = vec![0xcc, 2, 0xc3, 0xa1, b'x', 0xc4, 1, 0x42];
        let mut reader = MetacacheReader::new(Cursor::new(stream.clone()));
        assert!(reader.peek().await.expect("complete entry must decode").is_some());
        reader.skip(1).await.expect("current entry must advance");
        assert_eq!(reader.peek().await.expect_err("between-entry EOF must remain EOF"), Error::Unexpected);

        let mut reader = MetacacheReader::new(Cursor::new(stream));
        reader.skip(1).await.expect("complete entry must be skippable");
        assert_eq!(reader.peek().await.expect_err("skip must restore between-entry state"), Error::Unexpected);
    }

    #[tokio::test]
    async fn test_reader_truncated_entry_is_corruption_at_each_field() {
        // Missing name marker, name bytes, bin marker, bin length, or bin payload.
        for suffix in [vec![], vec![0xa1], vec![0xa0], vec![0xa0, 0xc4], vec![0xa0, 0xc4, 1]] {
            let mut stream = vec![0xcc, 2, 0xc3];
            stream.extend(suffix);
            let mut reader = MetacacheReader::new(Cursor::new(stream.clone()));
            assert_eq!(
                reader.peek().await.expect_err("incomplete entry must not become listing EOF"),
                Error::FileCorrupt
            );
            let mut reader = MetacacheReader::new(Cursor::new(stream));
            assert_eq!(
                reader
                    .skip(1)
                    .await
                    .expect_err("incomplete skipped entry must not become listing EOF"),
                Error::FileCorrupt
            );
        }
    }

    fn corrupt_stream_with_metadata_len(len_marker: u8, len: u32) -> Vec<u8> {
        let mut data = Vec::new();
        rmp::encode::write_u8(&mut data, METACACHE_STREAM_VERSION).unwrap();
        rmp::encode::write_bool(&mut data, true).unwrap();
        rmp::encode::write_str(&mut data, "object").unwrap();
        // Hand-written bin/str length prefix claiming an absurd payload size.
        data.push(len_marker);
        data.extend_from_slice(&len.to_be_bytes());
        data
    }

    /// Regression test for rustfs/rustfs#2715: a corrupted metadata length
    /// prefix must surface as a decode error instead of attempting a huge
    /// allocation that aborts the process.
    #[tokio::test]
    async fn test_reader_rejects_corrupt_bin_length_prefix() {
        // 0xc6 = bin32 marker.
        let data = corrupt_stream_with_metadata_len(0xc6, u32::MAX);
        let mut r = MetacacheReader::new(Cursor::new(data));
        let err = r.peek().await.expect_err("corrupt bin length prefix must fail to decode");
        assert_eq!(err, Error::FileCorrupt);
        assert!(
            r.buf.capacity() <= 2 * METACACHE_READ_CHUNK_SIZE,
            "a length prefix must not reserve its payload"
        );

        // The reader must stay in the error state instead of retrying.
        assert_eq!(r.peek().await.expect_err("decode errors must remain sticky"), err);
    }

    #[tokio::test]
    async fn test_reader_rejects_corrupt_str_length_prefix() {
        let mut data = Vec::new();
        rmp::encode::write_u8(&mut data, METACACHE_STREAM_VERSION).unwrap();
        rmp::encode::write_bool(&mut data, true).unwrap();
        // 0xdb = str32 marker with an absurd object-name length.
        data.push(0xdb);
        data.extend_from_slice(&u32::MAX.to_be_bytes());

        let mut r = MetacacheReader::new(Cursor::new(data));
        let err = r.peek().await.expect_err("corrupt str length prefix must fail to decode");
        assert_eq!(err, Error::FileCorrupt);
        assert!(
            r.buf.capacity() <= 2 * METACACHE_READ_CHUNK_SIZE,
            "a length prefix must not reserve its payload"
        );
        assert_eq!(r.peek().await.expect_err("decode errors must remain sticky"), err);
    }

    #[tokio::test]
    async fn test_reader_skip_rejects_corrupt_length_prefix() {
        let data = corrupt_stream_with_metadata_len(0xc6, u32::MAX);
        let mut r = MetacacheReader::new(Cursor::new(data));
        let err = r.skip(1).await.expect_err("skip over a corrupt length prefix must fail");
        assert_eq!(err, Error::FileCorrupt);
        assert!(r.buf.capacity() <= METACACHE_READ_CHUNK_SIZE);
        assert_eq!(r.skip(1).await.expect_err("skip errors must remain sticky"), err);
        assert_eq!(r.peek().await.expect_err("skip failure must prevent decoding another entry"), err);
    }

    #[tokio::test]
    async fn test_reader_truncated_payload_after_multiple_chunks() {
        let mut data = corrupt_stream_with_metadata_len(0xc6, u32::MAX);
        data.extend(std::iter::repeat_n(0xab, 2 * METACACHE_READ_CHUNK_SIZE + 7));
        let mut reader = MetacacheReader::new(Cursor::new(data));
        let err = reader.peek().await.expect_err("partial payload must fail");
        assert_eq!(err, Error::FileCorrupt);
        assert!(reader.buf.capacity() <= 8 * METACACHE_READ_CHUNK_SIZE);
        assert_eq!(reader.peek().await.expect_err("partial payload must leave a sticky error"), err);
    }

    /// The stream carries a whole xl.meta, whose aggregate size can exceed
    /// the limit for an individual value inside its metadata.
    #[tokio::test]
    async fn test_reader_accepts_large_legitimate_metadata() {
        for size in [16 << 20, (16 << 20) + 1] {
            let entry = MetaCacheEntry {
                name: "big-object".to_string(),
                metadata: vec![0xab; size],
                ..Default::default()
            };

            let mut f = Cursor::new(Vec::new());
            let mut w = MetacacheWriter::new(&mut f);
            w.write(std::slice::from_ref(&entry)).await.expect("large entry must encode");
            w.close().await.expect("large-entry stream must close");

            let mut r = MetacacheReader::new(Cursor::new(f.into_inner()));
            let decoded = r.read_all().await.expect("large entry must decode");
            assert_eq!(decoded, vec![entry]);
        }
    }

    #[tokio::test]
    async fn test_reader_large_inline_version_history_remains_listable() {
        const VERSIONS: u32 = 1700;
        let payload = vec![0xab; 10 << 10];
        let mut meta = FileMeta::new();
        let mut inline = vec![1];
        rmp::encode::write_map_len(&mut inline, VERSIONS).expect("inline version map must encode");
        for i in 1..=VERSIONS {
            let version_id = Uuid::from_u128(u128::from(i));
            let mut fi = FileInfo {
                version_id: Some(version_id),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(i64::from(i))),
                size: i64::try_from(payload.len()).expect("test payload size must fit"),
                ..Default::default()
            };
            fi.set_inline_data();
            meta.add_version(fi).expect("inline version metadata must be valid");
            rmp::encode::write_str(&mut inline, &version_id.to_string()).expect("inline version ID must encode");
            rmp::encode::write_bin(&mut inline, &payload).expect("inline payload must encode");
        }
        // Encode the inline map once; adding payload with each version would
        // repeatedly copy the growing map while constructing this fixture.
        meta.data.update(&inline);
        let metadata = meta.marshal_msg().expect("large xl.meta must encode");
        assert!(metadata.len() > 16 << 20);
        let entry = MetaCacheEntry {
            name: "versioned-object".to_string(),
            metadata,
            ..Default::default()
        };
        let mut output = Cursor::new(Vec::new());
        let mut writer = MetacacheWriter::new(&mut output);
        writer
            .write(std::slice::from_ref(&entry))
            .await
            .expect("large xl.meta must enter the stream");
        writer.close().await.expect("stream must close");
        let mut reader = MetacacheReader::new(Cursor::new(output.into_inner()));
        let decoded = reader
            .peek()
            .await
            .expect("large xl.meta must decode")
            .expect("object must be present");
        assert_eq!(decoded.metadata, entry.metadata);
        let versions = decoded
            .file_info_versions("bucket")
            .expect("version listing must decode xl.meta");
        assert_eq!(versions.versions.len(), usize::try_from(VERSIONS).expect("version count must fit"));
        let latest_id = Uuid::from_u128(u128::from(VERSIONS));
        assert_eq!(versions.versions[0].version_id, Some(latest_id));
        assert_eq!(versions.versions[0].size, i64::try_from(payload.len()).expect("payload size must fit"));
        let loaded = FileMeta::load(&decoded.metadata).expect("stream must preserve valid xl.meta");
        assert_eq!(
            loaded.data.find(&latest_id.to_string()).expect("inline payload must decode"),
            Some(payload)
        );
        assert!(reader.peek().await.expect("stream must terminate").is_none());
    }

    #[tokio::test]
    async fn test_reader_skip_large_entries_preserves_alignment_with_bounded_buffer() {
        // Fixed v1/v2 framing: uint8 version, bool, fixstr name, bin32 payload.
        // Build independently of MetacacheWriter to pin the existing wire shape.
        for version in [1, 2] {
            let mut stream = vec![0xcc, version];
            for size in [0, 2 * METACACHE_READ_CHUNK_SIZE, (16 << 20) + 1] {
                stream.extend_from_slice(&[0xc3, 0xa1, b'x', 0xc6]);
                stream.extend_from_slice(&u32::try_from(size).expect("payload length must fit bin32").to_be_bytes());
                stream.resize(stream.len() + size, 0xab);
            }
            stream.extend_from_slice(&[0xc3, 0xa1, b'y', 0xc4, 1, 0x42, 0xc2]);
            let mut reader = MetacacheReader::new(Cursor::new(stream));
            reader.skip(3).await.expect("large entries must be skippable");
            assert!(reader.buf.is_empty());
            assert!(
                reader.buf.capacity() <= METACACHE_READ_CHUNK_SIZE,
                "skip must not retain discarded entries"
            );
            let entry = reader
                .peek()
                .await
                .expect("following entry must decode")
                .expect("following entry must exist");
            assert_eq!(entry.name, "y");
            assert_eq!(entry.metadata, vec![0x42]);
            reader
                .skip(0)
                .await
                .expect("skipping zero with a current entry must be a no-op");
            reader.skip(1).await.expect("consume current entry without reading another");
            assert!(reader.peek().await.expect("stream must terminate").is_none());
        }
    }

    #[test]
    fn file_info_versions_with_free_versions_includes_persisted_tier_cleanup_records() {
        let version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let mut fm = FileMeta::new();
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("transitioned version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should create free-version metadata");

        let entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: fm.marshal_msg().expect("metadata should marshal"),
            ..Default::default()
        };

        let normal = entry.file_info_versions("bucket").expect("normal versions should parse");
        assert!(normal.free_versions.is_empty());

        let with_free = entry
            .file_info_versions_with_free_versions("bucket")
            .expect("versions with free versions should parse");
        assert_eq!(with_free.free_versions.len(), 1);
        assert!(with_free.free_versions[0].tier_free_version());
        assert_eq!(with_free.free_versions[0].transitioned_objname, "remote/object");
        assert_eq!(with_free.free_versions[0].transition_tier, "WARM");
        assert_eq!(with_free.free_versions[0].transition_version_id, Some(remote_version_id));
    }

    #[test]
    fn file_info_versions_excludes_free_versions_from_visible_version_count() {
        let object_version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let delete_marker_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("transitioned object version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should create a free-version record");

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(delete_marker_id),
            deleted: true,
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("delete marker should be added");

        let entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: fm.marshal_msg().expect("metadata should marshal"),
            ..Default::default()
        };

        let normal = entry.file_info_versions("bucket").expect("normal versions should parse");
        assert_eq!(normal.versions.len(), 1);
        assert!(normal.free_versions.is_empty());
        assert!(normal.versions[0].deleted);
        assert_eq!(normal.versions[0].num_versions, 1);

        let with_free = entry
            .file_info_versions_with_free_versions("bucket")
            .expect("versions with free versions should parse");
        assert_eq!(with_free.versions.len(), 1);
        assert_eq!(with_free.free_versions.len(), 1);
        assert_eq!(with_free.versions[0].num_versions, 1);
        assert_eq!(with_free.free_versions[0].num_versions, 1);
    }

    #[test]
    fn transitioned_delete_persists_recoverable_free_version_after_metadata_roundtrip() {
        let version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let mut fm = FileMeta::new();
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("transitioned version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());

        fm.delete_version(&delete_fi)
            .expect("transitioned delete should persist free-version metadata");
        let encoded = fm.marshal_msg().expect("metadata should marshal");
        let mut decoded = FileMeta::new();
        decoded
            .unmarshal_msg(&encoded)
            .expect("metadata should survive process restart roundtrip");

        let versions = decoded
            .get_file_info_versions("bucket", "object", false)
            .expect("versions with free versions should parse");

        assert!(versions.versions.is_empty());
        assert_eq!(versions.free_versions.len(), 1);
        let free_version = &versions.free_versions[0];
        assert_eq!(free_version.version_id, Some(free_version_id));
        assert!(free_version.tier_free_version());
        assert_eq!(free_version.transitioned_objname, "remote/object");
        assert_eq!(free_version.transition_version_id, Some(remote_version_id));
        assert_eq!(free_version.transition_tier, "WARM");
    }

    #[test]
    fn skip_tier_free_version_does_not_persist_cleanup_record() {
        let version_id = Uuid::new_v4();
        let mut fm = FileMeta::new();
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(Uuid::new_v4()),
            transition_tier: "WARM".to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("transitioned version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&Uuid::new_v4().to_string());
        delete_fi.set_skip_tier_free_version();

        fm.delete_version(&delete_fi)
            .expect("transitioned delete with skip flag should remove the local version");
        let versions = fm
            .get_file_info_versions("bucket", "object", false)
            .expect("versions should parse after skipped cleanup");

        assert!(versions.free_versions.is_empty());
        assert_eq!(versions.versions.len(), 1);
        assert!(versions.versions[0].deleted);
        assert!(!versions.versions[0].tier_free_version());
    }

    #[test]
    fn worker_style_free_version_delete_removes_persisted_cleanup_record() {
        let version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let mut fm = FileMeta::new();
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("transitioned version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should persist free-version metadata");

        let mut free_delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(free_version_id),
            deleted: true,
            ..Default::default()
        };
        free_delete_fi.set_tier_free_version();

        fm.delete_version(&free_delete_fi)
            .expect("worker-style free-version delete should remove the cleanup record");
        let versions = fm
            .get_file_info_versions("bucket", "object", false)
            .expect("versions should parse after free-version cleanup");

        assert!(versions.free_versions.is_empty());
        assert_eq!(versions.versions.len(), 1);
        assert!(versions.versions[0].deleted);
        assert!(!versions.versions[0].tier_free_version());
    }

    #[test]
    fn test_resolve_rebuilds_metadata_from_merged_versions() {
        let base_metadata = create_real_xlmeta().expect("base xl.meta");
        let base = FileMeta::load(&base_metadata).expect("load base xl.meta");

        let extra_version = FileMetaVersion {
            version_type: VersionType::Delete,
            object: None,
            delete_marker: Some(MetaDeleteMarker {
                version_id: Some(Uuid::from_u128(0x22222222333344445555666666666666)),
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp")),
                meta_sys: HashMap::new(),
            }),
            legacy_object: None,
            write_version: 99,
            uses_legacy_checksum: false,
        };

        let extra_shallow = FileMetaShallowVersion::try_from(extra_version).expect("build shallow delete version");

        let mut extended = base.clone();
        extended.versions.insert(0, extra_shallow);

        let base_versions = base.versions.len();
        let extended_versions = extended.versions.len();
        let extended_metadata = extended.marshal_msg().expect("serialize extended xl.meta");

        let resolved = MetaCacheEntries(vec![
            Some(MetaCacheEntry {
                name: "bucket/object".to_string(),
                metadata: extended_metadata,
                cached: Some(extended),
                reusable: false,
            }),
            Some(MetaCacheEntry {
                name: "bucket/object".to_string(),
                metadata: base_metadata,
                cached: Some(base),
                reusable: false,
            }),
        ])
        .resolve(MetadataResolutionParams {
            obj_quorum: 2,
            requested_versions: extended_versions,
            strict: true,
            ..Default::default()
        })
        .expect("merged entry should resolve");

        let cached = resolved.cached.expect("resolved entry should keep merged cached metadata");
        let decoded = FileMeta::load(&resolved.metadata).expect("resolved metadata should decode");

        assert_eq!(cached.versions.len(), base_versions);
        assert_eq!(decoded.versions.len(), base_versions);
        assert_eq!(decoded.versions, cached.versions);
        assert_ne!(extended_versions, cached.versions.len());
    }

    fn metacache_entry_with_mod_time(mod_time: OffsetDateTime, etag: &str) -> MetaCacheEntry {
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), etag.to_string());

        let mut meta = FileMeta::new();
        meta.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            size: 1,
            mod_time: Some(mod_time),
            metadata,
            ..Default::default()
        })
        .expect("test file metadata should accept object version");
        let encoded = meta.marshal_msg().expect("test file metadata should marshal");

        MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn metacache_entry_with_erasure(
        mod_time: OffsetDateTime,
        etag: &str,
        data_blocks: usize,
        parity_blocks: usize,
    ) -> MetaCacheEntry {
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), etag.to_string());

        let mut fi = FileInfo::new("object", data_blocks, parity_blocks);
        fi.volume = "bucket".to_string();
        fi.name = "object".to_string();
        fi.size = 1;
        fi.mod_time = Some(mod_time);
        fi.metadata = metadata;

        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("test file metadata should accept object version");
        let encoded = meta.marshal_msg().expect("test file metadata should marshal");

        MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn metacache_entry_with_erasure_versions(versions: &[(OffsetDateTime, &str, usize, usize)]) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        for (idx, (mod_time, etag, data_blocks, parity_blocks)) in versions.iter().enumerate() {
            let mut metadata = HashMap::new();
            metadata.insert("etag".to_string(), (*etag).to_string());

            let mut fi = FileInfo::new("object", *data_blocks, *parity_blocks);
            fi.volume = "bucket".to_string();
            fi.name = "object".to_string();
            let version_idx = u128::try_from(idx + 1).expect("test version index should fit u128");
            fi.version_id = Some(Uuid::from_u128(version_idx));
            fi.versioned = true;
            fi.size = 1;
            fi.mod_time = Some(*mod_time);
            fi.metadata = metadata;

            meta.add_version(fi).expect("test file metadata should accept object version");
        }
        let encoded = meta.marshal_msg().expect("test file metadata should marshal");

        MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn metacache_entry_without_header_ec(mut entry: MetaCacheEntry) -> MetaCacheEntry {
        let mut cached = entry.cached.take().expect("test entry should have cached metadata");
        for version in cached.versions.iter_mut() {
            version.header.ec_m = 0;
            version.header.ec_n = 0;
        }
        entry.metadata = cached.marshal_msg().expect("test file metadata should marshal");
        entry.cached = Some(cached);
        entry
    }

    fn metacache_dir_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// Build an entry holding a single object version with an explicit version id
    /// and mod_time, so a set of these can model DISJOINT per-disk version sets.
    fn metacache_entry_single_version(version_u128: u128, mod_time: OffsetDateTime, etag: &str) -> MetaCacheEntry {
        let mut metadata = HashMap::new();
        metadata.insert("etag".to_string(), etag.to_string());

        let mut fi = FileInfo::new("object", 4, 2);
        fi.volume = "bucket".to_string();
        fi.name = "object".to_string();
        fi.version_id = Some(Uuid::from_u128(version_u128));
        fi.versioned = true;
        fi.size = 1;
        fi.mod_time = Some(mod_time);
        fi.metadata = metadata;

        let mut meta = FileMeta::new();
        meta.add_version(fi).expect("test file metadata should accept object version");
        let encoded = meta.marshal_msg().expect("test file metadata should marshal");

        MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded,
            cached: Some(meta),
            reusable: false,
        }
    }

    /// backlog#920: `resolve_union` surfaces a version present on a SINGLE slot
    /// among four, while `resolve` at read-quorum (obj_quorum=2) drops it. This
    /// documents the exact sub-quorum durability gap the disk-walk closes.
    #[test]
    fn resolve_union_surfaces_single_disk_version() {
        let t0 = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let t1 = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");

        // Slot 0 carries a UNIQUE version (0xBEEF) present nowhere else; the other
        // three slots agree on a shared version (0xCAFE). Read-quorum sees only
        // the shared one; union must see BOTH.
        let unique = metacache_entry_single_version(0xBEEF, t1, "unique-etag");
        let shared_a = metacache_entry_single_version(0xCAFE, t0, "shared-etag");
        let shared_b = metacache_entry_single_version(0xCAFE, t0, "shared-etag");
        let shared_c = metacache_entry_single_version(0xCAFE, t0, "shared-etag");

        let entries = || {
            MetaCacheEntries(vec![
                Some(unique.clone()),
                Some(shared_a.clone()),
                Some(shared_b.clone()),
                Some(shared_c.clone()),
            ])
        };

        let union = entries().resolve_union("bucket").expect("union must resolve an entry");
        let union_meta = union.cached.expect("union entry keeps cached metadata");
        let union_ids: std::collections::HashSet<Option<Uuid>> =
            union_meta.versions.iter().map(|v| v.header.version_id).collect();
        assert!(
            union_ids.contains(&Some(Uuid::from_u128(0xBEEF))),
            "union must surface the single-slot version: {union_ids:?}"
        );
        assert!(
            union_ids.contains(&Some(Uuid::from_u128(0xCAFE))),
            "union must also keep the shared version: {union_ids:?}"
        );

        // Read-quorum resolution (obj_quorum=2) drops the single-slot version.
        let read_quorum = entries()
            .resolve(MetadataResolutionParams {
                obj_quorum: 2,
                dir_quorum: 2,
                strict: false,
                ..Default::default()
            })
            .expect("read-quorum must still resolve the shared version");
        let rq_meta = read_quorum.cached.expect("read-quorum entry keeps cached metadata");
        let rq_ids: std::collections::HashSet<Option<Uuid>> = rq_meta.versions.iter().map(|v| v.header.version_id).collect();
        assert!(
            !rq_ids.contains(&Some(Uuid::from_u128(0xBEEF))),
            "read-quorum must DROP the single-slot version (the gap): {rq_ids:?}"
        );
        assert!(
            rq_ids.contains(&Some(Uuid::from_u128(0xCAFE))),
            "read-quorum must keep the shared version: {rq_ids:?}"
        );
    }

    #[test]
    fn discover_heal_candidates_keeps_sub_quorum_versions_and_deduplicates() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let entries = MetaCacheEntries(vec![
            Some(metacache_entry_single_version(1, now, "one")),
            Some(metacache_entry_single_version(2, now, "two")),
            Some(metacache_entry_single_version(2, now, "two")),
            Some(metacache_entry_single_version(3, now, "three")),
        ]);

        let discovery = entries.discover_heal_candidates("bucket", 16);
        let ids: std::collections::HashSet<Uuid> = discovery
            .candidates
            .iter()
            .filter_map(|candidate| candidate.version_id)
            .collect();
        assert_eq!(
            ids,
            [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
                .into_iter()
                .collect()
        );
        assert_eq!(discovery.candidates.len(), 3, "duplicate tied versions must be emitted once");
        assert_eq!(
            discovery
                .candidates
                .iter()
                .find(|candidate| candidate.version_id == Some(Uuid::from_u128(2)))
                .expect("duplicate version should be discovered")
                .replica_count,
            2
        );
    }

    #[test]
    fn discover_heal_candidates_does_not_count_duplicate_versions_within_one_entry() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut meta = FileMeta::load(&metacache_entry_single_version(1, now, "duplicate").metadata)
            .expect("duplicate fixture should decode");
        meta.versions.push(meta.versions[0].clone());
        let entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: meta.marshal_msg().expect("duplicate metadata should marshal"),
            cached: Some(meta),
            reusable: false,
        };

        let discovery = MetaCacheEntries(vec![Some(entry)]).discover_heal_candidates("bucket", 16);
        let candidate = discovery
            .candidates
            .iter()
            .find(|candidate| candidate.version_id == Some(Uuid::from_u128(1)))
            .expect("duplicate fixture should be discovered");
        assert_eq!(candidate.replica_count, 1);
    }

    #[test]
    fn discover_heal_candidates_covers_divergent_quorum_boundaries_n2_n4_n6() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");

        for (disk_count, quorum) in [(2usize, 1usize), (4, 2), (6, 3)] {
            let target_id = Uuid::from_u128(0x1000 + disk_count as u128);
            for target_replicas in [quorum.saturating_sub(1), quorum, quorum + 1] {
                let entries = (0..disk_count)
                    .map(|disk| {
                        let version_id = if disk < target_replicas {
                            target_id
                        } else {
                            Uuid::from_u128(0x2000 + disk as u128)
                        };
                        Some(metacache_entry_single_version(version_id.as_u128(), now, "divergent"))
                    })
                    .collect();
                let discovery = MetaCacheEntries(entries).discover_heal_candidates("bucket", 32);
                let target = discovery
                    .candidates
                    .iter()
                    .find(|candidate| candidate.version_id == Some(target_id));
                assert_eq!(target.is_some(), target_replicas > 0, "N={disk_count}, replicas={target_replicas}");
                if let Some(target) = target {
                    assert_eq!(target.replica_count, target_replicas);
                }
            }
        }
    }

    #[test]
    fn discover_heal_candidates_separates_delete_markers_and_preserves_unversioned_objects() {
        let mut marker_meta = FileMeta::new();
        marker_meta
            .add_version(FileInfo {
                volume: "bucket".to_string(),
                name: "object".to_string(),
                version_id: Some(Uuid::from_u128(99)),
                deleted: true,
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
                ..Default::default()
            })
            .expect("delete marker should be added");
        let marker = MetaCacheEntry {
            name: "object".to_string(),
            metadata: marker_meta.marshal_msg().expect("delete marker metadata should marshal"),
            cached: Some(marker_meta),
            reusable: false,
        };

        let unversioned_entry = metacache_entry_with_mod_time(
            OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"),
            "unversioned",
        );
        let discovery = MetaCacheEntries(vec![Some(marker), Some(unversioned_entry)]).discover_heal_candidates("bucket", 16);
        assert!(discovery.candidates.iter().any(|candidate| {
            candidate.kind == MetaCacheHealCandidateKind::DeleteMarker && candidate.version_id == Some(Uuid::from_u128(99))
        }));
        assert!(discovery.candidates.iter().any(|candidate| {
            candidate.kind == MetaCacheHealCandidateKind::UnversionedObject && candidate.version_id.is_none()
        }));
    }

    #[test]
    fn discover_heal_candidates_rejects_delete_markers_without_ids() {
        let mut marker_meta = FileMeta::new();
        marker_meta
            .add_version(FileInfo {
                volume: "bucket".to_string(),
                name: "object".to_string(),
                deleted: true,
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
                ..Default::default()
            })
            .expect("nil delete marker should be added");
        let discovery = MetaCacheEntries(vec![Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: marker_meta.marshal_msg().expect("nil marker metadata should marshal"),
            cached: Some(marker_meta),
            reusable: false,
        })])
        .discover_heal_candidates("bucket", 16);

        assert!(
            !discovery
                .candidates
                .iter()
                .any(|candidate| candidate.kind == MetaCacheHealCandidateKind::DeleteMarker)
        );
        assert!(discovery.unverified_count >= 1);
    }

    #[test]
    fn discover_heal_candidates_skips_free_versions() {
        let object_id = Uuid::from_u128(100);
        let free_id = Uuid::from_u128(101);
        let mut meta = FileMeta::new();
        meta.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(Uuid::from_u128(102)),
            transition_tier: "WARM".to_string(),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        })
        .expect("transitioned object should be added");
        let mut delete = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete.set_tier_free_version_id(&free_id.to_string());
        meta.delete_version(&delete).expect("free version should be persisted");

        let discovery = MetaCacheEntries(vec![Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: meta.marshal_msg().expect("free version metadata should marshal"),
            cached: Some(meta),
            reusable: false,
        })])
        .discover_heal_candidates("bucket", 16);
        assert!(discovery.candidates.is_empty());
    }

    #[test]
    fn discover_heal_candidates_preserves_unversioned_legacy_object() {
        let legacy = MetaObjectV1 {
            version: "1.0.1".to_string(),
            format: "xl".to_string(),
            stat: MetaObjectV1Stat {
                size: 1,
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
                name: "object".to_string(),
                ..Default::default()
            },
            erasure: MetaObjectV1Erasure {
                data_blocks: 4,
                parity_blocks: 2,
                index: 1,
                distribution: vec![1, 2, 3, 4, 5, 6],
                ..Default::default()
            },
            ..Default::default()
        };
        let version = FileMetaVersion {
            version_type: VersionType::Legacy,
            legacy_object: Some(legacy),
            ..Default::default()
        };
        let mut meta = FileMeta::new();
        meta.versions
            .push(FileMetaShallowVersion::try_from(version).expect("legacy metadata should marshal"));
        let discovery = MetaCacheEntries(vec![Some(MetaCacheEntry {
            name: "object".to_string(),
            metadata: meta.marshal_msg().expect("legacy metadata should marshal"),
            cached: Some(meta),
            reusable: false,
        })])
        .discover_heal_candidates("bucket", 16);
        assert!(discovery.candidates.iter().any(|candidate| {
            candidate.kind == MetaCacheHealCandidateKind::UnversionedObject && candidate.version_id.is_none()
        }));
    }

    #[test]
    fn discover_heal_candidates_rejects_nil_and_malformed_metadata_and_is_bounded() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut nil = metacache_entry_single_version(1, now, "nil");
        let mut nil_meta = FileMeta::load(&nil.metadata).expect("nil fixture should decode");
        let mut nil_version = nil_meta.versions[0]
            .parse_version_meta()
            .expect("nil fixture version should decode");
        nil_version.object.as_mut().expect("object fixture").version_id = Some(Uuid::nil());
        nil_meta.versions[0] = FileMetaShallowVersion::try_from(nil_version).expect("nil fixture should marshal");
        nil.metadata = nil_meta.marshal_msg().expect("nil fixture metadata should marshal");

        let mut mismatched = metacache_entry_single_version(2, now, "mismatched");
        let mut mismatched_meta = FileMeta::load(&mismatched.metadata).expect("mismatched fixture should decode");
        mismatched_meta.versions[0].header.version_id = Some(Uuid::from_u128(200));
        mismatched.metadata = mismatched_meta.marshal_msg().expect("mismatched metadata should marshal");

        let mut short_parts = metacache_entry_single_version(3, now, "short-parts");
        let mut short_parts_meta = FileMeta::load(&short_parts.metadata).expect("short-parts fixture should decode");
        let mut short_parts_version = short_parts_meta.versions[0]
            .parse_version_meta()
            .expect("short-parts fixture version should decode");
        let object = short_parts_version.object.as_mut().expect("object fixture");
        object.part_numbers = vec![1];
        object.part_actual_sizes = vec![1];
        object.part_sizes.clear();
        short_parts_meta.versions[0] = FileMetaShallowVersion::try_from(short_parts_version).expect("short-parts should marshal");
        short_parts.metadata = short_parts_meta.marshal_msg().expect("short-parts metadata should marshal");

        let mut short_unversioned = metacache_entry_with_mod_time(now, "short-unversioned");
        let mut short_unversioned_meta =
            FileMeta::load(&short_unversioned.metadata).expect("short-unversioned fixture should decode");
        let mut short_unversioned_version = short_unversioned_meta.versions[0]
            .parse_version_meta()
            .expect("short-unversioned version should decode");
        let unversioned_object = short_unversioned_version.object.as_mut().expect("unversioned object fixture");
        unversioned_object.part_numbers = vec![1];
        unversioned_object.part_actual_sizes = vec![1];
        unversioned_object.part_sizes.clear();
        short_unversioned_meta.versions[0] =
            FileMetaShallowVersion::try_from(short_unversioned_version).expect("short-unversioned should marshal");
        short_unversioned.metadata = short_unversioned_meta
            .marshal_msg()
            .expect("short-unversioned metadata should marshal");

        let mut malformed = nil.clone();
        malformed.name = "malformed".to_string();
        malformed.metadata = vec![1, 2, 3];

        let entries = MetaCacheEntries(
            std::iter::once(Some(nil))
                .chain(std::iter::once(Some(mismatched)))
                .chain(std::iter::once(Some(short_parts)))
                .chain(std::iter::once(Some(short_unversioned)))
                .chain(std::iter::once(Some(malformed)))
                .chain((0..32).map(|id| Some(metacache_entry_single_version(id + 10, now, "bounded"))))
                .collect(),
        );
        let discovery = entries.discover_heal_candidates("bucket", 5);
        assert!(discovery.candidates.len() <= 5);
        assert!(discovery.truncated, "bounded discovery must expose dropped candidates");
        assert!(
            discovery
                .truncated_candidates
                .iter()
                .all(|candidate| candidate.version_id.is_some()),
            "overflow candidates must retain exact version identities"
        );
        assert!(
            discovery.truncated_objects.iter().any(|object| object == "object"),
            "bounded discovery must expose an object-level safe continuation"
        );
        assert!(
            !discovery
                .candidates
                .iter()
                .any(|candidate| candidate.version_id == Some(Uuid::nil()))
        );
        assert!(
            !discovery
                .candidates
                .iter()
                .any(|candidate| candidate.version_id == Some(Uuid::from_u128(2)))
        );
        assert!(
            !discovery
                .candidates
                .iter()
                .any(|candidate| candidate.version_id == Some(Uuid::from_u128(3)))
        );
        assert!(discovery.candidates.iter().any(|candidate| {
            candidate.kind == MetaCacheHealCandidateKind::UnversionedObject && candidate.version_id.is_none()
        }));
        assert!(
            discovery.unverified_count >= 1,
            "malformed and rejected metadata must remain observable during discovery"
        );

        for invalid_name in [
            "../object",
            "./object",
            "object/../other",
            "object//name",
            "object\u{0001}name",
            "object\0name",
        ] {
            let mut entry = metacache_entry_single_version(400, now, invalid_name);
            entry.name = invalid_name.to_string();
            let discovery = MetaCacheEntries(vec![Some(entry)]).discover_heal_candidates("bucket", 5);
            assert!(
                discovery.candidates.is_empty(),
                "invalid key should not become a heal candidate: {invalid_name:?}"
            );
        }

        #[cfg(windows)]
        {
            let mut entry = metacache_entry_single_version(400, now, "object\\name");
            entry.name = "object\\name".to_string();
            assert!(
                MetaCacheEntries(vec![Some(entry)])
                    .discover_heal_candidates("bucket", 5)
                    .candidates
                    .is_empty(),
                "backslash is a path separator on Windows"
            );
        }

        #[cfg(not(windows))]
        {
            let mut entry = metacache_entry_single_version(400, now, "object\\name");
            entry.name = "object\\name".to_string();
            assert_eq!(
                MetaCacheEntries(vec![Some(entry)])
                    .discover_heal_candidates("bucket", 5)
                    .candidates
                    .len(),
                1,
                "backslash is object-key data on Unix"
            );
        }

        for valid_name in ["trailing/", "prefix/object"] {
            let mut entry = metacache_entry_single_version(401, now, valid_name);
            entry.name = valid_name.to_string();
            let discovery = MetaCacheEntries(vec![Some(entry)]).discover_heal_candidates("bucket", 5);
            assert_eq!(discovery.candidates.len(), 1, "raw S3 key should remain opaque: {valid_name:?}");
        }
    }

    #[test]
    fn resolve_rejects_partial_latest_and_returns_committed_previous_metadata() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let old_entry = metacache_entry_with_mod_time(old_mod_time, "old-etag");
        let new_entry = metacache_entry_with_mod_time(new_mod_time, "new-etag");

        let resolved = MetaCacheEntries(vec![
            Some(new_entry.clone()),
            Some(new_entry),
            Some(old_entry.clone()),
            Some(old_entry.clone()),
            Some(old_entry.clone()),
            Some(old_entry.clone()),
            Some(old_entry),
        ])
        .resolve(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        })
        .expect("previous committed metadata should still satisfy write quorum");

        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(old_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("old-etag"));
    }

    #[test]
    fn resolve_rejects_low_parity_partial_latest_below_required_object_quorum() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let old_entry = metacache_entry_with_erasure(old_mod_time, "old-etag", 7, 1);
        let new_entry = metacache_entry_with_erasure(new_mod_time, "new-etag", 7, 1);

        let resolved = MetaCacheEntries(vec![
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry),
            Some(old_entry.clone()),
            Some(old_entry),
        ])
        .resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        });

        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_rejects_zero_parity_partial_latest_below_required_object_quorum() {
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let new_entry = metacache_entry_with_erasure(new_mod_time, "new-etag", 7, 0);

        let resolved = MetaCacheEntries(vec![
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry),
        ])
        .resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        });

        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_accepts_low_parity_latest_at_required_object_quorum() {
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let new_entry = metacache_entry_with_erasure(new_mod_time, "new-etag", 7, 1);

        let resolved = MetaCacheEntries(vec![
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry.clone()),
            Some(new_entry),
        ])
        .resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        })
        .expect("latest metadata should resolve after satisfying its own write quorum");

        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(new_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("new-etag"));
    }

    #[test]
    fn resolve_skips_low_parity_partial_latest_and_returns_committed_previous_version() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let old_entry = metacache_entry_with_erasure(old_mod_time, "old-etag", 4, 4);
        let new_and_old_entry =
            metacache_entry_with_erasure_versions(&[(old_mod_time, "old-etag", 4, 4), (new_mod_time, "new-etag", 7, 1)]);

        let resolved = MetaCacheEntries(vec![
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry),
            Some(old_entry.clone()),
            Some(old_entry),
        ])
        .resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        })
        .expect("previous committed metadata should resolve after rejecting partial latest");

        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(old_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("old-etag"));
    }

    #[test]
    fn resolve_skips_legacy_header_low_parity_partial_latest_using_payload_quorum() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let old_entry = metacache_entry_with_erasure(old_mod_time, "old-etag", 4, 4);
        let new_and_old_entry = metacache_entry_without_header_ec(metacache_entry_with_erasure_versions(&[
            (old_mod_time, "old-etag", 4, 4),
            (new_mod_time, "new-etag", 7, 1),
        ]));

        let resolved = MetaCacheEntries(vec![
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry.clone()),
            Some(new_and_old_entry),
            Some(old_entry.clone()),
            Some(old_entry),
        ])
        .resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 5,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        })
        .expect("previous committed metadata should resolve after rejecting legacy-header partial latest");

        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(old_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("old-etag"));
    }

    #[test]
    fn borrowed_resolution_counts_reader_slots_and_preserves_owned_semantics() {
        let time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("fixture time");
        for cached in [false, true] {
            for legacy in [false, true] {
                let mut entry = metacache_entry_with_erasure(time, "etag", 8, 8);
                if legacy {
                    entry = metacache_entry_without_header_ec(entry);
                }
                if !cached {
                    entry.cached = None;
                }
                for replicas in [0, 1, 7, 8, 9] {
                    let mut slots = vec![Some(&entry); replicas];
                    slots.extend([None, None]);
                    let params = MetadataResolutionParams {
                        dir_quorum: 8,
                        obj_quorum: 8,
                        requested_versions: 1,
                        write_quorum_slack: 8,
                        bucket: "bucket".to_owned(),
                        ..Default::default()
                    };
                    let borrowed = MetaCacheEntries::resolve_borrowed_with_write_quorum(&slots, params.clone());
                    let owned = MetaCacheEntries(slots.iter().map(|slot| slot.map(Clone::clone)).collect())
                        .resolve_with_write_quorum(params);
                    assert_eq!(borrowed.is_some(), replicas >= 8, "replicas={replicas}, cached={cached}, legacy={legacy}");
                    assert_eq!(
                        borrowed.map(|entry| (entry.name, entry.metadata)),
                        owned.map(|entry| (entry.name, entry.metadata))
                    );
                }
            }
        }
    }

    #[test]
    fn borrowed_header_agreement_matches_existing_strict_and_relaxed_contracts() {
        let time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("fixture time");
        let current = metacache_entry_with_erasure(time, "current", 8, 8);
        let older = metacache_entry_with_erasure(time - time::Duration::seconds(1), "older", 8, 8);
        let legacy = metacache_entry_without_header_ec(current.clone());
        let mut changed_signature = current.clone();
        changed_signature.cached.as_mut().expect("cached fixture").versions[0]
            .header
            .signature[0] ^= 1;
        changed_signature.metadata = changed_signature
            .cached
            .as_ref()
            .expect("cached fixture")
            .marshal_msg()
            .expect("changed header");
        let corrupt = MetaCacheEntry {
            name: "object".to_owned(),
            metadata: vec![0xff],
            ..Default::default()
        };
        let directory = metacache_dir_entry("object");
        let cases = [current, older, legacy, changed_signature, corrupt, directory];
        for strict in [false, true] {
            for cached in [false, true] {
                for left in &cases {
                    for right in &cases {
                        let mut left = left.clone();
                        let mut right = right.clone();
                        if !cached {
                            left.cached = None;
                            right.cached = None;
                        }
                        assert_eq!(
                            left.headers_agree([&right], strict),
                            left.matches(Some(&right), strict).1,
                            "strict={strict}, cached={cached}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn header_groups_keep_the_original_geometry_baseline_across_legacy_wildcards() {
        let time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("fixture time");
        let original = metacache_entry_with_erasure(time, "etag", 8, 8);
        let legacy = metacache_entry_without_header_ec(original.clone());
        let mut conflict = original.clone();
        let metadata = conflict.cached.as_mut().expect("fixture cache");
        metadata.versions[0].header.ec_m = 12;
        metadata.versions[0].header.ec_n = 4;
        conflict.metadata = metadata.marshal_msg().expect("conflicting geometry header");
        for strict in [false, true] {
            for cache_mask in 0..8 {
                let mut replicas = [original.clone(), legacy.clone(), conflict.clone()];
                for (index, entry) in replicas.iter_mut().enumerate() {
                    if cache_mask & (1 << index) == 0 {
                        entry.cached = None;
                    }
                }
                for base in &replicas {
                    for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
                        let group = order.map(|index| &replicas[index]);
                        let expected = group.iter().all(|candidate| base.matches(Some(candidate), strict).1);
                        assert_eq!(
                            base.headers_agree(group, strict),
                            expected,
                            "strict={strict}, mask={cache_mask}, order={order:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn agreed_resolution_preserves_buffers_without_materializing_unchanged_metadata() {
        let time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("fixture time");
        for cached in [false, true] {
            let mut entry = metacache_entry_with_erasure(time, "etag", 8, 8);
            if !cached {
                entry.cached = None;
            }
            let metadata = entry.metadata.as_ptr();
            let cached_version = entry.cached.as_ref().map(|meta| meta.versions[0].meta.as_ptr());
            let MetaCacheEntryResolution::Resolved(entry) = resolve_agreed_entry(
                entry,
                8,
                &MetadataResolutionParams {
                    dir_quorum: 8,
                    obj_quorum: 8,
                    requested_versions: 1,
                    write_quorum_slack: 8,
                    ..Default::default()
                },
                true,
            ) else {
                panic!("eight independent EC8 readers must resolve");
            };
            assert_eq!(entry.metadata.as_ptr(), metadata, "unchanged serialized metadata must move");
            assert_eq!(
                entry.cached.as_ref().map(|meta| meta.versions[0].meta.as_ptr()),
                cached_version,
                "cold metadata stays borrowed and existing decoded buffers move"
            );
        }
    }

    #[test]
    fn listing_quorum_never_relaxes_a_versions_read_quorum() {
        let time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("fixture time");
        for (data, parity, copies, expected) in [(8, 8, 8, true), (12, 4, 8, false), (12, 4, 11, false), (12, 4, 12, true)] {
            let entry = metacache_entry_with_erasure(time, "etag", data, parity);
            let entries = MetaCacheEntries(vec![Some(entry); copies]);
            let resolved = entries.resolve_with_write_quorum(MetadataResolutionParams {
                obj_quorum: 8,
                dir_quorum: 8,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 16 - copies,
                ..Default::default()
            });
            assert_eq!(resolved.is_some(), expected, "layout {data}+{parity}, {copies} copies");
        }
    }

    #[test]
    fn resolve_with_write_quorum_relaxes_requirement_by_unreachable_drives() {
        let mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        // EC 2+2 version, write quorum 3 of 4: committed with one holder drive
        // now offline, so only 2 of the 3 reachable drives carry the metadata.
        let entry = metacache_entry_with_erasure(mod_time, "etag", 2, 2);
        let entries = || MetaCacheEntries(vec![Some(entry.clone()), Some(entry.clone()), None]);

        let resolved = entries()
            .resolve_with_write_quorum(MetadataResolutionParams {
                obj_quorum: 2,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 1,
                ..Default::default()
            })
            .expect("committed version should resolve when the missing copy sits on an unreachable drive");
        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(mod_time));

        // Without the slack the same sample is rejected outright.
        let rejected = entries().resolve_with_write_quorum(MetadataResolutionParams {
            obj_quorum: 3,
            requested_versions: 1,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        });
        assert!(rejected.is_none());
    }

    #[test]
    fn resolve_with_write_quorum_slack_accepts_committed_latest_during_merge() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        // The newer EC 2+2 version committed at write quorum 3 of 4; one holder
        // drive is offline, and a third reachable drive still carries only the
        // previous version. The candidates disagree, so the merge path (not the
        // all-agree branch) must honor the slack.
        let old_entry = metacache_entry_with_erasure(old_mod_time, "old-etag", 2, 2);
        let new_and_old_entry =
            metacache_entry_with_erasure_versions(&[(old_mod_time, "old-etag", 2, 2), (new_mod_time, "new-etag", 2, 2)]);

        let resolved = MetaCacheEntries(vec![Some(new_and_old_entry.clone()), Some(new_and_old_entry), Some(old_entry)])
            .resolve_with_write_quorum(MetadataResolutionParams {
                obj_quorum: 2,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 1,
                ..Default::default()
            })
            .expect("committed latest should survive the merge when its missing copy is on an unreachable drive");
        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(new_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("new-etag"));
    }

    #[test]
    fn resolve_with_write_quorum_slack_keeps_partial_latest_hidden_during_merge() {
        let old_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        // The newer EC 2+2 version (write quorum 3) reached only ONE drive: even
        // with one drive unreachable (slack 1) it cannot prove the relaxed
        // quorum of 2, so the committed previous version must win the merge.
        let old_entry = metacache_entry_with_erasure(old_mod_time, "old-etag", 2, 2);
        let new_and_old_entry =
            metacache_entry_with_erasure_versions(&[(old_mod_time, "old-etag", 2, 2), (new_mod_time, "new-etag", 2, 2)]);

        let resolved = MetaCacheEntries(vec![Some(new_and_old_entry), Some(old_entry.clone()), Some(old_entry)])
            .resolve_with_write_quorum(MetadataResolutionParams {
                obj_quorum: 2,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 1,
                ..Default::default()
            })
            .expect("committed previous version should resolve after rejecting the partial latest");
        let info = resolved
            .to_fileinfo("bucket")
            .expect("resolved committed metadata should decode as file info");
        assert_eq!(info.mod_time, Some(old_mod_time));
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("old-etag"));
    }

    #[test]
    fn resolve_rejects_partial_directory_below_dir_quorum() {
        let partial_dir = metacache_dir_entry("prefix/");

        let resolved = MetaCacheEntries(vec![Some(partial_dir.clone()), Some(partial_dir)]).resolve(MetadataResolutionParams {
            dir_quorum: 5,
            obj_quorum: 5,
            bucket: "bucket".to_string(),
            strict: true,
            ..Default::default()
        });

        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_directory_marker_and_prefix_preserve_quorum_in_both_orders() {
        let modified = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut object = metacache_entry_with_erasure(modified, "marker-etag", 2, 2);
        object.name = "folder/".to_string();
        let directory = metacache_dir_entry("folder/");
        for enforce_write_quorum in [false, true] {
            for (object_copies, directory_copies) in [(3, 1), (3, 3), (2, 3), (0, 3), (2, 2)] {
                for reverse in [false, true] {
                    let mut entries = vec![Some(object.clone()); object_copies];
                    entries.extend(vec![Some(directory.clone()); directory_copies]);
                    if reverse {
                        entries.reverse();
                    }
                    let entries = MetaCacheEntries(entries);
                    let params = MetadataResolutionParams {
                        dir_quorum: 3,
                        obj_quorum: 3,
                        requested_versions: 1,
                        bucket: "bucket".to_string(),
                        strict: true,
                        ..Default::default()
                    };
                    let resolved = if enforce_write_quorum {
                        entries.resolve_with_write_quorum(params)
                    } else {
                        entries.resolve(params)
                    };
                    let context = format!(
                        "objects={object_copies}, directories={directory_copies}, reverse={reverse}, write={enforce_write_quorum}"
                    );
                    if object_copies >= 3 {
                        let resolved = resolved.expect("metadata at quorum must shadow a same-named prefix");
                        assert!(resolved.is_object(), "{context}");
                        assert_eq!(
                            resolved
                                .to_fileinfo("bucket")
                                .expect("marker metadata should decode")
                                .mod_time,
                            Some(modified)
                        );
                    } else if directory_copies >= 3 {
                        assert!(resolved.expect("prefix at quorum must remain a fallback").is_dir(), "{context}");
                    } else {
                        assert!(resolved.is_none(), "{context}");
                    }
                }
            }
        }
    }

    fn build_hashmap_cache(update_size: usize) -> Arc<Cache<HashMap<usize, usize>>> {
        let generation = Arc::new(AtomicUsize::new(0));
        Arc::new(Cache::new(
            Box::new(move || {
                let generation = Arc::clone(&generation);
                Box::pin(async move {
                    let v = generation.fetch_add(1, Ordering::SeqCst);
                    let mut m = HashMap::with_capacity(update_size);
                    for i in 0..update_size {
                        m.insert(i, i ^ v);
                    }
                    Ok(m)
                })
            }),
            Duration::ZERO,
            Opts::default(),
        ))
    }

    async fn run_cache_workload(cache: Arc<Cache<HashMap<usize, usize>>>, workers: usize, rounds: usize, probe_mod: usize) {
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let cache = Arc::clone(&cache);
            tasks.push(tokio::spawn(async move {
                for round in 0..rounds {
                    let m = Arc::clone(&cache).get().await.expect("cache get should succeed");
                    let key = (worker.wrapping_mul(17).wrapping_add(round)) % probe_mod;
                    assert!(m.contains_key(&key), "expected key {key} to exist");
                }
            }));
        }

        for task in tasks {
            task.await.expect("worker task should not panic");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_cache_concurrency_smoke() {
        let cache = build_hashmap_cache(2048);
        run_cache_workload(cache, 32, 120, 2048).await;
    }

    #[tokio::test]
    async fn test_cache_get_shared_reuses_fresh_value() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                move || {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move { Ok(calls.fetch_add(1, Ordering::SeqCst)) })
                }
            }),
            Duration::from_secs(60),
            Opts::default(),
        ));

        let first = Arc::clone(&cache).get_shared().await.expect("prime cache should succeed");
        let second = Arc::clone(&cache).get_shared().await.expect("fresh cache hit should succeed");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(*first, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cache_future_last_update_refreshes_instead_of_underflowing() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                move || {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move { Ok(calls.fetch_add(1, Ordering::SeqCst)) })
                }
            }),
            Duration::from_secs(60),
            Opts::default(),
        ));

        let prime = Arc::clone(&cache).get().await.expect("prime cache should succeed");
        assert_eq!(prime, 0);

        let now = Cache::<usize>::current_unix_secs();
        cache.last_update_secs.store(now.saturating_add(60), AtomicOrdering::SeqCst);

        let refreshed = Arc::clone(&cache)
            .get()
            .await
            .expect("future timestamp should force refresh instead of underflowing");
        assert_eq!(refreshed, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cache_no_wait_returns_stale_and_refreshes_in_background() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (bg_started_tx, bg_started_rx) = oneshot::channel::<()>();
        let (release_bg_tx, release_bg_rx) = oneshot::channel::<()>();
        let bg_started_tx = Arc::new(StdMutex::new(Some(bg_started_tx)));
        let release_bg_rx = Arc::new(StdMutex::new(Some(release_bg_rx)));

        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                let bg_started_tx = Arc::clone(&bg_started_tx);
                let release_bg_rx = Arc::clone(&release_bg_rx);
                move || {
                    let calls = Arc::clone(&calls);
                    let bg_started_tx = Arc::clone(&bg_started_tx);
                    let release_bg_rx = Arc::clone(&release_bg_rx);
                    Box::pin(async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        if call == 1 {
                            let tx = { bg_started_tx.lock().expect("start sender lock should not poison").take() };
                            if let Some(tx) = tx {
                                let _ = tx.send(());
                            }
                            let rx = { release_bg_rx.lock().expect("release receiver lock should not poison").take() };
                            if let Some(rx) = rx {
                                let _ = rx.await;
                            }
                        }
                        Ok(call)
                    })
                }
            }),
            Duration::from_secs(1),
            Opts {
                return_last_good: true,
                no_wait: true,
            },
        ));

        let prime = Arc::clone(&cache).get().await.expect("prime cache should succeed");
        assert_eq!(prime, 0);

        let now = Cache::<usize>::current_unix_secs();
        cache.last_update_secs.store(now.saturating_sub(1), AtomicOrdering::SeqCst);

        let stale = tokio::time::timeout(Duration::from_millis(200), Arc::clone(&cache).get())
            .await
            .expect("no_wait path should return without waiting for refresh")
            .expect("stale get should succeed");
        assert_eq!(stale, 0);

        tokio::time::timeout(Duration::from_millis(200), bg_started_rx)
            .await
            .expect("background refresh should start")
            .expect("background start signal should be delivered");

        release_bg_tx.send(()).expect("release signal should be delivered");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.cached_value().as_deref() == Some(&1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background refresh should complete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cache_no_wait_coalesces_background_refreshes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let release_refresh = Arc::new(Notify::new());
        let ttl = Duration::from_secs(60);
        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                let release_refresh = Arc::clone(&release_refresh);
                move || {
                    let calls = Arc::clone(&calls);
                    let release_refresh = Arc::clone(&release_refresh);
                    Box::pin(async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        if call > 0 {
                            release_refresh.notified().await;
                        }
                        Ok(call)
                    })
                }
            }),
            ttl,
            Opts {
                return_last_good: true,
                no_wait: true,
            },
        ));

        let prime = Arc::clone(&cache).get().await.expect("prime cache should succeed");
        assert_eq!(prime, 0);

        let now = Cache::<usize>::current_unix_secs();
        cache
            .last_update_secs
            .store(now.saturating_sub(ttl.as_secs()), AtomicOrdering::SeqCst);

        let stale = Arc::clone(&cache).get().await.expect("stale get should succeed");
        assert_eq!(stale, 0);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if calls.load(Ordering::SeqCst) == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background refresh should start");

        let mut readers = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            readers.push(tokio::spawn(
                async move { Arc::clone(&cache).get().await.expect("stale get should succeed") },
            ));
        }

        for reader in readers {
            assert_eq!(reader.await.expect("reader task should not panic"), 0);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        release_refresh.notify_waiters();
    }

    #[tokio::test]
    async fn test_cache_return_last_good_on_refresh_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                move || {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        if call == 0 {
                            Ok(42usize)
                        } else {
                            Err(std::io::Error::other("refresh failed"))
                        }
                    })
                }
            }),
            Duration::from_secs(1),
            Opts {
                return_last_good: true,
                no_wait: false,
            },
        ));

        let prime = Arc::clone(&cache).get().await.expect("prime cache should succeed");
        assert_eq!(prime, 42);

        let now = Cache::<usize>::current_unix_secs();
        cache.last_update_secs.store(now.saturating_sub(2), AtomicOrdering::SeqCst);

        let stale = Arc::clone(&cache)
            .get()
            .await
            .expect("return_last_good should keep stale value");
        assert_eq!(stale, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cache_refresh_error_without_return_last_good() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Arc::new(Cache::new(
            Box::new({
                let calls = Arc::clone(&calls);
                move || {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        if call == 0 {
                            Ok(7usize)
                        } else {
                            Err(std::io::Error::other("refresh failed"))
                        }
                    })
                }
            }),
            Duration::from_secs(1),
            Opts {
                return_last_good: false,
                no_wait: false,
            },
        ));

        let prime = Arc::clone(&cache).get().await.expect("prime cache should succeed");
        assert_eq!(prime, 7);

        let now = Cache::<usize>::current_unix_secs();
        cache.last_update_secs.store(now.saturating_sub(2), AtomicOrdering::SeqCst);

        let err = Arc::clone(&cache)
            .get()
            .await
            .expect_err("refresh error should be propagated when return_last_good is false");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    // ------------------------------------------------------------------
    // backlog#900: metacache boundaries live outside the HTTP CatchPanicLayer
    // (background / listing chains). A corrupt-part xl.meta must yield Err,
    // not panic, so it cannot poison a worker.
    // ------------------------------------------------------------------

    fn corrupt_parts_filemeta() -> FileMeta {
        use crate::{ChecksumAlgo, ErasureAlgo, MetaObject, VersionType};
        let mut fm = FileMeta::new();
        fm.add_version_filemata(FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(MetaObject {
                version_id: Some(Uuid::new_v4()),
                erasure_algorithm: ErasureAlgo::ReedSolomon,
                erasure_m: 2,
                erasure_n: 2,
                erasure_block_size: 1 << 20,
                bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
                part_numbers: vec![1, 2],
                part_sizes: vec![10], // corrupt: shorter than part_numbers
                part_actual_sizes: vec![10, 20],
                mod_time: Some(time::OffsetDateTime::now_utc()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("add version");
        // Round-trip once so the corrupt array survives exactly like on disk.
        FileMeta::load(&fm.marshal_msg().expect("marshal")).expect("load")
    }

    #[test]
    fn metacache_to_fileinfo_returns_err_not_panic_on_corrupt_parts() {
        let entry = MetaCacheEntry {
            name: "obj".to_string(),
            metadata: Vec::new(),
            cached: Some(corrupt_parts_filemeta()),
            reusable: false,
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| entry.to_fileinfo("bucket")));
        let inner = caught.expect("to_fileinfo must not panic");
        assert!(matches!(inner, Err(Error::FileCorrupt)), "expected FileCorrupt from metacache boundary");
    }

    #[test]
    fn metacache_file_info_versions_returns_err_not_panic_on_corrupt_parts() {
        // file_info_versions reads self.metadata (not cached), so fill marshaled bytes.
        let bytes = corrupt_parts_filemeta().marshal_msg().expect("marshal");
        let entry = MetaCacheEntry {
            name: "obj".to_string(),
            metadata: bytes,
            cached: None,
            reusable: false,
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| entry.file_info_versions("bucket")));
        let inner = caught.expect("file_info_versions must not panic");
        assert!(matches!(inner, Err(Error::FileCorrupt)), "expected FileCorrupt");
    }

    #[test]
    fn metacache_to_fileinfo_ignores_part_checksum_sidecar_regardless_of_cache_state() {
        let mut meta = FileMeta::load(&create_real_xlmeta().expect("create real xl.meta")).expect("load real xl.meta");
        let version_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").expect("valid fixture version id");
        let (index, mut version) = meta.find_version(Some(version_id)).expect("find fixture object version");
        rustfs_utils::http::insert_bytes(
            &mut version.object.as_mut().expect("fixture object").meta_sys,
            rustfs_utils::http::SUFFIX_PART_CHECKSUMS,
            b"not-json".to_vec(),
        );
        meta.versions[index] = FileMetaShallowVersion::try_from(version).expect("replace fixture object version");
        let encoded = meta.marshal_msg().expect("marshal object metadata");

        let uncached = MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded.clone(),
            cached: None,
            reusable: false,
        }
        .to_fileinfo("bucket")
        .expect("uncached metacache conversion must stay lazy");
        let cached = MetaCacheEntry {
            name: "object".to_string(),
            metadata: encoded,
            cached: Some(meta),
            reusable: false,
        }
        .to_fileinfo("bucket")
        .expect("cached metacache conversion must stay lazy");

        assert_eq!(cached, uncached);
        assert!(cached.parts.iter().all(|part| part.checksums.is_none()));
    }

    #[test]
    fn matches_strict_caches_self_and_matches_strict_tuple_contract() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut base = metacache_entry_single_version(1, now, "etag-1");
        base.cached = None;
        let mut same = base.clone();
        let mut different_etag = metacache_entry_single_version(1, now, "etag-2");
        different_etag.cached = None;

        let mut ec_variant_meta = FileMeta::load(&base.metadata).expect("decode base");
        ec_variant_meta.versions[0].header.ec_n = 0;
        ec_variant_meta.versions[0].header.ec_m = 0;
        let ec_variant = MetaCacheEntry {
            name: base.name.clone(),
            metadata: ec_variant_meta.marshal_msg().expect("marshal ec variant"),
            cached: None,
            reusable: false,
        };

        let corrupt = MetaCacheEntry {
            name: base.name.clone(),
            metadata: b"not-xl-meta".to_vec(),
            cached: None,
            reusable: false,
        };

        assert!(base.matches_strict(&same));
        assert!(base.cached.is_some(), "matches_strict must populate self.cached once");
        same.cached = base.cached.clone();
        assert!(base.matches_strict(&same));
        assert!(base.matches_strict(&ec_variant));
        assert!(!base.matches_strict(&different_etag));
        assert!(!base.matches_strict(&corrupt));

        for candidate in [&same, &ec_variant, &different_etag, &corrupt] {
            assert_eq!(base.matches_strict(candidate), base.matches(Some(candidate), true).1);
        }
    }

    #[test]
    fn matches_strict_and_matches_buf_strict_cover_dir_empty_and_count_boundaries() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let mut obj_a = metacache_entry_single_version(1, now, "etag-1");
        obj_a.cached = None;

        // Name mismatch short-circuits before decoding.
        let mut different_name = obj_a.clone();
        different_name.name = "other-object".to_string();
        assert!(!obj_a.matches_strict(&different_name));
        assert!(obj_a.cached.is_none(), "name mismatch must not decode self.metadata");

        // Directory vs directory and directory vs object.
        let mut dir_a = MetaCacheEntry {
            name: "prefix/".to_string(),
            metadata: Vec::new(),
            cached: None,
            reusable: false,
        };
        let dir_b = dir_a.clone();
        let mut dir_obj = obj_a.clone();
        dir_obj.name = "prefix/".to_string();
        assert!(dir_a.matches_strict(&dir_b));
        assert!(!dir_a.matches_strict(&dir_obj));
        assert!(!dir_obj.matches_strict(&dir_a));

        // Corrupt self.metadata returns false.
        let mut corrupt_self = MetaCacheEntry {
            name: obj_a.name.clone(),
            metadata: b"corrupt-self".to_vec(),
            cached: None,
            reusable: false,
        };
        assert!(!corrupt_self.matches_strict(&obj_a));

        // Version count mismatch in both uncached (matches_buf_strict) and cached paths.
        let mut two_versions_meta = FileMeta::load(&obj_a.metadata).expect("decode single version");
        let second = metacache_entry_single_version(2, now, "etag-2");
        let second_meta = FileMeta::load(&second.metadata).expect("decode second version");
        two_versions_meta.versions.extend(second_meta.versions);
        let mut two_versions_entry = MetaCacheEntry {
            name: obj_a.name.clone(),
            metadata: two_versions_meta.marshal_msg().expect("marshal two versions"),
            cached: None,
            reusable: false,
        };
        assert!(!obj_a.matches_strict(&two_versions_entry));
        two_versions_entry.cached = Some(two_versions_meta);
        assert!(!obj_a.matches_strict(&two_versions_entry));

        // Zero-length meta payload inside a valid XL2 envelope exercises the `meta.is_empty()`
        // fast-path in `FileMeta::matches_buf_strict`.
        let mut zero_meta_envelope = Vec::from(&b"XL2 "[..]);
        zero_meta_envelope.extend_from_slice(&1u16.to_le_bytes());
        zero_meta_envelope.extend_from_slice(&3u16.to_le_bytes());
        zero_meta_envelope.extend_from_slice(&[0xc6, 0, 0, 0, 0]);
        let empty_crc = u32::try_from(xxhash_rust::xxh64::xxh64(b"", 0) & 0xffff_ffff).expect("u32 crc");
        zero_meta_envelope.push(0xce);
        zero_meta_envelope.extend_from_slice(&empty_crc.to_be_bytes());
        let empty_fm = FileMeta::load(&zero_meta_envelope).expect("load zero-meta xl.meta");
        assert!(empty_fm.versions.is_empty());
        assert!(empty_fm.matches_buf_strict(&zero_meta_envelope));
        assert!(!empty_fm.matches_buf_strict(&obj_a.metadata));
        assert!(
            !obj_a
                .cached
                .as_ref()
                .expect("obj_a cached")
                .matches_buf_strict(&zero_meta_envelope)
        );
    }

    #[test]
    fn is_latest_delete_marker_covers_cached_indexed_and_corrupt_envelopes() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let obj_entry = metacache_entry_single_version(1, now, "etag-1");

        // Cached object -> false; uncached object -> false.
        let mut cached_obj = obj_entry.clone();
        assert!(cached_obj.cached.is_some());
        assert!(!cached_obj.is_latest_delete_marker());

        let mut uncached_obj = obj_entry.clone();
        uncached_obj.cached = None;
        assert!(!uncached_obj.is_latest_delete_marker());

        let mut dm_fm = FileMeta::new();
        dm_fm
            .add_version(FileInfo {
                version_id: Some(Uuid::from_u128(9)),
                deleted: true,
                mod_time: Some(now),
                ..Default::default()
            })
            .expect("add delete marker");
        let mut uncached_dm = MetaCacheEntry {
            name: "object".to_string(),
            metadata: dm_fm.marshal_msg().expect("marshal delete marker"),
            cached: None,
            reusable: false,
        };
        assert!(uncached_dm.is_latest_delete_marker());

        // Cached empty versions -> true; cached delete marker -> true.
        let mut cached_empty = MetaCacheEntry {
            name: "object".to_string(),
            metadata: Vec::new(),
            cached: Some(FileMeta::new()),
            reusable: false,
        };
        assert!(cached_empty.is_latest_delete_marker());
        let mut cached_dm = uncached_dm.clone();
        cached_dm.cached = Some(dm_fm);
        assert!(cached_dm.is_latest_delete_marker());

        // Minor version < 3 (e.g. v1.2) makes `is_indexed_meta` return `Ok((&[], &[]))` and
        // exercises the `self.xl_meta()` fallback branch for both Object and Delete versions.
        let mut v12_obj_bytes = obj_entry.metadata.clone();
        v12_obj_bytes[6..8].copy_from_slice(&2u16.to_le_bytes());
        let mut v12_obj = MetaCacheEntry {
            name: "object".to_string(),
            metadata: v12_obj_bytes,
            cached: None,
            reusable: false,
        };
        assert!(!v12_obj.is_latest_delete_marker());
        assert!(v12_obj.cached.is_some(), "v1.2 fallback must populate self.cached");

        let mut v12_dm_bytes = uncached_dm.metadata;
        v12_dm_bytes[6..8].copy_from_slice(&2u16.to_le_bytes());
        let mut v12_dm = MetaCacheEntry {
            name: "object".to_string(),
            metadata: v12_dm_bytes,
            cached: None,
            reusable: false,
        };
        assert!(v12_dm.is_latest_delete_marker());

        // Non-XL2 magic -> false.
        let mut non_xl2 = MetaCacheEntry {
            name: "object".to_string(),
            metadata: b"NOT_XL2_HEADER".to_vec(),
            cached: None,
            reusable: false,
        };
        assert!(!non_xl2.is_latest_delete_marker());

        // Valid XL2 header with corrupt CRC in indexed meta -> true.
        let mut corrupt_crc = obj_entry.metadata;
        let last = corrupt_crc.len() - 1;
        corrupt_crc[last] ^= 0xff;
        let mut corrupt_crc_entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: corrupt_crc,
            cached: None,
            reusable: false,
        };
        assert!(corrupt_crc_entry.is_latest_delete_marker());
    }

    #[test]
    fn file_info_versions_and_borrowed_slice_decode_match_cached_and_full_decoders() {
        let mut fm = FileMeta::load(&crate::test_data::create_realistic_object_xlmeta().expect("realistic xl.meta"))
            .expect("load realistic xl.meta");
        let mut obj = FileMetaVersion::try_from(fm.versions[0].meta.as_slice())
            .expect("decode version")
            .object
            .expect("v2 object");
        obj.meta_user.insert(
            rustfs_utils::http::headers::AMZ_META_UNENCRYPTED_CONTENT_LENGTH.to_string(),
            "128".to_string(),
        );
        obj.meta_user.insert(
            rustfs_utils::http::headers::AMZ_META_UNENCRYPTED_CONTENT_MD5.to_string(),
            "deadbeef".to_string(),
        );
        obj.meta_user
            .insert(crate::metadata_keys::STORAGE_CLASS.to_string(), "STANDARD".to_string());
        rustfs_utils::http::insert_bytes(&mut obj.meta_sys, rustfs_utils::http::SUFFIX_TIER_FV_MARKER, b"true".to_vec());
        fm.versions[0] = FileMetaShallowVersion::try_from(FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(obj),
            write_version: 1,
            ..Default::default()
        })
        .expect("re-encode object version");

        let t1 = OffsetDateTime::from_unix_timestamp(1_705_312_100).expect("t1");
        let mut free_meta_sys = HashMap::new();
        rustfs_utils::http::insert_bytes(&mut free_meta_sys, rustfs_utils::http::SUFFIX_FREE_VERSION, Vec::new());
        rustfs_utils::http::insert_bytes(&mut free_meta_sys, rustfs_utils::http::SUFFIX_TRANSITION_TIER, b"WARM".to_vec());
        fm.versions.push(
            FileMetaShallowVersion::try_from(FileMetaVersion {
                version_type: VersionType::Delete,
                delete_marker: Some(MetaDeleteMarker {
                    version_id: Some(Uuid::from_u128(1)),
                    mod_time: Some(t1),
                    meta_sys: free_meta_sys,
                }),
                write_version: 1,
                ..Default::default()
            })
            .expect("free version shallow"),
        );

        for sv in &fm.versions {
            for all_parts in [true, false] {
                let fast = sv.into_fileinfo("bucket", "object", all_parts).expect("borrowed decode");
                let full = FileMetaVersion::try_from(sv.meta.as_slice())
                    .expect("full try_from")
                    .into_fileinfo("bucket", "object", all_parts)
                    .expect("full into_fileinfo");
                assert_eq!(fast, full);
            }
        }

        let raw = fm.marshal_msg().expect("marshal multi-version xl.meta");
        let cached_entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: Vec::new(),
            cached: Some(fm),
            reusable: false,
        };
        let uncached_entry = MetaCacheEntry {
            name: "object".to_string(),
            metadata: raw,
            cached: None,
            reusable: false,
        };

        let cached_fi = cached_entry.to_fileinfo("bucket").expect("cached to_fileinfo");
        let uncached_fi = uncached_entry.to_fileinfo("bucket").expect("uncached to_fileinfo");
        assert_eq!(cached_fi, uncached_fi);
        assert_eq!(cached_fi.num_versions, 1);
        assert!(!cached_fi.metadata.contains_key(crate::metadata_keys::STORAGE_CLASS));
        assert!(!rustfs_utils::http::contains_key_str(
            &cached_fi.metadata,
            rustfs_utils::http::SUFFIX_TIER_FV_MARKER
        ));

        let cached_versions = cached_entry.file_info_versions("bucket").expect("cached versions");
        let uncached_versions = uncached_entry.file_info_versions("bucket").expect("uncached versions");
        assert_eq!(cached_versions.versions, uncached_versions.versions);
        assert!(cached_versions.free_versions.is_empty());
        assert_eq!(cached_versions.versions[0].num_versions, 1);

        let cached_with_free = cached_entry
            .file_info_versions_with_free_versions("bucket")
            .expect("cached versions with free");
        let uncached_with_free = uncached_entry
            .file_info_versions_with_free_versions("bucket")
            .expect("uncached versions with free");
        assert_eq!(cached_with_free.versions, uncached_with_free.versions);
        assert_eq!(cached_with_free.free_versions, uncached_with_free.free_versions);
        assert_eq!(cached_with_free.free_versions.len(), 1);
        assert_eq!(cached_with_free.free_versions[0].num_versions, 1);

        let empty_fm = FileMeta::new();
        let empty_raw = empty_fm.marshal_msg().expect("marshal empty xl.meta");
        let empty_cached = MetaCacheEntry {
            name: "empty".to_string(),
            metadata: Vec::new(),
            cached: Some(empty_fm),
            reusable: false,
        }
        .file_info_versions("bucket")
        .expect("empty cached versions");
        let empty_uncached = MetaCacheEntry {
            name: "empty".to_string(),
            metadata: empty_raw,
            cached: None,
            reusable: false,
        }
        .file_info_versions("bucket")
        .expect("empty uncached versions");
        assert_eq!(empty_cached.versions, empty_uncached.versions);
        assert_eq!(empty_cached.versions[0].num_versions, 1);

        // Verify fallback cases (legacy V1Obj, >64 MetaSys keys, rmp_serde legacy format)
        // and the 't'-prefixed internal key branches (`tier-free-marker` dropped,
        // `tier-free-versionID` preserved, `transitioned-versionID` normalized).
        let v1_fm = FileMeta::load(&crate::test_data::create_legacy_v1_object_xlmeta().expect("v1 xl.meta")).expect("load v1");
        assert_eq!(
            v1_fm.versions[0].into_fileinfo("bucket", "object", true).expect("v1 fast"),
            FileMetaVersion::try_from(v1_fm.versions[0].meta.as_slice())
                .expect("v1 try_from")
                .into_fileinfo("bucket", "object", true)
                .expect("v1 full"),
        );

        let mut large_obj = FileMetaVersion::try_from(cached_entry.cached.as_ref().expect("cached").versions[0].meta.as_slice())
            .expect("decode obj")
            .object
            .expect("v2 obj");
        for i in 0..65 {
            large_obj.meta_sys.insert(format!("x-rustfs-internal-custom-{i}"), vec![b'v']);
        }
        large_obj
            .meta_sys
            .insert("X-Rustfs-Internal-Tier-Free-Marker".to_string(), b"1".to_vec());
        rustfs_utils::http::insert_bytes(&mut large_obj.meta_sys, rustfs_utils::http::SUFFIX_TIER_FV_ID, b"keep-id".to_vec());
        rustfs_utils::http::insert_bytes(
            &mut large_obj.meta_sys,
            rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_ID,
            b"null".to_vec(),
        );
        rustfs_utils::http::insert_bytes(
            &mut large_obj.meta_sys,
            rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_STATE,
            b"suspended-null".to_vec(),
        );
        let large_ver = FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(large_obj),
            write_version: 1,
            ..Default::default()
        };
        let native_large_sv = FileMetaShallowVersion::try_from(large_ver.clone()).expect("native >64 MetaSys shallow");
        assert_eq!(
            native_large_sv
                .into_fileinfo("bucket", "object", true)
                .expect("native >64 fallback"),
            FileMetaVersion::try_from(native_large_sv.meta.as_slice())
                .expect("native >64 try_from")
                .into_fileinfo("bucket", "object", true)
                .expect("native >64 full"),
        );
        let rmp_legacy_bytes = rmp_serde::to_vec_named(&large_ver).expect("rmp_serde named");
        let rmp_legacy_fi = FileMetaShallowVersion {
            header: large_ver.header(),
            meta: rmp_legacy_bytes,
        }
        .into_fileinfo("bucket", "object", true)
        .expect("rmp_serde fallback");
        assert!(rmp_legacy_fi.uses_legacy_checksum);
        assert!(!rmp_legacy_fi.metadata.contains_key("X-Rustfs-Internal-Tier-Free-Marker"));
        assert!(rustfs_utils::http::contains_key_str(
            &rmp_legacy_fi.metadata,
            rustfs_utils::http::SUFFIX_TIER_FV_ID
        ));
        assert_eq!(rmp_legacy_fi.transition_version.as_deref(), Some("null"));
    }

    #[test]
    fn resolve_inner_borrowed_matches_distinct_ec_non_strict_and_multi_version_cases() {
        let now = OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("timestamp");
        let base = metacache_entry_single_version(1, now, "etag-1");
        let base_fm = FileMeta::load(&base.metadata).expect("load base");

        // 1. Distinct erasure_index per disk (different raw metadata bytes, identical version headers).
        let mut distinct_ec_entries = Vec::with_capacity(4);
        for disk_idx in 1..=4usize {
            let mut fm = base_fm.clone();
            let mut ver = FileMetaVersion::try_from(fm.versions[0].meta.as_slice()).expect("decode version");
            ver.object.as_mut().expect("object").erasure_index = disk_idx;
            fm.versions[0].meta = ver.marshal_msg().expect("marshal version");
            distinct_ec_entries.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: fm.marshal_msg().expect("marshal fm"),
                cached: if disk_idx == 2 { Some(fm) } else { None },
                reusable: false,
            }));
        }
        let resolved_ec = MetaCacheEntries(distinct_ec_entries)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 2,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve distinct ec");
        assert!(resolved_ec.cached.is_some());
        let resolved_ver =
            FileMetaVersion::try_from(resolved_ec.cached.as_ref().expect("cached").versions[0].meta.as_slice()).expect("ver");
        assert_eq!(resolved_ver.object.expect("obj").erasure_index, 4);

        // 2. Multi-version (> 2 versions, exercising BorrowedVersions::Multi) with non-strict signature preference.
        let mut multi_a = FileMeta::new();
        for v_idx in 1..=4u128 {
            let entry = metacache_entry_single_version(
                v_idx,
                now + time::Duration::seconds(i64::try_from(v_idx).expect("i64")),
                &format!("etag-{v_idx}"),
            );
            multi_a
                .versions
                .extend(FileMeta::load(&entry.metadata).expect("load v").versions);
        }
        let mut multi_b = multi_a.clone();
        multi_a.versions[0].header.signature = [1, 1, 1, 1];
        multi_b.versions[0].header.signature = [9, 9, 9, 9];
        let raw_a = multi_a.marshal_msg().expect("marshal multi_a");
        let raw_b = multi_b.marshal_msg().expect("marshal multi_b");

        // When higher-signature `multi_b` is first and `multi_a` is second, `!strict` keeps `multi_b`.
        let keep_first = MetaCacheEntries(vec![
            Some(MetaCacheEntry {
                name: "multi".to_string(),
                metadata: raw_b.clone(),
                cached: None,
                reusable: false,
            }),
            Some(MetaCacheEntry {
                name: "multi".to_string(),
                metadata: raw_a.clone(),
                cached: None,
                reusable: false,
            }),
        ])
        .resolve(MetadataResolutionParams {
            dir_quorum: 1,
            obj_quorum: 2,
            requested_versions: 0,
            bucket: "bucket".to_string(),
            strict: false,
            write_quorum_slack: 0,
            candidates: Vec::new(),
        })
        .expect("resolve keep_first");
        assert_eq!(keep_first.cached.as_ref().expect("cached").versions[0].header.signature, [9, 9, 9, 9]);

        // When lower-signature `multi_a` is first and `multi_b` is second, `!strict` prefers `multi_b`.
        let prefer_second = MetaCacheEntries(vec![
            Some(MetaCacheEntry {
                name: "multi".to_string(),
                metadata: raw_a,
                cached: None,
                reusable: false,
            }),
            Some(MetaCacheEntry {
                name: "multi".to_string(),
                metadata: raw_b,
                cached: None,
                reusable: false,
            }),
        ])
        .resolve(MetadataResolutionParams {
            dir_quorum: 1,
            obj_quorum: 2,
            requested_versions: 0,
            bucket: "bucket".to_string(),
            strict: false,
            write_quorum_slack: 0,
            candidates: Vec::new(),
        })
        .expect("resolve prefer_second");
        assert_eq!(prefer_second.cached.as_ref().expect("cached").versions[0].header.signature, [9, 9, 9, 9]);

        // 3. Exhaustive truth table for headers_match_strict over (ec_n, ec_m) pairs, including zero-parity headers.
        let base_hdr = base_fm.versions[0].header.clone();
        for s_m in [0u8, 1, 4, 7] {
            for s_n in [0u8, 1, 2] {
                for o_m in [0u8, 1, 4, 8] {
                    for o_n in [0u8, 1, 2] {
                        let mut s_hdr = base_hdr.clone();
                        s_hdr.ec_m = s_m;
                        s_hdr.ec_n = s_n;
                        let mut o_hdr = base_hdr.clone();
                        o_hdr.ec_m = o_m;
                        o_hdr.ec_n = o_n;

                        let expected = {
                            let mut s_cmp = s_hdr.clone();
                            let mut o_cmp = o_hdr.clone();
                            if s_cmp.has_ec() != o_cmp.has_ec() {
                                s_cmp.ec_m = 0;
                                s_cmp.ec_n = 0;
                                o_cmp.ec_m = 0;
                                o_cmp.ec_n = 0;
                            }
                            s_cmp == o_cmp
                        };
                        assert_eq!(
                            FileMeta::headers_match_strict(&s_hdr, &o_hdr),
                            expected,
                            "mismatch for s=({s_m},{s_n}) vs o=({o_m},{o_n})"
                        );
                    }
                }
            }
        }

        // Zero-parity unequal EC fields ((7, 0) vs (0, 0)) must disagree in strict mode (both cached and uncached).
        let mut fm_ec_7_0 = base_fm.clone();
        fm_ec_7_0.versions[0].header.ec_m = 7;
        fm_ec_7_0.versions[0].header.ec_n = 0;
        let mut fm_ec_0_0 = base_fm.clone();
        fm_ec_0_0.versions[0].header.ec_m = 0;
        fm_ec_0_0.versions[0].header.ec_n = 0;
        let mut e_7_0 = MetaCacheEntry {
            name: base.name.clone(),
            metadata: fm_ec_7_0.marshal_msg().expect("marshal 7_0"),
            cached: None,
            reusable: false,
        };
        let e_0_0_uncached = MetaCacheEntry {
            name: base.name.clone(),
            metadata: fm_ec_0_0.marshal_msg().expect("marshal 0_0"),
            cached: None,
            reusable: false,
        };
        let e_0_0_cached = MetaCacheEntry {
            name: base.name.clone(),
            metadata: e_0_0_uncached.metadata.clone(),
            cached: Some(fm_ec_0_0),
            reusable: false,
        };
        assert!(!e_7_0.matches_strict(&e_0_0_uncached));
        assert!(!e_7_0.matches_strict(&e_0_0_cached));

        // 4. Differential test: uncached vs cached disagreement merge (with distinct per-disk EC index and corrupt entry).
        let newer = metacache_entry_single_version(2, now + time::Duration::seconds(10), "etag-2");
        let newer_fm = FileMeta::load(&newer.metadata).expect("load newer");
        let mut uncached_disagree = Vec::with_capacity(6);
        let mut cached_disagree = Vec::with_capacity(6);
        for disk_idx in 0..5usize {
            let mut fm = if disk_idx == 0 { base_fm.clone() } else { newer_fm.clone() };
            let mut ver = FileMetaVersion::try_from(fm.versions[0].meta.as_slice()).expect("decode ver");
            ver.object.as_mut().expect("obj").erasure_index = disk_idx + 1;
            fm.versions[0].meta = ver.marshal_msg().expect("marshal ver");
            let raw = fm.marshal_msg().expect("marshal fm");
            uncached_disagree.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: raw.clone(),
                cached: None,
                reusable: false,
            }));
            cached_disagree.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: raw,
                cached: Some(fm),
                reusable: false,
            }));
        }
        // Append one corrupt uncached entry to both sets.
        let mut corrupt_raw = base.metadata.clone();
        if let Some(last) = corrupt_raw.last_mut() {
            *last ^= 0xff;
        }
        uncached_disagree.push(Some(MetaCacheEntry {
            name: base.name.clone(),
            metadata: corrupt_raw.clone(),
            cached: None,
            reusable: false,
        }));
        cached_disagree.push(Some(MetaCacheEntry {
            name: base.name.clone(),
            metadata: corrupt_raw.clone(),
            cached: None,
            reusable: false,
        }));

        let res_uncached = MetaCacheEntries(uncached_disagree)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 3,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve uncached disagree");
        let res_cached = MetaCacheEntries(cached_disagree)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 3,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve cached disagree");
        let fm_uncached = res_uncached.cached.expect("uncached fm");
        let fm_cached = res_cached.cached.expect("cached fm");
        assert_eq!(fm_uncached.versions[0].header.version_id, Some(Uuid::from_u128(2)));
        assert_eq!(fm_uncached.meta_ver, fm_cached.meta_ver);
        assert_eq!(fm_uncached.versions, fm_cached.versions);
        assert_eq!(res_uncached.metadata, res_cached.metadata);

        // An all-agree set with one corrupt entry must still take the fast agree path (`reusable == false`).
        let mut agree_with_corrupt: Vec<Option<MetaCacheEntry>> = (0..4)
            .map(|_| {
                Some(MetaCacheEntry {
                    name: base.name.clone(),
                    metadata: newer.metadata.clone(),
                    cached: None,
                    reusable: false,
                })
            })
            .collect();
        agree_with_corrupt.push(Some(MetaCacheEntry {
            name: base.name.clone(),
            metadata: corrupt_raw,
            cached: None,
            reusable: false,
        }));
        let res_agree_corrupt = MetaCacheEntries(agree_with_corrupt)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 3,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve agree with corrupt");
        assert!(!res_agree_corrupt.reusable);
        assert_eq!(res_agree_corrupt.metadata, newer.metadata);

        // Identical-byte supplement replicas + one divergent entry (exercising `entry.metadata == selected_ref.metadata` and `last_loaded`).
        let mut supplement_entries: Vec<Option<MetaCacheEntry>> = (0..4)
            .map(|_| {
                Some(MetaCacheEntry {
                    name: base.name.clone(),
                    metadata: newer.metadata.clone(),
                    cached: None,
                    reusable: false,
                })
            })
            .collect();
        for _ in 0..2 {
            supplement_entries.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: base.metadata.clone(),
                cached: None,
                reusable: false,
            }));
        }
        let res_supplement = MetaCacheEntries(supplement_entries)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 3,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve supplement");
        assert!(res_supplement.reusable);
        assert_eq!(
            res_supplement.cached.expect("cached").versions[0].header.version_id,
            Some(Uuid::from_u128(2))
        );

        // 5. Duplicate-header stream removal preserves remaining duplicates per stream.
        let dup_ver = base_fm.versions[0].clone();
        let dup_candidates = vec![vec![dup_ver.clone(), dup_ver.clone()]; 3];
        let merged_dups = crate::merge_file_meta_versions(2, true, 0, &dup_candidates);
        assert_eq!(merged_dups.len(), 2);
        assert_eq!(merged_dups[0].header, dup_ver.header);
        assert_eq!(merged_dups[1].header, dup_ver.header);

        // 6. Outlier last (drives 0..4 agree with distinct EC index, drive 1 cached, drive 4 disagrees),
        // exercising `stream_from_prevalidated_matching_buf` and `compare_cached` displaced-entry retention.
        let mut outlier_last = Vec::with_capacity(5);
        for disk_idx in 0..5usize {
            let mut fm = if disk_idx == 4 { base_fm.clone() } else { newer_fm.clone() };
            let mut ver = FileMetaVersion::try_from(fm.versions[0].meta.as_slice()).expect("decode ver");
            ver.object.as_mut().expect("obj").erasure_index = disk_idx + 1;
            fm.versions[0].meta = ver.marshal_msg().expect("marshal ver");
            let raw = fm.marshal_msg().expect("marshal fm");
            outlier_last.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: raw,
                cached: if disk_idx == 1 { Some(fm) } else { None },
                reusable: false,
            }));
        }
        let res_outlier_last = MetaCacheEntries(outlier_last)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 3,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve outlier_last");
        assert_eq!(
            res_outlier_last.cached.expect("cached").versions[0].header.version_id,
            Some(Uuid::from_u128(2))
        );

        // 7. Canonical 38-byte header unmarshal_msg and matches_buf_strict across negative, zero, and subsecond timestamps.
        for nanos in [-1_705_312_300_123_456_789i128, -1, 0, 1, 1_705_312_300_987_654_321] {
            let mut fm = base_fm.clone();
            let ts = OffsetDateTime::from_unix_timestamp_nanos(nanos).expect("valid ts");
            fm.versions[0].header.mod_time = (ts != OffsetDateTime::UNIX_EPOCH).then_some(ts);
            let encoded = fm.versions[0].header.marshal_msg().expect("marshal hdr");
            assert_eq!(encoded.len(), 38);
            assert!(crate::filemeta::parse_canonical_v3_header_fields(&encoded).is_some());
            let mut decoded = crate::FileMetaVersionHeader::default();
            assert_eq!(decoded.unmarshal_msg(&encoded).expect("unmarshal 38B"), 38);
            assert_eq!(decoded, fm.versions[0].header);
            let fm_raw = fm.marshal_msg().expect("marshal fm");
            assert!(fm.matches_buf_strict(&fm_raw));
            assert_eq!(
                fm.matches_buf_strict(&base.metadata),
                fm.versions[0].header.mod_time == base_fm.versions[0].header.mod_time
            );
        }

        // 8. Mixed empty V3 (`versions_len == 0`) and zero-meta-payload envelope preserve stream 0 during fallback merge.
        let empty_v3_raw = FileMeta::new().marshal_msg().expect("marshal empty v3");
        let mut zero_meta_envelope = Vec::from(&b"XL2 "[..]);
        zero_meta_envelope.extend_from_slice(&1u16.to_le_bytes());
        zero_meta_envelope.extend_from_slice(&3u16.to_le_bytes());
        zero_meta_envelope.extend_from_slice(&[0xc6, 0, 0, 0, 0]);
        let zero_crc = xxhash_rust::xxh64::xxh64(&[], 0) as u32;
        zero_meta_envelope.push(0xce);
        zero_meta_envelope.extend_from_slice(&zero_crc.to_be_bytes());

        let mut two_ver_fm = newer_fm;
        two_ver_fm.versions.push(base_fm.versions[0].clone());
        let two_ver_raw = two_ver_fm.marshal_msg().expect("marshal two_ver_fm");

        let mut mixed_empty_entries = vec![Some(MetaCacheEntry {
            name: base.name.clone(),
            metadata: empty_v3_raw,
            cached: None,
            reusable: false,
        })];
        for _ in 0..4 {
            mixed_empty_entries.push(Some(MetaCacheEntry {
                name: base.name.clone(),
                metadata: two_ver_raw.clone(),
                cached: None,
                reusable: false,
            }));
        }
        mixed_empty_entries.push(Some(MetaCacheEntry {
            name: base.name,
            metadata: zero_meta_envelope,
            cached: None,
            reusable: false,
        }));
        let res_mixed_empty = MetaCacheEntries(mixed_empty_entries)
            .resolve_with_write_quorum(MetadataResolutionParams {
                dir_quorum: 2,
                obj_quorum: 4,
                requested_versions: 1,
                bucket: "bucket".to_string(),
                strict: true,
                write_quorum_slack: 0,
                candidates: Vec::new(),
            })
            .expect("resolve mixed empty");
        assert_eq!(res_mixed_empty.cached.expect("cached").versions.len(), 1);
    }
}
