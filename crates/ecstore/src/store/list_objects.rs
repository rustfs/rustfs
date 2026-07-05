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

use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::utils::check_list_objs_args;
use crate::bucket::versioning::VersioningApi;
use crate::cache_value::metacache_set::{FallbackClaimTracker, ListPathRawOptions, list_path_raw_with_claim_tracker};
use crate::core::sets::Sets;
use crate::disk::error::DiskError;
use crate::disk::{DiskAPI, DiskInfo, DiskStore, WalkDirOptions};
use crate::error::{
    Error, Result, StorageError, is_all_not_found, is_all_volume_not_found, is_err_bucket_not_found, to_object_err,
};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::{
    list::{
        ListObjectsInfo as StorageListObjectsInfo, StorageListObjectVersionsInfo, StorageListObjectsV2Info,
        StorageObjectInfoOrErr, StorageWalkOptions, VersionMarker, WalkVersionsSortOrder,
    },
    object::ObjectOperations as _,
};
use crate::store::ECStore;
use crate::store::utils::is_reserved_or_invalid_bucket;
use futures::future::join_all;
use rand::seq::SliceRandom;
use rustfs_filemeta::{
    FileMeta, FileMetaShallowVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntriesSortedResult, MetaCacheEntry,
    MetacacheReader, MetadataResolutionParams, is_io_eof, merge_file_meta_versions,
};
use rustfs_io_metrics::{
    LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED, LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED, LIST_OBJECTS_SOURCE_WALKER,
    ListObjectsGatherObservation,
};
use rustfs_utils::path::{self, SLASH_SEPARATOR, base_dir_from_prefix};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::duplex;
use tokio::sync::OnceCell;
use tokio::sync::broadcast::{self};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};
use uuid::Uuid;

const MAX_OBJECT_LIST: i32 = 1000;
// const MAX_DELETE_LIST: i32 = 1000;
// const MAX_UPLOADS_LIST: i32 = 10000;
// const MAX_PARTS_LIST: i32 = 10000;

const METACACHE_SHARE_PREFIX: bool = false;

type ListObjectsInfo = StorageListObjectsInfo<ObjectInfo>;
type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&rustfs_filemeta::FileInfo) -> bool>;

fn normalize_max_keys(max_keys: i32) -> i32 {
    max_keys.min(MAX_OBJECT_LIST)
}

fn ensure_non_empty_listing_disks(bucket: &str, path: &str, disks: &[DiskStore]) -> Result<()> {
    if disks.is_empty() {
        warn!(
            bucket = %bucket,
            path = %path,
            "listing candidate disks collapsed to empty set"
        );
        return Err(StorageError::ErasureReadQuorum);
    }

    Ok(())
}

fn walk_result_from_set_errors(errs: &[Option<Error>]) -> Result<()> {
    if is_all_not_found(errs) {
        if is_all_volume_not_found(errs) {
            return Err(StorageError::VolumeNotFound);
        }

        return Ok(());
    }

    for err in errs.iter().flatten() {
        if err == &Error::Unexpected || err.is_not_found() {
            continue;
        }

        return Err(err.clone());
    }

    Ok(())
}

pub fn max_keys_plus_one(max_keys: i32, add_one: bool) -> i32 {
    let mut max_keys = max_keys;
    if !(0..=MAX_OBJECT_LIST).contains(&max_keys) {
        max_keys = MAX_OBJECT_LIST;
    }
    if add_one {
        max_keys += 1;
    }
    max_keys
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum GatherResultsState {
    LimitReached,
    InputClosed,
}

#[derive(Clone)]
struct ListPathLogContext {
    bucket: String,
    base_dir: String,
    prefix: String,
    marker: String,
    limit: i32,
}

impl ListPathLogContext {
    fn from_options(opts: &ListPathOptions) -> Self {
        Self {
            bucket: opts.bucket.clone(),
            base_dir: opts.base_dir.clone(),
            prefix: opts.prefix.clone(),
            marker: opts.marker.clone().unwrap_or_default(),
            limit: opts.limit,
        }
    }
}

fn log_list_path_worker_error<E>(component: &'static str, stage: &'static str, context: &ListPathLogContext, err: &E)
where
    E: std::fmt::Display + ?Sized,
{
    error!(
        component,
        stage,
        bucket = %context.bucket,
        prefix = %context.prefix,
        base_dir = %context.base_dir,
        limit = context.limit,
        marker = %context.marker,
        error = %err,
        "list_path worker failed"
    );
}

fn log_list_path_finished(
    component: &'static str,
    context: &ListPathLogContext,
    elapsed_ms: f64,
    candidate_count: usize,
    has_error: bool,
) {
    debug!(
        component,
        bucket = %context.bucket,
        prefix = %context.prefix,
        base_dir = %context.base_dir,
        limit = context.limit,
        marker = %context.marker,
        candidate_count,
        has_error,
        elapsed_ms,
        "list_path finished"
    );
}

#[derive(Debug, Default, Clone)]
pub struct ListPathOptions {
    pub id: Option<String>,

    // Bucket of the listing.
    pub bucket: String,

    // Directory inside the bucket.
    // When unset listPath will set this based on Prefix
    pub base_dir: String,

    // Scan/return only content with prefix.
    pub prefix: String,

    // FilterPrefix will return only results with this prefix when scanning.
    // Should never contain a slash.
    // Prefix should still be set.
    pub filter_prefix: Option<String>,

    // Marker to resume listing.
    pub marker: Option<String>,

    // Include marker itself in the returned entries. Version listings need this
    // when a version marker selects a later version from the marker object.
    pub include_marker: bool,

    // Limit the number of results.
    pub limit: i32,

    // The number of disks to ask.
    pub ask_disks: String,

    // InclDeleted will keep all entries where latest version is a delete marker.
    pub incl_deleted: bool,

    // Scan recursively.
    // If false only main directory will be scanned.
    // Should always be true if Separator is n SlashSeparator.
    pub recursive: bool,

    // Separator to use.
    pub separator: Option<String>,

    // Create indicates that the lister should not attempt to load an existing cache.
    pub create: bool,

    // Include pure directories.
    pub include_directories: bool,

    // Transient is set if the cache is transient due to an error or being a reserved bucket.
    // This means the cache metadata will not be persisted on disk.
    // A transient result will never be returned from the cache so knowing the list id is required.
    pub transient: bool,

    // Versioned is this a ListObjectVersions call.
    pub versioned: bool,

    pub stop_disk_at_limit: bool,

    pub pool_idx: Option<usize>,
    pub set_idx: Option<usize>,
}

const MARKER_TAG_VERSION: &str = "v2";
const LEGACY_MARKER_TAG_VERSIONS: &[&str] = &["v1", MARKER_TAG_VERSION];
const ENV_API_LIST_QUORUM: &str = "RUSTFS_API_LIST_QUORUM";
const DEFAULT_API_LIST_QUORUM: &str = "strict";
const ENV_API_LIST_OBJECTS_QUORUM: &str = "RUSTFS_LIST_OBJECTS_QUORUM";
const DEFAULT_API_LIST_OBJECTS_QUORUM: &str = "optimal";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListContinuationV2 {
    version: &'static str,
    id: Option<String>,
    pool_idx: Option<usize>,
    set_idx: Option<usize>,
}

impl ListContinuationV2 {
    fn encode_marker(&self, marker: &str) -> String {
        let mut marker_tag = String::with_capacity(marker.len() + 64);
        marker_tag.push_str(marker);
        marker_tag.push_str("[rustfs_cache:");
        marker_tag.push_str(self.version);
        match &self.id {
            Some(id) => {
                marker_tag.push_str(",id:");
                marker_tag.push_str(id);
            }
            None => marker_tag.push_str(",return:"),
        }
        if let Some(pool_idx) = self.pool_idx {
            marker_tag.push_str(",p:");
            marker_tag.push_str(&pool_idx.to_string());
        }
        if let Some(set_idx) = self.set_idx {
            marker_tag.push_str(",s:");
            marker_tag.push_str(&set_idx.to_string());
        }
        marker_tag.push(']');
        marker_tag
    }

    fn parse(marker_tag: &str) -> Option<(Self, bool)> {
        let mut parsed = Self {
            version: MARKER_TAG_VERSION,
            id: None,
            pool_idx: None,
            set_idx: None,
        };
        let mut has_list_cache = false;
        let mut should_create = false;

        for tag in marker_tag.split(',') {
            let Some((key, value)) = tag.split_once(':') else {
                continue;
            };

            match key {
                "rustfs_cache" => {
                    match value {
                        "v1" => parsed.version = "v1",
                        MARKER_TAG_VERSION => parsed.version = MARKER_TAG_VERSION,
                        _ => return None,
                    }
                    has_list_cache = true;
                }
                "id" => {
                    parsed.id = Some(value.to_owned());
                }
                "return" => {
                    parsed.id = Some(Uuid::new_v4().to_string());
                    should_create = true;
                }
                "p" => match value.parse::<usize>() {
                    Ok(res) => parsed.pool_idx = Some(res),
                    Err(_) => {
                        parsed.id = Some(Uuid::new_v4().to_string());
                        should_create = true;
                    }
                },
                "s" => match value.parse::<usize>() {
                    Ok(res) => parsed.set_idx = Some(res),
                    Err(_) => {
                        parsed.id = Some(Uuid::new_v4().to_string());
                        should_create = true;
                    }
                },
                _ => (),
            }
        }

        if has_list_cache && LEGACY_MARKER_TAG_VERSIONS.contains(&parsed.version) {
            Some((parsed, should_create))
        } else {
            None
        }
    }
}

fn normalize_list_quorum(value: &str) -> &'static str {
    let value = value.trim();
    if value.eq_ignore_ascii_case("disk") {
        "disk"
    } else if value.eq_ignore_ascii_case("reduced") {
        "reduced"
    } else if value.eq_ignore_ascii_case("optimal") {
        "optimal"
    } else if value.eq_ignore_ascii_case("auto") {
        "auto"
    } else {
        DEFAULT_API_LIST_QUORUM
    }
}

fn list_quorum_from_env() -> String {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_QUORUM, DEFAULT_API_LIST_QUORUM);
    normalize_list_quorum(&value).to_owned()
}

fn list_objects_quorum_from_env() -> String {
    let value = rustfs_utils::get_env_str(ENV_API_LIST_OBJECTS_QUORUM, DEFAULT_API_LIST_OBJECTS_QUORUM);
    normalize_list_quorum(&value).to_owned()
}

fn append_list_cache_id_to_marker(marker: String, cache_id: Option<&str>) -> String {
    let Some(id) = cache_id else {
        return marker;
    };

    let mut marker_tag = String::with_capacity(marker.len() + 24 + id.len());
    marker_tag.push_str(&marker);
    marker_tag.push_str("[rustfs_cache:");
    marker_tag.push_str(MARKER_TAG_VERSION);
    marker_tag.push_str(",id:");
    marker_tag.push_str(id);
    marker_tag.push(']');

    marker_tag
}

fn build_list_next_marker(objects: &[ObjectInfo], prefixes: &[String], cache_id: Option<&str>) -> Option<String> {
    if let Some(last) = objects.last() {
        Some(append_list_cache_id_to_marker(last.name.clone(), cache_id))
    } else {
        prefixes
            .last()
            .map(|marker| append_list_cache_id_to_marker(marker.clone(), cache_id))
    }
}

fn build_list_versions_next_marker(
    objects: &[ObjectInfo],
    prefixes: &[String],
    cache_id: Option<&str>,
) -> (Option<String>, Option<String>) {
    if let Some(last) = objects.last() {
        (
            Some(append_list_cache_id_to_marker(last.name.clone(), cache_id)),
            Some(last.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
        )
    } else if let Some(last_prefix) = prefixes.last() {
        (Some(append_list_cache_id_to_marker(last_prefix.clone(), cache_id)), None)
    } else {
        (None, None)
    }
}

fn list_objects_paginate(
    get_objects: Vec<ObjectInfo>,
    delimiter: &Option<String>,
    max_keys: i32,
    disk_has_more: bool,
    cache_id: Option<&str>,
    include_version_id: bool,
) -> (Vec<ObjectInfo>, Vec<String>, bool, Option<String>, Option<String>) {
    let mut get_objects = get_objects;
    let mut is_truncated = false;
    let mut next_marker = None;
    let mut next_version_idmarker = None;

    if max_keys <= 0 {
        get_objects.clear();
    } else if get_objects.len() > max_keys as usize {
        is_truncated = true;
        get_objects.truncate(max_keys as usize);
    }

    if is_truncated {
        if include_version_id {
            (next_marker, next_version_idmarker) = build_list_versions_next_marker(&get_objects, &[], cache_id);
        } else {
            next_marker = build_list_next_marker(&get_objects, &[], cache_id);
        }
    }

    let mut prefixes: Vec<String> = Vec::new();
    let mut prefix_set: HashSet<String> = HashSet::new();
    let mut objects = Vec::with_capacity(get_objects.len());
    for obj in get_objects {
        if delimiter.is_some() {
            if obj.is_dir && obj.mod_time.is_none() {
                if prefix_set.insert(obj.name.clone()) {
                    prefixes.push(obj.name);
                }
            } else {
                objects.push(obj);
            }
        } else {
            objects.push(obj);
        }
    }

    if !is_truncated && disk_has_more {
        let visible_count = objects.len() + prefixes.len();
        let should_truncate = if delimiter.is_none() {
            visible_count > 0
        } else {
            visible_count >= max_keys as usize
        };
        if should_truncate {
            is_truncated = true;
            if include_version_id {
                (next_marker, next_version_idmarker) = build_list_versions_next_marker(&objects, &prefixes, cache_id);
            } else {
                next_marker = build_list_next_marker(&objects, &prefixes, cache_id);
            }
        }
    }

    (objects, prefixes, is_truncated, next_marker, next_version_idmarker)
}

fn list_metadata_resolution_params(
    bucket: String,
    listing_quorum: usize,
    latest_object_quorum: usize,
    versioned: bool,
) -> MetadataResolutionParams {
    let quorum = if versioned {
        listing_quorum
    } else {
        latest_object_quorum.max(listing_quorum)
    };
    let mut resolver = MetadataResolutionParams {
        dir_quorum: quorum,
        obj_quorum: quorum,
        bucket,
        ..Default::default()
    };

    if !versioned {
        resolver.requested_versions = 1;
    }

    resolver
}

fn resolve_listing_entries(
    entries: MetaCacheEntries,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> Option<MetaCacheEntry> {
    if enforce_write_quorum {
        entries.resolve_with_write_quorum(resolver)
    } else {
        entries.resolve(resolver)
    }
}

enum ListingEntryResolution {
    Resolved(MetaCacheEntry),
    NeedsSupplement(MetaCacheEntry, Option<MetaCacheEntry>),
    Rejected,
}

#[derive(Clone)]
struct FallbackListingEntry {
    endpoint: String,
    entry: MetaCacheEntry,
}

type FallbackListingEntries = HashMap<String, Vec<FallbackListingEntry>>;

#[derive(Clone)]
struct ListingSupplementOptions {
    bucket: String,
    path: String,
    recursive: bool,
    filter_prefix: Option<String>,
    forward_to: Option<String>,
    per_disk_limit: i32,
    skip_total_timeout: bool,
}

struct ListingSupplement {
    options: ListingSupplementOptions,
    fallback_disks: Arc<Vec<DiskStore>>,
    claim_tracker: FallbackClaimTracker,
    entries: Option<Arc<OnceCell<FallbackListingEntries>>>,
}

impl ListingSupplement {
    fn new(
        options: ListingSupplementOptions,
        fallback_disks: Arc<Vec<DiskStore>>,
        claim_tracker: FallbackClaimTracker,
    ) -> Arc<Self> {
        let entries = if options.per_disk_limit > 0 {
            Some(Arc::new(OnceCell::new()))
        } else {
            None
        };

        Arc::new(Self {
            options,
            fallback_disks,
            claim_tracker,
            entries,
        })
    }

    fn is_empty(&self) -> bool {
        self.fallback_disks.is_empty()
    }

    async fn entries_for(&self, object: &str) -> Vec<Option<MetaCacheEntry>> {
        if self.fallback_disks.is_empty() {
            return Vec::new();
        }

        let claimed_keys = self.claim_tracker.claimed_keys().await;
        let Some(entries) = &self.entries else {
            return self.read_object_entries(object, &claimed_keys).await;
        };

        let entries = entries.get_or_init(|| self.load_entries()).await;
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        fallback_entries_for_object(entries, object, &claimed_keys)
    }

    async fn load_entries(&self) -> FallbackListingEntries {
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        let futures = self.fallback_disks.iter().filter_map(|disk| {
            let endpoint = disk.endpoint().to_string();
            if claimed_keys.contains(&endpoint) {
                return None;
            }

            Some(read_fallback_listing_disk(disk.clone(), endpoint, self.options.clone()))
        });

        let per_disk_entries = join_all(futures).await;
        let entry_count = per_disk_entries.iter().map(Vec::len).sum();
        let mut entries = FallbackListingEntries::with_capacity(entry_count);
        for disk_entries in per_disk_entries {
            for entry in disk_entries {
                entries.entry(entry.entry.name.clone()).or_default().push(entry);
            }
        }

        entries
    }

    async fn read_object_entries(&self, object: &str, claimed_keys: &HashSet<String>) -> Vec<Option<MetaCacheEntry>> {
        let futures = self.fallback_disks.iter().filter_map(|disk| {
            let endpoint = disk.endpoint().to_string();
            if claimed_keys.contains(&endpoint) {
                return None;
            }

            Some(read_fallback_object_disk(
                disk.clone(),
                endpoint,
                self.options.bucket.clone(),
                object.to_owned(),
            ))
        });

        let entries = join_all(futures).await;
        let claimed_keys = self.claim_tracker.claimed_keys().await;
        entries
            .into_iter()
            .flatten()
            .filter(|entry| !claimed_keys.contains(&entry.endpoint))
            .map(|entry| Some(entry.entry))
            .collect()
    }
}

fn fallback_entries_for_object(
    entries: &FallbackListingEntries,
    object: &str,
    claimed_keys: &HashSet<String>,
) -> Vec<Option<MetaCacheEntry>> {
    entries
        .get(object)
        .into_iter()
        .flat_map(|entries| entries.iter())
        .filter(|entry| !claimed_keys.contains(&entry.endpoint))
        .map(|entry| Some(entry.entry.clone()))
        .collect()
}

async fn read_fallback_listing_disk(
    disk: DiskStore,
    endpoint: String,
    options: ListingSupplementOptions,
) -> Vec<FallbackListingEntry> {
    let (rd, mut wr) = duplex(64);
    let walk_endpoint = endpoint.clone();
    let walk_job = tokio::spawn(async move {
        disk.walk_dir(
            WalkDirOptions {
                bucket: options.bucket,
                base_dir: options.path,
                recursive: options.recursive,
                report_notfound: false,
                filter_prefix: options.filter_prefix,
                forward_to: options.forward_to,
                limit: options.per_disk_limit,
                skip_total_timeout: options.skip_total_timeout,
                ..Default::default()
            },
            &mut wr,
        )
        .await
    });

    let mut reader = MetacacheReader::new(rd);
    let mut entries = Vec::new();
    let mut stream_ok = true;
    loop {
        match reader.peek().await {
            Ok(Some(entry)) => {
                entries.push(FallbackListingEntry {
                    endpoint: endpoint.clone(),
                    entry,
                });
                if let Err(err) = reader.skip(1).await {
                    debug!(
                        endpoint = %endpoint,
                        error = ?err,
                        "fallback listing supplement stream failed while advancing reader"
                    );
                    stream_ok = false;
                    break;
                }
            }
            Ok(None) => break,
            Err(err) if err == rustfs_filemeta::Error::Unexpected || is_io_eof(&err) => break,
            Err(err) => {
                debug!(
                    endpoint = %endpoint,
                    error = ?err,
                    "fallback listing supplement stream failed while reading entry"
                );
                stream_ok = false;
                break;
            }
        }
    }
    drop(reader);

    match walk_job.await {
        Ok(Ok(())) if stream_ok => entries,
        Ok(Ok(())) => Vec::new(),
        Ok(Err(err)) => {
            debug!(
                endpoint = %walk_endpoint,
                error = ?err,
                "fallback listing supplement walk_dir failed"
            );
            Vec::new()
        }
        Err(err) => {
            debug!(
                endpoint = %walk_endpoint,
                error = ?err,
                "fallback listing supplement task failed"
            );
            Vec::new()
        }
    }
}

async fn read_fallback_object_disk(
    disk: DiskStore,
    endpoint: String,
    bucket: String,
    object: String,
) -> Option<FallbackListingEntry> {
    match disk.read_xl(&bucket, &object, false).await {
        Ok(raw) if raw.buf.is_empty() => None,
        Ok(raw) => Some(FallbackListingEntry {
            endpoint,
            entry: MetaCacheEntry {
                name: object,
                metadata: raw.buf,
                cached: None,
                reusable: false,
            },
        }),
        Err(err) if DiskError::is_err_object_not_found(&err) || DiskError::is_err_version_not_found(&err) => None,
        Err(err) => {
            debug!(
                endpoint = %endpoint,
                bucket = %bucket,
                object = %object,
                error = ?err,
                "fallback listing supplement read_xl failed"
            );
            None
        }
    }
}

fn version_requires_supplement(
    version_required_quorum: usize,
    reader_disks: usize,
    selected_object_versions: usize,
    requested_versions: usize,
) -> bool {
    reader_disks < version_required_quorum && (requested_versions == 0 || selected_object_versions < requested_versions)
}

fn cached_entry_needs_supplement(
    cached: &FileMeta,
    reader_disks: usize,
    resolver: &MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> bool {
    if !enforce_write_quorum {
        return false;
    }

    let mut selected_object_versions = 0;
    for version in cached.versions.iter() {
        let required_quorum = version.write_quorum(resolver.obj_quorum).max(resolver.obj_quorum);
        if version_requires_supplement(required_quorum, reader_disks, selected_object_versions, resolver.requested_versions) {
            return true;
        }

        if !version.header.free_version() {
            selected_object_versions += 1;
        }
        if resolver.requested_versions > 0 && selected_object_versions == resolver.requested_versions {
            break;
        }
    }

    false
}

fn listing_entries_supplement_target(
    entries: &MetaCacheEntries,
    resolver: &MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> Option<String> {
    if !enforce_write_quorum {
        return None;
    }

    for entry in entries.0.iter().flatten() {
        if entry.is_dir() {
            continue;
        }

        let reader_disks = entries
            .0
            .iter()
            .filter(|candidate| {
                candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.name == entry.name && candidate.is_object())
            })
            .count();
        let mut entry = entry.clone();
        if let Ok(cached) = entry.xl_meta()
            && cached_entry_needs_supplement(&cached, reader_disks, resolver, enforce_write_quorum)
        {
            return Some(entry.name);
        }
    }

    None
}

async fn resolve_listing_entries_with_supplement(
    entries: MetaCacheEntries,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
    supplement: Arc<ListingSupplement>,
) -> Option<MetaCacheEntry> {
    if !supplement.is_empty()
        && let Some(object) = listing_entries_supplement_target(&entries, &resolver, enforce_write_quorum)
    {
        let mut candidates = entries.0;
        candidates.extend(supplement.entries_for(&object).await);
        return resolve_listing_entries(MetaCacheEntries(candidates), resolver, enforce_write_quorum);
    }

    resolve_listing_entries(entries, resolver, enforce_write_quorum)
}

async fn resolve_agreed_listing_entry_with_supplement(
    entry: MetaCacheEntry,
    reader_disks: usize,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
    supplement: Arc<ListingSupplement>,
) -> Option<MetaCacheEntry> {
    if supplement.is_empty() {
        return None;
    }

    let mut candidates = Vec::with_capacity(reader_disks);
    candidates.resize(reader_disks, Some(entry.clone()));
    candidates.extend(supplement.entries_for(&entry.name).await);
    resolve_listing_entries(MetaCacheEntries(candidates), resolver, enforce_write_quorum)
}

fn listing_entry_with_selected_versions(
    mut entry: MetaCacheEntry,
    meta_ver: u8,
    data: rustfs_filemeta::InlineData,
    selected_versions: Vec<FileMetaShallowVersion>,
) -> Option<MetaCacheEntry> {
    let merged_cached = FileMeta {
        meta_ver,
        data,
        versions: selected_versions,
    };
    let metadata = merged_cached.marshal_msg().ok()?;

    entry.metadata = metadata;
    entry.cached = Some(merged_cached);
    Some(entry)
}

fn resolve_agreed_listing_entry(
    mut entry: MetaCacheEntry,
    reader_disks: usize,
    resolver: MetadataResolutionParams,
    enforce_write_quorum: bool,
) -> ListingEntryResolution {
    if !enforce_write_quorum {
        return ListingEntryResolution::Resolved(entry);
    }

    if entry.is_dir() {
        return if reader_disks >= resolver.dir_quorum {
            ListingEntryResolution::Resolved(entry)
        } else {
            ListingEntryResolution::Rejected
        };
    }

    if reader_disks < resolver.obj_quorum {
        return ListingEntryResolution::Rejected;
    }

    let cached = match entry.xl_meta() {
        Ok(cached) => cached,
        Err(_) if resolver.obj_quorum <= 1 => return ListingEntryResolution::Resolved(entry),
        Err(_) => return ListingEntryResolution::Rejected,
    };
    let mut selected_versions = Vec::new();
    let mut selected_object_versions = 0;
    let mut first_selected_idx = None;
    let mut needs_supplement = false;

    for (idx, version) in cached.versions.iter().enumerate() {
        let required_quorum = version.write_quorum(resolver.obj_quorum).max(resolver.obj_quorum);
        if reader_disks < required_quorum {
            needs_supplement |=
                version_requires_supplement(required_quorum, reader_disks, selected_object_versions, resolver.requested_versions);
            continue;
        }

        if first_selected_idx.is_none() {
            first_selected_idx = Some(idx);
        }
        if !version.header.free_version() {
            selected_object_versions += 1;
        }
        selected_versions.push(version.clone());

        if resolver.requested_versions > 0 && selected_object_versions == resolver.requested_versions {
            break;
        }
    }

    if needs_supplement {
        let fallback = if selected_versions.is_empty() {
            None
        } else if selected_versions.len() == cached.versions.len() {
            Some(entry.clone())
        } else {
            listing_entry_with_selected_versions(entry.clone(), cached.meta_ver, cached.data, selected_versions)
        };
        return ListingEntryResolution::NeedsSupplement(entry, fallback);
    }

    if selected_versions.is_empty() {
        return ListingEntryResolution::Rejected;
    }

    let selected_latest = first_selected_idx == Some(0)
        && resolver.requested_versions == 1
        && selected_versions
            .first()
            .is_some_and(|version| !version.header.free_version());
    if selected_latest || selected_versions.len() == cached.versions.len() {
        return ListingEntryResolution::Resolved(entry);
    }

    listing_entry_with_selected_versions(entry, cached.meta_ver, cached.data, selected_versions)
        .map(ListingEntryResolution::Resolved)
        .unwrap_or(ListingEntryResolution::Rejected)
}

fn latest_listing_object_quorum(
    listing_quorum: usize,
    drive_count: usize,
    parity_count: usize,
    enforce_write_quorum: bool,
) -> usize {
    latest_listing_required_object_quorum(listing_quorum, drive_count, parity_count, enforce_write_quorum)
}

fn latest_listing_required_object_quorum(
    listing_quorum: usize,
    drive_count: usize,
    parity_count: usize,
    enforce_write_quorum: bool,
) -> usize {
    if !enforce_write_quorum {
        return listing_quorum;
    }

    write_quorum_for_drive_count(drive_count, parity_count).max(listing_quorum)
}

fn enforce_latest_listing_write_quorum(strict_latest: bool, ask_disks: &str) -> bool {
    strict_latest && !matches!(normalize_list_quorum(ask_disks), "disk" | "reduced")
}

#[cfg(test)]
fn latest_listing_allow_agreed_objects(enforce_write_quorum: bool, reader_disks: usize, object_quorum: usize) -> bool {
    !enforce_write_quorum || reader_disks >= object_quorum
}

fn latest_listing_raw_min_disks(listing_quorum: usize, object_quorum: usize, enforce_write_quorum: bool) -> usize {
    if enforce_write_quorum { object_quorum } else { listing_quorum }
}

fn positive_ask_disks(ask_disks: i32) -> Option<usize> {
    usize::try_from(ask_disks).ok().filter(|asked| *asked > 0)
}

fn listing_quorum_from_ask_disks(ask_disks: i32) -> usize {
    positive_ask_disks(ask_disks)
        .map(|asked| asked.div_ceil(2))
        .unwrap_or_default()
}

fn bounded_usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn clamp_ask_disks_to_available(ask_disks: i32, available_disks: usize) -> i32 {
    ask_disks.min(bounded_usize_to_i32(available_disks))
}

fn expand_ask_disks_for_object_quorum(ask_disks: i32, available_disks: usize, object_quorum: usize) -> i32 {
    let Some(asked_disks) = positive_ask_disks(ask_disks) else {
        return ask_disks;
    };

    bounded_usize_to_i32(asked_disks.max(object_quorum).min(available_disks))
}

fn write_quorum_for_drive_count(drive_count: usize, parity_count: usize) -> usize {
    if drive_count == 0 {
        return 0;
    }

    let data_drives = drive_count.saturating_sub(parity_count);
    if data_drives == parity_count {
        data_drives.saturating_add(1)
    } else {
        data_drives
    }
}

fn parse_version_marker(marker: String) -> Result<VersionMarker> {
    Ok(VersionMarker::parse(marker)?)
}

fn version_marker_for_entries(
    entries: Option<&MetaCacheEntriesSorted>,
    key_marker: Option<&str>,
    version_marker: Option<VersionMarker>,
) -> Option<VersionMarker> {
    let marker = version_marker?;
    let Some(key_marker) = key_marker else {
        return Some(marker);
    };

    entries
        .and_then(|entries| entries.entries().first().map(|entry| entry.name.as_str() == key_marker))
        .unwrap_or_default()
        .then_some(marker)
}

impl ListPathOptions {
    pub fn set_filter(&mut self) {
        if METACACHE_SHARE_PREFIX {
            return;
        }
        if self.prefix == self.base_dir {
            return;
        }

        let s = SLASH_SEPARATOR.chars().next().unwrap_or_default();
        self.filter_prefix = {
            let fp = self.prefix.trim_start_matches(&self.base_dir).trim_matches(s);

            if fp.contains(s) || fp.is_empty() {
                None
            } else {
                Some(fp.to_owned())
            }
        }
    }

    pub fn parse_marker(&mut self) {
        let Some(marker) = self.marker.clone() else {
            return;
        };
        let Some(start_idx) = marker.rfind("[rustfs_cache:") else {
            return;
        };
        let Some(end_offset) = marker[start_idx..].rfind(']') else {
            return;
        };

        let end_idx = start_idx + end_offset;
        let tag_body = &marker[start_idx + 1..end_idx];

        let Some((continuation, should_create)) = ListContinuationV2::parse(tag_body) else {
            return;
        };

        self.marker = Some(marker[..start_idx].to_owned());
        self.id = continuation.id;
        self.pool_idx = continuation.pool_idx;
        self.set_idx = continuation.set_idx;
        if should_create {
            self.create = true;
        }
    }

    pub fn encode_marker(&mut self, marker: &str) -> String {
        ListContinuationV2 {
            version: MARKER_TAG_VERSION,
            id: self.id.clone(),
            pool_idx: self.pool_idx,
            set_idx: self.set_idx,
        }
        .encode_marker(marker)
    }
}

impl ECStore {
    #[allow(clippy::too_many_arguments)]
    // @continuation_token marker
    // @start_after as marker when continuation_token empty
    // @delimiter default="/", empty when recursive
    // @max_keys limit
    pub async fn inner_list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        let marker = {
            if continuation_token.is_none() {
                start_after
            } else {
                continuation_token.clone()
            }
        };

        let loi = self
            .list_objects_generic(bucket, prefix, marker, delimiter, max_keys, incl_deleted)
            .await?;
        Ok(ListObjectsV2Info {
            is_truncated: loi.is_truncated,
            continuation_token,
            next_continuation_token: loi.next_marker,
            objects: loi.objects,
            prefixes: loi.prefixes,
        })
    }

    pub async fn list_objects_generic(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        incl_deleted: bool,
    ) -> Result<ListObjectsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            // Always request max_keys + 1 to detect if there are more results
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };

        // Optimization: use get for single object lookup with exact prefix
        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        };

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        // err=None means gather_results filled its limit → disk has more data
        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        // contextCanceled

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);
        let _ = next_version_idmarker;

        Ok(ListObjectsInfo {
            is_truncated,
            next_marker,
            objects,
            prefixes,
        })
    }

    pub async fn inner_list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        if marker.is_none() && version_marker.is_some() {
            return Err(StorageError::NotImplemented);
        }

        let has_version_marker = version_marker.is_some();
        let version_marker = if let Some(marker) = version_marker {
            Some(parse_version_marker(marker)?)
        } else {
            None
        };

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        // Always request max_keys + 1 to detect if there are more results
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted: true,
            ask_disks: list_objects_quorum_from_env(),
            versioned: true,
            include_marker: has_version_marker,
            ..Default::default()
        };

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        // err=None means gather_results filled its limit → disk has more data
        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn list_path(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        // tracing::warn!("list_path opt {:?}", &o);

        let list_path_started = std::time::Instant::now();

        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());

        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true
        }

        o.parse_marker();

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }
        let log_context = ListPathLogContext::from_options(&o);

        // cancel channel
        let cancel = CancellationToken::new();

        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);

        let (sender, recv) = mpsc::channel(o.limit as usize);

        let store = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = store.list_merged(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("store", "list_merged", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();

        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("store", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = {
            // receiver result
            tokio::select! {
               res = err_rx.recv() =>{

                match res{
                    Ok(err) => {
                        log_list_path_worker_error("store", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult{ entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("store", "error_channel_closed", &log_context, &err);

                        MetaCacheEntriesSortedResult{ entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
               },
               Some(result) = result_rx.recv()=>{
                result
               }
            }
        };

        // wait spawns exit
        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("store", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("store", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                }
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "store",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    // Read all
    async fn list_merged(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<Vec<ObjectInfo>> {
        // warn!("list_merged ops {:?}", &opts);
        let merge_started = std::time::Instant::now();

        debug!(
            list_path_limit = opts.limit,
            recursive = opts.recursive,
            pool_count = self.pools.len(),
            requested_marker = %opts.marker.as_deref().unwrap_or(""),
            "store list_merged started"
        );

        let mut futures = Vec::new();

        let mut inputs = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                let (send, recv) = mpsc::channel(100);

                inputs.push(recv);
                let opts = opts.clone();
                let rx_clone = rx.clone();
                futures.push(set.list_path(rx_clone, opts, send));
            }
        }

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, sender.clone(), 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        // let merge_res = merge_entry_channels(rx, inputs, sender.clone(), 1).await;

        // TODO: cancelList

        // let merge_res = merge_entry_channels(rx, inputs, sender.clone(), 1).await;

        let results = join_all(futures).await;

        let mut all_at_eof = true;

        let mut errs = Vec::new();
        for result in results {
            if let Err(err) = result {
                all_at_eof = false;
                errs.push(Some(err));
            } else {
                errs.push(None);
            }
        }

        // All sets returned not-found — the listing is simply empty.
        // We intentionally do NOT distinguish VolumeNotFound here: during
        // listing, a missing volume on a set is equivalent to "no entries"
        // and must not surface as an error to the caller. The original
        // VolumeNotFound check caused spurious errors when a pool had not
        // yet created the bucket prefix on every set.
        if is_all_not_found(&errs) {
            return Ok(Vec::new());
        }

        // merge_res?;

        // TODO check cancel

        for err in errs.iter() {
            if let Some(err) = err {
                if err == &Error::Unexpected {
                    continue;
                }

                return Err(err.clone());
            } else {
                all_at_eof = false;
                continue;
            }
        }

        // check all_at_eof

        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_list_merged",
            merge_started.elapsed().as_secs_f64() * 1000.0,
        );

        debug!(
            set_pair_count = self.pools.len(),
            all_at_eof = all_at_eof,
            error_count = errs.iter().filter(|err| err.is_some()).count(),
            "store list_merged finished"
        );

        _ = all_at_eof;

        Ok(Vec::new())
    }

    #[allow(unused_assignments)]
    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for eset in self.pools.iter() {
            for set in eset.disk_set.iter() {
                let (mut disks, infos, _) = set.get_online_disks_with_healing_and_info(true).await;
                let opts = opts.clone();

                let (sender, list_out_rx) = mpsc::channel::<MetaCacheEntry>(1);
                inputs.push(list_out_rx);
                let rx_clone = rx.clone();
                futures.push(async move {
                    let mut ask_disks = get_list_quorum(&opts.ask_disks, set.set_drive_count as i32);
                    if ask_disks == -1 {
                        let new_disks = get_quorum_disks(&disks, &infos, disks.len().div_ceil(2));
                        if !new_disks.is_empty() {
                            disks = new_disks;
                        } else {
                            ask_disks = get_list_quorum("strict", set.set_drive_count as i32);
                        }
                    }

                    if set.set_drive_count == 4 {
                        ask_disks = bounded_usize_to_i32(disks.len());
                    } else if ask_disks > bounded_usize_to_i32(disks.len()) {
                        ask_disks = clamp_ask_disks_to_available(ask_disks, disks.len());
                    }

                    let listing_quorum = listing_quorum_from_ask_disks(ask_disks);
                    let enforce_write_quorum = enforce_latest_listing_write_quorum(opts.latest_only, &opts.ask_disks);
                    let write_quorum_parity = set.default_parity_count;
                    let required_obj_quorum = latest_listing_required_object_quorum(
                        listing_quorum,
                        set.set_drive_count,
                        write_quorum_parity,
                        enforce_write_quorum,
                    );
                    ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
                    let fallback_disks = {
                        if let Some(asked_disks) = positive_ask_disks(ask_disks)
                            && disks.len() > asked_disks
                        {
                            let mut rand = rand::rng();
                            disks.shuffle(&mut rand);
                            disks.split_off(asked_disks)
                        } else {
                            Vec::new()
                        }
                    };
                    let fallback_disks = Arc::new(fallback_disks);
                    let claim_tracker = FallbackClaimTracker::default();

                    let obj_quorum = latest_listing_object_quorum(
                        listing_quorum,
                        set.set_drive_count,
                        write_quorum_parity,
                        enforce_write_quorum,
                    );
                    let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, obj_quorum, enforce_write_quorum);

                    let resolver =
                        list_metadata_resolution_params(bucket.to_owned(), listing_quorum, obj_quorum, !opts.latest_only);
                    let agreed_resolver = resolver.clone();
                    let partial_resolver = resolver.clone();
                    let reader_disks = disks.len();

                    let path = base_dir_from_prefix(prefix);
                    ensure_non_empty_listing_disks(bucket, &path, &disks)?;

                    let mut filter_prefix = {
                        prefix
                            .trim_start_matches(&path)
                            .trim_start_matches(SLASH_SEPARATOR)
                            .trim_end_matches(SLASH_SEPARATOR)
                            .to_owned()
                    };

                    if filter_prefix == path {
                        filter_prefix = "".to_owned();
                    }

                    // let (sender, rx1) = mpsc::channel(100);

                    let tx1 = sender.clone();
                    let tx2 = sender.clone();
                    let supplement = ListingSupplement::new(
                        ListingSupplementOptions {
                            bucket: bucket.to_owned(),
                            path: path.clone(),
                            recursive: true,
                            filter_prefix: Some(filter_prefix.clone()),
                            forward_to: opts.marker.clone(),
                            per_disk_limit: bounded_usize_to_i32(opts.limit),
                            skip_total_timeout: false,
                        },
                        fallback_disks.clone(),
                        claim_tracker.clone(),
                    );
                    let agreed_supplement = supplement.clone();
                    let partial_supplement = supplement;

                    list_path_raw_with_claim_tracker(
                        rx_clone,
                        ListPathRawOptions {
                            disks: disks.iter().cloned().map(Some).collect(),
                            fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                            bucket: bucket.to_owned(),
                            path,
                            recursive: true,
                            filter_prefix: Some(filter_prefix),
                            forward_to: opts.marker.clone(),
                            min_disks: raw_min_disks,
                            per_disk_limit: bounded_usize_to_i32(opts.limit),
                            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                                Box::pin({
                                    let value = tx1.clone();
                                    let resolver = agreed_resolver.clone();
                                    let supplement = agreed_supplement.clone();
                                    async move {
                                        let entry = match resolve_agreed_listing_entry(
                                            entry,
                                            reader_disks,
                                            resolver.clone(),
                                            enforce_write_quorum,
                                        ) {
                                            ListingEntryResolution::Resolved(entry) => entry,
                                            ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                                let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                                    entry,
                                                    reader_disks,
                                                    resolver,
                                                    enforce_write_quorum,
                                                    supplement,
                                                )
                                                .await
                                                .or(fallback) else {
                                                    return;
                                                };
                                                entry
                                            }
                                            ListingEntryResolution::Rejected => return,
                                        };
                                        if entry.is_dir() {
                                            return;
                                        }
                                        if let Err(err) = value.send(entry).await {
                                            error!("list_path send fail {:?}", err);
                                        }
                                    }
                                })
                            })),
                            partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                                Box::pin({
                                    let value = tx2.clone();
                                    let resolver = partial_resolver.clone();
                                    let supplement = partial_supplement.clone();
                                    async move {
                                        if let Some(entry) = resolve_listing_entries_with_supplement(
                                            entries,
                                            resolver,
                                            enforce_write_quorum,
                                            supplement,
                                        )
                                        .await
                                            && let Err(err) = value.send(entry).await
                                        {
                                            error!("list_path send fail {:?}", err);
                                        }
                                    }
                                })
                            })),
                            finished: None,
                            ..Default::default()
                        },
                        claim_tracker,
                    )
                    .await
                });
            }
        }

        let (merge_tx, mut merge_rx) = mpsc::channel::<MetaCacheEntry>(100);

        let bucket = bucket.to_owned();
        let bucket_clone = bucket.clone();

        let vcf = match get_versioning_config(&bucket).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        tokio::spawn(
            async move {
                let mut sent_err = false;
                while let Some(entry) = merge_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_clone) {
                            Ok(res) => res,
                            Err(err) => {
                                if !sent_err {
                                    let item = ObjectInfoOrErr {
                                        item: None,
                                        err: Some(err.into()),
                                    };

                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }

                                    sent_err = true;

                                    return;
                                }

                                continue;
                            }
                        };

                        if let Some(filter) = opts.filter {
                            if filter(&fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                        continue;
                    }

                    let fvs = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_clone)
                    } else {
                        entry.file_info_versions(&bucket_clone)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                            return;
                        }
                    };

                    if opts.versions_sort == WalkVersionsSortOrder::Ascending {
                        //TODO: SORT
                    }

                    for fi in fvs.versions.iter() {
                        if let Some(filter) = opts.filter {
                            if filter(fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };

                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                    }

                    if opts.include_free_versions {
                        for fi in fvs.free_versions.iter() {
                            if let Some(filter) = opts.filter {
                                if filter(fi) {
                                    let item = ObjectInfoOrErr {
                                        item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                            if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                        })),
                                        err: None,
                                    };

                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                }
                            } else {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };

                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, merge_tx, 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        let walk_started = std::time::Instant::now();
        let walk_results = join_all(futures).await;
        let mut errs = Vec::new();
        for walk_result in walk_results {
            match walk_result {
                Ok(()) => errs.push(None),
                Err(err) => errs.push(Some(err.into())),
            }
        }
        rustfs_io_metrics::record_stage_duration(
            "store_list_objects_walk_internal",
            walk_started.elapsed().as_secs_f64() * 1000.0,
        );

        let result = walk_result_from_set_errors(&errs);
        if let Err(err) = &result {
            error!(
                bucket = %bucket,
                prefix = %prefix,
                error = ?err,
                set_errors = ?errs,
                "walk_internal list_path_raw tasks failed"
            );
        }

        result
    }
}

async fn gather_results(
    rx: CancellationToken,
    opts: ListPathOptions,
    recv: Receiver<MetaCacheEntry>,
    results_tx: Sender<MetaCacheEntriesSortedResult>,
) -> Result<GatherResultsState> {
    let mut recv = recv;
    let mut entries = Vec::new();
    let gather_started = std::time::Instant::now();
    let mut scanned_entries = 0usize;
    let mut candidate_entries = 0usize;

    while let Some(mut entry) = recv.recv().await {
        scanned_entries += 1;
        #[cfg(windows)]
        {
            // normalize windows path separator
            entry.name = entry.name.replace("\\", "/");
        }

        // TODO: rx.recv()

        if let Some(marker) = &opts.marker
            && ((!opts.include_marker && &entry.name <= marker) || (opts.include_marker && &entry.name < marker))
        {
            continue;
        }

        if !entry.name.starts_with(&opts.prefix) {
            continue;
        }

        if let Some(separator) = &opts.separator
            && !opts.recursive
            && !entry.is_in_dir(&opts.prefix, separator)
        {
            continue;
        }

        let is_object = entry.is_object();
        let is_latest_delete_marker = is_object && entry.is_latest_delete_marker();

        if !opts.include_directories && (entry.is_dir() || (!opts.versioned && is_latest_delete_marker)) {
            continue;
        }

        if !opts.incl_deleted && is_latest_delete_marker && !entry.is_object_dir() {
            continue;
        }

        // TODO: Lifecycle

        entries.push(Some(entry));
        candidate_entries += 1;

        if opts.limit > 0 && entries.len() >= opts.limit as usize {
            rx.cancel();
            let filtered = scanned_entries.saturating_sub(candidate_entries);
            rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
                source: LIST_OBJECTS_SOURCE_WALKER,
                outcome: LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED,
                limit: opts.limit,
                scanned_entries,
                returned_entries: candidate_entries,
                duration_ms: gather_started.elapsed().as_secs_f64() * 1000.0,
                has_prefix: !opts.prefix.is_empty(),
                has_delimiter: opts.separator.as_ref().is_some_and(|separator| !separator.is_empty()),
                has_marker: opts.marker.is_some(),
            });
            debug!(
                bucket = %opts.bucket,
                prefix = %opts.prefix,
                limit = opts.limit,
                scanned_entries = scanned_entries,
                returned_entries = candidate_entries,
                filtered_entries = filtered,
                marker = %opts.marker.as_deref().unwrap_or(""),
                "list_objects gather_results reached page limit and cancelled upstream listing"
            );
            rustfs_io_metrics::record_stage_duration(
                "store_list_objects_gather",
                gather_started.elapsed().as_secs_f64() * 1000.0,
            );

            results_tx
                .send(MetaCacheEntriesSortedResult {
                    entries: Some(MetaCacheEntriesSorted {
                        o: MetaCacheEntries(entries),
                        ..Default::default()
                    }),
                    err: None,
                })
                .await
                .map_err(Error::other)?;
            return Ok(GatherResultsState::LimitReached);
        }
    }

    // finish not full, return eof
    let filtered = scanned_entries.saturating_sub(candidate_entries);
    rustfs_io_metrics::record_list_objects_gather(ListObjectsGatherObservation {
        source: LIST_OBJECTS_SOURCE_WALKER,
        outcome: LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED,
        limit: opts.limit,
        scanned_entries,
        returned_entries: candidate_entries,
        duration_ms: gather_started.elapsed().as_secs_f64() * 1000.0,
        has_prefix: !opts.prefix.is_empty(),
        has_delimiter: opts.separator.as_ref().is_some_and(|separator| !separator.is_empty()),
        has_marker: opts.marker.is_some(),
    });
    debug!(
        bucket = %opts.bucket,
        prefix = %opts.prefix,
        scanned_entries = scanned_entries,
        returned_entries = candidate_entries,
        filtered_entries = filtered,
        marker = %opts.marker.as_deref().unwrap_or(""),
        "list_objects gather_results drained all candidates without hitting limit"
    );
    rustfs_io_metrics::record_stage_duration("store_list_objects_gather", gather_started.elapsed().as_secs_f64() * 1000.0);

    results_tx
        .send(MetaCacheEntriesSortedResult {
            entries: Some(MetaCacheEntriesSorted {
                o: MetaCacheEntries(entries),
                ..Default::default()
            }),
            err: Some(Error::Unexpected.into()),
        })
        .await
        .map_err(Error::other)?;

    Ok(GatherResultsState::InputClosed)
}

async fn select_from(
    rx: &CancellationToken,
    in_channels: &mut [Receiver<MetaCacheEntry>],
    idx: usize,
    top: &mut [Option<MetaCacheEntry>],
    n_done: &mut usize,
) -> Result<bool> {
    let entry = tokio::select! {
        entry = in_channels[idx].recv() => entry,
        _ = rx.cancelled() => return Ok(false),
    };

    match entry {
        Some(entry) => {
            top[idx] = Some(entry);
        }
        None => {
            top[idx] = None;
            *n_done += 1;
        }
    }
    Ok(true)
}

async fn send_or_cancel(rx: &CancellationToken, out_channel: &Sender<MetaCacheEntry>, entry: MetaCacheEntry) -> Result<bool> {
    tokio::select! {
        result = out_channel.send(entry) => {
            result.map_err(Error::other)?;
            Ok(true)
        }
        _ = rx.cancelled() => Ok(false),
    }
}

async fn merge_entry_channels(
    rx: CancellationToken,
    in_channels: Vec<Receiver<MetaCacheEntry>>,
    out_channel: Sender<MetaCacheEntry>,
    read_quorum: usize,
) -> Result<()> {
    if in_channels.is_empty() {
        return Ok(());
    }

    rustfs_io_metrics::record_list_objects_merge(LIST_OBJECTS_SOURCE_WALKER, in_channels.len(), read_quorum);

    let mut in_channels = in_channels;
    if in_channels.len() == 1 {
        loop {
            tokio::select! {
                has_entry = in_channels[0].recv()=>{
                    if let Some(entry) = has_entry{
                        // warn!("merge_entry_channels entry {}", &entry.name);
                        if !send_or_cancel(&rx, &out_channel, entry).await? {
                            return Ok(());
                        }
                    } else {
                        return Ok(())
                    }
                },
                _ = rx.cancelled()=>{
                    info!("merge_entry_channels rx.recv() cancel");
                    return Ok(())
                },
            }
        }
    }

    let mut top: Vec<Option<MetaCacheEntry>> = vec![None; in_channels.len()];
    let mut n_done = 0;

    let in_channels_len = in_channels.len();

    for idx in 0..in_channels_len {
        if !select_from(&rx, &mut in_channels, idx, &mut top, &mut n_done).await? {
            return Ok(());
        }
    }

    let mut last = String::new();
    let mut to_merge: Vec<usize> = Vec::new();
    loop {
        if n_done == in_channels.len() {
            return Ok(());
        }

        let mut best = top[0].clone();
        let mut best_idx = 0;
        to_merge.clear();

        // Note: `select_from` mutates `top[idx]` during the inner loop, but this is safe
        // because each borrow from `top[other_idx]` is only used before any later
        // `select_from` call that can mutate that slot.

        for other_idx in 1..top.len() {
            if let Some(other_entry) = &top[other_idx] {
                if let Some(best_entry) = &best {
                    if path::clean(&best_entry.name) == path::clean(&other_entry.name) {
                        let dir_matches = best_entry.is_dir() && other_entry.is_dir();
                        let suffix_matches =
                            best_entry.name.ends_with(SLASH_SEPARATOR) == other_entry.name.ends_with(SLASH_SEPARATOR);

                        if dir_matches && suffix_matches {
                            to_merge.push(other_idx);
                            continue;
                        }

                        if !dir_matches {
                            // dir and object has the save name
                            if other_entry.is_dir() {
                                // TODO: read next entry to top
                                if !select_from(&rx, &mut in_channels, other_idx, &mut top, &mut n_done).await? {
                                    return Ok(());
                                }
                                continue;
                            }

                            to_merge.clear();

                            best = Some(other_entry.clone());
                            best_idx = other_idx;
                            continue;
                        }
                    }

                    if best_entry.name > other_entry.name {
                        to_merge.clear();
                        best = Some(other_entry.clone());
                        best_idx = other_idx;
                    }
                } else {
                    best = Some(other_entry.clone());
                    best_idx = other_idx;
                }
            }
        }

        if !to_merge.is_empty() {
            if let Some(entry) = &best {
                let mut versions = Vec::with_capacity(to_merge.len() + 1);

                let mut has_xl = { entry.clone().xl_meta().ok() };

                if let Some(x) = &has_xl {
                    versions.push(x.versions.clone());
                }

                for &idx in to_merge.iter() {
                    let has_entry = top[idx].clone();

                    if let Some(entry) = has_entry {
                        let xl2 = match entry.clone().xl_meta() {
                            Ok(res) => res,
                            Err(_) => {
                                if !select_from(&rx, &mut in_channels, idx, &mut top, &mut n_done).await? {
                                    return Ok(());
                                }

                                continue;
                            }
                        };

                        versions.push(xl2.versions.clone());

                        if has_xl.is_none() {
                            if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
                                return Ok(());
                            }

                            best_idx = idx;
                            best = Some(entry.clone());
                            has_xl = Some(xl2);
                        } else {
                            if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
                                return Ok(());
                            }
                        }
                    }
                }

                if let Some(xl) = has_xl.as_mut()
                    && !versions.is_empty()
                {
                    xl.versions = merge_file_meta_versions(read_quorum, true, 0, &versions);

                    if let Ok(meta) = xl.marshal_msg()
                        && let Some(b) = best.as_mut()
                    {
                        b.metadata = meta;
                        b.cached = Some(xl.clone());
                    }
                }
            }

            to_merge.clear();
        }

        if let Some(best_entry) = &best
            && best_entry.name > last
        {
            if !send_or_cancel(&rx, &out_channel, best_entry.clone()).await? {
                return Ok(());
            }
            last = best_entry.name.clone();
        }

        if !select_from(&rx, &mut in_channels, best_idx, &mut top, &mut n_done).await? {
            return Ok(());
        }
    }
}

impl Sets {
    #[allow(clippy::too_many_arguments)]
    pub async fn inner_list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        let marker = if continuation_token.is_none() {
            start_after
        } else {
            continuation_token.clone()
        };

        let loi = self
            .list_objects_generic(bucket, prefix, marker, delimiter, max_keys, incl_deleted)
            .await?;
        Ok(ListObjectsV2Info {
            is_truncated: loi.is_truncated,
            continuation_token,
            next_continuation_token: loi.next_marker,
            objects: loi.objects,
            prefixes: loi.prefixes,
        })
    }

    pub async fn list_objects_generic(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        incl_deleted: bool,
    ) -> Result<ListObjectsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };

        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        }

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);
        let _ = next_version_idmarker;

        Ok(ListObjectsInfo {
            is_truncated,
            next_marker,
            objects,
            prefixes,
        })
    }

    pub async fn inner_list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        if marker.is_none() && version_marker.is_some() {
            return Err(StorageError::NotImplemented);
        }

        let has_version_marker = version_marker.is_some();
        let version_marker = if let Some(marker) = version_marker {
            Some(parse_version_marker(marker)?)
        } else {
            None
        };

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted: true,
            ask_disks: list_objects_quorum_from_env(),
            versioned: true,
            include_marker: has_version_marker,
            ..Default::default()
        };

        let mut list_result = self
            .list_path(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn list_path(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        let list_path_started = std::time::Instant::now();

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());
        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true;
        }

        o.parse_marker();

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }
        let log_context = ListPathLogContext::from_options(&o);

        let cancel = CancellationToken::new();
        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);
        let (sender, recv) = mpsc::channel(o.limit as usize);

        let sets = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = sets.list_merged(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("sets", "list_merged", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("sets", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = tokio::select! {
            res = err_rx.recv() => {
                match res {
                    Ok(err) => {
                        log_list_path_worker_error("sets", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult { entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("sets", "error_channel_closed", &log_context, &err);
                        MetaCacheEntriesSortedResult { entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
            }
            Some(result) = result_rx.recv() => result,
        };

        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("sets", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("sets", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                };
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        log_list_path_finished(
            "sets",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    async fn list_merged(
        &self,
        rx: CancellationToken,
        opts: ListPathOptions,
        sender: Sender<MetaCacheEntry>,
    ) -> Result<Vec<ObjectInfo>> {
        let merge_started = std::time::Instant::now();

        debug!(
            list_path_limit = opts.limit,
            recursive = opts.recursive,
            set_count = self.disk_set.len(),
            requested_marker = %opts.marker.as_deref().unwrap_or(""),
            "sets list_merged started"
        );

        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for set in &self.disk_set {
            let (send, recv) = mpsc::channel(100);
            inputs.push(recv);
            let opts = opts.clone();
            let rx_clone = rx.clone();
            let set = set.clone();
            futures.push(async move { set.list_path(rx_clone, opts, send).await });
        }

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, sender.clone(), 1).await {
                    error!("merge_entry_channels err {:?}", err);
                }
            }
            .instrument(tracing::Span::current()),
        );

        let results = join_all(futures).await;
        let mut all_at_eof = true;
        let mut errs = Vec::new();
        for result in results {
            if let Err(err) = result {
                all_at_eof = false;
                errs.push(Some(err));
            } else {
                errs.push(None);
            }
        }

        if is_all_not_found(&errs) {
            return Ok(Vec::new());
        }

        for err in &errs {
            if let Some(err) = err {
                if err == &Error::Unexpected {
                    continue;
                }
                return Err(err.clone());
            } else {
                all_at_eof = false;
            }
        }

        rustfs_io_metrics::record_stage_duration("sets_list_objects_list_merged", merge_started.elapsed().as_secs_f64() * 1000.0);

        debug!(
            set_count = self.disk_set.len(),
            all_at_eof = all_at_eof,
            error_count = errs.iter().filter(|err| err.is_some()).count(),
            "sets list_merged finished"
        );

        _ = all_at_eof;
        Ok(Vec::new())
    }

    #[allow(unused_assignments)]
    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let mut futures = Vec::new();
        let mut inputs = Vec::new();

        for set in &self.disk_set {
            let (mut disks, infos, _) = set.get_online_disks_with_healing_and_info(true).await;
            let opts = opts.clone();
            let (sender, list_out_rx) = mpsc::channel::<MetaCacheEntry>(1);
            inputs.push(list_out_rx);
            let rx_clone = rx.clone();
            let set = set.clone();
            futures.push(async move {
                let mut ask_disks = get_list_quorum(&opts.ask_disks, set.set_drive_count as i32);
                if ask_disks == -1 {
                    let new_disks = get_quorum_disks(&disks, &infos, disks.len().div_ceil(2));
                    if !new_disks.is_empty() {
                        disks = new_disks;
                    } else {
                        ask_disks = get_list_quorum("strict", set.set_drive_count as i32);
                    }
                }

                if set.set_drive_count == 4 {
                    ask_disks = bounded_usize_to_i32(disks.len());
                } else if ask_disks > bounded_usize_to_i32(disks.len()) {
                    ask_disks = clamp_ask_disks_to_available(ask_disks, disks.len());
                }

                let listing_quorum = listing_quorum_from_ask_disks(ask_disks);
                let enforce_write_quorum = enforce_latest_listing_write_quorum(opts.latest_only, &opts.ask_disks);
                let write_quorum_parity = set.default_parity_count;
                let required_obj_quorum = latest_listing_required_object_quorum(
                    listing_quorum,
                    set.set_drive_count,
                    write_quorum_parity,
                    enforce_write_quorum,
                );
                ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
                let fallback_disks = if let Some(asked_disks) = positive_ask_disks(ask_disks)
                    && disks.len() > asked_disks
                {
                    let mut rand = rand::rng();
                    disks.shuffle(&mut rand);
                    disks.split_off(asked_disks)
                } else {
                    Vec::new()
                };
                let fallback_disks = Arc::new(fallback_disks);
                let claim_tracker = FallbackClaimTracker::default();

                let obj_quorum =
                    latest_listing_object_quorum(listing_quorum, set.set_drive_count, write_quorum_parity, enforce_write_quorum);
                let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, obj_quorum, enforce_write_quorum);
                let resolver = list_metadata_resolution_params(bucket.to_owned(), listing_quorum, obj_quorum, !opts.latest_only);
                let agreed_resolver = resolver.clone();
                let partial_resolver = resolver.clone();
                let reader_disks = disks.len();

                let path = base_dir_from_prefix(prefix);
                ensure_non_empty_listing_disks(bucket, &path, &disks)?;

                let mut filter_prefix = prefix
                    .trim_start_matches(&path)
                    .trim_start_matches(SLASH_SEPARATOR)
                    .trim_end_matches(SLASH_SEPARATOR)
                    .to_owned();
                if filter_prefix == path {
                    filter_prefix = "".to_owned();
                }

                let tx1 = sender.clone();
                let tx2 = sender.clone();
                let supplement = ListingSupplement::new(
                    ListingSupplementOptions {
                        bucket: bucket.to_owned(),
                        path: path.clone(),
                        recursive: true,
                        filter_prefix: Some(filter_prefix.clone()),
                        forward_to: opts.marker.clone(),
                        per_disk_limit: bounded_usize_to_i32(opts.limit),
                        skip_total_timeout: false,
                    },
                    fallback_disks.clone(),
                    claim_tracker.clone(),
                );
                let agreed_supplement = supplement.clone();
                let partial_supplement = supplement;

                list_path_raw_with_claim_tracker(
                    rx_clone,
                    ListPathRawOptions {
                        disks: disks.iter().cloned().map(Some).collect(),
                        fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                        bucket: bucket.to_owned(),
                        path,
                        recursive: true,
                        filter_prefix: Some(filter_prefix),
                        forward_to: opts.marker.clone(),
                        min_disks: raw_min_disks,
                        per_disk_limit: bounded_usize_to_i32(opts.limit),
                        agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                            Box::pin({
                                let value = tx1.clone();
                                let resolver = agreed_resolver.clone();
                                let supplement = agreed_supplement.clone();
                                async move {
                                    let entry = match resolve_agreed_listing_entry(
                                        entry,
                                        reader_disks,
                                        resolver.clone(),
                                        enforce_write_quorum,
                                    ) {
                                        ListingEntryResolution::Resolved(entry) => entry,
                                        ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                            let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                                entry,
                                                reader_disks,
                                                resolver,
                                                enforce_write_quorum,
                                                supplement,
                                            )
                                            .await
                                            .or(fallback) else {
                                                return;
                                            };
                                            entry
                                        }
                                        ListingEntryResolution::Rejected => return,
                                    };
                                    if entry.is_dir() {
                                        return;
                                    }
                                    if let Err(err) = value.send(entry).await {
                                        error!("list_path send fail {:?}", err);
                                    }
                                }
                            })
                        })),
                        partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                            Box::pin({
                                let value = tx2.clone();
                                let resolver = partial_resolver.clone();
                                let supplement = partial_supplement.clone();
                                async move {
                                    if let Some(entry) = resolve_listing_entries_with_supplement(
                                        entries,
                                        resolver,
                                        enforce_write_quorum,
                                        supplement,
                                    )
                                    .await
                                        && let Err(err) = value.send(entry).await
                                    {
                                        error!("list_path send fail {:?}", err);
                                    }
                                }
                            })
                        })),
                        finished: None,
                        ..Default::default()
                    },
                    claim_tracker,
                )
                .await
            });
        }

        let (merge_tx, mut merge_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let bucket = bucket.to_owned();
        let bucket_clone = bucket.clone();

        let vcf = match get_versioning_config(&bucket).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        tokio::spawn(
            async move {
                let mut sent_err = false;
                while let Some(entry) = merge_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_clone) {
                            Ok(res) => res,
                            Err(err) => {
                                if !sent_err {
                                    let item = ObjectInfoOrErr {
                                        item: None,
                                        err: Some(err.into()),
                                    };
                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                    sent_err = true;
                                    return;
                                }
                                continue;
                            }
                        };

                        if let Some(filter) = opts.filter {
                            if filter(&fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(&fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                        continue;
                    }

                    let fvs = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_clone)
                    } else {
                        entry.file_info_versions(&bucket_clone)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                            return;
                        }
                    };

                    for fi in &fvs.versions {
                        if let Some(filter) = opts.filter {
                            if filter(fi) {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        } else {
                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                    if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                })),
                                err: None,
                            };
                            if let Err(err) = result.send(item).await {
                                error!("walk result send err {:?}", err);
                            }
                        }
                    }

                    if opts.include_free_versions {
                        for fi in &fvs.free_versions {
                            if let Some(filter) = opts.filter {
                                if filter(fi) {
                                    let item = ObjectInfoOrErr {
                                        item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                            if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                        })),
                                        err: None,
                                    };
                                    if let Err(err) = result.send(item).await {
                                        error!("walk result send err {:?}", err);
                                    }
                                }
                            } else {
                                let item = ObjectInfoOrErr {
                                    item: Some(ObjectInfo::from_file_info(fi, &bucket_clone, &fi.name, {
                                        if let Some(v) = &vcf { v.versioned(&fi.name) } else { false }
                                    })),
                                    err: None,
                                };
                                if let Err(err) = result.send(item).await {
                                    error!("walk result send err {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        tokio::spawn(
            async move {
                if let Err(err) = merge_entry_channels(rx, inputs, merge_tx, 1).await {
                    error!("merge_entry_channels err {:?}", err)
                }
            }
            .instrument(tracing::Span::current()),
        );

        let walk_started = std::time::Instant::now();
        let walk_results = join_all(futures).await;
        let mut errs = Vec::new();
        for walk_result in walk_results {
            match walk_result {
                Ok(()) => errs.push(None),
                Err(err) => errs.push(Some(err.into())),
            }
        }
        rustfs_io_metrics::record_stage_duration(
            "sets_list_objects_walk_internal",
            walk_started.elapsed().as_secs_f64() * 1000.0,
        );

        let result = walk_result_from_set_errors(&errs);
        if let Err(err) = &result {
            error!(
                bucket = %bucket,
                prefix = %prefix,
                error = ?err,
                set_errors = ?errs,
                "walk_internal list_path_raw tasks failed"
            );
        }

        result
    }
}

impl SetDisks {
    #[allow(clippy::too_many_arguments)]
    pub async fn inner_list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<ListObjectsV2Info> {
        let marker = if continuation_token.is_none() {
            start_after
        } else {
            continuation_token.clone()
        };

        let loi = self
            .list_objects_generic(bucket, prefix, marker, delimiter, max_keys, incl_deleted)
            .await?;
        Ok(ListObjectsV2Info {
            is_truncated: loi.is_truncated,
            continuation_token,
            next_continuation_token: loi.next_marker,
            objects: loi.objects,
            prefixes: loi.prefixes,
        })
    }

    pub async fn list_objects_generic(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        incl_deleted: bool,
    ) -> Result<ListObjectsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted,
            ask_disks: list_objects_quorum_from_env(),
            ..Default::default()
        };

        if !opts.prefix.is_empty() && max_keys == 1 && opts.marker.is_none() {
            match self
                .get_object_info(
                    &opts.bucket,
                    &opts.prefix,
                    &ObjectOptions {
                        no_lock: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(res) => {
                    return Ok(ListObjectsInfo {
                        objects: vec![res],
                        ..Default::default()
                    });
                }
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(err);
                    }
                }
            };
        }

        let mut list_result = self
            .list_path_result(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, _next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), false);

        Ok(ListObjectsInfo {
            is_truncated,
            next_marker,
            objects,
            prefixes,
        })
    }

    pub async fn inner_list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo> {
        let max_keys = normalize_max_keys(max_keys);
        if marker.is_none() && version_marker.is_some() {
            return Err(StorageError::NotImplemented);
        }

        let has_version_marker = version_marker.is_some();
        let version_marker = if let Some(marker) = version_marker {
            Some(parse_version_marker(marker)?)
        } else {
            None
        };

        let effective_max_keys = if max_keys <= 0 { 0 } else { max_keys_plus_one(max_keys, true) };
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: effective_max_keys,
            marker,
            incl_deleted: true,
            ask_disks: list_objects_quorum_from_env(),
            versioned: true,
            include_marker: has_version_marker,
            ..Default::default()
        };

        let mut list_result = self
            .list_path_result(&opts)
            .await
            .unwrap_or_else(|err| MetaCacheEntriesSortedResult {
                err: Some(err.into()),
                ..Default::default()
            });
        let next_cache_id = list_result.entries.as_ref().and_then(|entries| entries.list_id.clone());

        let disk_has_more = list_result.err.is_none();

        if let Some(err) = list_result.err.take()
            && err != rustfs_filemeta::Error::Unexpected
        {
            return Err(to_object_err(err.into(), vec![bucket, prefix]));
        }

        if let Some(result) = list_result.entries.as_mut()
            && !has_version_marker
        {
            result.forward_past(opts.marker.clone());
        }

        let version_marker = version_marker_for_entries(list_result.entries.as_ref(), opts.marker.as_deref(), version_marker);

        let get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        let (objects, prefixes, is_truncated, next_marker, next_version_idmarker) =
            list_objects_paginate(get_objects, &delimiter, max_keys, disk_has_more, next_cache_id.as_deref(), true);

        Ok(ListObjectVersionsInfo {
            is_truncated,
            next_marker,
            next_version_idmarker,
            objects,
            prefixes,
        })
    }

    pub async fn walk_internal(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        check_list_objs_args(bucket, prefix, &None)?;

        let (entry_tx, mut entry_rx) = mpsc::channel::<MetaCacheEntry>(100);
        let bucket_name = bucket.to_owned();
        let bucket_name_for_list = bucket_name.clone();

        let versioning_config = match get_versioning_config(&bucket_name).await {
            Ok((res, _)) => Some(res),
            Err(_) => None,
        };

        let result_task = tokio::spawn(
            async move {
                while let Some(entry) = entry_rx.recv().await {
                    if opts.latest_only {
                        let fi = match entry.to_fileinfo(&bucket_name) {
                            Ok(res) => res,
                            Err(err) => {
                                let item = ObjectInfoOrErr {
                                    item: None,
                                    err: Some(err.into()),
                                };
                                let _ = result.send(item).await;
                                return;
                            }
                        };

                        if let Some(filter) = opts.filter
                            && !filter(&fi)
                        {
                            continue;
                        }

                        let item = ObjectInfoOrErr {
                            item: Some(ObjectInfo::from_file_info(&fi, &bucket_name, &fi.name, {
                                if let Some(v) = &versioning_config {
                                    v.versioned(&fi.name)
                                } else {
                                    false
                                }
                            })),
                            err: None,
                        };
                        let _ = result.send(item).await;
                        continue;
                    }

                    let versions = match if opts.include_free_versions {
                        entry.file_info_versions_with_free_versions(&bucket_name)
                    } else {
                        entry.file_info_versions(&bucket_name)
                    } {
                        Ok(res) => res,
                        Err(err) => {
                            let item = ObjectInfoOrErr {
                                item: None,
                                err: Some(err.into()),
                            };
                            let _ = result.send(item).await;
                            return;
                        }
                    };

                    for fi in &versions.versions {
                        if let Some(filter) = opts.filter
                            && !filter(fi)
                        {
                            continue;
                        }

                        let item = ObjectInfoOrErr {
                            item: Some(ObjectInfo::from_file_info(fi, &bucket_name, &fi.name, {
                                if let Some(v) = &versioning_config {
                                    v.versioned(&fi.name)
                                } else {
                                    false
                                }
                            })),
                            err: None,
                        };
                        let _ = result.send(item).await;
                    }

                    if opts.include_free_versions {
                        for fi in &versions.free_versions {
                            if let Some(filter) = opts.filter
                                && !filter(fi)
                            {
                                continue;
                            }

                            let item = ObjectInfoOrErr {
                                item: Some(ObjectInfo::from_file_info(fi, &bucket_name, &fi.name, {
                                    if let Some(v) = &versioning_config {
                                        v.versioned(&fi.name)
                                    } else {
                                        false
                                    }
                                })),
                                err: None,
                            };
                            let _ = result.send(item).await;
                        }
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let limit = i32::try_from(opts.limit).unwrap_or(i32::MAX);
        let list_result = self
            .list_path(
                rx,
                ListPathOptions {
                    bucket: bucket_name_for_list,
                    prefix: prefix.to_owned(),
                    marker: opts.marker.clone(),
                    limit,
                    ask_disks: opts.ask_disks.clone(),
                    incl_deleted: true,
                    recursive: true,
                    versioned: true,
                    ..Default::default()
                },
                entry_tx,
            )
            .await;

        let _ = result_task.await;

        match list_result {
            Ok(()) => Ok(()),
            Err(err) => walk_result_from_set_errors(&[Some(err)]),
        }
    }

    pub async fn list_path_result(self: Arc<Self>, o: &ListPathOptions) -> Result<MetaCacheEntriesSortedResult> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;

        let list_path_started = std::time::Instant::now();

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker
            && !o.prefix.is_empty()
            && !marker.starts_with(&o.prefix)
        {
            return Err(Error::Unexpected);
        }

        if o.limit == 0 {
            return Err(Error::Unexpected);
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::Unexpected);
        }

        let slash_separator = Some(SLASH_SEPARATOR.to_owned());
        o.include_directories = o.separator == slash_separator;

        if (o.separator == slash_separator || o.separator.is_none()) && !o.recursive {
            o.recursive = o.separator != slash_separator;
            o.separator = slash_separator;
        } else {
            o.recursive = true;
        }

        o.parse_marker();

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }
        let log_context = ListPathLogContext::from_options(&o);

        let cancel = CancellationToken::new();
        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);
        let (sender, recv) = mpsc::channel(o.limit as usize);

        let set = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1_context = log_context.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = set.list_path(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    log_list_path_worker_error("set_disks", "list_path", &job1_context, &err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2_context = log_context.clone();
        let job2 = tokio::spawn(
            async move {
                match gather_results(cancel_rx2, opts, recv, result_tx).await {
                    Ok(GatherResultsState::LimitReached) => cancel.cancel(),
                    Ok(GatherResultsState::InputClosed) => {}
                    Err(err) => {
                        log_list_path_worker_error("set_disks", "gather_results", &job2_context, &err);
                        let _ = err_tx2.send(Arc::new(err));
                        cancel.cancel();
                    }
                }
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = tokio::select! {
            res = err_rx.recv() => {
                match res {
                    Ok(err) => {
                        log_list_path_worker_error("set_disks", "worker_error", &log_context, err.as_ref());
                        MetaCacheEntriesSortedResult { entries: None, err: Some(err.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        log_list_path_worker_error("set_disks", "error_channel_closed", &log_context, &err);
                        MetaCacheEntriesSortedResult { entries: None, err: Some(rustfs_filemeta::Error::other(err)) }
                    },
                }
            }
            Some(result) = result_rx.recv() => result,
        };

        join_all(vec![job1, job2]).await;

        if let Ok(err) = err_rx.try_recv() {
            log_list_path_worker_error("set_disks", "trailing_worker_error", &log_context, err.as_ref());
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
            log_list_path_finished("set_disks", &log_context, list_path_started.elapsed().as_secs_f64() * 1000.0, 0, true);
            return Ok(result);
        }

        if let Some(entries) = result.entries.as_mut() {
            entries.reuse = true;
            let truncated = !entries.entries().is_empty() || result.err.is_none();
            entries.o.0.truncate(o.limit as usize);
            if !o.transient && truncated {
                entries.list_id = if let Some(id) = o.id {
                    Some(id)
                } else {
                    Some(Uuid::new_v4().to_string())
                };
            }

            if !truncated {
                result.err = Some(Error::Unexpected.into());
            }
        }

        rustfs_io_metrics::record_stage_duration(
            "sets_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        log_list_path_finished(
            "set_disks",
            &log_context,
            list_path_started.elapsed().as_secs_f64() * 1000.0,
            result
                .entries
                .as_ref()
                .map(|entries| entries.entries().len())
                .unwrap_or_default(),
            result.err.is_some(),
        );

        Ok(result)
    }

    pub async fn list_path(&self, rx: CancellationToken, opts: ListPathOptions, sender: Sender<MetaCacheEntry>) -> Result<()> {
        let list_path_started = std::time::Instant::now();

        let (mut disks, infos, _) = self.get_online_disks_with_healing_and_info(true).await;

        let mut ask_disks = get_list_quorum(&opts.ask_disks, self.set_drive_count as i32);
        if ask_disks == -1 {
            let new_disks = get_quorum_disks(&disks, &infos, disks.len().div_ceil(2));
            if !new_disks.is_empty() {
                disks = new_disks;
                ask_disks = 1;
            } else {
                ask_disks = get_list_quorum("strict", self.set_drive_count as i32);
            }
        }

        if self.set_drive_count == 4 {
            ask_disks = bounded_usize_to_i32(disks.len());
        } else if ask_disks > bounded_usize_to_i32(disks.len()) {
            ask_disks = clamp_ask_disks_to_available(ask_disks, disks.len());
        }

        let listing_quorum = listing_quorum_from_ask_disks(ask_disks);
        ensure_non_empty_listing_disks(&opts.bucket, &opts.base_dir, &disks)?;

        let enforce_write_quorum = enforce_latest_listing_write_quorum(!opts.versioned, &opts.ask_disks);
        let write_quorum_parity = self.default_parity_count;
        let required_obj_quorum = latest_listing_required_object_quorum(
            listing_quorum,
            self.set_drive_count,
            write_quorum_parity,
            enforce_write_quorum,
        );
        ask_disks = expand_ask_disks_for_object_quorum(ask_disks, disks.len(), required_obj_quorum);
        let mut fallback_disks = Vec::new();

        if let Some(asked_disks) = positive_ask_disks(ask_disks)
            && disks.len() > asked_disks
        {
            let mut rand = rand::rng();
            disks.shuffle(&mut rand);

            fallback_disks = disks.split_off(asked_disks);
        }
        let fallback_disks = Arc::new(fallback_disks);
        let claim_tracker = FallbackClaimTracker::default();

        let bucket = opts.bucket.clone();
        let base_dir = opts.base_dir.clone();
        let latest_object_quorum =
            latest_listing_object_quorum(listing_quorum, self.set_drive_count, write_quorum_parity, enforce_write_quorum);
        let raw_min_disks = latest_listing_raw_min_disks(listing_quorum, latest_object_quorum, enforce_write_quorum);
        let resolver = list_metadata_resolution_params(bucket.clone(), listing_quorum, latest_object_quorum, opts.versioned);
        let agreed_resolver = resolver.clone();
        let partial_resolver = resolver.clone();
        let reader_disks = disks.len();

        debug!(
            bucket = %bucket,
            prefix = %base_dir,
            set_drive_count = self.set_drive_count,
            asked_disks = ask_disks,
            listing_quorum = listing_quorum,
            latest_object_quorum = latest_object_quorum,
            raw_min_disks = raw_min_disks,
            fallback_disks = fallback_disks.len(),
            limit = opts.limit,
            stop_disk_at_limit = opts.stop_disk_at_limit,
            "set_disks list_path selected listing quorum and fallback disks"
        );

        let limit = {
            if opts.limit > 0 && opts.stop_disk_at_limit {
                opts.limit + 4 + (opts.limit / 16)
            } else {
                0
            }
        };

        let tx1 = sender.clone();
        let tx2 = sender.clone();
        let cancel_for_send1 = rx.clone();
        let cancel_for_send2 = rx.clone();
        let supplement = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: bucket.clone(),
                path: opts.base_dir.clone(),
                recursive: opts.recursive,
                filter_prefix: opts.filter_prefix.clone(),
                forward_to: opts.marker.clone(),
                per_disk_limit: limit,
                skip_total_timeout: false,
            },
            fallback_disks.clone(),
            claim_tracker.clone(),
        );
        let agreed_supplement = supplement.clone();
        let partial_supplement = supplement;

        let result = list_path_raw_with_claim_tracker(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursive: opts.recursive,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: raw_min_disks,
                per_disk_limit: limit,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin({
                        let value = tx1.clone();
                        let cancel_token = cancel_for_send1.clone();
                        let resolver = agreed_resolver.clone();
                        let supplement = agreed_supplement.clone();
                        async move {
                            if cancel_token.is_cancelled() {
                                return;
                            }

                            let entry =
                                match resolve_agreed_listing_entry(entry, reader_disks, resolver.clone(), enforce_write_quorum) {
                                    ListingEntryResolution::Resolved(entry) => entry,
                                    ListingEntryResolution::NeedsSupplement(entry, fallback) => {
                                        if cancel_token.is_cancelled() {
                                            return;
                                        }

                                        let Some(entry) = resolve_agreed_listing_entry_with_supplement(
                                            entry,
                                            reader_disks,
                                            resolver,
                                            enforce_write_quorum,
                                            supplement,
                                        )
                                        .await
                                        .or(fallback) else {
                                            return;
                                        };
                                        entry
                                    }
                                    ListingEntryResolution::Rejected => return,
                                };

                            if let Err(err) = send_or_cancel(&cancel_token, &value, entry).await
                                && !cancel_token.is_cancelled()
                            {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    Box::pin({
                        let value = tx2.clone();
                        let resolver = partial_resolver.clone();
                        let cancel_token = cancel_for_send2.clone();
                        let supplement = partial_supplement.clone();
                        async move {
                            if cancel_token.is_cancelled() {
                                return;
                            }

                            if let Some(entry) =
                                resolve_listing_entries_with_supplement(entries, resolver, enforce_write_quorum, supplement).await
                                && let Err(err) = send_or_cancel(&cancel_token, &value, entry).await
                                && !cancel_token.is_cancelled()
                            {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                finished: None,
                ..Default::default()
            },
            claim_tracker,
        )
        .await;

        rustfs_io_metrics::record_stage_duration(
            "set_disks_list_objects_list_path",
            list_path_started.elapsed().as_secs_f64() * 1000.0,
        );

        if let Err(ref err) = result {
            debug!(
                bucket = %bucket,
                path = %base_dir,
                disk_count = disks.len(),
                fallback_disks = fallback_disks.len(),
                asked_disks = ask_disks,
                listing_quorum = listing_quorum,
                stop_disk_at_limit = opts.stop_disk_at_limit,
                limit = opts.limit,
                per_disk_limit = limit,
                elapsed_ms = list_path_started.elapsed().as_secs_f64() * 1000.0,
                list_error = %err,
                "set_disks list_path finished with error"
            );
        } else {
            debug!(
                bucket = %bucket,
                path = %base_dir,
                disk_count = disks.len(),
                fallback_disks = fallback_disks.len(),
                asked_disks = ask_disks,
                listing_quorum = listing_quorum,
                stop_disk_at_limit = opts.stop_disk_at_limit,
                limit = opts.limit,
                per_disk_limit = limit,
                elapsed_ms = list_path_started.elapsed().as_secs_f64() * 1000.0,
                "set_disks list_path finished"
            );
        }

        result.map_err(Error::from)
    }
}

fn get_list_quorum(quorum: &str, drive_count: i32) -> i32 {
    match quorum {
        "disk" => 1,
        "reduced" => 2,
        "optimal" => (drive_count + 1) / 2,
        "auto" => -1,
        _ => drive_count, // defaults to 'strict'
    }
}

fn get_quorum_disk_infos(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> (Vec<DiskStore>, Vec<DiskInfo>) {
    let common_mutations = calc_common_counter(infos, read_quorum);
    let mut new_disks = Vec::new();
    let mut new_infos = Vec::new();

    for (i, info) in infos.iter().enumerate() {
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        if mutations >= common_mutations {
            new_disks.push(disks[i].clone());
            new_infos.push(infos[i].clone());
        }
    }

    (new_disks, new_infos)
}

fn get_quorum_disks(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> Vec<DiskStore> {
    let (new_disks, _) = get_quorum_disk_infos(disks, infos, read_quorum);
    new_disks
}

fn calc_common_counter(infos: &[DiskInfo], read_quorum: usize) -> u64 {
    let mut max = 0;
    let mut common_count = 0;
    let mut signature_map: HashMap<u64, usize> = HashMap::new();

    for info in infos {
        if !info.error.is_empty() {
            continue;
        }
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        *signature_map.entry(mutations).or_insert(0) += 1;
    }

    for (&ops, &count) in &signature_map {
        if max < count && common_count < ops {
            max = count;
            common_count = ops;
        }
    }

    if max < read_quorum {
        return 0;
    }
    common_count
}

// list_path_raw

#[cfg(test)]
mod test {
    use super::{
        ENV_API_LIST_OBJECTS_QUORUM, ENV_API_LIST_QUORUM, FallbackListingEntries, FallbackListingEntry, GatherResultsState,
        ListPathOptions, ListPathRawOptions, ListingEntryResolution, ListingSupplement, ListingSupplementOptions,
        MAX_OBJECT_LIST, VersionMarker, enforce_latest_listing_write_quorum, expand_ask_disks_for_object_quorum,
        fallback_entries_for_object, gather_results, latest_listing_allow_agreed_objects, latest_listing_object_quorum,
        latest_listing_raw_min_disks, latest_listing_required_object_quorum, list_metadata_resolution_params,
        list_objects_quorum_from_env, list_quorum_from_env, max_keys_plus_one, merge_entry_channels, normalize_list_quorum,
        parse_version_marker, resolve_agreed_listing_entry, resolve_listing_entries, send_or_cancel, version_marker_for_entries,
        walk_result_from_set_errors,
    };
    use crate::cache_value::metacache_set::{FallbackClaimTracker, TestReaderBehavior, list_path_raw};
    use crate::disk::{DiskAPI, DiskOption, endpoint::Endpoint, error::DiskError, new_disk};
    use crate::error::StorageError;
    use rustfs_filemeta::{
        FileInfo, FileMeta, FileMetaVersion, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntry, MetaDeleteMarker,
        VersionType,
    };
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    fn test_meta_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn test_object_meta_entry(name: &str) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        meta.add_version(FileInfo {
            volume: "bucket".to_owned(),
            name: name.to_owned(),
            size: 1,
            mod_time: Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp")),
            ..Default::default()
        })
        .expect("test metadata should accept object version");
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    #[test]
    fn fallback_entries_for_object_filters_claimed_physical_disks() {
        let mut entries = FallbackListingEntries::new();
        entries.insert(
            "object".to_string(),
            vec![
                FallbackListingEntry {
                    endpoint: "disk-a".to_string(),
                    entry: test_meta_entry("object"),
                },
                FallbackListingEntry {
                    endpoint: "disk-b".to_string(),
                    entry: test_meta_entry("object"),
                },
            ],
        );
        let claimed = HashSet::from(["disk-a".to_string()]);

        let candidates = fallback_entries_for_object(&entries, "object", &claimed);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].as_ref().map(|entry| entry.name.as_str()), Some("object"));
    }

    #[test]
    fn listing_supplement_uses_page_cache_only_for_bounded_walks() {
        let bounded = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 100,
                skip_total_timeout: true,
            },
            Arc::new(Vec::new()),
            FallbackClaimTracker::default(),
        );
        let unbounded = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 0,
                skip_total_timeout: false,
            },
            Arc::new(Vec::new()),
            FallbackClaimTracker::default(),
        );

        assert!(bounded.entries.is_some());
        assert!(bounded.options.skip_total_timeout);
        assert!(unbounded.entries.is_none());
        assert!(!unbounded.options.skip_total_timeout);
    }

    #[tokio::test]
    async fn listing_supplement_unbounded_reads_object_metadata_from_fallback_disk() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("local disk should be created");
        let mut fi = FileInfo::new("object", 1, 1);
        fi.volume = "bucket".to_owned();
        fi.name = "object".to_owned();
        fi.size = 1;
        fi.fresh = true;
        fi.erasure.index = 1;
        fi.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        fi.metadata.insert("etag".to_owned(), "object-etag".to_owned());
        disk.write_metadata("", "bucket", "object", fi)
            .await
            .expect("test metadata should be written");

        let supplement = ListingSupplement::new(
            ListingSupplementOptions {
                bucket: "bucket".to_string(),
                path: String::new(),
                recursive: true,
                filter_prefix: None,
                forward_to: None,
                per_disk_limit: 0,
                skip_total_timeout: false,
            },
            Arc::new(vec![disk]),
            FallbackClaimTracker::default(),
        );

        let entries = supplement.entries_for("object").await;

        assert!(supplement.entries.is_none());
        assert_eq!(entries.len(), 1);
        let entry = entries
            .into_iter()
            .next()
            .and_then(|entry| entry)
            .expect("fallback object metadata should be returned");
        let info = entry.to_fileinfo("bucket").expect("fallback metadata should decode");
        assert_eq!(entry.name, "object");
        assert_eq!(info.metadata.get("etag").map(String::as_str), Some("object-etag"));
    }

    fn test_object_meta_entry_with_erasure_versions(
        name: &str,
        versions: &[(time::OffsetDateTime, &str, usize, usize)],
    ) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        for (idx, (mod_time, etag, data_blocks, parity_blocks)) in versions.iter().enumerate() {
            let mut metadata = HashMap::new();
            metadata.insert("etag".to_string(), (*etag).to_string());

            let mut fi = FileInfo::new(name, *data_blocks, *parity_blocks);
            fi.volume = "bucket".to_owned();
            fi.name = name.to_owned();
            let version_idx = u128::try_from(idx + 1).expect("test version index should fit u128");
            fi.version_id = Some(Uuid::from_u128(version_idx));
            fi.versioned = true;
            fi.size = 1;
            fi.mod_time = Some(*mod_time);
            fi.metadata = metadata;

            meta.add_version(fi).expect("test metadata should accept object version");
        }
        let metadata = meta.marshal_msg().expect("test metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_delete_marker_meta_entry(name: &str, mod_time: time::OffsetDateTime) -> MetaCacheEntry {
        let mut meta = FileMeta::new();
        let delete_marker = FileMetaVersion {
            version_type: VersionType::Delete,
            object: None,
            delete_marker: Some(MetaDeleteMarker {
                version_id: Some(Uuid::from_u128(0x44444444555566667777888888888888)),
                mod_time: Some(mod_time),
                meta_sys: HashMap::new(),
            }),
            legacy_object: None,
            write_version: 1,
            uses_legacy_checksum: false,
        };
        meta.versions.push(
            delete_marker
                .try_into()
                .expect("test delete marker should convert to shallow version"),
        );
        let metadata = meta.marshal_msg().expect("test delete marker metadata should marshal");

        MetaCacheEntry {
            name: name.to_owned(),
            metadata,
            cached: Some(meta),
            reusable: false,
        }
    }

    fn test_dir_meta_entry(name: &str) -> MetaCacheEntry {
        MetaCacheEntry {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn sorted_entries(names: &[&str]) -> MetaCacheEntriesSorted {
        MetaCacheEntriesSorted {
            o: MetaCacheEntries(names.iter().map(|name| Some(test_meta_entry(name))).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn list_path_gather_results_returns_after_limit_without_waiting_for_input_close() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("test entry should be queued");

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                limit: 1,
                incl_deleted: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("limited result should be sent promptly")
            .expect("limited result should be present");
        assert_eq!(result.entries.unwrap().entries().len(), 1);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::LimitReached);
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_keeps_marker_entry_for_version_marker_listing() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("first test entry should be queued");
        entry_tx
            .send(test_meta_entry("obj-b"))
            .await
            .expect("second test entry should be queued");

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                include_marker: true,
                limit: 2,
                incl_deleted: true,
                versioned: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("limited result should be sent promptly")
            .expect("limited result should be present");
        let entries = result.entries.unwrap();
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-a", "obj-b"]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::LimitReached);
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn version_marker_is_applied_only_when_key_marker_entry_is_present() {
        let version_marker = Some(VersionMarker::Null);

        let listed_after_deleted_marker = sorted_entries(&["obj-b", "obj-c"]);
        assert_eq!(
            version_marker_for_entries(Some(&listed_after_deleted_marker), Some("obj-a"), version_marker),
            None
        );

        let listed_with_marker = sorted_entries(&["obj-a", "obj-b"]);
        assert_eq!(
            version_marker_for_entries(Some(&listed_with_marker), Some("obj-a"), version_marker),
            version_marker
        );
    }

    #[tokio::test]
    async fn list_path_gather_results_skips_marker_entry_by_default() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();

        entry_tx
            .send(test_meta_entry("obj-a"))
            .await
            .expect("first test entry should be queued");
        entry_tx
            .send(test_meta_entry("obj-b"))
            .await
            .expect("second test entry should be queued");
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                limit: 2,
                incl_deleted: true,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("eof result should be sent promptly")
            .expect("eof result should be present");
        let entries = result.entries.unwrap();
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-b"]);
        assert!(result.err.is_some());

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after input closes")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::InputClosed);
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn list_path_gather_results_filters_delete_marker_after_name_filters() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");

        entry_tx
            .send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("delete marker should be queued");
        entry_tx
            .send(test_object_meta_entry("obj-b"))
            .await
            .expect("object should be queued");
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            cancel.clone(),
            ListPathOptions {
                bucket: "bucket".to_owned(),
                marker: Some("obj-a".to_owned()),
                limit: 2,
                incl_deleted: false,
                ..Default::default()
            },
            entry_rx,
            result_tx,
        ));

        let result = timeout(Duration::from_secs(1), result_rx.recv())
            .await
            .expect("result should be sent promptly")
            .expect("result should be present");
        let entries = result.entries.unwrap();
        let names = entries
            .entries()
            .into_iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["obj-b"]);

        let state = timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
        assert_eq!(state, GatherResultsState::InputClosed);
        assert!(!cancel.is_cancelled());
    }

    #[test]
    fn list_path_forward_past_is_idempotent_for_same_marker() {
        let mut first_page = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        first_page.forward_past(Some("obj-0002".to_string()));

        let first_page_names = first_page
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        let mut second_page = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        second_page.forward_past(Some("obj-0002".to_string()));

        let second_page_names = second_page
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        assert_eq!(first_page_names, second_page_names);
        assert_eq!(first_page_names, vec!["obj-0003".to_string(), "obj-0004".to_string()]);
    }

    #[test]
    fn list_path_forward_past_keeps_source_switch_page_boundary() {
        let mut walker_page_after_index = sorted_entries(&["obj-0001", "obj-0002", "obj-0003", "obj-0004"]);
        walker_page_after_index.forward_past(Some("obj-0002".to_string()));

        let page_names = walker_page_after_index
            .entries()
            .into_iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();

        assert_eq!(page_names, vec!["obj-0003".to_string(), "obj-0004".to_string()]);
    }

    #[test]
    fn list_path_parse_marker_replay_still_stable() {
        let marker = "photos/2026/image.jpg[rustfs_cache:v1,id:list-cache-id,p:3,s:7]".to_string();

        let mut parsed = ListPathOptions {
            marker: Some(marker),
            ..Default::default()
        };
        parsed.parse_marker();
        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));

        let base_marker = parsed.marker.clone().expect("marker should parse");
        let replay_marker = parsed.encode_marker(base_marker.as_str());
        let mut replay = ListPathOptions {
            marker: Some(replay_marker),
            ..Default::default()
        };
        replay.parse_marker();

        assert_eq!(replay.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(replay.id.as_deref(), Some("list-cache-id"));
        assert_eq!(replay.pool_idx, Some(3));
        assert_eq!(replay.set_idx, Some(7));
        assert!(!replay.create);
    }

    #[test]
    fn test_max_keys_plus_one_caps_before_lookahead() {
        assert_eq!(max_keys_plus_one(999, true), 1000);
        assert_eq!(max_keys_plus_one(MAX_OBJECT_LIST, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(MAX_OBJECT_LIST + 1, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(i32::MAX, true), MAX_OBJECT_LIST + 1);
        assert_eq!(max_keys_plus_one(-1, true), MAX_OBJECT_LIST + 1);
    }

    #[test]
    fn normalize_list_quorum_accepts_supported_values() {
        assert_eq!(normalize_list_quorum("strict"), "strict");
        assert_eq!(normalize_list_quorum("disk"), "disk");
        assert_eq!(normalize_list_quorum("reduced"), "reduced");
        assert_eq!(normalize_list_quorum("optimal"), "optimal");
        assert_eq!(normalize_list_quorum("auto"), "auto");
        assert_eq!(normalize_list_quorum(" OPTIMAL "), "optimal");
    }

    #[test]
    fn normalize_list_quorum_falls_back_to_strict() {
        assert_eq!(normalize_list_quorum(""), "strict");
        assert_eq!(normalize_list_quorum("unknown"), "strict");
    }

    #[test]
    #[serial_test::serial]
    fn list_quorum_from_env_defaults_to_strict() {
        temp_env::with_var_unset(ENV_API_LIST_QUORUM, || {
            assert_eq!(list_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_quorum_from_env_honors_supported_value() {
        temp_env::with_var(ENV_API_LIST_QUORUM, Some("auto"), || {
            assert_eq!(list_quorum_from_env(), "auto");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_quorum_from_env_rejects_unknown_value() {
        temp_env::with_var(ENV_API_LIST_QUORUM, Some("unsafe"), || {
            assert_eq!(list_quorum_from_env(), "strict");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_defaults_to_optimal() {
        temp_env::with_var_unset(ENV_API_LIST_OBJECTS_QUORUM, || {
            assert_eq!(list_objects_quorum_from_env(), "optimal");
        });
    }

    #[test]
    #[serial_test::serial]
    fn list_objects_quorum_from_env_honors_supported_value() {
        temp_env::with_var(ENV_API_LIST_OBJECTS_QUORUM, Some("strict"), || {
            assert_eq!(list_objects_quorum_from_env(), "strict");
        });
    }

    #[test]
    fn list_metadata_resolution_params_limits_plain_listing_to_latest_version() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 2, 3, false);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 1);
    }

    #[test]
    fn list_metadata_resolution_params_keeps_all_versions_for_version_listing() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, true);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 0);
    }

    #[test]
    fn latest_listing_object_quorum_uses_write_quorum_for_strict_latest_listing() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 4, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 8, required_quorum);

        assert_eq!(required_quorum, 5);
        assert_eq!(ask_disks, 5);
        assert_eq!(latest_listing_object_quorum(2, 8, 4, true), 5);
    }

    #[test]
    fn latest_listing_object_quorum_calculates_low_parity_write_quorum() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 1, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 8, required_quorum);

        assert_eq!(required_quorum, 7);
        assert_eq!(ask_disks, 7);
        assert_eq!(latest_listing_object_quorum(2, 8, 1, true), 7);
    }

    #[test]
    fn latest_listing_object_quorum_keeps_reduced_and_versioned_listing_quorum() {
        assert!(!enforce_latest_listing_write_quorum(true, "reduced"));
        assert!(!enforce_latest_listing_write_quorum(true, "disk"));
        assert!(enforce_latest_listing_write_quorum(true, "optimal"));
        assert!(!enforce_latest_listing_write_quorum(false, "optimal"));
        assert_eq!(latest_listing_required_object_quorum(1, 4, 2, false), 1);
        assert_eq!(latest_listing_object_quorum(1, 4, 2, false), 1);
        assert_eq!(latest_listing_required_object_quorum(2, 4, 2, false), 2);
        assert_eq!(latest_listing_object_quorum(2, 4, 2, false), 2);
        assert_eq!(expand_ask_disks_for_object_quorum(2, 4, 2), 2);
    }

    #[test]
    fn latest_listing_object_quorum_requires_write_quorum_when_degraded_cannot_satisfy_it() {
        let required_quorum = latest_listing_required_object_quorum(2, 8, 4, true);
        let ask_disks = expand_ask_disks_for_object_quorum(4, 4, required_quorum);

        assert_eq!(required_quorum, 5);
        assert_eq!(ask_disks, 4);
        assert_eq!(latest_listing_object_quorum(2, 8, 4, true), 5);
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_requires_enough_readers_for_write_quorum() {
        let entry = test_object_meta_entry("object");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let allow_agreed_objects = latest_listing_allow_agreed_objects(true, 4, 5);
        let raw_min_disks = latest_listing_raw_min_disks(2, 5, true);

        assert!(!allow_agreed_objects);
        assert_eq!(raw_min_disks, 5);
        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry.clone()]); 4],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    Box::pin(async move {
                        if !allow_agreed_objects && !entry.is_dir() {
                            return;
                        }
                        seen.lock().expect("seen mutex poisoned").push(entry.name);
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect_err("latest listing should fail when selected readers cannot satisfy write quorum");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(seen.lock().expect("seen mutex poisoned").is_empty());

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let allow_agreed_objects = latest_listing_allow_agreed_objects(true, 5, 5);
        let raw_min_disks = latest_listing_raw_min_disks(3, 5, true);

        assert!(allow_agreed_objects);
        assert_eq!(raw_min_disks, 5);
        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    Box::pin(async move {
                        if !allow_agreed_objects && !entry.is_dir() {
                            return;
                        }
                        seen.lock().expect("seen mutex poisoned").push(entry.name);
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("write-quorum all-agree listing should complete");

        assert_eq!(seen.lock().expect("seen mutex poisoned").as_slice(), &["object".to_string()]);
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_reconciles_low_parity_partial_latest() {
        let old_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions(
            "object",
            &[(old_mod_time, "old-etag", 4, 4), (new_mod_time, "new-etag", 7, 1)],
        );
        let fallback_old_entry = test_object_meta_entry_with_erasure_versions("object", &[(old_mod_time, "old-etag", 4, 4)]);
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    let fallback_old_entry = fallback_old_entry.clone();
                    Box::pin(async move {
                        let entry = match resolve_agreed_listing_entry(entry, 5, resolver.clone(), true) {
                            ListingEntryResolution::Resolved(entry) => entry,
                            ListingEntryResolution::NeedsSupplement(entry, _) => {
                                let mut candidates = vec![Some(entry); 5];
                                candidates.extend(vec![Some(fallback_old_entry); 2]);
                                let Some(entry) = resolve_listing_entries(MetaCacheEntries(candidates), resolver, true) else {
                                    return;
                                };
                                entry
                            }
                            ListingEntryResolution::Rejected => return,
                        };
                        let info = entry.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            entry.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(old_mod_time), Some("old-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_falls_back_to_previous_version_without_supplement() {
        let old_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions(
            "object",
            &[(old_mod_time, "old-etag", 4, 4), (new_mod_time, "new-etag", 7, 1)],
        );
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    Box::pin(async move {
                        let entry = match resolve_agreed_listing_entry(entry, 5, resolver, true) {
                            ListingEntryResolution::Resolved(entry) => entry,
                            ListingEntryResolution::NeedsSupplement(_, Some(entry)) => entry,
                            ListingEntryResolution::NeedsSupplement(_, None) | ListingEntryResolution::Rejected => return,
                        };
                        let info = entry.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            entry.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(old_mod_time), Some("old-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_path_supplements_committed_low_parity_latest() {
        let new_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_400).expect("valid timestamp");
        let entry = test_object_meta_entry_with_erasure_versions("object", &[(new_mod_time, "new-etag", 7, 1)]);
        let fallback_entry = entry.clone();
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, 5, false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None, None],
                min_disks: 5,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 5],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    let resolver = resolver.clone();
                    let fallback_entry = fallback_entry.clone();
                    Box::pin(async move {
                        let ListingEntryResolution::NeedsSupplement(entry, _) =
                            resolve_agreed_listing_entry(entry, 5, resolver.clone(), true)
                        else {
                            return;
                        };
                        let mut candidates = vec![Some(entry); 5];
                        candidates.extend(vec![Some(fallback_entry); 2]);
                        let resolved = resolve_listing_entries(MetaCacheEntries(candidates), resolver, true)
                            .expect("supplemented low-parity latest should satisfy object write quorum");
                        let info = resolved.to_fileinfo("bucket").expect("resolved entry should decode");
                        seen.lock().expect("seen mutex poisoned").push((
                            resolved.name,
                            info.mod_time,
                            info.metadata.get("etag").cloned(),
                        ));
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect("all-agree listing should complete");

        assert_eq!(
            seen.lock().expect("seen mutex poisoned").as_slice(),
            &[("object".to_string(), Some(new_mod_time), Some("new-etag".to_string()))]
        );
    }

    #[tokio::test]
    async fn latest_listing_agreed_directory_requires_write_quorum() {
        let entry = test_dir_meta_entry("prefix/");
        let raw_min_disks = latest_listing_raw_min_disks(2, 5, true);

        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None, None, None],
                min_disks: raw_min_disks,
                test_reader_behaviors: vec![TestReaderBehavior::Entries(vec![entry]); 4],
                agreed: Some(Box::new(move |_| Box::pin(async {}))),
                ..Default::default()
            },
        )
        .await
        .expect_err("latest listing should not expose prefixes below write quorum");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn test_null_version_marker_handling() {
        let parsed = parse_version_marker("null".to_string()).expect("null marker should parse");
        assert_eq!(parsed, VersionMarker::Null);

        let valid_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let parsed = parse_version_marker(valid_uuid.to_string()).expect("uuid marker should parse");
        assert_eq!(parsed, VersionMarker::Version(Uuid::parse_str(valid_uuid).unwrap()));
    }

    /// Test that next_version_idmarker returns "null" for non-versioned objects
    #[test]
    fn test_next_version_idmarker_null_string() {
        // When version_id is None, next_version_idmarker should be "null"
        let version_id: Option<Uuid> = None;
        let next_version_idmarker = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(next_version_idmarker, "null");

        // When version_id is Some, next_version_idmarker should be the UUID string
        let version_id: Option<Uuid> = Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
        let next_version_idmarker = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(next_version_idmarker, "550e8400-e29b-41d4-a716-446655440000");
    }

    /// Test the round-trip: next_version_idmarker -> VersionIdMarker parameter -> parsing
    #[test]
    fn test_version_marker_round_trip() {
        // Scenario 1: Non-versioned object
        // Server returns "null" as NextVersionIdMarker
        // Client sends "null" as VersionIdMarker
        // Server parses "null" as an explicit null-version marker
        let server_response = "null";
        let client_request = server_response;
        let parsed = parse_version_marker(client_request.to_string()).expect("null marker should parse");
        assert_eq!(parsed, VersionMarker::Null);

        // Scenario 2: Versioned object
        // Server returns UUID as NextVersionIdMarker
        // Client sends UUID as VersionIdMarker
        // Server parses UUID correctly
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let server_response = uuid_str;
        let client_request = server_response;
        let parsed = parse_version_marker(client_request.to_string()).expect("uuid marker should parse");
        assert_eq!(parsed, VersionMarker::Version(Uuid::parse_str(uuid_str).unwrap()));
    }

    #[test]
    fn list_path_marker_round_trip_preserves_set_index() {
        let mut opts = ListPathOptions {
            id: Some("list-cache-id".to_string()),
            pool_idx: Some(3),
            set_idx: Some(7),
            ..Default::default()
        };

        let marker = opts.encode_marker("photos/2026/image.jpg");
        let expected_marker = format!(
            "photos/2026/image.jpg[rustfs_cache:{},id:list-cache-id,p:3,s:7]",
            super::MARKER_TAG_VERSION
        );
        assert_eq!(marker, expected_marker);

        let mut parsed = ListPathOptions {
            marker: Some(marker),
            ..Default::default()
        };
        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_uses_trailing_cache_tag() {
        let mut parsed = ListPathOptions {
            marker: Some(format!(
                "photos/[archive]/image.jpg[rustfs_cache:{},id:list-cache-id,p:3,s:7]",
                super::MARKER_TAG_VERSION
            )),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/[archive]/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("list-cache-id"));
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
    }

    #[test]
    fn list_path_marker_parser_return_tag_forces_cache_refresh() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v2,return:,p:3,s:7]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert!(parsed.id.is_some());
        assert_eq!(parsed.pool_idx, Some(3));
        assert_eq!(parsed.set_idx, Some(7));
        assert!(parsed.create);
    }

    #[test]
    fn list_path_marker_parser_recovers_from_corrupt_set_index() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v2,id:list-cache-id,p:3,s:not-a-number]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert!(parsed.id.is_some());
        assert_ne!(parsed.id.as_deref(), Some("list-cache-id"));
        assert!(parsed.create);
        assert_eq!(parsed.pool_idx, Some(3));
        assert!(parsed.set_idx.is_none());
    }

    #[test]
    fn list_path_marker_parser_accepts_legacy_v1_tag() {
        let mut parsed = ListPathOptions {
            marker: Some("photos/2026/image.jpg[rustfs_cache:v1,id:legacy-cache-id,p:4,s:1]".to_string()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some("photos/2026/image.jpg"));
        assert_eq!(parsed.id.as_deref(), Some("legacy-cache-id"));
        assert_eq!(parsed.pool_idx, Some(4));
        assert_eq!(parsed.set_idx, Some(1));
        assert!(!parsed.create);
    }

    #[test]
    fn list_path_marker_parser_ignores_unsupported_cache_tag_version() {
        let marker = "photos/image.jpg[rustfs_cache:v0,id:list-cache-id,p:3,s:7]".to_string();
        let mut parsed = ListPathOptions {
            marker: Some(marker.clone()),
            ..Default::default()
        };

        parsed.parse_marker();

        assert_eq!(parsed.marker.as_deref(), Some(marker.as_str()));
        assert!(parsed.id.is_none());
        assert!(parsed.pool_idx.is_none());
        assert!(parsed.set_idx.is_none());
        assert!(!parsed.create);
    }

    #[test]
    fn walk_result_from_set_errors_returns_non_eof_error() {
        let err = walk_result_from_set_errors(&[Some(StorageError::Unexpected), Some(StorageError::FileAccessDenied)])
            .expect_err("walk should fail when any set reports a real listing error");

        assert_eq!(err, StorageError::FileAccessDenied);
    }

    #[test]
    fn walk_result_from_set_errors_prefers_real_error_over_not_found() {
        let err = walk_result_from_set_errors(&[
            Some(StorageError::VolumeNotFound),
            Some(StorageError::DiskNotFound),
            Some(StorageError::FileAccessDenied),
        ])
        .expect_err("walk should report the real listing error");

        assert_eq!(err, StorageError::FileAccessDenied);
    }

    #[test]
    fn walk_result_from_set_errors_preserves_volume_not_found() {
        let err = walk_result_from_set_errors(&[Some(StorageError::VolumeNotFound), Some(StorageError::VolumeNotFound)])
            .expect_err("all volume-not-found set errors should remain visible");

        assert_eq!(err, StorageError::VolumeNotFound);
    }

    #[test]
    fn walk_result_from_set_errors_allows_missing_entries() {
        walk_result_from_set_errors(&[Some(StorageError::FileNotFound), Some(StorageError::VolumeNotFound)])
            .expect("missing objects under an existing listing path should not fail the walk");
    }

    #[test]
    fn walk_result_from_set_errors_ignores_only_unexpected_and_successes() {
        walk_result_from_set_errors(&[None, Some(StorageError::Unexpected)])
            .expect("successful sets and unexpected EOF-style markers should not fail the walk");
    }

    // use std::sync::Arc;

    // use crate::cache_value::metacache_set::list_path_raw;
    // use crate::cache_value::metacache_set::ListPathRawOptions;
    // use crate::disk::endpoint::Endpoint;
    // use crate::disk::error::is_err_eof;
    // use crate::disk::format::FormatV3;
    // use crate::disk::new_disk;
    // use crate::disk::DiskAPI;
    // use crate::disk::DiskOption;
    // use crate::disk::MetaCacheEntries;
    // use crate::disk::MetaCacheEntry;
    // use crate::disk::WalkDirOptions;
    // use crate::layout::endpoints::EndpointServerPools;
    // use crate::error::Error;
    // use crate::metacache::writer::MetacacheReader;
    // use crate::set_disk::SetDisks;
    // use crate::store::list_objects::ListPathOptions;
    // use crate::store::list_objects::WalkOptions;
    // use crate::store::list_objects::WalkVersionsSortOrder;
    // use futures::future::join_all;
    // use rustfs_lock::namespace_lock::NsLockMap;
    // use tokio::sync::broadcast;
    // use tokio::sync::mpsc;
    // use tokio::sync::RwLock;
    // use uuid::Uuid;

    // #[tokio::test]
    // async fn test_walk_dir() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

    //     // let disk = match LocalDisk::new(&ep, false).await {
    //     //     Ok(res) => res,
    //     //     Err(err) => {
    //     //         println!("LocalDisk::new err {:?}", err);
    //     //         return;
    //     //     }
    //     // };

    //     let (rd, mut wr) = tokio::io::duplex(64);

    //     let job = tokio::spawn(async move {
    //         let opts = WalkDirOptions {
    //             bucket: "dada".to_owned(),
    //             base_dir: "".to_owned(),
    //             recursive: true,
    //             ..Default::default()
    //         };

    //         println!("walk opts {:?}", opts);
    //         if let Err(err) = disk.walk_dir(opts, &mut wr).await {
    //             println!("walk_dir err {:?}", err);
    //         }
    //     });

    //     let job2 = tokio::spawn(async move {
    //         let mut mrd = MetacacheReader::new(rd);

    //         loop {
    //             match mrd.peek().await {
    //                 Ok(res) => {
    //                     if let Some(info) = res {
    //                         println!("info {:?}", info.name)
    //                     } else {
    //                         break;
    //                     }
    //                 }
    //                 Err(err) => {
    //                     if is_err_eof(&err) {
    //                         break;
    //                     }

    //                     println!("get err {:?}", err);
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //     join_all(vec![job, job2]).await;
    // }

    // #[tokio::test]
    // async fn test_list_path_raw() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

    //     // let disk = match LocalDisk::new(&ep, false).await {
    //     //     Ok(res) => res,
    //     //     Err(err) => {
    //     //         println!("LocalDisk::new err {:?}", err);
    //     //         return;
    //     //     }
    //     // };

    //     let (_, rx) = broadcast::channel(1);
    //     let bucket = "dada".to_owned();
    //     let forward_to = None;
    //     let disks = vec![Some(disk)];
    //     let fallback_disks = Vec::new();

    //     list_path_raw(
    //         rx,
    //         ListPathRawOptions {
    //             disks,
    //             fallback_disks,
    //             bucket,
    //             path: "".to_owned(),
    //             recursice: true,
    //             forward_to,
    //             min_disks: 1,
    //             report_not_found: false,
    //             agreed: Some(Box::new(move |entry: MetaCacheEntry| {
    //                 Box::pin(async move { println!("get entry: {}", entry.name) })
    //             })),
    //             partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
    //                 Box::pin(async move { println!("get entries: {:?}", entries) })
    //             })),
    //             finished: None,
    //             ..Default::default()
    //         },
    //     )
    //     .await
    //     .unwrap();
    // }

    // #[tokio::test]
    // async fn test_set_list_path() {
    //     let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
    //     ep.pool_idx = 0;
    //     ep.set_idx = 0;
    //     ep.disk_idx = 0;
    //     ep.is_local = true;

    //     let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");
    //     let _ = disk.set_disk_id(Some(Uuid::new_v4())).await;

    //     let set = SetDisks {
    //         lockers: Vec::new(),
    //         locker_owner: String::new(),
    //         ns_mutex: Arc::new(RwLock::new(NsLockMap::new(false))),
    //         disks: RwLock::new(vec![Some(disk)]),
    //         set_endpoints: Vec::new(),
    //         set_drive_count: 1,
    //         default_parity_count: 0,
    //         set_index: 0,
    //         pool_index: 0,
    //         format: FormatV3::new(1, 1),
    //     };

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = "dada".to_owned();

    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         ..Default::default()
    //     };

    //     let (sender, mut recv) = mpsc::channel(10);

    //     set.list_path(rx, opts, sender).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry.name)
    //     }
    // }

    // #[tokio::test]
    //walk() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = "dada".to_owned();
    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         ..Default::default()
    //     };

    //     let (sender, mut recv) = mpsc::channel(10);

    //     store.list_merged(rx, opts, sender).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry.name)
    //     }
    // }

    // #[tokio::test]
    // async fn test_list_path() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let bucket = "dada".to_owned();
    //     let opts = ListPathOptions {
    //         bucket,
    //         recursive: true,
    //         limit: 100,

    //         ..Default::default()
    //     };

    //     let ret = store.list_path(&opts).await.unwrap();
    //     println!("ret {:?}", ret);
    // }

    // #[tokio::test]
    // async fn test_list_objects_v2() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     let ret = store.list_objects_v2("data", "", "", "", 100, false, "").await.unwrap();
    //     println!("ret {:?}", ret);
    // }

    // #[tokio::test]
    // async fn test_walk() {
    //     let server_address = "localhost:9000";

    //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
    //         server_address,
    //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
    //     )
    //     .unwrap();

    //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
    //         .await
    //         .unwrap();

    //     ECStore::init(store.clone()).await.unwrap();

    //     let (_tx, rx) = broadcast::channel(1);

    //     let bucket = ".rustfs.sys";
    //     let prefix = "config/iam/sts/";

    //     let (sender, mut recv) = mpsc::channel(10);

    //     let opts = WalkOptions::default();

    //     store.walk(rx, bucket, prefix, sender, opts).await.unwrap();

    //     while let Some(entry) = recv.recv().await {
    //         println!("get entry {:?}", entry)
    //     }
    // }

    #[tokio::test]
    async fn merge_entry_channels_produces_sorted_unique_output_from_two_channels() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        // Send sorted entries from two channels with some overlap
        tx_a.send(test_meta_entry("obj-a")).await.unwrap();
        tx_a.send(test_meta_entry("obj-c")).await.unwrap();
        tx_a.send(test_meta_entry("obj-e")).await.unwrap();
        drop(tx_a);

        tx_b.send(test_meta_entry("obj-b")).await.unwrap();
        tx_b.send(test_meta_entry("obj-d")).await.unwrap();
        drop(tx_b);

        let rx = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(rx, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        // Results should be sorted and deduplicated
        assert_eq!(results, vec!["obj-a", "obj-b", "obj-c", "obj-d", "obj-e"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_deduplicates_entries_across_channels() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        // Both channels have the same entry
        tx_a.send(test_meta_entry("obj-a")).await.unwrap();
        tx_a.send(test_meta_entry("obj-c")).await.unwrap();
        drop(tx_a);

        tx_b.send(test_meta_entry("obj-a")).await.unwrap();
        tx_b.send(test_meta_entry("obj-b")).await.unwrap();
        drop(tx_b);

        let rx = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(rx, vec![rx_a, rx_b], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        // "obj-a" should appear only once despite being in both channels
        assert_eq!(results, vec!["obj-a", "obj-b", "obj-c"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_documents_candidate_metadata_authority_risk() {
        let (tx_a, rx_a) = mpsc::channel(4);
        let (tx_b, rx_b) = mpsc::channel(4);
        let (tx_c, rx_c) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_500).expect("valid timestamp");
        let stale_mod_time = time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp");
        let stale_object = test_object_meta_entry_with_erasure_versions("obj-a", &[(stale_mod_time, "stale-etag", 4, 2)]);

        tx_a.send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("first delete marker should queue");
        tx_b.send(test_delete_marker_meta_entry("obj-a", mod_time))
            .await
            .expect("second delete marker should queue");
        tx_c.send(stale_object).await.expect("stale object should queue");
        drop(tx_a);
        drop(tx_b);
        drop(tx_c);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx_a, rx_b, rx_c], out_tx, 2));

        let mut merged = timeout(Duration::from_secs(1), out_rx.recv())
            .await
            .expect("merged entry should be emitted promptly")
            .expect("merged entry should be present");
        assert_eq!(merged.name, "obj-a");
        assert!(
            !merged.is_latest_delete_marker(),
            "current merge consumes candidate metadata bytes; future index-backed strong modes must live-verify metadata instead"
        );
        assert!(
            matches!(timeout(Duration::from_secs(1), out_rx.recv()).await, Ok(None)),
            "merge should not emit a duplicate entry for the same key"
        );

        handle
            .await
            .expect("merge task should not panic")
            .expect("merge task should succeed");
    }

    #[tokio::test]
    async fn merge_entry_channels_handles_single_channel() {
        let (tx, rx) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        tx.send(test_meta_entry("obj-a")).await.unwrap();
        tx.send(test_meta_entry("obj-b")).await.unwrap();
        drop(tx);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(merge_entry_channels(cancel, vec![rx], out_tx, 1));

        let mut results = Vec::new();
        while let Some(entry) = out_rx.recv().await {
            results.push(entry.name.clone());
        }

        handle.await.unwrap().unwrap();

        assert_eq!(results, vec!["obj-a", "obj-b"]);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation() {
        let (tx, rx) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, mut out_rx) = mpsc::channel(8);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Use a single channel so the cancellation check in tokio::select! is exercised.
        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx], out_tx, 1));

        // Send an entry to prove the function is running and processing data.
        tx.send(test_meta_entry("a")).await.unwrap();
        let received = timeout(Duration::from_millis(500), out_rx.recv())
            .await
            .expect("should receive entry before cancellation")
            .map(|e| e.name);
        assert_eq!(received, Some("a".to_string()));

        // Cancel while the sender is still alive (channel is not closed).
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("merge should not hang after cancellation")
            .expect("task should not panic");
        assert!(result.is_ok(), "merge should return Ok on cancellation");

        // Keep tx alive until after the assertion so the channel doesn't close prematurely.
        drop(tx);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation_with_multiple_live_channels() {
        let (tx_a, rx_a) = mpsc::channel::<MetaCacheEntry>(4);
        let (tx_b, rx_b) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, _out_rx) = mpsc::channel(8);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx_a, rx_b], out_tx, 1));
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("multi-channel merge should not hang after cancellation")
            .expect("task should not panic");
        assert!(result.is_ok(), "multi-channel merge should return Ok on cancellation");

        drop(tx_a);
        drop(tx_b);
    }

    #[tokio::test]
    async fn merge_entry_channels_respects_cancellation_when_output_is_full() {
        let (tx_a, rx_a) = mpsc::channel::<MetaCacheEntry>(4);
        let (tx_b, rx_b) = mpsc::channel::<MetaCacheEntry>(4);
        let (out_tx, _out_rx) = mpsc::channel(1);

        out_tx
            .send(test_meta_entry("already-buffered"))
            .await
            .expect("output channel should accept initial entry");
        tx_a.send(test_meta_entry("a"))
            .await
            .expect("input channel a should accept entry");
        tx_b.send(test_meta_entry("b"))
            .await
            .expect("input channel b should accept entry");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(merge_entry_channels(cancel_clone, vec![rx_a, rx_b], out_tx, 1));
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("merge should not hang when output is full and cancellation fires")
            .expect("task should not panic");
        assert!(result.is_ok(), "merge should return Ok when cancelled during output send");

        drop(tx_a);
        drop(tx_b);
    }

    #[tokio::test]
    async fn send_or_cancel_returns_false_when_cancelled_while_output_is_full() {
        let (out_tx, _out_rx) = mpsc::channel::<MetaCacheEntry>(1);
        out_tx
            .send(test_meta_entry("already-buffered"))
            .await
            .expect("output channel should accept initial entry");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move { send_or_cancel(&cancel_clone, &out_tx, test_meta_entry("next")).await });
        cancel.cancel();

        let result = timeout(Duration::from_secs(2), handle)
            .await
            .expect("send_or_cancel should not hang when cancellation wins over backpressure")
            .expect("task should not panic")
            .expect("send_or_cancel should succeed");
        assert!(!result, "send_or_cancel should report cancellation instead of a successful send");
    }
}
