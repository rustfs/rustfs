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
use crate::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use crate::disk::error::DiskError;
use crate::disk::{DiskInfo, DiskStore};
use crate::error::{
    Error, Result, StorageError, is_all_not_found, is_all_volume_not_found, is_err_bucket_not_found, to_object_err,
};
use crate::set_disk::SetDisks;
use crate::store_api::{
    ListObjectVersionsInfo, ListObjectsInfo, ObjectInfo, ObjectInfoOrErr, ObjectOperations, ObjectOptions, WalkOptions,
};
use crate::store_utils::is_reserved_or_invalid_bucket;
use crate::{store::ECStore, store_api::ListObjectsV2Info};
use futures::future::join_all;
use rand::seq::SliceRandom;
use rustfs_filemeta::{
    MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntriesSortedResult, MetaCacheEntry, MetadataResolutionParams,
    merge_file_meta_versions,
};
use rustfs_storage_api::{VersionMarker, WalkVersionsSortOrder};
use rustfs_utils::path::{self, SLASH_SEPARATOR, base_dir_from_prefix};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast::{self};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};
use uuid::Uuid;

const MAX_OBJECT_LIST: i32 = 1000;
// const MAX_DELETE_LIST: i32 = 1000;
// const MAX_UPLOADS_LIST: i32 = 10000;
// const MAX_PARTS_LIST: i32 = 10000;

const METACACHE_SHARE_PREFIX: bool = false;

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

const MARKER_TAG_VERSION: &str = "v1";
const ENV_API_LIST_QUORUM: &str = "RUSTFS_API_LIST_QUORUM";
const DEFAULT_API_LIST_QUORUM: &str = "strict";

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

fn list_metadata_resolution_params(bucket: String, listing_quorum: usize, versioned: bool) -> MetadataResolutionParams {
    let mut resolver = MetadataResolutionParams {
        dir_quorum: listing_quorum,
        obj_quorum: listing_quorum,
        bucket,
        ..Default::default()
    };

    if !versioned {
        resolver.requested_versions = 1;
    }

    resolver
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
        let tag_body = marker[start_idx + 1..end_idx].to_owned();
        let mut supported_marker = false;

        for tag in tag_body.split(',') {
            let Some((key, value)) = tag.split_once(':') else {
                continue;
            };
            if key == "rustfs_cache" {
                if value != MARKER_TAG_VERSION {
                    return;
                }
                supported_marker = true;
            }
        }

        if !supported_marker {
            return;
        }

        self.marker = Some(marker[..start_idx].to_owned());
        for tag in tag_body.split(',') {
            let Some((key, value)) = tag.split_once(':') else {
                continue;
            };

            match key {
                "rustfs_cache" => {}
                "id" => self.id = Some(value.to_owned()),
                "return" => {
                    self.id = Some(Uuid::new_v4().to_string());
                    self.create = true;
                }
                "p" => match value.parse::<usize>() {
                    Ok(res) => self.pool_idx = Some(res),
                    Err(_) => {
                        self.id = Some(Uuid::new_v4().to_string());
                        self.create = true;
                    }
                },
                "s" => match value.parse::<usize>() {
                    Ok(res) => self.set_idx = Some(res),
                    Err(_) => {
                        self.id = Some(Uuid::new_v4().to_string());
                        self.create = true;
                    }
                },
                _ => (),
            }
        }
    }
    pub fn encode_marker(&mut self, marker: &str) -> String {
        if let Some(id) = &self.id {
            format!(
                "{}[rustfs_cache:{},id:{},p:{},s:{}]",
                marker,
                MARKER_TAG_VERSION,
                id.to_owned(),
                self.pool_idx.unwrap_or_default(),
                self.set_idx.unwrap_or_default(),
            )
        } else {
            format!("{marker}[rustfs_cache:{MARKER_TAG_VERSION},return:]")
        }
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
            ask_disks: list_quorum_from_env(),
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

        let mut get_objects = ObjectInfo::from_meta_cache_entries_sorted_infos(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
        )
        .await;

        // Determine if there are more results: we requested max_keys + 1, so if we got more
        // than max_keys, there are more results available
        let mut is_truncated = false;
        if max_keys <= 0 {
            get_objects.clear();
        } else if get_objects.len() > max_keys as usize {
            is_truncated = true;
            // Truncate to max_keys if we have more results
            get_objects.truncate(max_keys as usize);
        }

        let mut next_marker = {
            if is_truncated {
                get_objects.last().map(|last| last.name.clone())
            } else {
                None
            }
        };

        let mut prefixes: Vec<String> = Vec::new();
        let mut prefix_set: HashSet<String> = HashSet::new();

        let mut objects = Vec::with_capacity(get_objects.len());
        for obj in get_objects.into_iter() {
            if delimiter.is_some() {
                if obj.is_dir && obj.mod_time.is_none() {
                    // Check if prefix already exists to avoid duplicates
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

        // After delimiter collapse, re-evaluate is_truncated based on visible results.
        // No delimiter: reduction is from skipped entries → disk_has_more && non-empty.
        // With delimiter: reduction may be from collapse → only when visible >= max_keys.
        if !is_truncated && disk_has_more {
            let visible_count = objects.len() + prefixes.len();
            let should_truncate = if delimiter.is_none() {
                visible_count > 0
            } else {
                visible_count >= max_keys as usize
            };
            if should_truncate {
                is_truncated = true;
                // Compute next_marker from visible results since get_objects was consumed.
                // Prefer last object name; fall back to last prefix for marker.
                next_marker = objects
                    .last()
                    .map(|last| last.name.clone())
                    .or_else(|| prefixes.last().cloned());
            }
        }

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
            ask_disks: list_quorum_from_env(),
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

        let mut get_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &list_result.entries.unwrap_or_default(),
            bucket,
            prefix,
            delimiter.clone(),
            version_marker,
        )
        .await;

        // Determine if there are more results: we requested max_keys + 1, so if we got more
        // than max_keys, there are more results available
        let mut is_truncated = false;
        if max_keys <= 0 {
            get_objects.clear();
        } else if get_objects.len() > max_keys as usize {
            is_truncated = true;
            // Truncate to max_keys if we have more results
            get_objects.truncate(max_keys as usize);
        }

        let mut next_marker: Option<String> = None;
        let mut next_version_idmarker: Option<String> = None;
        if is_truncated && let Some(last) = get_objects.last() {
            next_marker = Some(last.name.clone());
            // AWS S3 API returns "null" for non-versioned objects
            next_version_idmarker = Some(last.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string()));
        }

        let mut prefixes: Vec<String> = Vec::new();
        let mut prefix_set: HashSet<String> = HashSet::new();

        let mut objects = Vec::with_capacity(get_objects.len());
        for obj in get_objects.into_iter() {
            if delimiter.is_some() {
                if obj.is_dir && obj.mod_time.is_none() {
                    // Check if prefix already exists to avoid duplicates
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

        // After delimiter collapse, re-evaluate is_truncated based on visible results.
        // Two distinct scenarios (see list_objects_generic for detailed rationale):
        // 1. No delimiter: reduction from skipped entries → disk_has_more && non-empty
        // 2. With delimiter: reduction from collapse → only when visible >= max_keys
        if !is_truncated && disk_has_more {
            let visible_count = objects.len() + prefixes.len();
            let should_truncate = if delimiter.is_none() {
                visible_count > 0
            } else {
                visible_count >= max_keys as usize
            };
            if should_truncate {
                is_truncated = true;
                // Compute markers from visible results since get_objects was consumed.
                if let Some(last) = objects.last() {
                    next_marker = Some(last.name.clone());
                    next_version_idmarker = Some(last.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string()));
                } else if let Some(last_prefix) = prefixes.last().cloned() {
                    next_marker = Some(last_prefix);
                    next_version_idmarker = None;
                }
            }
        }

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

        // cancel channel
        let cancel = CancellationToken::new();

        let (err_tx, mut err_rx) = broadcast::channel::<Arc<Error>>(1);

        let (sender, recv) = mpsc::channel(o.limit as usize);

        let store = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel.clone();
        let cancel_rx1_for_err = cancel_rx1.clone();
        let err_tx1 = err_tx.clone();
        let job1 = tokio::spawn(
            async move {
                let mut opts = opts;
                opts.stop_disk_at_limit = true;
                if let Err(err) = store.list_merged(cancel_rx1, opts, sender).await
                    && !cancel_rx1_for_err.is_cancelled()
                {
                    error!("list_merged err {:?}", err);
                    let _ = err_tx1.send(Arc::new(err));
                }
            }
            .instrument(tracing::Span::current()),
        );

        let cancel_rx2 = cancel.clone();

        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2 = tokio::spawn(
            async move {
                if let Err(err) = gather_results(cancel_rx2, opts, recv, result_tx).await {
                    error!("gather_results err {:?}", err);
                    let _ = err_tx2.send(Arc::new(err));
                }

                // cancel call exit spawns
                cancel.cancel();
            }
            .instrument(tracing::Span::current()),
        );

        let mut result = {
            // receiver result
            tokio::select! {
               res = err_rx.recv() =>{

                match res{
                    Ok(o) => {
                        error!("list_path err_rx.recv() ok {:?}", &o);
                        MetaCacheEntriesSortedResult{ entries: None, err: Some(o.as_ref().clone().into()) }
                    },
                    Err(err) => {
                        error!("list_path err_rx.recv() err {:?}", &err);

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
            error!("list_path err_rx.try_recv() ok {:?}", &err);
            result.err = Some(err.as_ref().clone().into());
        }

        if result.err.is_some() {
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

                    if set.set_drive_count == 4 || ask_disks > disks.len() as i32 {
                        ask_disks = disks.len() as i32;
                    }

                    let fallback_disks = {
                        if ask_disks > 0 && disks.len() > ask_disks as usize {
                            let mut rand = rand::rng();
                            disks.shuffle(&mut rand);
                            disks.split_off(ask_disks as usize)
                        } else {
                            Vec::new()
                        }
                    };

                    let listing_quorum = ((ask_disks + 1) / 2) as usize;

                    let resolver = MetadataResolutionParams {
                        dir_quorum: listing_quorum,
                        obj_quorum: listing_quorum,
                        bucket: bucket.to_owned(),
                        ..Default::default()
                    };

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

                    list_path_raw(
                        rx_clone,
                        ListPathRawOptions {
                            disks: disks.iter().cloned().map(Some).collect(),
                            fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                            bucket: bucket.to_owned(),
                            path,
                            recursive: true,
                            filter_prefix: Some(filter_prefix),
                            forward_to: opts.marker.clone(),
                            min_disks: listing_quorum,
                            per_disk_limit: opts.limit as i32,
                            agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                                Box::pin({
                                    let value = tx1.clone();
                                    async move {
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
                                    let resolver = resolver.clone();
                                    async move {
                                        if let Some(entry) = entries.resolve(resolver)
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

        tokio::spawn(async move { merge_entry_channels(rx, inputs, merge_tx, 1).await }.instrument(tracing::Span::current()));

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
) -> Result<()> {
    let mut recv = recv;
    let mut entries = Vec::new();
    while let Some(mut entry) = recv.recv().await {
        #[cfg(windows)]
        {
            // normalize windows path separator
            entry.name = entry.name.replace("\\", "/");
        }

        // TODO: rx.recv()

        // TODO: isLatestDeletemarker
        if !opts.include_directories
            && (entry.is_dir() || (!opts.versioned && entry.is_object() && entry.is_latest_delete_marker()))
        {
            continue;
        }

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

        if !opts.incl_deleted && entry.is_object() && entry.is_latest_delete_marker() && !entry.is_object_dir() {
            continue;
        }

        // TODO: Lifecycle

        entries.push(Some(entry));

        if opts.limit > 0 && entries.len() >= opts.limit as usize {
            rx.cancel();

            results_tx
                .send(MetaCacheEntriesSortedResult {
                    entries: Some(MetaCacheEntriesSorted {
                        o: MetaCacheEntries(entries.clone()),
                        ..Default::default()
                    }),
                    err: None,
                })
                .await
                .map_err(Error::other)?;
            return Ok(());
        }
    }

    // finish not full, return eof
    results_tx
        .send(MetaCacheEntriesSortedResult {
            entries: Some(MetaCacheEntriesSorted {
                o: MetaCacheEntries(entries.clone()),
                ..Default::default()
            }),
            err: Some(Error::Unexpected.into()),
        })
        .await
        .map_err(Error::other)?;

    Ok(())
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

impl SetDisks {
    pub async fn list_path(&self, rx: CancellationToken, opts: ListPathOptions, sender: Sender<MetaCacheEntry>) -> Result<()> {
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

        if self.set_drive_count == 4 || ask_disks > disks.len() as i32 {
            ask_disks = disks.len() as i32;
        }

        let listing_quorum = ((ask_disks + 1) / 2) as usize;
        ensure_non_empty_listing_disks(&opts.bucket, &opts.base_dir, &disks)?;

        let mut fallback_disks = Vec::new();

        if ask_disks > 0 && disks.len() > ask_disks as usize {
            let mut rand = rand::rng();
            disks.shuffle(&mut rand);

            fallback_disks = disks.split_off(ask_disks as usize);
        }

        let resolver = list_metadata_resolution_params(opts.bucket.clone(), listing_quorum, opts.versioned);

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

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursive: opts.recursive,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: listing_quorum,
                per_disk_limit: limit,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin({
                        let value = tx1.clone();
                        let cancel_token = cancel_for_send1.clone();
                        async move {
                            if let Err(err) = value.send(entry).await
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
                        let resolver = resolver.clone();
                        let cancel_token = cancel_for_send2.clone();
                        async move {
                            if let Some(entry) = entries.resolve(resolver)
                                && let Err(err) = value.send(entry).await
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
        )
        .await
        .map_err(Error::from)
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
        ENV_API_LIST_QUORUM, ListPathOptions, MAX_OBJECT_LIST, VersionMarker, gather_results, list_metadata_resolution_params,
        list_quorum_from_env, max_keys_plus_one, merge_entry_channels, normalize_list_quorum, parse_version_marker,
        version_marker_for_entries, walk_result_from_set_errors,
    };
    use crate::error::StorageError;
    use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntry};
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

    fn sorted_entries(names: &[&str]) -> MetaCacheEntriesSorted {
        MetaCacheEntriesSorted {
            o: MetaCacheEntries(names.iter().map(|name| Some(test_meta_entry(name))).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn gather_results_returns_after_limit_without_waiting_for_input_close() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);

        entry_tx.send(test_meta_entry("obj-a")).await.unwrap();

        let handle = tokio::spawn(gather_results(
            CancellationToken::new(),
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

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
    }

    #[tokio::test]
    async fn gather_results_keeps_marker_entry_for_version_marker_listing() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);

        entry_tx.send(test_meta_entry("obj-a")).await.unwrap();
        entry_tx.send(test_meta_entry("obj-b")).await.unwrap();

        let handle = tokio::spawn(gather_results(
            CancellationToken::new(),
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

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after sending a limited result")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
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
    async fn gather_results_skips_marker_entry_by_default() {
        let (entry_tx, entry_rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel(1);

        entry_tx.send(test_meta_entry("obj-a")).await.unwrap();
        entry_tx.send(test_meta_entry("obj-b")).await.unwrap();
        drop(entry_tx);

        let handle = tokio::spawn(gather_results(
            CancellationToken::new(),
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

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("gather_results should finish after input closes")
            .expect("gather_results task should not panic")
            .expect("gather_results should succeed");
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
    fn list_metadata_resolution_params_limits_plain_listing_to_latest_version() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 2, false);

        assert_eq!(resolver.dir_quorum, 2);
        assert_eq!(resolver.obj_quorum, 2);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 1);
    }

    #[test]
    fn list_metadata_resolution_params_keeps_all_versions_for_version_listing() {
        let resolver = list_metadata_resolution_params("bucket".to_string(), 3, true);

        assert_eq!(resolver.dir_quorum, 3);
        assert_eq!(resolver.obj_quorum, 3);
        assert_eq!(resolver.bucket, "bucket");
        assert_eq!(resolver.requested_versions, 0);
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
    // use crate::endpoints::EndpointServerPools;
    // use crate::error::Error;
    // use crate::metacache::writer::MetacacheReader;
    // use crate::set_disk::SetDisks;
    // use crate::store_list_objects::ListPathOptions;
    // use crate::store_list_objects::WalkOptions;
    // use crate::store_list_objects::WalkVersionsSortOrder;
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
}
