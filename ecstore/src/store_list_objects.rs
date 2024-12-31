use crate::cache_value::metacache_set::{list_path_raw, ListPathRawOptions};
use crate::disk::error::{is_all_not_found, is_all_volume_not_found, is_err_eof, DiskError};
use crate::disk::{
    DiskInfo, DiskStore, MetaCacheEntries, MetaCacheEntriesSorted, MetaCacheEntriesSortedResult, MetaCacheEntry,
    MetadataResolutionParams,
};
use crate::error::{Error, Result};
use crate::file_meta::merge_file_meta_versions;
use crate::peer::is_reserved_or_invalid_bucket;
use crate::set_disk::SetDisks;
use crate::store::check_list_objs_args;
use crate::store_api::{ListObjectVersionsInfo, ListObjectsInfo, ObjectInfo, ObjectOptions};
use crate::store_err::{is_err_bucket_not_found, to_object_err, StorageError};
use crate::utils::path::{self, base_dir_from_prefix, SLASH_SEPARATOR};
use crate::StorageAPI;
use crate::{store::ECStore, store_api::ListObjectsV2Info};
use futures::future::join_all;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::broadcast::{self, Receiver as B_Receiver};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::error;
use uuid::Uuid;

const MAX_OBJECT_LIST: i32 = 1000;
// const MAX_DELETE_LIST: i32 = 1000;
// const MAX_UPLOADS_LIST: i32 = 10000;
// const MAX_PARTS_LIST: i32 = 10000;

const METACACHE_SHARE_PREFIX: bool = false;

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
    // The response will be the first entry >= this object name.
    pub marker: Option<String>,

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
        if let Some(marker) = &self.marker {
            let s = marker.clone();
            if !s.contains(format!("[rustfs_cache:{}", MARKER_TAG_VERSION).as_str()) {
                return;
            }

            if let (Some(start_idx), Some(end_idx)) = (s.find("["), s.find("]")) {
                self.marker = Some(s[0..start_idx].to_owned());
                let tags: Vec<_> = s[start_idx..end_idx].trim_matches(['[', ']']).split(",").collect();

                for &tag in tags.iter() {
                    let kv: Vec<_> = tag.split(":").collect();
                    if kv.len() != 2 {
                        continue;
                    }

                    match kv[0] {
                        "rustfs_cache" => {
                            if kv[1] != MARKER_TAG_VERSION {
                                continue;
                            }
                        }
                        "id" => self.id = Some(kv[1].to_owned()),
                        "return" => {
                            self.id = Some(Uuid::new_v4().to_string());
                            self.create = true;
                        }
                        "p" => match kv[1].parse::<usize>() {
                            Ok(res) => self.pool_idx = Some(res),
                            Err(_) => {
                                self.id = Some(Uuid::new_v4().to_string());
                                self.create = true;
                                continue;
                            }
                        },
                        "s" => match kv[1].parse::<usize>() {
                            Ok(res) => self.set_idx = Some(res),
                            Err(_) => {
                                self.id = Some(Uuid::new_v4().to_string());
                                self.create = true;
                                continue;
                            }
                        },
                        _ => (),
                    }
                }
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
                self.pool_idx.unwrap_or_default(),
            )
        } else {
            format!("{}[rustfs_cache:{},return:]", marker, MARKER_TAG_VERSION)
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
    ) -> Result<ListObjectsV2Info> {
        let marker = {
            if continuation_token.is_none() {
                start_after
            } else {
                continuation_token.clone()
            }
        };

        let loi = self.list_objects_generic(bucket, prefix, marker, delimiter, max_keys).await?;
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
    ) -> Result<ListObjectsInfo> {
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: max_keys_plus_one(max_keys, marker.is_some()),
            marker,
            incl_deleted: false,
            ask_disks: "strict".to_owned(), //TODO: from config
            ..Default::default()
        };

        // warn!("list_objects_generic opts {:?}", &opts);

        // use get
        if !opts.prefix.is_empty() && opts.limit == 1 && opts.marker.is_none() {
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

        let mut list_result = match self.list_path(&opts).await {
            Ok(res) => res,
            Err(err) => MetaCacheEntriesSortedResult {
                err: Some(err),
                ..Default::default()
            },
        };

        if let Some(err) = &list_result.err {
            if !is_err_eof(err) {
                return Err(to_object_err(list_result.err.unwrap(), vec![bucket, prefix]));
            }
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        // contextCanceled

        let mut get_objects = list_result
            .entries
            .unwrap_or_default()
            .file_infos(bucket, prefix, delimiter.clone())
            .await;

        let is_truncated = {
            if max_keys > 0 && get_objects.len() > max_keys as usize {
                get_objects.truncate(max_keys as usize);
                true
            } else {
                list_result.err.is_none() && !get_objects.is_empty()
            }
        };

        let next_marker = {
            if is_truncated {
                get_objects.last().map(|last| last.name.clone())
            } else {
                None
            }
        };

        let mut prefixes: Vec<String> = Vec::new();

        let mut objects = Vec::with_capacity(get_objects.len());
        for obj in get_objects.into_iter() {
            if let Some(delimiter) = &delimiter {
                if obj.is_dir && obj.mod_time.is_none() {
                    let mut found = false;
                    if delimiter != SLASH_SEPARATOR {
                        for p in prefixes.iter() {
                            if found {
                                break;
                            }
                            found = p == &obj.name;
                        }
                    }
                    if !found {
                        prefixes.push(obj.name.clone());
                    }
                } else {
                    objects.push(obj);
                }
            } else {
                objects.push(obj);
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
        if marker.is_none() && version_marker.is_some() {
            return Err(Error::new(StorageError::NotImplemented));
        }

        // if marker set, limit +1
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.clone(),
            limit: max_keys_plus_one(max_keys, marker.is_some()),
            marker,
            incl_deleted: true,
            ask_disks: "strict".to_owned(),
            versioned: true,
            ..Default::default()
        };

        let mut list_result = match self.list_path(&opts).await {
            Ok(res) => res,
            Err(err) => MetaCacheEntriesSortedResult {
                err: Some(err),
                ..Default::default()
            },
        };

        if let Some(err) = &list_result.err {
            if !is_err_eof(err) {
                return Err(to_object_err(list_result.err.unwrap(), vec![bucket, prefix]));
            }
        }

        if let Some(result) = list_result.entries.as_mut() {
            result.forward_past(opts.marker);
        }

        let mut get_objects = list_result
            .entries
            .unwrap_or_default()
            .file_info_versions(bucket, prefix, delimiter.clone(), version_marker)
            .await;

        let is_truncated = {
            if max_keys > 0 && get_objects.len() > max_keys as usize {
                get_objects.truncate(max_keys as usize);
                true
            } else {
                list_result.err.is_none() && !get_objects.is_empty()
            }
        };

        let (next_marker, next_version_idmarker) = {
            if is_truncated {
                get_objects
                    .last()
                    .map(|last| (Some(last.name.clone()), last.version_id.map(|v| v.to_string())))
                    .unwrap_or_default()
            } else {
                (None, None)
            }
        };

        let mut prefixes: Vec<String> = Vec::new();

        let mut objects = Vec::with_capacity(get_objects.len());
        for obj in get_objects.into_iter() {
            if let Some(delimiter) = &delimiter {
                if obj.is_dir && obj.mod_time.is_none() {
                    let mut found = false;
                    if delimiter != SLASH_SEPARATOR {
                        for p in prefixes.iter() {
                            if found {
                                break;
                            }
                            found = p == &obj.name;
                        }
                    }
                    if !found {
                        prefixes.push(obj.name.clone());
                    }
                } else {
                    objects.push(obj);
                }
            } else {
                objects.push(obj);
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
        // warn!("list_path opt {:?}", &o);

        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut o = o.clone();
        o.marker = o.marker.filter(|v| v >= &o.prefix);

        if let Some(marker) = &o.marker {
            if !o.prefix.is_empty() && !marker.starts_with(&o.prefix) {
                return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
            }
        }

        if o.limit == 0 {
            return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        if o.prefix.starts_with(SLASH_SEPARATOR) {
            return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
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
        let (cancel_tx, cancel_rx) = broadcast::channel(1);
        let (err_tx, mut err_rx) = broadcast::channel::<Error>(1);

        let (sender, recv) = mpsc::channel(o.limit as usize);

        let store = self.clone();
        let opts = o.clone();
        let cancel_rx1 = cancel_rx.resubscribe();
        let err_tx1 = err_tx.clone();
        let job1 = tokio::spawn(async move {
            let mut opts = opts;
            opts.stop_disk_at_limit = true;
            if let Err(err) = store.list_merged(cancel_rx1, opts, sender).await {
                error!("list_merged err {:?}", err);
                let _ = err_tx1.send(err);
            }
        });

        let cancel_rx2 = cancel_rx.resubscribe();

        let (result_tx, mut result_rx) = mpsc::channel(1);
        let err_tx2 = err_tx.clone();
        let opts = o.clone();
        let job2 = tokio::spawn(async move {
            if let Err(err) = gather_results(cancel_rx2, opts, recv, result_tx).await {
                error!("gather_results err {:?}", err);
                let _ = err_tx2.send(err);
            }
        });

        let mut result = {
            // receiver result
            tokio::select! {
               res = err_rx.recv() =>{

                match res{
                    Ok(o) => {
                        error!("list_path err_rx.recv() ok {:?}", &o);
                        MetaCacheEntriesSortedResult{ entries: None, err: Some(o) }
                    },
                    Err(err) => {
                        error!("list_path err_rx.recv() err {:?}", &err);

                        MetaCacheEntriesSortedResult{ entries: None, err: Some(Error::new(err)) }
                    },
                }
               },
               Some(result) = result_rx.recv()=>{
                result
               }
            }
        };

        // cancel call exit spawns
        cancel_tx.send(true)?;

        // wait spawns exit
        join_all(vec![job1, job2]).await;

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
                result.err = Some(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
            }
        }

        Ok(result)
    }

    // 读所有
    async fn list_merged(
        &self,
        rx: B_Receiver<bool>,
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

                let rx = rx.resubscribe();
                futures.push(set.list_path(rx, opts, send));
            }
        }

        tokio::spawn(async move {
            if let Err(err) = merge_entry_channels(rx, inputs, sender.clone(), 1).await {
                println!("merge_entry_channels err {:?}", err)
            }
        });

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

        if is_all_not_found(&errs) {
            if is_all_volume_not_found(&errs) {
                return Err(Error::new(DiskError::VolumeNotFound));
            }

            return Ok(Vec::new());
        }

        // merge_res?;

        // TODO check cancel

        for err in errs.iter() {
            if let Some(err) = err {
                if is_err_eof(err) {
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
}

async fn gather_results(
    _rx: B_Receiver<bool>,
    opts: ListPathOptions,
    recv: Receiver<MetaCacheEntry>,
    results_tx: Sender<MetaCacheEntriesSortedResult>,
) -> Result<()> {
    let mut returned = false;

    let mut sender = Some(results_tx);

    let mut recv = recv;
    let mut entrys = Vec::new();
    while let Some(mut entry) = recv.recv().await {
        if returned {
            continue;
        }

        // TODO: rx.recv()

        // TODO: isLatestDeletemarker
        if !opts.include_directories
            && (entry.is_dir() || (!opts.versioned && entry.is_object() && entry.is_latest_delete_marker()))
        {
            continue;
        }

        if let Some(marker) = &opts.marker {
            if &entry.name < marker {
                continue;
            }
        }

        if !entry.name.starts_with(&opts.prefix) {
            continue;
        }

        if let Some(separator) = &opts.separator {
            if !opts.recursive && !entry.is_in_dir(&opts.prefix, separator) {
                continue;
            }
        }

        if !opts.incl_deleted && entry.is_object() && entry.is_latest_delete_marker() && entry.is_object_dir() {
            continue;
        }

        // TODO: Lifecycle

        if opts.limit > 0 && entrys.len() >= opts.limit as usize {
            if let Some(tx) = sender {
                tx.send(MetaCacheEntriesSortedResult {
                    entries: Some(MetaCacheEntriesSorted {
                        o: MetaCacheEntries(entrys.clone()),
                        ..Default::default()
                    }),
                    err: None,
                })
                .await?;

                returned = true;
                sender = None;
            }
            continue;
        }

        entrys.push(Some(entry));
        // entrys.push(entry);
    }

    // finish not full, return eof
    if let Some(tx) = sender {
        tx.send(MetaCacheEntriesSortedResult {
            entries: Some(MetaCacheEntriesSorted {
                o: MetaCacheEntries(entrys.clone()),
                ..Default::default()
            }),
            err: Some(Error::new(std::io::Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"))),
        })
        .await?;
    }

    Ok(())
}

async fn select_from(
    in_channels: &mut [Receiver<MetaCacheEntry>],
    idx: usize,
    top: &mut [Option<MetaCacheEntry>],
    n_done: &mut usize,
) -> Result<()> {
    match in_channels[idx].recv().await {
        Some(entry) => {
            top[idx] = Some(entry);
        }
        None => {
            top[idx] = None;
            *n_done += 1;
        }
    }
    Ok(())
}

// TODO: exit when cancel
async fn merge_entry_channels(
    rx: B_Receiver<bool>,
    in_channels: Vec<Receiver<MetaCacheEntry>>,
    out_channel: Sender<MetaCacheEntry>,
    read_quorum: usize,
) -> Result<()> {
    let mut rx = rx;
    let mut in_channels = in_channels;
    if in_channels.len() == 1 {
        loop {
            tokio::select! {
                has_entry = in_channels[0].recv()=>{
                    if let Some(entry) = has_entry{
                        // warn!("merge_entry_channels entry {}", &entry.name);
                        out_channel.send(entry).await?;
                    } else {
                        return Ok(())
                    }
                },
                _ = rx.recv()=>return Err(Error::msg("cancel")),
            }
        }
    }

    let mut top: Vec<Option<MetaCacheEntry>> = vec![None; in_channels.len()];
    let mut n_done = 0;

    let in_channels_len = in_channels.len();

    for idx in 0..in_channels_len {
        select_from(&mut in_channels, idx, &mut top, &mut n_done).await?;
    }

    let mut last = String::new();
    let mut to_merge: Vec<usize> = Vec::new();
    loop {
        if n_done == in_channels.len() {
            return Ok(());
        }

        let mut best: Option<MetaCacheEntry> = None;
        let mut best_idx = 0;
        to_merge.clear();

        // FIXME: top move when select_from call
        let vtop = top.clone();

        for (i, other) in vtop.iter().enumerate() {
            if let Some(other_entry) = other {
                if let Some(best_entry) = &best {
                    let other_idx = i;

                    // println!("get other_entry {:?}", other_entry.name);

                    if path::clean(&best_entry.name) == path::clean(&other_entry.name) {
                        let dir_matches = best_entry.is_dir() && other_entry.is_dir();
                        let suffix_matche =
                            best_entry.name.ends_with(SLASH_SEPARATOR) == other_entry.name.ends_with(SLASH_SEPARATOR);

                        if dir_matches && suffix_matche {
                            to_merge.push(other_idx);
                            continue;
                        }

                        if !dir_matches {
                            // dir and object has the save name
                            if other_entry.is_dir() {
                                // TODO: read next entry to top
                                select_from(&mut in_channels, other_idx, &mut top, &mut n_done).await?;
                                continue;
                            }

                            to_merge.clear();

                            best = Some(other_entry.clone());
                            best_idx = other_idx;
                            continue;
                        }
                    } else if best_entry.name > other_entry.name {
                        to_merge.clear();
                        best = Some(other_entry.clone());
                        best_idx = i;
                    }
                } else {
                    best = Some(other_entry.clone());
                    best_idx = i;
                }
            }
        }

        // println!("get best_entry {} {:?}", &best_idx, &best.clone().unwrap_or_default().name);

        // TODO:
        if !to_merge.is_empty() {
            if let Some(entry) = &best {
                let mut versions = Vec::with_capacity(to_merge.len() + 1);

                let mut has_xl = {
                    if let Ok(meta) = entry.clone().xl_meta() {
                        Some(meta)
                    } else {
                        None
                    }
                };

                if let Some(x) = &has_xl {
                    versions.push(x.versions.clone());
                }

                for &idx in to_merge.iter() {
                    let has_entry = { top.get(idx).cloned() };

                    if let Some(Some(entry)) = has_entry {
                        let xl2 = match entry.clone().xl_meta() {
                            Ok(res) => res,
                            Err(_) => {
                                select_from(&mut in_channels, idx, &mut top, &mut n_done).await?;

                                continue;
                            }
                        };

                        versions.push(xl2.versions.clone());

                        if has_xl.is_none() {
                            select_from(&mut in_channels, best_idx, &mut top, &mut n_done).await?;

                            best_idx = idx;
                            best = Some(entry.clone());
                            has_xl = Some(xl2);
                        } else {
                            select_from(&mut in_channels, best_idx, &mut top, &mut n_done).await?;
                        }
                    }
                }

                if let Some(xl) = has_xl.as_mut() {
                    if !versions.is_empty() {
                        xl.versions = merge_file_meta_versions(read_quorum, true, 0, &versions);

                        if let Ok(meta) = xl.marshal_msg() {
                            if let Some(b) = best.as_mut() {
                                b.metadata = meta;
                                b.cached = Some(xl.clone());
                            }
                        }
                    }
                }
            }

            to_merge.clear();
        }

        if let Some(best_entry) = &best {
            if best_entry.name > last {
                out_channel.send(best_entry.clone()).await?;
                last = best_entry.name.clone();
            }
            top[best_idx] = None; // Replace entry we just sent
            select_from(&mut in_channels, best_idx, &mut top, &mut n_done).await?;
        }
    }
}

impl SetDisks {
    pub async fn list_path(&self, rx: B_Receiver<bool>, opts: ListPathOptions, sender: Sender<MetaCacheEntry>) -> Result<()> {
        let (mut disks, infos, _) = self.get_online_disks_with_healing_and_info(true).await;

        let mut ask_disks = get_list_quorum(&opts.ask_disks, self.set_drive_count as i32);
        if ask_disks == -1 {
            let new_disks = get_quorum_disks(&disks, &infos, (disks.len() + 1) / 2);
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

        let mut fallback_disks = Vec::new();

        if ask_disks > 0 && disks.len() > ask_disks as usize {
            let mut rand = thread_rng();
            disks.shuffle(&mut rand);

            fallback_disks = disks.split_off(ask_disks as usize);
        }

        let mut resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: opts.bucket.clone(),
            ..Default::default()
        };

        if opts.versioned {
            resolver.requested_versions = 1;
        }

        let limit = {
            if opts.limit > 0 && opts.stop_disk_at_limit {
                opts.limit + 4 + (opts.limit / 16)
            } else {
                0
            }
        };

        let tx1 = sender.clone();
        let tx2 = sender.clone();

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursice: opts.recursive,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: listing_quorum,
                per_disk_limit: limit,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin({
                        let value = tx1.clone();
                        async move {
                            if let Err(err) = value.send(entry).await {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                    Box::pin({
                        let value = tx2.clone();
                        let resolver = resolver.clone();
                        async move {
                            if let Ok(Some(entry)) = entries.resolve(resolver) {
                                if let Err(err) = value.send(entry).await {
                                    error!("list_path send fail {:?}", err);
                                }
                            }
                        }
                    })
                })),
                finished: None,
                ..Default::default()
            },
        )
        .await
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
            new_disks.push(disks[i].clone()); // Assuming StorageAPI derives Clone
            new_infos.push(infos[i].clone()); // Assuming DiskInfo derives Clone
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

// #[cfg(test)]
// mod test {
//     use std::sync::Arc;

//     use crate::cache_value::metacache_set::list_path_raw;
//     use crate::cache_value::metacache_set::ListPathRawOptions;
//     use crate::disk::endpoint::Endpoint;
//     use crate::disk::error::is_err_eof;
//     use crate::disk::format::FormatV3;
//     use crate::disk::new_disk;
//     use crate::disk::DiskAPI;
//     use crate::disk::DiskOption;
//     use crate::disk::MetaCacheEntries;
//     use crate::disk::MetaCacheEntry;
//     use crate::disk::WalkDirOptions;
//     use crate::endpoints::EndpointServerPools;
//     use crate::error::Error;
//     use crate::metacache::writer::MetacacheReader;
//     use crate::set_disk::SetDisks;
//     use crate::store::ECStore;
//     use crate::store_list_objects::ListPathOptions;
//     use futures::future::join_all;
//     use lock::namespace_lock::NsLockMap;
//     use tokio::sync::broadcast;
//     use tokio::sync::mpsc;
//     use tokio::sync::RwLock;
//     use uuid::Uuid;

//     #[tokio::test]
//     async fn test_walk_dir() {
//         let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
//         ep.pool_idx = 0;
//         ep.set_idx = 0;
//         ep.disk_idx = 0;
//         ep.is_local = true;

//         let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

//         // let disk = match LocalDisk::new(&ep, false).await {
//         //     Ok(res) => res,
//         //     Err(err) => {
//         //         println!("LocalDisk::new err {:?}", err);
//         //         return;
//         //     }
//         // };

//         let (rd, mut wr) = tokio::io::duplex(64);

//         let job = tokio::spawn(async move {
//             let opts = WalkDirOptions {
//                 bucket: "dada".to_owned(),
//                 base_dir: "".to_owned(),
//                 recursive: true,
//                 ..Default::default()
//             };

//             println!("walk opts {:?}", opts);
//             if let Err(err) = disk.walk_dir(opts, &mut wr).await {
//                 println!("walk_dir err {:?}", err);
//             }
//         });

//         let job2 = tokio::spawn(async move {
//             let mut mrd = MetacacheReader::new(rd);

//             loop {
//                 match mrd.peek().await {
//                     Ok(res) => {
//                         if let Some(info) = res {
//                             println!("info {:?}", info.name)
//                         } else {
//                             break;
//                         }
//                     }
//                     Err(err) => {
//                         if is_err_eof(&err) {
//                             break;
//                         }

//                         println!("get err {:?}", err);
//                         break;
//                     }
//                 }
//             }
//         });
//         join_all(vec![job, job2]).await;
//     }

//     #[tokio::test]
//     async fn test_list_path_raw() {
//         let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
//         ep.pool_idx = 0;
//         ep.set_idx = 0;
//         ep.disk_idx = 0;
//         ep.is_local = true;

//         let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

//         // let disk = match LocalDisk::new(&ep, false).await {
//         //     Ok(res) => res,
//         //     Err(err) => {
//         //         println!("LocalDisk::new err {:?}", err);
//         //         return;
//         //     }
//         // };

//         let (_, rx) = broadcast::channel(1);
//         let bucket = "dada".to_owned();
//         let forward_to = None;
//         let disks = vec![Some(disk)];
//         let fallback_disks = Vec::new();

//         list_path_raw(
//             rx,
//             ListPathRawOptions {
//                 disks,
//                 fallback_disks,
//                 bucket,
//                 path: "".to_owned(),
//                 recursice: true,
//                 forward_to,
//                 min_disks: 1,
//                 report_not_found: false,
//                 agreed: Some(Box::new(move |entry: MetaCacheEntry| {
//                     Box::pin(async move { println!("get entry: {}", entry.name) })
//                 })),
//                 partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
//                     Box::pin(async move { println!("get entries: {:?}", entries) })
//                 })),
//                 finished: None,
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap();
//     }

//     #[tokio::test]
//     async fn test_set_list_path() {
//         let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
//         ep.pool_idx = 0;
//         ep.set_idx = 0;
//         ep.disk_idx = 0;
//         ep.is_local = true;

//         let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");
//         let _ = disk.set_disk_id(Some(Uuid::new_v4())).await;

//         let set = SetDisks {
//             lockers: Vec::new(),
//             locker_owner: String::new(),
//             ns_mutex: Arc::new(RwLock::new(NsLockMap::new(false))),
//             disks: RwLock::new(vec![Some(disk)]),
//             set_endpoints: Vec::new(),
//             set_drive_count: 1,
//             default_parity_count: 0,
//             set_index: 0,
//             pool_index: 0,
//             format: FormatV3::new(1, 1),
//         };

//         let (_tx, rx) = broadcast::channel(1);

//         let bucket = "dada".to_owned();

//         let opts = ListPathOptions {
//             bucket,
//             recursive: true,
//             ..Default::default()
//         };

//         let (sender, mut recv) = mpsc::channel(10);

//         set.list_path(rx, opts, sender).await.unwrap();

//         while let Some(entry) = recv.recv().await {
//             println!("get entry {:?}", entry.name)
//         }
//     }

//     #[tokio::test]
//     async fn test_list_merged() {
//         let server_address = "localhost:9000";

//         let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
//             server_address,
//             vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
//         )
//         .unwrap();

//         let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
//             .await
//             .unwrap();

//         let (_tx, rx) = broadcast::channel(1);

//         let bucket = "dada".to_owned();
//         let opts = ListPathOptions {
//             bucket,
//             recursive: true,
//             ..Default::default()
//         };

//         let (sender, mut recv) = mpsc::channel(10);

//         store.list_merged(rx, opts, sender).await.unwrap();

//         while let Some(entry) = recv.recv().await {
//             println!("get entry {:?}", entry.name)
//         }
//     }

//     #[tokio::test]
//     async fn test_list_path() {
//         let server_address = "localhost:9000";

//         let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
//             server_address,
//             vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
//         )
//         .unwrap();

//         let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
//             .await
//             .unwrap();

//         let bucket = "dada".to_owned();
//         let opts = ListPathOptions {
//             bucket,
//             recursive: true,
//             limit: 100,

//             ..Default::default()
//         };

//         let ret = store.list_path(&opts).await.unwrap();
//         println!("ret {:?}", ret);
//     }

//     // #[tokio::test]
//     // async fn test_list_objects_v2() {
//     //     let server_address = "localhost:9000";

//     //     let (endpoint_pools, _setup_type) = EndpointServerPools::from_volumes(
//     //         server_address,
//     //         vec!["/Users/weisd/project/weisd/s3-rustfs/target/volume/test".to_string()],
//     //     )
//     //     .unwrap();

//     //     let store = ECStore::new(server_address.to_string(), endpoint_pools.clone())
//     //         .await
//     //         .unwrap();

//     //     let ret = store.list_objects_v2("data", "", "", "", 100, false, "").await.unwrap();
//     //     println!("ret {:?}", ret);
//     // }
// }
