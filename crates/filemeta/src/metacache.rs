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
    merge_file_meta_versions,
};
use arc_swap::ArcSwapOption;
use rmp::Marker;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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
use tracing::warn;

const SLASH_SEPARATOR: &str = "/";

#[derive(Clone, Debug, Default)]
pub struct MetadataResolutionParams {
    pub dir_quorum: usize,
    pub obj_quorum: usize,
    pub requested_versions: usize,
    pub bucket: String,
    pub strict: bool,
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
            if cached.versions.is_empty() {
                return true;
            }
            return cached.versions[0].header.version_type == VersionType::Delete;
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
            Ok(res) => {
                if res.versions.is_empty() {
                    return true;
                }
                res.versions[0].header.version_type == VersionType::Delete
            }
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

            let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false, true)?;
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

        let mut fm = FileMeta::new();
        fm.unmarshal_msg(&self.metadata)?;
        fm.into_file_info_versions(bucket, self.name.as_str(), false)
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
            Some(file_meta) => file_meta.clone(),
            None => match FileMeta::load(&self.metadata) {
                Ok(meta) => meta,
                Err(_) => return (None, false),
            },
        };

        let other_vers = match &other.cached {
            Some(file_meta) => file_meta.clone(),
            None => match FileMeta::load(&other.metadata) {
                Ok(meta) => meta,
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
            if s_version.header != o_version.header {
                if s_version.header.has_ec() != o_version.header.has_ec() {
                    // One version has EC and the other doesn't - may have been written later.
                    // Compare without considering EC.
                    let (mut a, mut b) = (s_version.header.clone(), o_version.header.clone());
                    (a.ec_n, a.ec_m, b.ec_n, b.ec_m) = (0, 0, 0, 0);
                    if a == b {
                        continue;
                    }
                }

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

    pub fn xl_meta(&mut self) -> Result<FileMeta> {
        if self.is_dir() {
            return Err(Error::FileNotFound);
        }

        if let Some(meta) = &self.cached {
            Ok(meta.clone())
        } else {
            if self.metadata.is_empty() {
                return Err(Error::FileNotFound);
            }

            let meta = FileMeta::load(&self.metadata)?;
            self.cached = Some(meta.clone());
            Ok(meta)
        }
    }
}

#[derive(Debug, Default)]
pub struct MetaCacheEntries(pub Vec<Option<MetaCacheEntry>>);

impl MetaCacheEntries {
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[Option<MetaCacheEntry>] {
        &self.0
    }

    pub fn resolve(&self, mut params: MetadataResolutionParams) -> Option<MetaCacheEntry> {
        if self.0.is_empty() {
            warn!("decommission_pool: entries resolve empty");
            return None;
        }

        let mut dir_exists = 0;
        let mut selected = None;

        params.candidates.clear();
        let mut objs_agree = 0;
        let mut objs_valid = 0;

        for entry in self.0.iter().flatten() {
            let mut entry = entry.clone();

            warn!("decommission_pool: entries resolve entry {:?}", entry.name);
            if entry.name.is_empty() {
                continue;
            }
            if entry.is_dir() {
                dir_exists += 1;
                selected = Some(entry.clone());
                warn!("decommission_pool: entries resolve entry dir {:?}", entry.name);
                continue;
            }

            let xl = match entry.xl_meta() {
                Ok(xl) => xl,
                Err(e) => {
                    warn!("decommission_pool: entries resolve entry xl_meta {:?}", e);
                    continue;
                }
            };

            objs_valid += 1;
            params.candidates.push(xl.versions.clone());

            if selected.is_none() {
                selected = Some(entry.clone());
                objs_agree = 1;
                warn!("decommission_pool: entries resolve entry selected {:?}", entry.name);
                continue;
            }

            if let (prefer, true) = entry.matches(selected.as_ref(), params.strict) {
                selected = prefer;
                objs_agree += 1;
                warn!("decommission_pool: entries resolve entry prefer {:?}", entry.name);
                continue;
            }
        }

        let Some(selected) = selected else {
            warn!("decommission_pool: entries resolve entry no selected");
            return None;
        };

        if selected.is_dir() && dir_exists >= params.dir_quorum {
            warn!("decommission_pool: entries resolve entry dir selected {:?}", selected.name);
            return Some(selected);
        }

        // If we would never be able to reach read quorum.
        if objs_valid < params.obj_quorum {
            warn!(
                "decommission_pool: entries resolve entry not enough objects {} < {}",
                objs_valid, params.obj_quorum
            );
            return None;
        }

        if objs_agree == objs_valid {
            warn!("decommission_pool: entries resolve entry all agree {} == {}", objs_agree, objs_valid);
            return Some(selected);
        }

        let Some(cached) = selected.cached else {
            warn!("decommission_pool: entries resolve entry no cached");
            return None;
        };

        let versions = merge_file_meta_versions(params.obj_quorum, params.strict, params.requested_versions, &params.candidates);
        if versions.is_empty() {
            warn!("decommission_pool: entries resolve entry no versions");
            return None;
        }

        let merged_cached = FileMeta {
            meta_ver: cached.meta_ver,
            versions,
            ..Default::default()
        };

        let metadata = match merged_cached.marshal_msg() {
            Ok(meta) => meta,
            Err(e) => {
                warn!("decommission_pool: entries resolve entry marshal_msg {:?}", e);
                return None;
            }
        };

        // Merge if we have disagreement.
        // Create a new merged result.
        let new_selected = MetaCacheEntry {
            name: selected.name.clone(),
            cached: Some(merged_cached),
            reusable: true,
            metadata,
        };

        warn!("decommission_pool: entries resolve entry selected {:?}", new_selected.name);
        Some(new_selected)
    }

    pub fn first_found(&self) -> (Option<MetaCacheEntry>, usize) {
        (self.0.iter().find(|x| x.is_some()).cloned().unwrap_or_default(), self.0.len())
    }
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

            self.write_obj(obj).await?;
        }

        Ok(())
    }

    pub async fn write_obj(&mut self, obj: &MetaCacheEntry) -> Result<()> {
        self.init().await?;

        rmp::encode::write_bool(&mut self.buf, true).map_err(|e| Error::other(format!("{e:?}")))?;
        rmp::encode::write_str(&mut self.buf, &obj.name).map_err(|e| Error::other(format!("{e:?}")))?;
        rmp::encode::write_bin(&mut self.buf, &obj.metadata).map_err(|e| Error::other(format!("{e:?}")))?;
        self.flush().await?;

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        rmp::encode::write_bool(&mut self.buf, false).map_err(|e| Error::other(format!("{e:?}")))?;
        self.flush().await?;
        Ok(())
    }
}

pub struct MetacacheReader<R> {
    rd: R,
    init: bool,
    err: Option<Error>,
    buf: Vec<u8>,
    offset: usize,
    current: Option<MetaCacheEntry>,
}

impl<R: AsyncRead + Unpin> MetacacheReader<R> {
    pub fn new(rd: R) -> Self {
        Self {
            rd,
            init: false,
            err: None,
            buf: Vec::new(),
            offset: 0,
            current: None,
        }
    }

    pub async fn read_more(&mut self, read_size: usize) -> Result<&[u8]> {
        let ext_size = read_size + self.offset;

        let extra = ext_size - self.offset;
        if self.buf.capacity() >= ext_size {
            // Extend the buffer if we have enough space.
            self.buf.resize(ext_size, 0);
        } else {
            self.buf.extend(vec![0u8; extra]);
        }

        let pref = self.offset;

        self.rd.read_exact(&mut self.buf[pref..ext_size]).await?;

        self.offset += read_size;

        let data = &self.buf[pref..ext_size];

        Ok(data)
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.offset = 0;
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

        let mut n = size;

        if self.current.is_some() {
            n -= 1;
            self.current = None;
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

            let l = self.read_str_len().await?;
            let _ = self.read_more(l as usize).await?;
            let l = self.read_bin_len().await?;
            let _ = self.read_more(l as usize).await?;

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

        let l = self.read_str_len().await?;

        let buf = self.read_more(l as usize).await?;
        let name_buf = buf.to_vec();
        let name = match from_utf8(&name_buf) {
            Ok(decoded) => decoded.to_owned(),
            Err(err) => {
                self.err = Some(Error::other(err.to_string()));
                return Err(Error::other(err.to_string()));
            }
        };

        let l = self.read_bin_len().await?;

        let buf = self.read_more(l as usize).await?;

        let metadata = buf.to_vec();

        self.reset();

        let entry = Some(MetaCacheEntry {
            name,
            metadata,
            cached: None,
            reusable: false,
        });
        self.current = entry.clone();

        Ok(entry)
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
    use crate::{FileMetaVersion, MetaDeleteMarker};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;
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
}
