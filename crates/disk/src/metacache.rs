use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::versioning::VersioningApi;
use crate::store_api::ObjectInfo;
use crate::utils::path::SLASH_SEPARATOR;
use rustfs_error::{Error, Result};
use rustfs_filemeta::merge_file_meta_versions;
use rustfs_filemeta::FileInfo;
use rustfs_filemeta::FileInfoVersions;
use rustfs_filemeta::FileMeta;
use rustfs_filemeta::FileMetaShallowVersion;
use rustfs_filemeta::VersionType;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Ordering;
use time::OffsetDateTime;
use tracing::warn;

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
    // name is the full name of the object including prefixes
    pub name: String,
    // Metadata. If none is present it is not an object but only a prefix.
    // Entries without metadata will only be present in non-recursive scans.
    pub metadata: Vec<u8>,

    // cached contains the metadata if decoded.
    pub cached: Option<FileMeta>,

    // Indicates the entry can be reused and only one reference to metadata is expected.
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

        match FileMeta::check_xl2_v1(&self.metadata) {
            Ok((meta, _, _)) => {
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

        if self.cached.is_some() {
            let fm = self.cached.as_ref().unwrap();
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

            let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

            return Ok(fi);
        }

        let mut fm = FileMeta::new();
        fm.unmarshal_msg(&self.metadata)?;

        let fi = fm.into_fileinfo(bucket, self.name.as_str(), "", false, false)?;

        return Ok(fi);
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

        let mut prefer = None;
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
                Err(_) => {
                    return (None, false);
                }
            },
        };
        let other_vers = match &other.cached {
            Some(file_meta) => file_meta.clone(),
            None => match FileMeta::load(&other.metadata) {
                Ok(meta) => meta,
                Err(_) => {
                    return (None, false);
                }
            },
        };

        if self_vers.versions.len() != other_vers.versions.len() {
            match self_vers.lastest_mod_time().cmp(&other_vers.lastest_mod_time()) {
                Ordering::Greater => {
                    return (Some(self.clone()), false);
                }
                Ordering::Less => {
                    return (Some(other.clone()), false);
                }
                _ => {}
            }

            if self_vers.versions.len() > other_vers.versions.len() {
                return (Some(self.clone()), false);
            }
            return (Some(other.clone()), false);
        }

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

        let metadata = match cached.marshal_msg() {
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
            cached: Some(FileMeta {
                meta_ver: cached.meta_ver,
                versions,
                ..Default::default()
            }),
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

// impl MetaCacheEntriesSortedResult {
//     pub fn entriy_list(&self) -> Vec<&MetaCacheEntry> {
//         if let Some(entries) = &self.entries {
//             entries.entries()
//         } else {
//             Vec::new()
//         }
//     }
// }

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
        if let Some(val) = marker {
            // TODO: reuse
            if let Some(idx) = self.o.0.iter().flatten().position(|v| v.name > val) {
                self.o.0 = self.o.0.split_off(idx);
            }
        }
    }
    pub async fn file_infos(&self, bucket: &str, prefix: &str, delimiter: Option<String>) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(self.o.as_ref().len());
        let mut prev_prefix = "";
        for entry in self.o.as_ref().iter().flatten() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                if let Ok(fi) = entry.to_fileinfo(bucket) {
                    // TODO:VersionPurgeStatus
                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        objects
    }

    pub async fn file_info_versions(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_v: Option<String>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(self.o.as_ref().len());
        let mut prev_prefix = "";
        let mut after_v = after_v;
        for entry in self.o.as_ref().iter().flatten() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let mut fiv = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(_err) => {
                        //
                        continue;
                    }
                };

                let fi_versions = 'c: {
                    if let Some(after_val) = &after_v {
                        if let Some(idx) = fiv.find_version_index(after_val) {
                            after_v = None;
                            break 'c fiv.versions.split_off(idx + 1);
                        }

                        after_v = None;
                        break 'c fiv.versions;
                    } else {
                        break 'c fiv.versions;
                    }
                };

                for fi in fi_versions.into_iter() {
                    // VersionPurgeStatus

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));
                }

                continue;
            }

            if entry.is_dir() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        objects
    }
}
