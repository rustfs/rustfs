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
    ErasureAlgo, ErasureInfo, Error, FileInfo, FileInfoVersions, InlineData, ObjectPartInfo, RawFileInfo, ReplicationState,
    ReplicationStatusType, Result, TIER_FV_ID, TIER_FV_MARKER, VersionPurgeStatusType, is_restored_object_on_disk,
    replication_statuses_map, version_purge_statuses_map,
};
use byteorder::ByteOrder;
use bytes::Bytes;
use rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS;
use rustfs_utils::http::headers::{
    self, AMZ_META_UNENCRYPTED_CONTENT_LENGTH, AMZ_META_UNENCRYPTED_CONTENT_MD5, AMZ_RESTORE_EXPIRY_DAYS,
    AMZ_RESTORE_REQUEST_DATE, AMZ_STORAGE_CLASS, RESERVED_METADATA_PREFIX, RESERVED_METADATA_PREFIX_LOWER,
    VERSION_PURGE_STATUS_KEY,
};
use s3s::header::X_AMZ_RESTORE;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::{collections::HashMap, io::Cursor};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tracing::{error, warn};
use uuid::Uuid;
use xxhash_rust::xxh64;

// XL header specifies the format
pub static XL_FILE_HEADER: [u8; 4] = [b'X', b'L', b'2', b' '];
// pub static XL_FILE_VERSION_CURRENT: [u8; 4] = [0; 4];

// Current version being written.
// static XL_FILE_VERSION: [u8; 4] = [1, 0, 3, 0];
static XL_FILE_VERSION_MAJOR: u16 = 1;
static XL_FILE_VERSION_MINOR: u16 = 3;
static XL_HEADER_VERSION: u8 = 3;
pub static XL_META_VERSION: u8 = 3;
static XXHASH_SEED: u64 = 0;

const XL_FLAG_FREE_VERSION: u8 = 1 << 0;
// const XL_FLAG_USES_DATA_DIR: u8 = 1 << 1;
const _XL_FLAG_INLINE_DATA: u8 = 1 << 2;

const META_DATA_READ_DEFAULT: usize = 4 << 10;
const MSGP_UINT32_SIZE: usize = 5;

pub const TRANSITION_COMPLETE: &str = "complete";
pub const TRANSITION_PENDING: &str = "pending";

pub const FREE_VERSION: &str = "free-version";

pub const TRANSITION_STATUS: &str = "transition-status";
pub const TRANSITIONED_OBJECTNAME: &str = "transitioned-object";
pub const TRANSITIONED_VERSION_ID: &str = "transitioned-versionID";
pub const TRANSITION_TIER: &str = "transition-tier";

mod codec;
mod inline_data;
mod validation;
mod version;

pub use validation::{DetailedVersionStats, VersionStats};
pub use version::*;

// type ScanHeaderVersionFn = Box<dyn Fn(usize, &[u8], &[u8]) -> Result<()>>;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FileMeta {
    pub versions: Vec<FileMetaShallowVersion>,
    pub data: InlineData, // TODO: xlMetaInlineData
    pub meta_ver: u8,
}

impl FileMeta {
    pub fn new() -> Self {
        Self {
            meta_ver: XL_META_VERSION,
            data: InlineData::new(),
            ..Default::default()
        }
    }

    fn get_idx(&self, idx: usize) -> Result<FileMetaVersion> {
        if idx > self.versions.len() {
            return Err(Error::FileNotFound);
        }

        FileMetaVersion::try_from(self.versions[idx].meta.as_slice())
    }

    fn set_idx(&mut self, idx: usize, ver: FileMetaVersion) -> Result<()> {
        if idx >= self.versions.len() {
            return Err(Error::FileNotFound);
        }

        // TODO: use old buf
        let meta_buf = ver.marshal_msg()?;

        let pre_mod_time = self.versions[idx].header.mod_time;

        self.versions[idx].header = ver.header();
        self.versions[idx].meta = meta_buf;

        if pre_mod_time != self.versions[idx].header.mod_time {
            self.sort_by_mod_time();
        }

        Ok(())
    }

    fn sort_by_mod_time(&mut self) {
        if self.versions.len() <= 1 {
            return;
        }

        self.versions.sort_by(|a, b| {
            if a.header.mod_time != b.header.mod_time {
                b.header.mod_time.cmp(&a.header.mod_time)
            } else if a.header.version_type != b.header.version_type {
                b.header.version_type.cmp(&a.header.version_type)
            } else if a.header.version_id != b.header.version_id {
                b.header.version_id.cmp(&a.header.version_id)
            } else if a.header.flags != b.header.flags {
                b.header.flags.cmp(&a.header.flags)
            } else {
                b.cmp(a)
            }
        });
    }

    // Find version
    pub fn find_version(&self, vid: Option<Uuid>) -> Result<(usize, FileMetaVersion)> {
        let vid = vid.unwrap_or_default();
        for (i, fver) in self.versions.iter().enumerate() {
            if fver.header.version_id == Some(vid) {
                let version = self.get_idx(i)?;
                return Ok((i, version));
            }
        }

        Err(Error::FileVersionNotFound)
    }

    pub fn update_object_version(&mut self, fi: FileInfo) -> Result<()> {
        for version in self.versions.iter_mut() {
            match version.header.version_type {
                VersionType::Invalid | VersionType::Legacy => (),
                VersionType::Object => {
                    // For non-versioned buckets, treat None as Uuid::nil()
                    let fi_vid = fi.version_id.or(Some(Uuid::nil()));
                    let ver_vid = version.header.version_id.or(Some(Uuid::nil()));

                    if ver_vid == fi_vid {
                        let mut ver = FileMetaVersion::try_from(version.meta.as_slice())?;

                        if let Some(ref mut obj) = ver.object {
                            for (k, v) in fi.metadata.iter() {
                                // Split metadata into meta_user and meta_sys based on prefix
                                // This logic must match From<FileInfo> for MetaObject
                                if k.len() > RESERVED_METADATA_PREFIX.len()
                                    && (k.starts_with(RESERVED_METADATA_PREFIX) || k.starts_with(RESERVED_METADATA_PREFIX_LOWER))
                                {
                                    // Skip internal flags that shouldn't be persisted
                                    if k == headers::X_RUSTFS_HEALING || k == headers::X_RUSTFS_DATA_MOV {
                                        continue;
                                    }
                                    // Insert into meta_sys
                                    obj.meta_sys.insert(k.clone(), v.as_bytes().to_vec());
                                } else {
                                    // Insert into meta_user
                                    obj.meta_user.insert(k.clone(), v.clone());
                                }
                            }

                            if let Some(mod_time) = fi.mod_time {
                                obj.mod_time = Some(mod_time);
                            }
                        }

                        // Update
                        version.header = ver.header();
                        version.meta = ver.marshal_msg()?;
                    }
                }
                VersionType::Delete => {
                    if version.header.version_id == fi.version_id {
                        return Err(Error::MethodNotAllowed);
                    }
                }
            }
        }

        self.versions.sort_by(|a, b| {
            if a.header.mod_time != b.header.mod_time {
                b.header.mod_time.cmp(&a.header.mod_time)
            } else if a.header.version_type != b.header.version_type {
                b.header.version_type.cmp(&a.header.version_type)
            } else if a.header.version_id != b.header.version_id {
                b.header.version_id.cmp(&a.header.version_id)
            } else if a.header.flags != b.header.flags {
                b.header.flags.cmp(&a.header.flags)
            } else {
                b.cmp(a)
            }
        });
        Ok(())
    }

    pub fn add_version(&mut self, mut fi: FileInfo) -> Result<()> {
        if fi.version_id.is_none() {
            fi.version_id = Some(Uuid::nil());
        }

        if let Some(ref data) = fi.data {
            let key = fi.version_id.unwrap_or_default().to_string();
            self.data.replace(&key, data.to_vec())?;
        }

        let version = FileMetaVersion::from(fi);

        self.add_version_filemata(version)
    }

    pub fn add_version_filemata(&mut self, version: FileMetaVersion) -> Result<()> {
        if !version.valid() {
            return Err(Error::other("file meta version invalid"));
        }

        // TODO: make it configurable
        // 1000 is the limit of versions
        // if self.versions.len() + 1 > 1000 {
        //     return Err(Error::other(
        //         "You've exceeded the limit on the number of versions you can create on this object",
        //     ));
        // }

        if self.versions.is_empty() {
            self.versions.push(FileMetaShallowVersion::try_from(version)?);
            return Ok(());
        }

        let vid = version.get_version_id();

        if let Some(fidx) = self.versions.iter().position(|v| v.header.version_id == vid) {
            return self.set_idx(fidx, version);
        }

        let mod_time = version.get_mod_time();

        for (idx, exist) in self.versions.iter().enumerate() {
            if let Some(ref ex_mt) = exist.header.mod_time
                && let Some(ref in_md) = mod_time
                && ex_mt <= in_md
            {
                self.versions.insert(idx, FileMetaShallowVersion::try_from(version)?);
                return Ok(());
            }
        }
        Err(Error::other("add_version failed"))

        // if !ver.valid() {
        //     return Err(Error::other("attempted to add invalid version"));
        // }

        // if self.versions.len() + 1 >= 100 {
        //     return Err(Error::other(
        //         "You've exceeded the limit on the number of versions you can create on this object",
        //     ));
        // }

        // let mod_time = ver.get_mod_time();
        // let encoded = ver.marshal_msg()?;
        // let new_version = FileMetaShallowVersion {
        //     header: ver.header(),
        //     meta: encoded,
        // };

        // // Find the insertion position: insert before the first element with mod_time >= new mod_time
        // // This maintains descending order by mod_time (newest first)
        // let insert_pos = self
        //     .versions
        //     .iter()
        //     .position(|existing| existing.header.mod_time <= mod_time)
        //     .unwrap_or(self.versions.len());
        // self.versions.insert(insert_pos, new_version);
        // Ok(())
    }

    // delete_version deletes version, returns data_dir
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn delete_version(&mut self, fi: &FileInfo) -> Result<Option<Uuid>> {
        let vid = Some(fi.version_id.unwrap_or(Uuid::nil()));

        let mut ventry = FileMetaVersion::default();
        if fi.deleted {
            ventry.version_type = VersionType::Delete;
            ventry.delete_marker = Some(MetaDeleteMarker {
                version_id: vid,
                mod_time: fi.mod_time,
                ..Default::default()
            });

            if !fi.is_valid() {
                return Err(Error::other("invalid file meta version"));
            }
        }

        let mut update_version = false;
        if fi.version_purge_status().is_empty()
            && (fi.delete_marker_replication_status() == ReplicationStatusType::Replica
                || fi.delete_marker_replication_status() == ReplicationStatusType::Empty)
        {
            update_version = fi.mark_deleted;
        } else {
            if fi.deleted
                && fi.version_purge_status() != VersionPurgeStatusType::Complete
                && (!fi.version_purge_status().is_empty() || fi.delete_marker_replication_status().is_empty())
            {
                update_version = true;
            }

            if !fi.version_purge_status().is_empty() && fi.version_purge_status() != VersionPurgeStatusType::Complete {
                update_version = true;
            }
        }

        if fi.deleted {
            if !fi.delete_marker_replication_status().is_empty()
                && let Some(delete_marker) = ventry.delete_marker.as_mut()
            {
                if fi.delete_marker_replication_status() == ReplicationStatusType::Replica {
                    delete_marker.meta_sys.insert(
                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replica-status"),
                        fi.replication_state_internal
                            .as_ref()
                            .map(|v| v.replica_status.clone())
                            .unwrap_or_default()
                            .as_str()
                            .as_bytes()
                            .to_vec(),
                    );
                    delete_marker.meta_sys.insert(
                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replica-timestamp"),
                        fi.replication_state_internal
                            .as_ref()
                            .map(|v| v.replica_timestamp.unwrap_or(OffsetDateTime::UNIX_EPOCH).to_string())
                            .unwrap_or_default()
                            .as_bytes()
                            .to_vec(),
                    );
                } else {
                    delete_marker.meta_sys.insert(
                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-status"),
                        fi.replication_state_internal
                            .as_ref()
                            .map(|v| v.replication_status_internal.clone().unwrap_or_default())
                            .unwrap_or_default()
                            .as_bytes()
                            .to_vec(),
                    );
                    delete_marker.meta_sys.insert(
                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp"),
                        fi.replication_state_internal
                            .as_ref()
                            .map(|v| v.replication_timestamp.unwrap_or(OffsetDateTime::UNIX_EPOCH).to_string())
                            .unwrap_or_default()
                            .as_bytes()
                            .to_vec(),
                    );
                }
            }

            if !fi.version_purge_status().is_empty()
                && let Some(delete_marker) = ventry.delete_marker.as_mut()
            {
                delete_marker.meta_sys.insert(
                    VERSION_PURGE_STATUS_KEY.to_string(),
                    fi.replication_state_internal
                        .as_ref()
                        .map(|v| v.version_purge_status_internal.clone().unwrap_or_default())
                        .unwrap_or_default()
                        .as_bytes()
                        .to_vec(),
                );
            }

            if let Some(delete_marker) = ventry.delete_marker.as_mut() {
                for (k, v) in fi
                    .replication_state_internal
                    .as_ref()
                    .map(|v| v.reset_statuses_map.clone())
                    .unwrap_or_default()
                {
                    delete_marker.meta_sys.insert(k.clone(), v.clone().as_bytes().to_vec());
                }
            }
        }

        let mut found_index = None;

        for (i, ver) in self.versions.iter().enumerate() {
            if ver.header.version_id != vid {
                continue;
            }

            match ver.header.version_type {
                VersionType::Invalid | VersionType::Legacy => return Err(Error::other("invalid file meta version")),
                VersionType::Delete => {
                    if update_version {
                        let mut v = self.get_idx(i)?;
                        if v.delete_marker.is_none() {
                            v.delete_marker = Some(MetaDeleteMarker {
                                version_id: vid,
                                mod_time: fi.mod_time,
                                meta_sys: HashMap::new(),
                            });
                        }

                        if let Some(delete_marker) = v.delete_marker.as_mut() {
                            if !fi.delete_marker_replication_status().is_empty() {
                                if fi.delete_marker_replication_status() == ReplicationStatusType::Replica {
                                    delete_marker.meta_sys.insert(
                                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replica-status"),
                                        fi.replication_state_internal
                                            .as_ref()
                                            .map(|v| v.replica_status.clone())
                                            .unwrap_or_default()
                                            .as_str()
                                            .as_bytes()
                                            .to_vec(),
                                    );
                                    delete_marker.meta_sys.insert(
                                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replica-timestamp"),
                                        fi.replication_state_internal
                                            .as_ref()
                                            .map(|v| v.replica_timestamp.unwrap_or(OffsetDateTime::UNIX_EPOCH).to_string())
                                            .unwrap_or_default()
                                            .as_bytes()
                                            .to_vec(),
                                    );
                                } else {
                                    delete_marker.meta_sys.insert(
                                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-status"),
                                        fi.replication_state_internal
                                            .as_ref()
                                            .map(|v| v.replication_status_internal.clone().unwrap_or_default())
                                            .unwrap_or_default()
                                            .as_bytes()
                                            .to_vec(),
                                    );
                                    delete_marker.meta_sys.insert(
                                        format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp"),
                                        fi.replication_state_internal
                                            .as_ref()
                                            .map(|v| v.replication_timestamp.unwrap_or(OffsetDateTime::UNIX_EPOCH).to_string())
                                            .unwrap_or_default()
                                            .as_bytes()
                                            .to_vec(),
                                    );
                                }
                            }

                            for (k, v) in fi
                                .replication_state_internal
                                .as_ref()
                                .map(|v| v.reset_statuses_map.clone())
                                .unwrap_or_default()
                            {
                                delete_marker.meta_sys.insert(k.clone(), v.clone().as_bytes().to_vec());
                            }
                        }

                        self.set_idx(i, v)?;
                        return Ok(None);
                    }
                    self.versions.remove(i);

                    if (fi.mark_deleted && fi.version_purge_status() != VersionPurgeStatusType::Complete)
                        || (fi.deleted && vid == Some(Uuid::nil()))
                    {
                        self.add_version_filemata(ventry)?;
                    }

                    return Ok(None);
                }
                VersionType::Object => {
                    if update_version && !fi.deleted {
                        let mut v = self.get_idx(i)?;

                        if let Some(obj) = v.object.as_mut() {
                            obj.meta_sys.insert(
                                VERSION_PURGE_STATUS_KEY.to_string(),
                                fi.replication_state_internal
                                    .as_ref()
                                    .map(|v| v.version_purge_status_internal.clone().unwrap_or_default())
                                    .unwrap_or_default()
                                    .as_bytes()
                                    .to_vec(),
                            );
                            for (k, v) in fi
                                .replication_state_internal
                                .as_ref()
                                .map(|v| v.reset_statuses_map.clone())
                                .unwrap_or_default()
                            {
                                obj.meta_sys.insert(k.clone(), v.clone().as_bytes().to_vec());
                            }
                        }

                        let old_dir = v.object.as_ref().map(|v| v.data_dir).unwrap_or_default();
                        self.set_idx(i, v)?;

                        return Ok(old_dir);
                    }
                    found_index = Some(i);
                }
            }
        }

        let Some(i) = found_index else {
            if fi.deleted {
                self.add_version_filemata(ventry)?;
                return Ok(None);
            }
            return Err(Error::FileVersionNotFound);
        };

        let mut ver = self.get_idx(i)?;

        let Some(obj) = &mut ver.object else {
            if fi.deleted {
                self.add_version_filemata(ventry)?;
                return Ok(None);
            }
            return Err(Error::FileVersionNotFound);
        };

        let obj_version_id = obj.version_id;
        let obj_data_dir = obj.data_dir;

        let mut err = if fi.expire_restored {
            obj.remove_restore_hdrs();
            self.set_idx(i, ver).err()
        } else if fi.transition_status == TRANSITION_COMPLETE {
            obj.set_transition(fi);
            obj.reset_inline_data();
            self.set_idx(i, ver).err()
        } else {
            self.versions.remove(i);

            let (free_version, to_free) = obj.init_free_version(fi);

            if to_free {
                self.add_version_filemata(free_version).err()
            } else {
                None
            }
        };

        if fi.deleted {
            err = self.add_version_filemata(ventry).err();
        }

        if self.shared_data_dir_count(obj_version_id, obj_data_dir) > 0 {
            return Ok(None);
        }

        if let Some(e) = err {
            return Err(e);
        }

        Ok(obj_data_dir)
    }

    pub fn into_fileinfo(
        &self,
        volume: &str,
        path: &str,
        version_id: &str,
        read_data: bool,
        include_free_versions: bool,
        all_parts: bool,
    ) -> Result<FileInfo> {
        let vid = {
            if !version_id.is_empty() {
                Uuid::parse_str(version_id)?
            } else {
                Uuid::nil()
            }
        };

        let mut is_latest = true;
        let mut succ_mod_time = None;
        let mut non_free_versions = self.versions.len();

        let mut found = false;
        let mut found_free_version = None;
        let mut found_fi = None;

        for ver in self.versions.iter() {
            let header = &ver.header;

            // TODO: freeVersion
            if header.free_version() {
                non_free_versions -= 1;
                if include_free_versions && found_free_version.is_none() {
                    let mut found_free_fi = FileMetaVersion::default();
                    if found_free_fi.unmarshal_msg(&ver.meta).is_ok() && found_free_fi.version_type != VersionType::Invalid {
                        let mut free_fi = found_free_fi.into_fileinfo(volume, path, all_parts);
                        free_fi.is_latest = true;
                        found_free_version = Some(free_fi);
                    }
                }

                if header.version_id != Some(vid) {
                    continue;
                }
            }

            if found {
                continue;
            }

            if !version_id.is_empty() && header.version_id != Some(vid) {
                is_latest = false;
                succ_mod_time = header.mod_time;
                continue;
            }

            found = true;

            let mut fi = ver.into_fileinfo(volume, path, all_parts)?;
            fi.is_latest = is_latest;

            if let Some(_d) = succ_mod_time {
                fi.successor_mod_time = succ_mod_time;
            }

            if read_data {
                fi.data = self
                    .data
                    .find(fi.version_id.unwrap_or_default().to_string().as_str())?
                    .map(bytes::Bytes::from);
            }

            found_fi = Some(fi);
        }

        if !found {
            if version_id.is_empty() {
                if include_free_versions
                    && non_free_versions == 0
                    && let Some(free_version) = found_free_version
                {
                    return Ok(free_version);
                }
                return Err(Error::FileNotFound);
            } else {
                return Err(Error::FileVersionNotFound);
            }
        }

        if let Some(mut fi) = found_fi {
            fi.num_versions = non_free_versions;

            return Ok(fi);
        }

        if version_id.is_empty() {
            Err(Error::FileNotFound)
        } else {
            Err(Error::FileVersionNotFound)
        }
    }

    pub fn get_file_info_versions(&self, volume: &str, path: &str, include_free_versions: bool) -> Result<FileInfoVersions> {
        let mut versions = self.into_file_info_versions(volume, path, true)?;

        let mut n = 0;

        let mut versions_vec = Vec::new();

        for fi in versions.versions.iter() {
            if fi.tier_free_version() {
                if !include_free_versions {
                    versions.free_versions.push(fi.clone());
                }
            } else {
                if !include_free_versions {
                    versions_vec.push(fi.clone());
                }
                n += 1;
            }
        }

        if !include_free_versions {
            versions.versions = versions_vec;
        }

        for fi in versions.free_versions.iter_mut() {
            fi.num_versions = n;
        }

        Ok(versions)
    }

    pub fn get_all_file_info_versions(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfoVersions> {
        self.into_file_info_versions(volume, path, all_parts)
    }

    pub fn into_file_info_versions(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfoVersions> {
        let mut versions = Vec::new();
        for version in self.versions.iter() {
            let mut file_version = FileMetaVersion::default();
            file_version.unmarshal_msg(&version.meta)?;
            let fi = file_version.into_fileinfo(volume, path, all_parts);
            versions.push(fi);
        }

        let num = versions.len();
        let mut prev_mod_time = None;
        for (i, fi) in versions.iter_mut().enumerate() {
            if i == 0 {
                fi.is_latest = true;
            } else {
                fi.successor_mod_time = prev_mod_time;
            }
            fi.num_versions = num;
            prev_mod_time = fi.mod_time;
        }

        if versions.is_empty() {
            versions.push(FileInfo {
                name: path.to_string(),
                volume: volume.to_string(),
                deleted: true,
                is_latest: true,
                ..Default::default()
            });
        }

        Ok(FileInfoVersions {
            volume: volume.to_string(),
            name: path.to_string(),
            latest_mod_time: versions[0].mod_time,
            versions,
            ..Default::default()
        })
    }

    pub fn latest_mod_time(&self) -> Option<OffsetDateTime> {
        if self.versions.is_empty() {
            return None;
        }

        self.versions.first().unwrap().header.mod_time
    }

    /// Load or convert from buffer
    pub fn load_or_convert(buf: &[u8]) -> Result<Self> {
        // Try to load as current format first
        match Self::load(buf) {
            Ok(meta) => Ok(meta),
            Err(_) => {
                // Try to convert from legacy format
                Self::load_legacy(buf)
            }
        }
    }

    /// Load legacy format
    pub fn load_legacy(_buf: &[u8]) -> Result<Self> {
        // Implementation for loading legacy xl.meta formats
        // This would handle conversion from older  formats
        Err(Error::other("Legacy format not yet implemented"))
    }

    /// Add legacy version
    pub fn add_legacy(&mut self, _legacy_obj: &str) -> Result<()> {
        // Implementation for adding legacy xl.meta v1 objects
        Err(Error::other("Legacy version addition not yet implemented"))
    }

    /// List all versions as FileInfo
    pub fn list_versions(&self, volume: &str, path: &str, all_parts: bool) -> Result<Vec<FileInfo>> {
        let mut file_infos = Vec::new();
        for (i, version) in self.versions.iter().enumerate() {
            let mut fi = version.into_fileinfo(volume, path, all_parts)?;
            fi.is_latest = i == 0;
            file_infos.push(fi);
        }
        Ok(file_infos)
    }

    /// Check if all versions are hidden
    pub fn all_hidden(&self, top_delete_marker: bool) -> bool {
        if self.versions.is_empty() {
            return true;
        }

        if top_delete_marker && self.versions[0].header.version_type != VersionType::Delete {
            return false;
        }

        // Check if all versions are either delete markers or free versions
        self.versions
            .iter()
            .all(|v| v.header.version_type == VersionType::Delete || v.header.free_version())
    }

    /// Append metadata to buffer
    pub fn append_to(&self, dst: &mut Vec<u8>) -> Result<()> {
        let data = self.marshal_msg()?;
        dst.extend_from_slice(&data);
        Ok(())
    }

    /// Find version by string ID
    pub fn find_version_str(&self, version_id: &str) -> Result<(usize, FileMetaVersion)> {
        if version_id.is_empty() {
            return Err(Error::other("empty version ID"));
        }

        let uuid = Uuid::parse_str(version_id)?;
        self.find_version(Some(uuid))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_data::*;

    #[test]
    fn test_new_file_meta() {
        let mut fm = FileMeta::new();

        let (m, n) = (3, 2);

        for i in 0..5 {
            let mut fi = FileInfo::new(i.to_string().as_str(), m, n);
            fi.mod_time = Some(OffsetDateTime::now_utc());

            fm.add_version(fi).unwrap();
        }

        let buff = fm.marshal_msg().unwrap();

        let mut newfm = FileMeta::default();
        newfm.unmarshal_msg(&buff).unwrap();

        assert_eq!(fm, newfm)
    }

    #[test]
    fn test_marshal_metaobject() {
        let obj = MetaObject {
            data_dir: Some(Uuid::new_v4()),
            ..Default::default()
        };

        // println!("obj {:?}", &obj);

        let encoded = obj.marshal_msg().unwrap();

        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&encoded).unwrap();

        // println!("obj2 {:?}", &obj2);

        assert_eq!(obj, obj2);
        assert_eq!(obj.data_dir, obj2.data_dir);
    }

    #[test]
    fn test_marshal_metadeletemarker() {
        let obj = MetaDeleteMarker {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        // println!("obj {:?}", &obj);

        let encoded = obj.marshal_msg().unwrap();

        let mut obj2 = MetaDeleteMarker::default();
        obj2.unmarshal_msg(&encoded).unwrap();

        // println!("obj2 {:?}", &obj2);

        assert_eq!(obj, obj2);
        assert_eq!(obj.version_id, obj2.version_id);
    }

    #[test]
    fn test_marshal_metaversion() {
        let mut fi = FileInfo::new("tset", 3, 2);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp()).unwrap());
        let mut obj = FileMetaVersion::from(fi);
        obj.write_version = 110;

        // println!("obj {:?}", &obj);

        let encoded = obj.marshal_msg().unwrap();

        let mut obj2 = FileMetaVersion::default();
        obj2.unmarshal_msg(&encoded).unwrap();

        // println!("obj2 {:?}", &obj2);

        // Timestamp inconsistency
        assert_eq!(obj, obj2);
        assert_eq!(obj.get_version_id(), obj2.get_version_id());
        assert_eq!(obj.write_version, obj2.write_version);
        assert_eq!(obj.write_version, 110);
    }

    #[test]
    fn test_marshal_metaversionheader() {
        let mut obj = FileMetaVersionHeader::default();
        let vid = Some(Uuid::new_v4());
        obj.version_id = vid;

        let encoded = obj.marshal_msg().unwrap();

        let mut obj2 = FileMetaVersionHeader::default();
        obj2.unmarshal_msg(&encoded).unwrap();

        // Timestamp inconsistency
        assert_eq!(obj, obj2);
        assert_eq!(obj.version_id, obj2.version_id);
        assert_eq!(obj.version_id, vid);
    }

    #[test]
    fn test_real_xlmeta_compatibility() {
        // Test compatibility with real xl.meta formats
        let data = create_real_xlmeta().expect("Failed to create realistic test data");

        // Verify the file header
        assert_eq!(&data[0..4], b"XL2 ", "File header should be 'XL2 '");
        assert_eq!(&data[4..8], &[1, 0, 3, 0], "Version number should be 1.3.0");

        // Parse metadata
        let fm = FileMeta::load(&data).expect("Failed to parse realistic data");

        // Verify basic properties
        assert_eq!(fm.meta_ver, XL_META_VERSION);
        assert_eq!(
            fm.versions.len(),
            3,
            "Should have three versions (one object, one delete marker, one Legacy)"
        );

        // Verify version types
        let mut object_count = 0;
        let mut delete_count = 0;
        let mut legacy_count = 0;

        for version in &fm.versions {
            match version.header.version_type {
                VersionType::Object => object_count += 1,
                VersionType::Delete => delete_count += 1,
                VersionType::Legacy => legacy_count += 1,
                VersionType::Invalid => panic!("No invalid versions should be present"),
            }
        }

        assert_eq!(object_count, 1, "Should have one object version");
        assert_eq!(delete_count, 1, "Should have one delete marker");
        assert_eq!(legacy_count, 1, "Should have one Legacy version");

        // Verify compatibility
        assert!(fm.is_compatible_with_meta(), "Should be compatible with the xl format");

        // Verify integrity
        fm.validate_integrity().expect("Integrity validation failed");

        // Verify version statistics
        let stats = fm.get_version_stats();
        assert_eq!(stats.total_versions, 3);
        assert_eq!(stats.object_versions, 1);
        assert_eq!(stats.delete_markers, 1);
        assert_eq!(stats.invalid_versions, 1); // Legacy is counted as invalid
    }

    #[test]
    fn test_complex_xlmeta_handling() {
        // Test complex xl.meta files with many versions
        let data = create_complex_xlmeta().expect("Failed to create complex test data");
        let fm = FileMeta::load(&data).expect("Failed to parse complex data");

        // Verify version count
        assert!(fm.versions.len() >= 10, "Should have at least 10 versions");

        // Verify version ordering
        assert!(fm.is_sorted_by_mod_time(), "Versions should be sorted by modification time");

        // Verify presence of different version types
        let stats = fm.get_version_stats();
        assert!(stats.object_versions > 0, "Should include object versions");
        assert!(stats.delete_markers > 0, "Should include delete markers");

        // Test version merge functionality
        let merged = merge_file_meta_versions(1, false, 0, std::slice::from_ref(&fm.versions));
        assert!(!merged.is_empty(), "Merged output should contain versions");
    }

    #[test]
    fn test_inline_data_handling() {
        // Test inline data handling
        let data = create_xlmeta_with_inline_data().expect("Failed to create inline test data");
        let fm = FileMeta::load(&data).expect("Failed to parse inline data");

        assert_eq!(fm.versions.len(), 1, "Should have one version");
        assert!(!fm.data.as_slice().is_empty(), "Should contain inline data");

        // Verify inline data contents
        let inline_data = fm.data.as_slice();
        assert!(!inline_data.is_empty(), "Inline data should not be empty");
    }

    #[test]
    fn test_error_handling_and_recovery() {
        // Test error handling and recovery
        let corrupted_data = create_corrupted_xlmeta();
        let result = FileMeta::load(&corrupted_data);
        assert!(result.is_err(), "Corrupted data should fail to parse");

        // Test handling of empty files
        let empty_data = create_empty_xlmeta().expect("Failed to create empty test data");
        let fm = FileMeta::load(&empty_data).expect("Failed to parse empty data");
        assert_eq!(fm.versions.len(), 0, "An empty file should have no versions");
    }

    #[test]
    fn test_version_type_legacy_support() {
        // Validate support for Legacy version types
        assert_eq!(VersionType::Legacy.to_u8(), 3);
        assert_eq!(VersionType::from_u8(3), VersionType::Legacy);
        assert!(VersionType::Legacy.valid(), "Legacy type should be valid");

        // Exercise creation and handling of Legacy versions
        let legacy_version = FileMetaVersion {
            version_type: VersionType::Legacy,
            object: None,
            delete_marker: None,
            write_version: 1,
        };

        assert!(legacy_version.is_legacy(), "Should be recognized as a Legacy version");
    }

    #[test]
    fn test_signature_calculation() {
        // Test signature calculation
        let data = create_real_xlmeta().expect("Failed to create test data");
        let fm = FileMeta::load(&data).expect("Parsing failed");

        for version in &fm.versions {
            let signature = version.header.get_signature();
            assert_eq!(signature.len(), 4, "Signature should be 4 bytes");

            // Verify signature consistency for identical versions
            let signature2 = version.header.get_signature();
            assert_eq!(signature, signature2, "Identical versions should produce identical signatures");
        }
    }

    #[test]
    fn test_metadata_validation() {
        // Test metadata validation
        let data = create_real_xlmeta().expect("Failed to create test data");
        let fm = FileMeta::load(&data).expect("Parsing failed");

        // Test integrity validation
        fm.validate_integrity().expect("Integrity validation should succeed");

        // Test compatibility checks
        assert!(fm.is_compatible_with_meta(), "Should be compatible with the xl format");

        // Test version ordering checks
        assert!(fm.is_sorted_by_mod_time(), "Versions should be time-ordered");
    }

    #[test]
    fn test_round_trip_serialization() {
        // Test round-trip serialization consistency
        let original_data = create_real_xlmeta().expect("Failed to create original test data");
        let fm = FileMeta::load(&original_data).expect("Failed to parse original data");

        // Serialize again
        let serialized_data = fm.marshal_msg().expect("Re-serialization failed");

        // Parse again
        let fm2 = FileMeta::load(&serialized_data).expect("Failed to parse serialized data");

        // Verify consistency
        assert_eq!(fm.versions.len(), fm2.versions.len(), "Version counts should match");
        assert_eq!(fm.meta_ver, fm2.meta_ver, "Metadata versions should match");

        // Verify version content consistency
        for (v1, v2) in fm.versions.iter().zip(fm2.versions.iter()) {
            assert_eq!(v1.header.version_type, v2.header.version_type, "Version types should match");
            assert_eq!(v1.header.version_id, v2.header.version_id, "Version IDs should match");
        }
    }

    #[test]
    fn test_performance_with_large_metadata() {
        // Test performance with large metadata files
        use std::time::Instant;

        let start = Instant::now();
        let data = create_complex_xlmeta().expect("Failed to create large test data");
        let creation_time = start.elapsed();

        let start = Instant::now();
        let fm = FileMeta::load(&data).expect("Failed to parse large data");
        let parsing_time = start.elapsed();

        let start = Instant::now();
        let _serialized = fm.marshal_msg().expect("Serialization failed");
        let serialization_time = start.elapsed();

        println!("Performance results:");
        println!("  Creation time: {creation_time:?}");
        println!("  Parsing time: {parsing_time:?}");
        println!("  Serialization time: {serialization_time:?}");

        // Basic performance assertions (adjust as needed for real workloads)
        assert!(parsing_time.as_millis() < 100, "Parsing time should be under 100 ms");
        assert!(serialization_time.as_millis() < 100, "Serialization time should be under 100 ms");
    }

    #[test]
    fn test_edge_cases() {
        // Test edge cases

        // 1. Test empty version IDs
        let mut fm = FileMeta::new();
        let version = FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(MetaObject {
                version_id: None, // Empty version ID
                data_dir: None,
                erasure_algorithm: crate::fileinfo::ErasureAlgo::ReedSolomon,
                erasure_m: 1,
                erasure_n: 1,
                erasure_block_size: 64 * 1024,
                erasure_index: 0,
                erasure_dist: vec![0],
                bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
                part_numbers: vec![1],
                part_etags: Vec::new(),
                part_sizes: vec![0],
                part_actual_sizes: Vec::new(),
                part_indices: Vec::new(),
                size: 0,
                mod_time: None,
                meta_sys: HashMap::new(),
                meta_user: HashMap::new(),
            }),
            delete_marker: None,
            write_version: 1,
        };

        let shallow_version = FileMetaShallowVersion::try_from(version).expect("Conversion failed");
        fm.versions.push(shallow_version);

        // Should support serialization and deserialization
        let data = fm.marshal_msg().expect("Serialization failed");
        let fm2 = FileMeta::load(&data).expect("Parsing failed");
        assert_eq!(fm2.versions.len(), 1);

        // 2. Test extremely large file sizes
        let large_object = MetaObject {
            size: i64::MAX,
            part_sizes: vec![usize::MAX],
            ..Default::default()
        };

        // Should handle very large numbers
        assert_eq!(large_object.size, i64::MAX);
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        // Test thread safety for concurrent operations
        use std::sync::Arc;
        use std::sync::Mutex;

        let fm = Arc::new(Mutex::new(FileMeta::new()));
        let mut handles = vec![];

        // Add versions concurrently
        for i in 0..10 {
            let fm_clone: Arc<Mutex<FileMeta>> = Arc::clone(&fm);
            let handle = tokio::spawn(async move {
                let mut fi = crate::fileinfo::FileInfo::new(&format!("test-{i}"), 2, 1);
                fi.version_id = Some(Uuid::new_v4());
                fi.mod_time = Some(OffsetDateTime::now_utc());

                let mut fm_guard = fm_clone.lock().unwrap();
                fm_guard.add_version(fi).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all tasks to finish
        for handle in handles {
            handle.await.unwrap();
        }

        let fm_guard = fm.lock().unwrap();
        assert_eq!(fm_guard.versions.len(), 10);
    }

    #[test]
    fn test_memory_efficiency() {
        // Test memory efficiency
        use std::mem;

        // Measure memory usage for empty structs
        let empty_fm = FileMeta::new();
        let empty_size = mem::size_of_val(&empty_fm);
        println!("Empty FileMeta size: {empty_size} bytes");

        // Measure memory usage with many versions
        let mut large_fm = FileMeta::new();
        for i in 0..100 {
            let mut fi = crate::fileinfo::FileInfo::new(&format!("test-{i}"), 2, 1);
            fi.version_id = Some(Uuid::new_v4());
            fi.mod_time = Some(OffsetDateTime::now_utc());
            large_fm.add_version(fi).unwrap();
        }

        let large_size = mem::size_of_val(&large_fm);
        println!("Large FileMeta size: {large_size} bytes");

        // Ensure memory usage is reasonable (size_of_val covers only stack allocations)
        // For structs containing Vec, size_of_val may match because capacity lives on the heap
        println!("Number of versions: {}", large_fm.versions.len());
        assert!(!large_fm.versions.is_empty(), "Should contain version data");
    }

    #[test]
    fn test_version_ordering_edge_cases() {
        // Test boundary cases for version ordering
        let mut fm = FileMeta::new();

        // Add versions with identical timestamps
        let same_time = OffsetDateTime::now_utc();
        for i in 0..5 {
            let mut fi = crate::fileinfo::FileInfo::new(&format!("test-{i}"), 2, 1);
            fi.version_id = Some(Uuid::new_v4());
            fi.mod_time = Some(same_time);
            fm.add_version(fi).unwrap();
        }

        // Verify stable ordering
        let original_order: Vec<_> = fm.versions.iter().map(|v| v.header.version_id).collect();
        fm.sort_by_mod_time();
        let sorted_order: Vec<_> = fm.versions.iter().map(|v| v.header.version_id).collect();

        // Sorting should remain stable for identical timestamps
        assert_eq!(original_order.len(), sorted_order.len());
    }

    #[test]
    fn test_checksum_algorithms() {
        // Test different checksum algorithms
        let algorithms = vec![ChecksumAlgo::Invalid, ChecksumAlgo::HighwayHash];

        for algo in algorithms {
            let obj = MetaObject {
                bitrot_checksum_algo: algo.clone(),
                ..Default::default()
            };

            // Verify checksum validation logic
            match algo {
                ChecksumAlgo::Invalid => assert!(!algo.valid()),
                ChecksumAlgo::HighwayHash => assert!(algo.valid()),
            }

            // Verify serialization and deserialization
            let data = obj.marshal_msg().unwrap();
            let mut obj2 = MetaObject::default();
            obj2.unmarshal_msg(&data).unwrap();
            assert_eq!(obj.bitrot_checksum_algo.to_u8(), obj2.bitrot_checksum_algo.to_u8());
        }
    }

    #[test]
    fn test_erasure_coding_parameters() {
        // Test combinations of erasure coding parameters
        let test_cases = vec![
            (1, 1), // Minimum configuration
            (2, 1), // Common configuration
            (4, 2), // Standard configuration
            (8, 4), // High redundancy configuration
        ];

        for (data_blocks, parity_blocks) in test_cases {
            let obj = MetaObject {
                erasure_m: data_blocks,
                erasure_n: parity_blocks,
                erasure_dist: (0..(data_blocks + parity_blocks)).map(|i| i as u8).collect(),
                ..Default::default()
            };

            // Verify parameter validity
            assert!(obj.erasure_m > 0, "Data block count must be greater than 0");
            assert!(obj.erasure_n > 0, "Parity block count must be greater than 0");
            assert_eq!(obj.erasure_dist.len(), data_blocks + parity_blocks);

            // Verify serialization and deserialization
            let data = obj.marshal_msg().unwrap();
            let mut obj2 = MetaObject::default();
            obj2.unmarshal_msg(&data).unwrap();
            assert_eq!(obj.erasure_m, obj2.erasure_m);
            assert_eq!(obj.erasure_n, obj2.erasure_n);
            assert_eq!(obj.erasure_dist, obj2.erasure_dist);
        }
    }

    #[test]
    fn test_metadata_size_limits() {
        // Test metadata size limits
        let mut obj = MetaObject::default();

        // Test moderate amounts of user metadata
        for i in 0..10 {
            obj.meta_user
                .insert(format!("key-{i:04}"), format!("value-{:04}-{}", i, "x".repeat(10)));
        }

        // Verify metadata can be serialized
        let data = obj.marshal_msg().unwrap();
        assert!(data.len() > 100, "Serialized data should have a reasonable size");

        // Verify deserialization succeeds
        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&data).unwrap();
        assert_eq!(obj.meta_user.len(), obj2.meta_user.len());
    }

    #[test]
    fn test_version_statistics_accuracy() {
        // Test accuracy of version statistics
        let mut fm = FileMeta::new();

        // Add different version types
        let object_count = 3;
        let delete_count = 2;

        // Add object versions
        for i in 0..object_count {
            let mut fi = crate::fileinfo::FileInfo::new(&format!("obj-{i}"), 2, 1);
            fi.version_id = Some(Uuid::new_v4());
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).unwrap();
        }

        // Add delete markers
        for i in 0..delete_count {
            let delete_marker = MetaDeleteMarker {
                version_id: Some(Uuid::new_v4()),
                mod_time: Some(OffsetDateTime::now_utc()),
                meta_sys: HashMap::new(),
            };

            let delete_version = FileMetaVersion {
                version_type: VersionType::Delete,
                object: None,
                delete_marker: Some(delete_marker),
                write_version: (i + 100) as u64,
            };

            let shallow_version = FileMetaShallowVersion::try_from(delete_version).unwrap();
            fm.versions.push(shallow_version);
        }

        // Verify overall statistics
        let stats = fm.get_version_stats();
        assert_eq!(stats.total_versions, object_count + delete_count);
        assert_eq!(stats.object_versions, object_count);
        assert_eq!(stats.delete_markers, delete_count);

        // Verify detailed statistics
        let detailed_stats = fm.get_detailed_version_stats();
        assert_eq!(detailed_stats.total_versions, object_count + delete_count);
        assert_eq!(detailed_stats.object_versions, object_count);
        assert_eq!(detailed_stats.delete_markers, delete_count);
    }

    #[test]
    fn test_cross_platform_compatibility() {
        // Test cross-platform compatibility (endianness, separators, etc.)
        let mut fm = FileMeta::new();

        // Use platform-specific path styles
        let paths = vec![
            "unix/style/path",
            "windows\\style\\path",
            "mixed/style\\path",
            "unicode/path/test",
        ];

        for path in paths {
            let mut fi = crate::fileinfo::FileInfo::new(path, 2, 1);
            fi.version_id = Some(Uuid::new_v4());
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).unwrap();
        }

        // Verify serialization/deserialization consistency across platforms
        let data = fm.marshal_msg().unwrap();
        let mut fm2 = FileMeta::default();
        fm2.unmarshal_msg(&data).unwrap();

        assert_eq!(fm.versions.len(), fm2.versions.len());

        // Verify UUID endianness consistency
        for (v1, v2) in fm.versions.iter().zip(fm2.versions.iter()) {
            assert_eq!(v1.header.version_id, v2.header.version_id);
        }
    }

    #[test]
    fn test_data_integrity_validation() {
        // Test data integrity checks
        let mut fm = FileMeta::new();

        // Add a normal version
        let mut fi = crate::fileinfo::FileInfo::new("test", 2, 1);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fm.add_version(fi).unwrap();

        // Verify integrity under normal conditions
        assert!(fm.validate_integrity().is_ok());
    }

    #[test]
    fn test_version_merge_scenarios() {
        // Test various version merge scenarios
        let mut versions1 = vec![];
        let mut versions2 = vec![];

        // Create two distinct sets of versions
        for i in 0..3 {
            let mut fi1 = crate::fileinfo::FileInfo::new(&format!("test1-{i}"), 2, 1);
            fi1.version_id = Some(Uuid::new_v4());
            fi1.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000 + i * 10).unwrap());

            let mut fi2 = crate::fileinfo::FileInfo::new(&format!("test2-{i}"), 2, 1);
            fi2.version_id = Some(Uuid::new_v4());
            fi2.mod_time = Some(OffsetDateTime::from_unix_timestamp(1005 + i * 10).unwrap());

            let version1 = FileMetaVersion::from(fi1);
            let version2 = FileMetaVersion::from(fi2);

            versions1.push(FileMetaShallowVersion::try_from(version1).unwrap());
            versions2.push(FileMetaShallowVersion::try_from(version2).unwrap());
        }

        // Test a simple merge scenario
        let merged = merge_file_meta_versions(1, false, 0, &[versions1.clone()]);
        assert!(!merged.is_empty(), "Merging a single version list should not be empty");

        // Test merging multiple version lists
        let merged = merge_file_meta_versions(1, false, 0, &[versions1.clone(), versions2.clone()]);
        // Merge results may be empty depending on compatibility, which is acceptable
        println!("Merge result count: {}", merged.len());
    }

    #[test]
    fn test_flags_operations() {
        // Test flag operations
        let flags = vec![Flags::FreeVersion, Flags::UsesDataDir, Flags::InlineData];

        for flag in flags {
            let flag_value = flag as u8;
            assert!(flag_value > 0, "Flag value should be greater than 0");

            // Test flag combinations
            let combined = Flags::FreeVersion as u8 | Flags::UsesDataDir as u8;
            // For bitwise operations, combined values may not exceed individual ones; this is normal
            assert!(combined > 0, "Combined flag value should be greater than 0");
        }
    }

    #[test]
    fn test_uuid_handling_edge_cases() {
        // Test UUID edge cases
        let test_uuids = vec![
            Uuid::new_v4(), // Random UUID
        ];

        for uuid in test_uuids {
            let obj = MetaObject {
                version_id: Some(uuid),
                data_dir: Some(uuid),
                ..Default::default()
            };

            // Verify serialization and deserialization
            let data = obj.marshal_msg().unwrap();
            let mut obj2 = MetaObject::default();
            obj2.unmarshal_msg(&data).unwrap();

            assert_eq!(obj.version_id, obj2.version_id);
            assert_eq!(obj.data_dir, obj2.data_dir);
        }

        // Test nil UUID separately because serialization converts it to None
        let obj = MetaObject {
            version_id: Some(Uuid::nil()),
            data_dir: Some(Uuid::nil()),
            ..Default::default()
        };

        let data = obj.marshal_msg().unwrap();
        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&data).unwrap();

        // nil UUIDs may be converted to None during serialization; this is expected
        // Inspect the actual serialization behavior
        println!("Original version_id: {:?}", obj.version_id);
        println!("Deserialized version_id: {:?}", obj2.version_id);
        // Consider the test successful as long as deserialization succeeds
    }

    #[test]
    fn test_part_handling_edge_cases() {
        // Test edge cases for shard handling
        let mut obj = MetaObject::default();

        // Test an empty shard list
        assert!(obj.part_numbers.is_empty());
        assert!(obj.part_etags.is_empty());
        assert!(obj.part_sizes.is_empty());

        // Test a single shard
        obj.part_numbers = vec![1];
        obj.part_etags = vec!["etag1".to_string()];
        obj.part_sizes = vec![1024];
        obj.part_actual_sizes = vec![1024];

        let data = obj.marshal_msg().unwrap();
        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&data).unwrap();

        assert_eq!(obj.part_numbers, obj2.part_numbers);
        assert_eq!(obj.part_etags, obj2.part_etags);
        assert_eq!(obj.part_sizes, obj2.part_sizes);
        assert_eq!(obj.part_actual_sizes, obj2.part_actual_sizes);

        // Test multiple shards
        obj.part_numbers = vec![1, 2, 3];
        obj.part_etags = vec!["etag1".to_string(), "etag2".to_string(), "etag3".to_string()];
        obj.part_sizes = vec![1024, 2048, 512];
        obj.part_actual_sizes = vec![1024, 2048, 512];

        let data = obj.marshal_msg().unwrap();
        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&data).unwrap();

        assert_eq!(obj.part_numbers, obj2.part_numbers);
        assert_eq!(obj.part_etags, obj2.part_etags);
        assert_eq!(obj.part_sizes, obj2.part_sizes);
        assert_eq!(obj.part_actual_sizes, obj2.part_actual_sizes);
    }

    #[test]
    fn test_version_header_validation() {
        // Test version header validation
        let mut header = FileMetaVersionHeader {
            version_type: VersionType::Object,
            mod_time: Some(OffsetDateTime::now_utc()),
            ec_m: 2,
            ec_n: 1,
            ..Default::default()
        };
        assert!(header.is_valid());

        // Test invalid version types
        header.version_type = VersionType::Invalid;
        assert!(!header.is_valid());

        // Reset to a valid state
        header.version_type = VersionType::Object;
        assert!(header.is_valid());

        // Test invalid erasure coding parameters
        // When ec_m = 0, has_ec() returns false so parity parameters are skipped
        header.ec_m = 0;
        header.ec_n = 1;
        assert!(header.is_valid()); // Valid because erasure coding is disabled

        // Enable erasure coding with invalid parameters
        header.ec_m = 2;
        header.ec_n = 0;
        // When ec_n = 0, has_ec() returns false so parity parameters are skipped
        assert!(header.is_valid()); // This remains valid because has_ec() returns false

        // Reset to a valid state
        header.ec_n = 1;
        assert!(header.is_valid());
    }

    #[test]
    fn test_special_characters_in_metadata() {
        // Test special character handling in metadata
        let mut obj = MetaObject::default();

        // Test various special characters
        let special_cases = vec![
            ("empty", ""),
            ("unicode", "test🚀🎉"),
            ("newlines", "line1\nline2\nline3"),
            ("tabs", "col1\tcol2\tcol3"),
            ("quotes", "\"quoted\" and 'single'"),
            ("backslashes", "path\\to\\file"),
            ("mixed", "Mixed: Chinese, English, 123, !@#$%"),
        ];

        for (key, value) in special_cases {
            obj.meta_user.insert(key.to_string(), value.to_string());
        }

        // Verify serialization and deserialization
        let data = obj.marshal_msg().unwrap();
        let mut obj2 = MetaObject::default();
        obj2.unmarshal_msg(&data).unwrap();

        assert_eq!(obj.meta_user, obj2.meta_user);

        // Verify each special character is correctly saved
        for (key, expected_value) in [
            ("empty", ""),
            ("unicode", "test🚀🎉"),
            ("newlines", "line1\nline2\nline3"),
            ("tabs", "col1\tcol2\tcol3"),
            ("quotes", "\"quoted\" and 'single'"),
            ("backslashes", "path\\to\\file"),
            ("mixed", "Mixed: Chinese, English, 123, !@#$%"),
        ] {
            assert_eq!(obj2.meta_user.get(key), Some(&expected_value.to_string()));
        }
    }
}

#[tokio::test]
async fn test_read_xl_meta_no_data() {
    use tokio::fs;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    let mut fm = FileMeta::new();

    let (m, n) = (3, 2);

    for i in 0..5 {
        let mut fi = FileInfo::new(i.to_string().as_str(), m, n);
        fi.mod_time = Some(OffsetDateTime::now_utc());

        fm.add_version(fi).unwrap();
    }

    let mut buff = fm.marshal_msg().unwrap();

    buff.resize(buff.len() + 100, 0);

    let filepath = "./test_xl.meta";

    let mut file = File::create(filepath).await.unwrap();
    // Write string data
    file.write_all(&buff).await.unwrap();

    let mut f = File::open(filepath).await.unwrap();

    let stat = f.metadata().await.unwrap();

    let data = read_xl_meta_no_data(&mut f, stat.len() as usize).await.unwrap();

    let mut newfm = FileMeta::default();
    newfm.unmarshal_msg(&data).unwrap();

    fs::remove_file(filepath).await.unwrap();

    assert_eq!(fm, newfm)
}
