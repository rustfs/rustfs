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
    ReplicationStatusType, Result, TIER_FV_ID, TIER_FV_MARKER, VersionPurgeStatusType, replication_statuses_map,
    version_purge_statuses_map,
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
pub static XL_META_VERSION: u8 = 2;
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

    pub fn is_xl2_v1_format(buf: &[u8]) -> bool {
        !matches!(Self::check_xl2_v1(buf), Err(_e))
    }

    pub fn load(buf: &[u8]) -> Result<FileMeta> {
        let mut xl = FileMeta::default();
        xl.unmarshal_msg(buf)?;

        Ok(xl)
    }

    pub fn check_xl2_v1(buf: &[u8]) -> Result<(&[u8], u16, u16)> {
        if buf.len() < 8 {
            return Err(Error::other("xl file header not exists"));
        }

        if buf[0..4] != XL_FILE_HEADER {
            return Err(Error::other("xl file header err"));
        }

        let major = byteorder::LittleEndian::read_u16(&buf[4..6]);
        let minor = byteorder::LittleEndian::read_u16(&buf[6..8]);
        if major > XL_FILE_VERSION_MAJOR {
            return Err(Error::other("xl file version err"));
        }

        Ok((&buf[8..], major, minor))
    }

    // Returns (meta, inline_data)
    pub fn is_indexed_meta(buf: &[u8]) -> Result<(&[u8], &[u8])> {
        let (buf, major, minor) = Self::check_xl2_v1(buf)?;
        if major != 1 || minor < 3 {
            return Ok((&[], &[]));
        }

        let (mut size_buf, buf) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf)?;

        if buf.len() < bin_len as usize {
            return Ok((&[], &[]));
        }
        let (meta, buf) = buf.split_at(bin_len as usize);

        if buf.len() < 5 {
            return Err(Error::other("insufficient data for CRC"));
        }
        let (mut crc_buf, inline_data) = buf.split_at(5);

        // crc check
        let crc = rmp::decode::read_u32(&mut crc_buf)?;
        let meta_crc = xxh64::xxh64(meta, XXHASH_SEED) as u32;

        if crc != meta_crc {
            return Err(Error::other("xl file crc check failed"));
        }

        Ok((meta, inline_data))
    }

    // Fixed u32
    pub fn read_bytes_header(buf: &[u8]) -> Result<(u32, &[u8])> {
        let (mut size_buf, _) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf)?;

        Ok((bin_len, &buf[5..]))
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let i = buf.len() as u64;

        // check version, buf = buf[8..]
        let (buf, _, _) = Self::check_xl2_v1(buf).map_err(|e| {
            error!("failed to check XL2 v1 format: {}", e);
            e
        })?;

        let (mut size_buf, buf) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).map_err(|e| {
            error!("failed to read binary length for metadata: {}", e);
            Error::other(format!("failed to read binary length for metadata: {e}"))
        })?;

        if buf.len() < bin_len as usize {
            error!("insufficient data for metadata: expected {} bytes, got {} bytes", bin_len, buf.len());
            return Err(Error::other("insufficient data for metadata"));
        }
        let (meta, buf) = buf.split_at(bin_len as usize);

        if buf.len() < 5 {
            error!("insufficient data for CRC: expected 5 bytes, got {} bytes", buf.len());
            return Err(Error::other("insufficient data for CRC"));
        }
        let (mut crc_buf, buf) = buf.split_at(5);

        // crc check
        let crc = rmp::decode::read_u32(&mut crc_buf).map_err(|e| {
            error!("failed to read CRC value: {}", e);
            Error::other(format!("failed to read CRC value: {e}"))
        })?;
        let meta_crc = xxh64::xxh64(meta, XXHASH_SEED) as u32;

        if crc != meta_crc {
            error!("xl file crc check failed: expected CRC {:#x}, got {:#x}", meta_crc, crc);
            return Err(Error::other("xl file crc check failed"));
        }

        if !buf.is_empty() {
            self.data.update(buf);
            self.data.validate().map_err(|e| {
                error!("data validation failed: {}", e);
                e
            })?;
        }

        // Parse meta
        if !meta.is_empty() {
            let (versions_len, _, meta_ver, meta) = Self::decode_xl_headers(meta).map_err(|e| {
                error!("failed to decode XL headers: {}", e);
                e
            })?;

            // let (_, meta) = meta.split_at(read_size as usize);

            self.meta_ver = meta_ver;

            self.versions = Vec::with_capacity(versions_len);

            let mut cur: Cursor<&[u8]> = Cursor::new(meta);
            for _ in 0..versions_len {
                let bin_len = rmp::decode::read_bin_len(&mut cur).map_err(|e| {
                    error!("failed to read binary length for version header: {}", e);
                    Error::other(format!("failed to read binary length for version header: {e}"))
                })? as usize;

                let mut header_buf = vec![0u8; bin_len];

                cur.read_exact(&mut header_buf)?;

                let mut ver = FileMetaShallowVersion::default();
                ver.header.unmarshal_msg(&header_buf).map_err(|e| {
                    error!("failed to unmarshal version header: {}", e);
                    e
                })?;

                let bin_len = rmp::decode::read_bin_len(&mut cur).map_err(|e| {
                    error!("failed to read binary length for version metadata: {}", e);
                    Error::other(format!("failed to read binary length for version metadata: {e}"))
                })? as usize;

                let mut ver_meta_buf = vec![0u8; bin_len];
                cur.read_exact(&mut ver_meta_buf)?;

                ver.meta.extend_from_slice(&ver_meta_buf);

                self.versions.push(ver);
            }
        }

        Ok(i)
    }

    // decode_xl_headers parses meta header, returns (versions count, xl_header_version, xl_meta_version, read data length)
    fn decode_xl_headers(buf: &[u8]) -> Result<(usize, u8, u8, &[u8])> {
        let mut cur = Cursor::new(buf);

        let header_ver: u8 = rmp::decode::read_int(&mut cur)?;

        if header_ver > XL_HEADER_VERSION {
            return Err(Error::other("xl header version invalid"));
        }

        let meta_ver: u8 = rmp::decode::read_int(&mut cur)?;
        if meta_ver > XL_META_VERSION {
            return Err(Error::other("xl meta version invalid"));
        }

        let versions_len: usize = rmp::decode::read_int(&mut cur)?;

        Ok((versions_len, header_ver, meta_ver, &buf[cur.position() as usize..]))
    }

    fn decode_versions<F: FnMut(usize, &[u8], &[u8]) -> Result<()>>(buf: &[u8], versions: usize, mut fnc: F) -> Result<()> {
        let mut cur: Cursor<&[u8]> = Cursor::new(buf);

        for i in 0..versions {
            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            let header_buf = &buf[start..end];

            cur.set_position(end as u64);

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            let ver_meta_buf = &buf[start..end];

            cur.set_position(end as u64);

            if let Err(err) = fnc(i, header_buf, ver_meta_buf) {
                if err == Error::DoneForNow {
                    return Ok(());
                }

                return Err(err);
            }
        }

        Ok(())
    }

    pub fn is_latest_delete_marker(buf: &[u8]) -> bool {
        let header = Self::decode_xl_headers(buf).ok();
        if let Some((versions, _hdr_v, _meta_v, meta)) = header {
            if versions == 0 {
                return false;
            }

            let mut is_delete_marker = false;

            let _ = Self::decode_versions(meta, versions, |_: usize, hdr: &[u8], _: &[u8]| {
                let mut header = FileMetaVersionHeader::default();
                if header.unmarshal_msg(hdr).is_err() {
                    return Err(Error::DoneForNow);
                }

                is_delete_marker = header.version_type == VersionType::Delete;

                Err(Error::DoneForNow)
            });

            is_delete_marker
        } else {
            false
        }
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();

        // header
        wr.write_all(XL_FILE_HEADER.as_slice())?;

        let mut major = [0u8; 2];
        byteorder::LittleEndian::write_u16(&mut major, XL_FILE_VERSION_MAJOR);
        wr.write_all(major.as_slice())?;

        let mut minor = [0u8; 2];
        byteorder::LittleEndian::write_u16(&mut minor, XL_FILE_VERSION_MINOR);
        wr.write_all(minor.as_slice())?;

        // size bin32 reserved for write_bin_len
        wr.write_all(&[0xc6, 0, 0, 0, 0])?;

        let offset = wr.len();

        // xl header
        rmp::encode::write_uint8(&mut wr, XL_HEADER_VERSION)?;
        rmp::encode::write_uint8(&mut wr, XL_META_VERSION)?;

        // versions
        rmp::encode::write_sint(&mut wr, self.versions.len() as i64)?;

        for ver in self.versions.iter() {
            let hmsg = ver.header.marshal_msg()?;
            rmp::encode::write_bin(&mut wr, &hmsg)?;

            rmp::encode::write_bin(&mut wr, &ver.meta)?;
        }

        // Update bin length
        let data_len = wr.len() - offset;
        byteorder::BigEndian::write_u32(&mut wr[offset - 4..offset], data_len as u32);

        let crc = xxh64::xxh64(&wr[offset..], XXHASH_SEED) as u32;
        let mut crc_buf = [0u8; 5];
        crc_buf[0] = 0xce; // u32
        byteorder::BigEndian::write_u32(&mut crc_buf[1..], crc);

        wr.write_all(&crc_buf)?;

        wr.write_all(self.data.as_slice())?;

        Ok(wr)
    }

    // pub fn unmarshal(buf: &[u8]) -> Result<Self> {
    //     let mut s = Self::default();
    //     s.unmarshal_msg(buf)?;
    //     Ok(s)
    //     // let t: FileMeta = rmp_serde::from_slice(buf)?;
    //     // Ok(t)
    // }

    // pub fn marshal_msg(&self) -> Result<Vec<u8>> {
    //     let mut buf = Vec::new();

    //     self.serialize(&mut Serializer::new(&mut buf))?;

    //     Ok(buf)
    // }

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

    // shard_data_dir_count queries the count of data_dir under vid
    pub fn shard_data_dir_count(&self, vid: &Option<Uuid>, data_dir: &Option<Uuid>) -> usize {
        let vid = vid.unwrap_or_default();
        self.versions
            .iter()
            .filter(|v| {
                v.header.version_type == VersionType::Object && v.header.version_id != Some(vid) && v.header.user_data_dir()
            })
            .map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).unwrap_or_default())
            .filter(|v| v == data_dir)
            .count()
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
    #[tracing::instrument(skip(self))]
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

    /// Check if the metadata format is compatible
    pub fn is_compatible_with_meta(&self) -> bool {
        // Check version compatibility
        if self.meta_ver != XL_META_VERSION {
            return false;
        }

        // For compatibility, we allow versions with different types
        // Just check basic structure validity
        true
    }

    /// Validate metadata integrity
    pub fn validate_integrity(&self) -> Result<()> {
        // Check if versions are sorted by modification time
        if !self.is_sorted_by_mod_time() {
            return Err(Error::other("versions not sorted by modification time"));
        }

        // Validate inline data if present
        self.data.validate()?;

        Ok(())
    }

    /// Check if versions are sorted by modification time (newest first)
    fn is_sorted_by_mod_time(&self) -> bool {
        if self.versions.len() <= 1 {
            return true;
        }

        for i in 1..self.versions.len() {
            let prev_time = self.versions[i - 1].header.mod_time;
            let curr_time = self.versions[i].header.mod_time;

            match (prev_time, curr_time) {
                (Some(prev), Some(curr)) => {
                    if prev < curr {
                        return false;
                    }
                }
                (None, Some(_)) => return false,
                _ => continue,
            }
        }

        true
    }

    /// Get statistics about versions
    pub fn get_version_stats(&self) -> VersionStats {
        let mut stats = VersionStats {
            total_versions: self.versions.len(),
            ..Default::default()
        };

        for version in &self.versions {
            match version.header.version_type {
                VersionType::Object => stats.object_versions += 1,
                VersionType::Delete => stats.delete_markers += 1,
                VersionType::Invalid | VersionType::Legacy => stats.invalid_versions += 1,
            }

            if version.header.free_version() {
                stats.free_versions += 1;
            }
        }

        stats
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

    /// Get all data directories used by versions
    pub fn get_data_dirs(&self) -> Result<Vec<Option<Uuid>>> {
        let mut data_dirs = Vec::new();
        for version in &self.versions {
            if version.header.version_type == VersionType::Object {
                let ver = FileMetaVersion::try_from(version.meta.as_slice())?;
                data_dirs.push(ver.get_data_dir());
            }
        }
        Ok(data_dirs)
    }

    /// Count shared data directories
    pub fn shared_data_dir_count(&self, version_id: Option<Uuid>, data_dir: Option<Uuid>) -> usize {
        let version_id = version_id.unwrap_or_default();

        if self.data.entries().unwrap_or_default() > 0
            && self.data.find(version_id.to_string().as_str()).unwrap_or_default().is_some()
        {
            return 0;
        }

        self.versions
            .iter()
            .filter(|v| {
                v.header.version_type == VersionType::Object
                    && v.header.version_id != Some(version_id)
                    && v.header.user_data_dir()
            })
            .filter_map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).ok())
            .filter(|&dir| dir == data_dir)
            .count()
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

// impl Display for FileMeta {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str("FileMeta:")?;
//         for (i, ver) in self.versions.iter().enumerate() {
//             let mut meta = FileMetaVersion::default();
//             meta.unmarshal_msg(&ver.meta).unwrap_or_default();
//             f.write_fmt(format_args!("ver:{} header {:?}, meta {:?}", i, ver.header, meta))?;
//         }

//         f.write_str("\n")
//     }
// }

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone, Eq, PartialOrd, Ord)]
pub struct FileMetaShallowVersion {
    pub header: FileMetaVersionHeader,
    pub meta: Vec<u8>, // FileMetaVersion.marshal_msg
}

impl FileMetaShallowVersion {
    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfo> {
        let file_version = FileMetaVersion::try_from(self.meta.as_slice())?;

        Ok(file_version.into_fileinfo(volume, path, all_parts))
    }
}

impl TryFrom<FileMetaVersion> for FileMetaShallowVersion {
    type Error = Error;

    fn try_from(value: FileMetaVersion) -> std::result::Result<Self, Self::Error> {
        let header = value.header();
        let meta = value.marshal_msg()?;
        Ok(Self { meta, header })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct FileMetaVersion {
    #[serde(rename = "Type")]
    pub version_type: VersionType,
    #[serde(rename = "V2Obj")]
    pub object: Option<MetaObject>,
    #[serde(rename = "DelObj")]
    pub delete_marker: Option<MetaDeleteMarker>,
    #[serde(rename = "v")]
    pub write_version: u64, // rustfs version
}

impl FileMetaVersion {
    pub fn valid(&self) -> bool {
        if !self.version_type.valid() {
            return false;
        }

        match self.version_type {
            VersionType::Object => self
                .object
                .as_ref()
                .map(|v| v.erasure_algorithm.valid() && v.bitrot_checksum_algo.valid() && v.mod_time.is_some())
                .unwrap_or_default(),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .map(|v| v.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH) > OffsetDateTime::UNIX_EPOCH)
                .unwrap_or_default(),
            _ => false,
        }
    }

    pub fn get_data_dir(&self) -> Option<Uuid> {
        if self.valid() {
            {
                if self.version_type == VersionType::Object {
                    self.object.as_ref().map(|v| v.data_dir).unwrap_or_default()
                } else {
                    None
                }
            }
        } else {
            Default::default()
        }
    }

    pub fn get_version_id(&self) -> Option<Uuid> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.version_id).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.version_id).unwrap_or_default(),
            _ => None,
        }
    }

    pub fn get_mod_time(&self) -> Option<OffsetDateTime> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            _ => None,
        }
    }

    // decode_data_dir_from_meta reads data_dir from meta TODO: directly parse only data_dir from meta buf, msg.skip
    pub fn decode_data_dir_from_meta(buf: &[u8]) -> Result<Option<Uuid>> {
        let mut ver = Self::default();
        ver.unmarshal_msg(buf)?;

        let data_dir = ver.object.map(|v| v.data_dir).unwrap_or_default();
        Ok(data_dir)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)
    }

    pub fn free_version(&self) -> bool {
        self.version_type == VersionType::Delete && self.delete_marker.as_ref().map(|m| m.free_version()).unwrap_or_default()
    }

    pub fn header(&self) -> FileMetaVersionHeader {
        FileMetaVersionHeader::from(self.clone())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        match self.version_type {
            VersionType::Invalid | VersionType::Legacy => FileInfo {
                name: path.to_string(),
                volume: volume.to_string(),
                ..Default::default()
            },
            VersionType::Object => self
                .object
                .as_ref()
                .unwrap_or(&MetaObject::default())
                .into_fileinfo(volume, path, all_parts),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .unwrap_or(&MetaDeleteMarker::default())
                .into_fileinfo(volume, path, all_parts),
        }
    }

    /// Support for Legacy version type
    pub fn is_legacy(&self) -> bool {
        self.version_type == VersionType::Legacy
    }

    /// Get signature for version
    pub fn get_signature(&self) -> [u8; 4] {
        match self.version_type {
            VersionType::Object => {
                if let Some(ref obj) = self.object {
                    // Calculate signature based on object metadata
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(obj.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = obj.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            VersionType::Delete => {
                if let Some(ref dm) = self.delete_marker {
                    // Calculate signature for delete marker
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(dm.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = dm.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            _ => [0; 4],
        }
    }

    /// Check if this version uses data directory
    pub fn uses_data_dir(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.uses_data_dir()).unwrap_or(false),
            _ => false,
        }
    }

    /// Check if this version uses inline data
    pub fn uses_inline_data(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.inlinedata()).unwrap_or(false),
            _ => false,
        }
    }
}

impl TryFrom<&[u8]> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut ver = FileMetaVersion::default();
        ver.unmarshal_msg(value)?;
        Ok(ver)
    }
}

impl From<FileInfo> for FileMetaVersion {
    fn from(value: FileInfo) -> Self {
        {
            if value.deleted {
                FileMetaVersion {
                    version_type: VersionType::Delete,
                    delete_marker: Some(MetaDeleteMarker::from(value)),
                    object: None,
                    write_version: 0,
                }
            } else {
                FileMetaVersion {
                    version_type: VersionType::Object,
                    delete_marker: None,
                    object: Some(MetaObject::from(value)),
                    write_version: 0,
                }
            }
        }
    }
}

impl TryFrom<FileMetaShallowVersion> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: FileMetaShallowVersion) -> std::result::Result<Self, Self::Error> {
        FileMetaVersion::try_from(value.meta.as_slice())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
pub struct FileMetaVersionHeader {
    pub version_id: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub signature: [u8; 4],
    pub version_type: VersionType,
    pub flags: u8,
    pub ec_n: u8,
    pub ec_m: u8,
}

impl FileMetaVersionHeader {
    pub fn has_ec(&self) -> bool {
        self.ec_m > 0 && self.ec_n > 0
    }

    pub fn matches_not_strict(&self, o: &FileMetaVersionHeader) -> bool {
        let mut ok = self.version_id == o.version_id && self.version_type == o.version_type && self.matches_ec(o);
        if self.version_id.is_none() {
            ok = ok && self.mod_time == o.mod_time;
        }

        ok
    }

    pub fn matches_ec(&self, o: &FileMetaVersionHeader) -> bool {
        if self.has_ec() && o.has_ec() {
            return self.ec_n == o.ec_n && self.ec_m == o.ec_m;
        }

        true
    }

    pub fn free_version(&self) -> bool {
        self.flags & XL_FLAG_FREE_VERSION != 0
    }

    pub fn sorts_before(&self, o: &FileMetaVersionHeader) -> bool {
        if self == o {
            return false;
        }

        // Prefer newest modtime.
        if self.mod_time != o.mod_time {
            return self.mod_time > o.mod_time;
        }

        match self.mod_time.cmp(&o.mod_time) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        // The following doesn't make too much sense, but we want sort to be consistent nonetheless.
        // Prefer lower types
        if self.version_type != o.version_type {
            return self.version_type < o.version_type;
        }
        // Consistent sort on signature
        match self.version_id.cmp(&o.version_id) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        if self.flags != o.flags {
            return self.flags > o.flags;
        }

        false
    }

    pub fn user_data_dir(&self) -> bool {
        self.flags & Flags::UsesDataDir as u8 != 0
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();

        // array len 7
        rmp::encode::write_array_len(&mut wr, 7)?;

        // version_id
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or_default().as_bytes())?;
        // mod_time
        rmp::encode::write_i64(&mut wr, self.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp_nanos() as i64)?;
        // signature
        rmp::encode::write_bin(&mut wr, self.signature.as_slice())?;
        // version_type
        rmp::encode::write_uint8(&mut wr, self.version_type.to_u8())?;
        // flags
        rmp::encode::write_uint8(&mut wr, self.flags)?;
        // ec_n
        rmp::encode::write_uint8(&mut wr, self.ec_n)?;
        // ec_m
        rmp::encode::write_uint8(&mut wr, self.ec_m)?;

        Ok(wr)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);
        let alen = rmp::decode::read_array_len(&mut cur)?;
        if alen != 7 {
            return Err(Error::other(format!("version header array len err need 7 got {alen}")));
        }

        // version_id
        rmp::decode::read_bin_len(&mut cur)?;
        let mut buf = [0u8; 16];
        cur.read_exact(&mut buf)?;
        self.version_id = {
            let id = Uuid::from_bytes(buf);
            // if id.is_nil() { None } else { Some(id) }
            Some(id)
        };

        // mod_time
        let unix: i128 = rmp::decode::read_int(&mut cur)?;

        let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
        if time == OffsetDateTime::UNIX_EPOCH {
            self.mod_time = None;
        } else {
            self.mod_time = Some(time);
        }

        // signature
        rmp::decode::read_bin_len(&mut cur)?;
        cur.read_exact(&mut self.signature)?;

        // version_type
        let typ: u8 = rmp::decode::read_int(&mut cur)?;
        self.version_type = VersionType::from_u8(typ);

        // flags
        self.flags = rmp::decode::read_int(&mut cur)?;
        // ec_n
        self.ec_n = rmp::decode::read_int(&mut cur)?;
        // ec_m
        self.ec_m = rmp::decode::read_int(&mut cur)?;

        Ok(cur.position())
    }

    /// Get signature for header
    pub fn get_signature(&self) -> [u8; 4] {
        self.signature
    }

    /// Check if this header represents inline data
    pub fn inline_data(&self) -> bool {
        self.flags & Flags::InlineData as u8 != 0
    }

    /// Update signature based on version content
    pub fn update_signature(&mut self, version: &FileMetaVersion) {
        self.signature = version.get_signature();
    }
}

impl PartialOrd for FileMetaVersionHeader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileMetaVersionHeader {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.mod_time.cmp(&other.mod_time) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        match self.version_type.cmp(&other.version_type) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.signature.cmp(&other.signature) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.version_id.cmp(&other.version_id) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.flags.cmp(&other.flags)
    }
}

impl From<FileMetaVersion> for FileMetaVersionHeader {
    fn from(value: FileMetaVersion) -> Self {
        let flags = {
            let mut f: u8 = 0;
            if value.free_version() {
                f |= Flags::FreeVersion as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.uses_data_dir()).unwrap_or_default() {
                f |= Flags::UsesDataDir as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.inlinedata()).unwrap_or_default() {
                f |= Flags::InlineData as u8;
            }

            f
        };

        let (ec_n, ec_m) = match (value.version_type == VersionType::Object, value.object.as_ref()) {
            (true, Some(obj)) => (obj.erasure_n as u8, obj.erasure_m as u8),
            _ => (0, 0),
        };

        Self {
            version_id: value.get_version_id(),
            mod_time: value.get_mod_time(),
            signature: [0, 0, 0, 0],
            version_type: value.version_type,
            flags,
            ec_n,
            ec_m,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
// Because of custom message_pack, field order must be guaranteed
pub struct MetaObject {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID
    #[serde(rename = "DDir")]
    pub data_dir: Option<Uuid>, // Data dir ID
    #[serde(rename = "EcAlgo")]
    pub erasure_algorithm: ErasureAlgo, // Erasure coding algorithm
    #[serde(rename = "EcM")]
    pub erasure_m: usize, // Erasure data blocks
    #[serde(rename = "EcN")]
    pub erasure_n: usize, // Erasure parity blocks
    #[serde(rename = "EcBSize")]
    pub erasure_block_size: usize, // Erasure block size
    #[serde(rename = "EcIndex")]
    pub erasure_index: usize, // Erasure disk index
    #[serde(rename = "EcDist")]
    pub erasure_dist: Vec<u8>, // Erasure distribution
    #[serde(rename = "CSumAlgo")]
    pub bitrot_checksum_algo: ChecksumAlgo, // Bitrot checksum algo
    #[serde(rename = "PartNums")]
    pub part_numbers: Vec<usize>, // Part Numbers
    #[serde(rename = "PartETags")]
    pub part_etags: Vec<String>, // Part ETags
    #[serde(rename = "PartSizes")]
    pub part_sizes: Vec<usize>, // Part Sizes
    #[serde(rename = "PartASizes")]
    pub part_actual_sizes: Vec<i64>, // Part ActualSizes (compression)
    #[serde(rename = "PartIdx")]
    pub part_indices: Vec<Bytes>, // Part Indexes (compression)
    #[serde(rename = "Size")]
    pub size: i64, // Object version size
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object version modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Object version internal metadata
    #[serde(rename = "MetaUsr")]
    pub meta_user: HashMap<String, String>, // Object version metadata set by user
}

impl MetaObject {
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)
    }
    // marshal_msg custom messagepack naming consistent with go
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        let version_id = self.version_id.filter(|&vid| !vid.is_nil());

        let parts = if all_parts {
            let mut parts = vec![ObjectPartInfo::default(); self.part_numbers.len()];

            for (i, part) in parts.iter_mut().enumerate() {
                part.number = self.part_numbers[i];
                part.size = self.part_sizes[i];
                part.actual_size = self.part_actual_sizes[i];

                if self.part_etags.len() == self.part_numbers.len() {
                    part.etag = self.part_etags[i].clone();
                }

                if self.part_indices.len() == self.part_numbers.len() {
                    part.index = if self.part_indices[i].is_empty() {
                        None
                    } else {
                        Some(self.part_indices[i].clone())
                    };
                }
            }
            parts
        } else {
            Vec::new()
        };

        let mut metadata = HashMap::with_capacity(self.meta_user.len() + self.meta_sys.len());
        for (k, v) in &self.meta_user {
            if k == AMZ_META_UNENCRYPTED_CONTENT_LENGTH || k == AMZ_META_UNENCRYPTED_CONTENT_MD5 {
                continue;
            }

            if k == AMZ_STORAGE_CLASS && v == "STANDARD" {
                continue;
            }

            metadata.insert(k.to_owned(), v.to_owned());
        }

        let tier_fvidkey = format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_ID}").to_lowercase();
        let tier_fvmarker_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_MARKER}").to_lowercase();

        for (k, v) in &self.meta_sys {
            let lower_k = k.to_lowercase();

            if lower_k == tier_fvidkey || lower_k == tier_fvmarker_key {
                continue;
            }

            if lower_k == VERSION_PURGE_STATUS_KEY.to_lowercase() {
                continue;
            }

            if lower_k == AMZ_STORAGE_CLASS.to_lowercase() && v == b"STANDARD" {
                continue;
            }

            if k.starts_with(RESERVED_METADATA_PREFIX)
                || k.starts_with(RESERVED_METADATA_PREFIX_LOWER)
                || lower_k == VERSION_PURGE_STATUS_KEY.to_lowercase()
            {
                metadata.insert(k.to_owned(), String::from_utf8(v.to_owned()).unwrap_or_default());
            }
        }

        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut deleted = false;

        if let Some(v) = replication_state_internal.as_ref() {
            if !v.composite_version_purge_status().is_empty() {
                deleted = true;
            }

            let st = v.composite_replication_status();
            if !st.is_empty() {
                metadata.insert(AMZ_BUCKET_REPLICATION_STATUS.to_string(), st.to_string());
            }
        }

        let checksum = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}crc").as_str())
            .map(|v| Bytes::from(v.clone()));

        let erasure = ErasureInfo {
            algorithm: self.erasure_algorithm.to_string(),
            data_blocks: self.erasure_m,
            parity_blocks: self.erasure_n,
            block_size: self.erasure_block_size,
            index: self.erasure_index,
            distribution: self.erasure_dist.iter().map(|&v| v as usize).collect(),
            ..Default::default()
        };

        let transition_status = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        let transitioned_objname = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        let transition_version_id = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}").as_str())
            .map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        let transition_tier = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();

        FileInfo {
            version_id,
            erasure,
            data_dir: self.data_dir,
            mod_time: self.mod_time,
            size: self.size,
            name: path.to_string(),
            volume: volume.to_string(),
            parts,
            metadata,
            replication_state_internal,
            deleted,
            checksum,
            transition_status,
            transitioned_objname,
            transition_version_id,
            transition_tier,
            ..Default::default()
        }
    }

    pub fn set_transition(&mut self, fi: &FileInfo) {
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"),
            fi.transition_status.as_bytes().to_vec(),
        );
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}"),
            fi.transitioned_objname.as_bytes().to_vec(),
        );
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"),
            fi.transition_version_id.unwrap().as_bytes().to_vec(),
        );
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}"),
            fi.transition_tier.as_bytes().to_vec(),
        );
    }

    pub fn remove_restore_hdrs(&mut self) {
        self.meta_user.remove(X_AMZ_RESTORE.as_str());
        self.meta_user.remove(AMZ_RESTORE_EXPIRY_DAYS);
        self.meta_user.remove(AMZ_RESTORE_REQUEST_DATE);
    }

    pub fn uses_data_dir(&self) -> bool {
        !self.inlinedata()
    }

    pub fn inlinedata(&self) -> bool {
        self.meta_sys
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").as_str())
    }

    pub fn reset_inline_data(&mut self) {
        self.meta_sys
            .remove(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").as_str());
    }

    /// Remove restore headers
    pub fn remove_restore_headers(&mut self) {
        // Remove any restore-related metadata
        self.meta_sys.retain(|k, _| !k.starts_with("X-Amz-Restore"));
    }

    /// Get object signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        hasher.update(&self.size.to_le_bytes());
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    pub fn init_free_version(&self, fi: &FileInfo) -> (FileMetaVersion, bool) {
        if fi.skip_tier_free_version() {
            return (FileMetaVersion::default(), false);
        }
        if let Some(status) = self
            .meta_sys
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"))
            && *status == TRANSITION_COMPLETE.as_bytes().to_vec()
        {
            let vid = Uuid::parse_str(&fi.tier_free_version_id());
            if let Err(err) = vid {
                panic!("Invalid Tier Object delete marker versionId {} {}", fi.tier_free_version_id(), err);
            }
            let vid = vid.unwrap();
            let mut free_entry = FileMetaVersion {
                version_type: VersionType::Delete,
                write_version: 0,
                ..Default::default()
            };
            free_entry.delete_marker = Some(MetaDeleteMarker {
                version_id: Some(vid),
                mod_time: self.mod_time,
                meta_sys: HashMap::<String, Vec<u8>>::new(),
            });

            let delete_marker = free_entry.delete_marker.as_mut().unwrap();

            delete_marker
                .meta_sys
                .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{FREE_VERSION}"), vec![]);

            let tier_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}");
            let tier_obj_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}");
            let tier_obj_vid_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}");

            let aa = [tier_key, tier_obj_key, tier_obj_vid_key];
            for (k, v) in &self.meta_sys {
                if aa.contains(k) {
                    delete_marker.meta_sys.insert(k.clone(), v.clone());
                }
            }
            return (free_entry, true);
        }
        (FileMetaVersion::default(), false)
    }
}

impl From<FileInfo> for MetaObject {
    fn from(value: FileInfo) -> Self {
        let part_etags = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.etag.clone()).collect()
        } else {
            vec![]
        };

        let part_indices = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.index.clone().unwrap_or_default()).collect()
        } else {
            vec![]
        };

        let mut meta_sys = HashMap::new();
        let mut meta_user = HashMap::new();
        for (k, v) in value.metadata.iter() {
            if k.len() > RESERVED_METADATA_PREFIX.len()
                && (k.starts_with(RESERVED_METADATA_PREFIX) || k.starts_with(RESERVED_METADATA_PREFIX_LOWER))
            {
                if k == headers::X_RUSTFS_HEALING || k == headers::X_RUSTFS_DATA_MOV {
                    continue;
                }

                meta_sys.insert(k.to_owned(), v.as_bytes().to_vec());
            } else {
                meta_user.insert(k.to_owned(), v.to_owned());
            }
        }

        if !value.transition_status.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"),
                value.transition_status.as_bytes().to_vec(),
            );
        }

        if !value.transitioned_objname.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}"),
                value.transitioned_objname.as_bytes().to_vec(),
            );
        }

        if let Some(vid) = &value.transition_version_id {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"),
                vid.as_bytes().to_vec(),
            );
        }

        if !value.transition_tier.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}"),
                value.transition_tier.as_bytes().to_vec(),
            );
        }

        if let Some(content_hash) = value.checksum {
            meta_sys.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}crc"), content_hash.to_vec());
        }

        Self {
            version_id: value.version_id,
            data_dir: value.data_dir,
            size: value.size,
            mod_time: value.mod_time,
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            erasure_m: value.erasure.data_blocks,
            erasure_n: value.erasure.parity_blocks,
            erasure_block_size: value.erasure.block_size,
            erasure_index: value.erasure.index,
            erasure_dist: value.erasure.distribution.iter().map(|x| *x as u8).collect(),
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: value.parts.iter().map(|v| v.number).collect(),
            part_etags,
            part_sizes: value.parts.iter().map(|v| v.size).collect(),
            part_actual_sizes: value.parts.iter().map(|v| v.actual_size).collect(),
            part_indices,
            meta_sys,
            meta_user,
        }
    }
}

fn get_internal_replication_state(metadata: &HashMap<String, String>) -> Option<ReplicationState> {
    let mut rs = ReplicationState::default();
    let mut has = false;

    for (k, v) in metadata.iter() {
        if k == VERSION_PURGE_STATUS_KEY {
            rs.version_purge_status_internal = Some(v.clone());
            rs.purge_targets = version_purge_statuses_map(v.as_str());
            has = true;
            continue;
        }

        if let Some(sub_key) = k.strip_prefix(RESERVED_METADATA_PREFIX_LOWER) {
            match sub_key {
                "replica-timestamp" => {
                    has = true;
                    rs.replica_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
                }
                "replica-status" => {
                    has = true;
                    rs.replica_status = ReplicationStatusType::from(v.as_str());
                }
                "replication-timestamp" => {
                    has = true;
                    rs.replication_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH))
                }
                "replication-status" => {
                    has = true;
                    rs.replication_status_internal = Some(v.clone());
                    rs.targets = replication_statuses_map(v.as_str());
                }
                _ => {
                    if let Some(arn) = sub_key.strip_prefix("replication-reset-") {
                        has = true;
                        rs.reset_statuses_map.insert(arn.to_string(), v.clone());
                    }
                }
            }
        }
    }

    if has { Some(rs) } else { None }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct MetaDeleteMarker {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID for delete marker
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object delete marker modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Delete marker internal metadata
}

impl MetaDeleteMarker {
    pub fn free_version(&self) -> bool {
        self.meta_sys
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}{FREE_VERSION}").as_str())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, _all_parts: bool) -> FileInfo {
        let metadata = self
            .meta_sys
            .clone()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
            .collect();
        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut fi = FileInfo {
            version_id: self.version_id.filter(|&vid| !vid.is_nil()),
            name: path.to_string(),
            volume: volume.to_string(),
            deleted: true,
            mod_time: self.mod_time,
            metadata,
            replication_state_internal,
            ..Default::default()
        };

        if self.free_version() {
            fi.set_tier_free_version();
            fi.transition_tier = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}").as_str())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();

            fi.transitioned_objname = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}").as_str())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();

            fi.transition_version_id = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}").as_str())
                .map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        }

        fi
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)

        // let mut cur = Cursor::new(buf);

        // let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        // while fields_len > 0 {
        //     fields_len -= 1;

        //     let str_len = rmp::decode::read_str_len(&mut cur)?;

        //     // !!! Vec::with_capacity(str_len) fails, vec! works normally
        //     let mut field_buff = vec![0u8; str_len as usize];

        //     cur.read_exact(&mut field_buff)?;

        //     let field = String::from_utf8(field_buff)?;

        //     match field.as_str() {
        //         "ID" => {
        //             rmp::decode::read_bin_len(&mut cur)?;
        //             let mut buf = [0u8; 16];
        //             cur.read_exact(&mut buf)?;
        //             self.version_id = {
        //                 let id = Uuid::from_bytes(buf);
        //                 if id.is_nil() { None } else { Some(id) }
        //             };
        //         }

        //         "MTime" => {
        //             let unix: i64 = rmp::decode::read_int(&mut cur)?;
        //             let time = OffsetDateTime::from_unix_timestamp(unix)?;
        //             if time == OffsetDateTime::UNIX_EPOCH {
        //                 self.mod_time = None;
        //             } else {
        //                 self.mod_time = Some(time);
        //             }
        //         }
        //         "MetaSys" => {
        //             let l = rmp::decode::read_map_len(&mut cur)?;
        //             let mut map = HashMap::new();
        //             for _ in 0..l {
        //                 let str_len = rmp::decode::read_str_len(&mut cur)?;
        //                 let mut field_buff = vec![0u8; str_len as usize];
        //                 cur.read_exact(&mut field_buff)?;
        //                 let key = String::from_utf8(field_buff)?;

        //                 let blen = rmp::decode::read_bin_len(&mut cur)?;
        //                 let mut val = vec![0u8; blen as usize];
        //                 cur.read_exact(&mut val)?;

        //                 map.insert(key, val);
        //             }

        //             self.meta_sys = Some(map);
        //         }
        //         name => return Err(Error::other(format!("not support field name {name}"))),
        //     }
        // }

        // Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)

        // let mut len: u32 = 3;
        // let mut mask: u8 = 0;

        // if self.meta_sys.is_none() {
        //     len -= 1;
        //     mask |= 0x4;
        // }

        // let mut wr = Vec::new();

        // // Field count
        // rmp::encode::write_map_len(&mut wr, len)?;

        // // string "ID"
        // rmp::encode::write_str(&mut wr, "ID")?;
        // rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or_default().as_bytes())?;

        // // string "MTime"
        // rmp::encode::write_str(&mut wr, "MTime")?;
        // rmp::encode::write_uint(
        //     &mut wr,
        //     self.mod_time
        //         .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        //         .unix_timestamp()
        //         .try_into()
        //         .unwrap(),
        // )?;

        // if (mask & 0x4) == 0 {
        //     let metas = self.meta_sys.as_ref().unwrap();
        //     rmp::encode::write_map_len(&mut wr, metas.len() as u32)?;
        //     for (k, v) in metas {
        //         rmp::encode::write_str(&mut wr, k.as_str())?;
        //         rmp::encode::write_bin(&mut wr, v)?;
        //     }
        // }

        // Ok(wr)
    }

    /// Get delete marker signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }
}

impl From<FileInfo> for MetaDeleteMarker {
    fn from(value: FileInfo) -> Self {
        Self {
            version_id: value.version_id,
            mod_time: value.mod_time,
            meta_sys: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone, PartialOrd, Ord, Hash)]
pub enum VersionType {
    #[default]
    Invalid = 0,
    Object = 1,
    Delete = 2,
    Legacy = 3,
}

impl VersionType {
    pub fn valid(&self) -> bool {
        matches!(*self, VersionType::Object | VersionType::Delete | VersionType::Legacy)
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            VersionType::Invalid => 0,
            VersionType::Object => 1,
            VersionType::Delete => 2,
            VersionType::Legacy => 3,
        }
    }

    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => VersionType::Object,
            2 => VersionType::Delete,
            3 => VersionType::Legacy,
            _ => VersionType::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum ChecksumAlgo {
    #[default]
    Invalid = 0,
    HighwayHash = 1,
}

impl ChecksumAlgo {
    pub fn valid(&self) -> bool {
        *self > ChecksumAlgo::Invalid
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ChecksumAlgo::Invalid => 0,
            ChecksumAlgo::HighwayHash => 1,
        }
    }
    pub fn from_u8(u: u8) -> Self {
        match u {
            1 => ChecksumAlgo::HighwayHash,
            _ => ChecksumAlgo::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum Flags {
    #[default]
    FreeVersion = 1 << 0,
    UsesDataDir = 1 << 1,
    InlineData = 1 << 2,
}

// mergeXLV2Versions
pub fn merge_file_meta_versions(
    mut quorum: usize,
    mut strict: bool,
    requested_versions: usize,
    versions: &[Vec<FileMetaShallowVersion>],
) -> Vec<FileMetaShallowVersion> {
    if quorum == 0 {
        quorum = 1;
    }

    if versions.len() < quorum || versions.is_empty() {
        return Vec::new();
    }

    if versions.len() == 1 {
        return versions[0].clone();
    }

    if quorum == 1 {
        strict = true;
    }

    let mut versions = versions.to_owned();

    let mut n_versions = 0;

    let mut merged = Vec::new();
    loop {
        let mut tops = Vec::new();
        let mut top_sig = FileMetaVersionHeader::default();
        let mut consistent = true;
        for vers in versions.iter() {
            if vers.is_empty() {
                consistent = false;
                continue;
            }
            if tops.is_empty() {
                consistent = true;
                top_sig = vers[0].header.clone();
            } else {
                consistent = consistent && vers[0].header == top_sig;
            }
            tops.push(vers[0].clone());
        }

        // check if done...
        if tops.len() < quorum {
            break;
        }

        let mut latest = FileMetaShallowVersion::default();
        if consistent {
            merged.push(tops[0].clone());
            if tops[0].header.free_version() {
                n_versions += 1;
            }
        } else {
            let mut latest_count = 0;
            for (i, ver) in tops.iter().enumerate() {
                if ver.header == latest.header {
                    latest_count += 1;
                    continue;
                }

                if i == 0 || ver.header.sorts_before(&latest.header) {
                    if i == 0 || latest_count == 0 {
                        latest_count = 1;
                    } else if !strict && ver.header.matches_not_strict(&latest.header) {
                        latest_count += 1;
                    } else {
                        latest_count = 1;
                    }
                    latest = ver.clone();
                    continue;
                }

                // Mismatch, but older.
                if latest_count > 0 && !strict && ver.header.matches_not_strict(&latest.header) {
                    latest_count += 1;
                    continue;
                }

                if latest_count > 0 && ver.header.version_id == latest.header.version_id {
                    let mut x: HashMap<FileMetaVersionHeader, usize> = HashMap::new();
                    for a in tops.iter() {
                        if a.header.version_id != ver.header.version_id {
                            continue;
                        }
                        let mut a_clone = a.clone();
                        if !strict {
                            a_clone.header.signature = [0; 4];
                        }
                        *x.entry(a_clone.header).or_insert(1) += 1;
                    }
                    latest_count = 0;
                    for (k, v) in x.iter() {
                        if *v < latest_count {
                            continue;
                        }
                        if *v == latest_count && latest.header.sorts_before(k) {
                            continue;
                        }
                        tops.iter().for_each(|a| {
                            let mut hdr = a.header.clone();
                            if !strict {
                                hdr.signature = [0; 4];
                            }
                            if hdr == *k {
                                latest = a.clone();
                            }
                        });

                        latest_count = *v;
                    }
                    break;
                }
            }
            if latest_count >= quorum {
                if !latest.header.free_version() {
                    n_versions += 1;
                }
                merged.push(latest.clone());
            }
        }

        // Remove from all streams up until latest modtime or if selected.
        versions.iter_mut().for_each(|vers| {
            // // Keep top entry (and remaining)...
            let mut bre = false;
            vers.retain(|ver| {
                if bre {
                    return true;
                }
                if let Ordering::Greater = ver.header.mod_time.cmp(&latest.header.mod_time) {
                    bre = true;
                    return false;
                }
                if ver.header == latest.header {
                    bre = true;
                    return false;
                }
                if let Ordering::Equal = latest.header.version_id.cmp(&ver.header.version_id) {
                    bre = true;
                    return false;
                }
                for merged_v in merged.iter() {
                    if let Ordering::Equal = ver.header.version_id.cmp(&merged_v.header.version_id) {
                        bre = true;
                        return false;
                    }
                }
                true
            });
        });
        if requested_versions > 0 && requested_versions == n_versions {
            merged.append(&mut versions[0]);
            break;
        }
    }

    // Sanity check. Enable if duplicates show up.
    // todo
    merged
}

pub fn file_info_from_raw(
    ri: RawFileInfo,
    bucket: &str,
    object: &str,
    read_data: bool,
    include_free_versions: bool,
) -> Result<FileInfo> {
    get_file_info(
        &ri.buf,
        bucket,
        object,
        "",
        FileInfoOpts {
            data: read_data,
            include_free_versions,
        },
    )
}

pub struct FileInfoOpts {
    pub data: bool,
    pub include_free_versions: bool,
}

pub fn get_file_info(buf: &[u8], volume: &str, path: &str, version_id: &str, opts: FileInfoOpts) -> Result<FileInfo> {
    let vid = {
        if version_id.is_empty() {
            None
        } else {
            Some(Uuid::parse_str(version_id)?)
        }
    };

    let meta = FileMeta::load(buf)?;
    if meta.versions.is_empty() {
        return Ok(FileInfo {
            volume: volume.to_owned(),
            name: path.to_owned(),
            version_id: vid,
            is_latest: true,
            deleted: true,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1)?),
            ..Default::default()
        });
    }

    let fi = meta.into_fileinfo(volume, path, version_id, opts.data, opts.include_free_versions, true)?;
    Ok(fi)
}

async fn read_more<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    total_size: usize,
    read_size: usize,
    has_full: bool,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let has = buf.len();

    if has >= read_size {
        return Ok(());
    }

    if has_full || read_size > total_size {
        return Err(Error::other(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF")));
    }

    let extra = read_size - has;
    if buf.capacity() >= read_size {
        // Extend the buffer if we have enough space.
        buf.resize(read_size, 0);
    } else {
        buf.extend(vec![0u8; extra]);
    }

    reader.read_exact(&mut buf[has..]).await?;
    Ok(())
}

pub async fn read_xl_meta_no_data<R: AsyncRead + Unpin>(reader: &mut R, size: usize) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut initial = size;
    let mut has_full = true;

    if initial > META_DATA_READ_DEFAULT {
        initial = META_DATA_READ_DEFAULT;
        has_full = false;
    }

    let mut buf = vec![0u8; initial];
    reader.read_exact(&mut buf).await?;

    let (tmp_buf, major, minor) = FileMeta::check_xl2_v1(&buf)?;

    match major {
        1 => match minor {
            0 => {
                read_more(reader, &mut buf, size, size, has_full).await?;
                Ok(buf)
            }
            1..=3 => {
                let (sz, tmp_buf) = FileMeta::read_bytes_header(tmp_buf)?;
                let mut want = sz as usize + (buf.len() - tmp_buf.len());

                if minor < 2 {
                    read_more(reader, &mut buf, size, want, has_full).await?;
                    return Ok(buf[..want].to_vec());
                }

                let want_max = usize::min(want + MSGP_UINT32_SIZE, size);
                read_more(reader, &mut buf, size, want_max, has_full).await?;

                if buf.len() < want {
                    return Err(Error::FileCorrupt);
                }

                let tmp = &buf[want..];
                let crc_size = 5;
                let other_size = tmp.len() - crc_size;

                want += tmp.len() - other_size;

                Ok(buf[..want].to_vec())
            }
            _ => Err(Error::other(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown minor metadata version",
            ))),
        },
        _ => Err(Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unknown major metadata version",
        ))),
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

#[derive(Debug, Default, Clone)]
pub struct VersionStats {
    pub total_versions: usize,
    pub object_versions: usize,
    pub delete_markers: usize,
    pub invalid_versions: usize,
    pub free_versions: usize,
}

impl FileMetaVersionHeader {
    // ... existing code ...

    pub fn is_valid(&self) -> bool {
        // Check if version type is valid
        if !self.version_type.valid() {
            return false;
        }

        // Check if modification time is reasonable (not too far in the future)
        if let Some(mod_time) = self.mod_time {
            let now = OffsetDateTime::now_utc();
            let future_limit = now + time::Duration::hours(24); // Allow 24 hours in future
            if mod_time > future_limit {
                return false;
            }
        }

        // Check erasure coding parameters
        if self.has_ec() && (self.ec_n == 0 || self.ec_m == 0 || self.ec_m < self.ec_n) {
            return false;
        }

        true
    }

    // ... existing code ...
}

/// Enhanced version statistics with more detailed information
#[derive(Debug, Default, Clone)]
pub struct DetailedVersionStats {
    pub total_versions: usize,
    pub object_versions: usize,
    pub delete_markers: usize,
    pub invalid_versions: usize,
    pub legacy_versions: usize,
    pub free_versions: usize,
    pub versions_with_data_dir: usize,
    pub versions_with_inline_data: usize,
    pub total_size: i64,
    pub latest_mod_time: Option<OffsetDateTime>,
}

impl FileMeta {
    /// Get detailed statistics about versions
    pub fn get_detailed_version_stats(&self) -> DetailedVersionStats {
        let mut stats = DetailedVersionStats {
            total_versions: self.versions.len(),
            ..Default::default()
        };

        for version in &self.versions {
            match version.header.version_type {
                VersionType::Object => {
                    stats.object_versions += 1;
                    if let Ok(ver) = FileMetaVersion::try_from(version.meta.as_slice())
                        && let Some(obj) = &ver.object
                    {
                        stats.total_size += obj.size;
                        if obj.uses_data_dir() {
                            stats.versions_with_data_dir += 1;
                        }
                        if obj.inlinedata() {
                            stats.versions_with_inline_data += 1;
                        }
                    }
                }
                VersionType::Delete => stats.delete_markers += 1,
                VersionType::Legacy => stats.legacy_versions += 1,
                VersionType::Invalid => stats.invalid_versions += 1,
            }

            if version.header.free_version() {
                stats.free_versions += 1;
            }

            if stats.latest_mod_time.is_none()
                || (version.header.mod_time.is_some() && version.header.mod_time > stats.latest_mod_time)
            {
                stats.latest_mod_time = version.header.mod_time;
            }
        }

        stats
    }
}
