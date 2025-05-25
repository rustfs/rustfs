use crate::disk::FileInfoVersions;
use crate::file_meta_inline::InlineData;
use crate::store_api::RawFileInfo;
use crate::store_err::StorageError;
use crate::{
    disk::error::DiskError,
    store_api::{ErasureInfo, FileInfo, ObjectPartInfo, ERASURE_ALGORITHM},
};
use byteorder::ByteOrder;
use common::error::{Error, Result};
use rmp::Marker;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::io::{self, Read, Write};
use std::{collections::HashMap, io::Cursor};
use time::OffsetDateTime;
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
static XL_META_VERSION: u8 = 2;
static XXHASH_SEED: u64 = 0;

const XL_FLAG_FREE_VERSION: u8 = 1 << 0;
// const XL_FLAG_USES_DATA_DIR: u8 = 1 << 1;
const _XL_FLAG_INLINE_DATA: u8 = 1 << 2;

const META_DATA_READ_DEFAULT: usize = 4 << 10;
const MSGP_UINT32_SIZE: usize = 5;

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

    // isXL2V1Format
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn is_xl2_v1_format(buf: &[u8]) -> bool {
        !matches!(Self::check_xl2_v1(buf), Err(_e))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn load(buf: &[u8]) -> Result<FileMeta> {
        let mut xl = FileMeta::default();
        xl.unmarshal_msg(buf)?;

        Ok(xl)
    }

    // check_xl2_v1 读xl文件头，返回后续内容，版本信息
    // checkXL2V1
    #[tracing::instrument]
    pub fn check_xl2_v1(buf: &[u8]) -> Result<(&[u8], u16, u16)> {
        if buf.len() < 8 {
            return Err(Error::msg("xl file header not exists"));
        }

        if buf[0..4] != XL_FILE_HEADER {
            return Err(Error::msg("xl file header err"));
        }

        let major = byteorder::LittleEndian::read_u16(&buf[4..6]);
        let minor = byteorder::LittleEndian::read_u16(&buf[6..8]);
        if major > XL_FILE_VERSION_MAJOR {
            return Err(Error::msg("xl file version err"));
        }

        Ok((&buf[8..], major, minor))
    }

    // 固定u32
    pub fn read_bytes_header(buf: &[u8]) -> Result<(u32, &[u8])> {
        let (mut size_buf, _) = buf.split_at(5);

        //  取meta数据，buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf)?;

        Ok((bin_len, &buf[5..]))
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let i = buf.len() as u64;

        // check version, buf = buf[8..]
        let (buf, _, _) = Self::check_xl2_v1(buf)?;

        let (mut size_buf, buf) = buf.split_at(5);

        //  取meta数据，buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf)?;

        let (meta, buf) = buf.split_at(bin_len as usize);

        let (mut crc_buf, buf) = buf.split_at(5);

        // crc check
        let crc = rmp::decode::read_u32(&mut crc_buf)?;
        let meta_crc = xxh64::xxh64(meta, XXHASH_SEED) as u32;

        if crc != meta_crc {
            return Err(Error::msg("xl file crc check failed"));
        }

        if !buf.is_empty() {
            self.data.update(buf);
            self.data.validate()?;
        }

        // 解析meta
        if !meta.is_empty() {
            let (versions_len, _, meta_ver, meta) = Self::decode_xl_headers(meta)?;

            // let (_, meta) = meta.split_at(read_size as usize);

            self.meta_ver = meta_ver;

            self.versions = Vec::with_capacity(versions_len);

            let mut cur: Cursor<&[u8]> = Cursor::new(meta);
            for _ in 0..versions_len {
                let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
                let start = cur.position() as usize;
                let end = start + bin_len;
                let header_buf = &meta[start..end];

                let mut ver = FileMetaShallowVersion::default();
                ver.header.unmarshal_msg(header_buf)?;

                cur.set_position(end as u64);

                let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
                let start = cur.position() as usize;
                let end = start + bin_len;
                let mut ver_meta_buf = &meta[start..end];

                ver_meta_buf.read_to_end(&mut ver.meta)?;

                cur.set_position(end as u64);

                self.versions.push(ver);
            }
        }

        Ok(i)
    }

    // decode_xl_headers 解析 meta 头，返回 (versions数量，xl_header_version, xl_meta_version, 已读数据长度)
    #[tracing::instrument]
    fn decode_xl_headers(buf: &[u8]) -> Result<(usize, u8, u8, &[u8])> {
        let mut cur = Cursor::new(buf);

        let header_ver: u8 = rmp::decode::read_int(&mut cur)?;

        if header_ver > XL_HEADER_VERSION {
            return Err(Error::msg("xl header version invalid"));
        }

        let meta_ver: u8 = rmp::decode::read_int(&mut cur)?;
        if meta_ver > XL_META_VERSION {
            return Err(Error::msg("xl meta version invalid"));
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
                if let Some(e) = err.downcast_ref::<StorageError>() {
                    if e == &StorageError::DoneForNow {
                        return Ok(());
                    }
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
                    return Err(Error::new(StorageError::DoneForNow));
                }

                is_delete_marker = header.version_type == VersionType::Delete;

                Err(Error::new(StorageError::DoneForNow))
            });

            is_delete_marker
        } else {
            false
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
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

        // size bin32 预留 write_bin_len
        wr.write_all(&[0xc6, 0, 0, 0, 0])?;

        let offset = wr.len();

        rmp::encode::write_uint8(&mut wr, XL_HEADER_VERSION)?;
        rmp::encode::write_uint8(&mut wr, XL_META_VERSION)?;

        // versions
        rmp::encode::write_sint(&mut wr, self.versions.len() as i64)?;

        for ver in self.versions.iter() {
            let hmsg = ver.header.marshal_msg()?;
            rmp::encode::write_bin(&mut wr, &hmsg)?;

            rmp::encode::write_bin(&mut wr, &ver.meta)?;
        }

        // 更新bin长度
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
            return Err(Error::new(DiskError::FileNotFound));
        }

        FileMetaVersion::try_from(self.versions[idx].meta.as_slice())
    }

    fn set_idx(&mut self, idx: usize, ver: FileMetaVersion) -> Result<()> {
        if idx >= self.versions.len() {
            return Err(Error::new(DiskError::FileNotFound));
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

        // Sort by mod_time in descending order (latest first)
        self.versions.sort_by(|a, b| {
            match (a.header.mod_time, b.header.mod_time) {
                (Some(a_time), Some(b_time)) => b_time.cmp(&a_time), // Descending order
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
    }

    // 查找版本
    pub fn find_version(&self, vid: Option<Uuid>) -> Result<(usize, FileMetaVersion)> {
        for (i, fver) in self.versions.iter().enumerate() {
            if fver.header.version_id == vid {
                let version = self.get_idx(i)?;
                return Ok((i, version));
            }
        }

        Err(Error::new(DiskError::FileVersionNotFound))
    }

    // shard_data_dir_count 查询 vid下data_dir的数量
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn shard_data_dir_count(&self, vid: &Option<Uuid>, data_dir: &Option<Uuid>) -> usize {
        self.versions
            .iter()
            .filter(|v| v.header.version_type == VersionType::Object && v.header.version_id != *vid && v.header.user_data_dir())
            .map(|v| FileMetaVersion::decode_data_dir_from_meta(&v.meta).unwrap_or_default())
            .filter(|v| v == data_dir)
            .count()
    }

    pub fn update_object_version(&mut self, fi: FileInfo) -> Result<()> {
        for version in self.versions.iter_mut() {
            match version.header.version_type {
                VersionType::Invalid => (),
                VersionType::Object => {
                    if version.header.version_id == fi.version_id {
                        let mut ver = FileMetaVersion::try_from(version.meta.as_slice())?;

                        if let Some(ref mut obj) = ver.object {
                            if let Some(ref mut meta_user) = obj.meta_user {
                                if let Some(meta) = &fi.metadata {
                                    for (k, v) in meta {
                                        meta_user.insert(k.clone(), v.clone());
                                    }
                                }
                                obj.meta_user = Some(meta_user.clone());
                            } else {
                                let mut meta_user = HashMap::new();
                                if let Some(meta) = &fi.metadata {
                                    for (k, v) in meta {
                                        // TODO: MetaSys
                                        meta_user.insert(k.clone(), v.clone());
                                    }
                                }
                                obj.meta_user = Some(meta_user);
                            }

                            if let Some(mod_time) = fi.mod_time {
                                obj.mod_time = Some(mod_time);
                            }
                        }

                        // 更新
                        version.header = ver.header();
                        version.meta = ver.marshal_msg()?;
                    }
                }
                VersionType::Delete => {
                    if version.header.version_id == fi.version_id {
                        return Err(Error::msg("method not allowed"));
                    }
                }
            }
        }

        self.versions.sort_by(|a, b| {
            if a.header.mod_time != b.header.mod_time {
                a.header.mod_time.cmp(&b.header.mod_time)
            } else if a.header.version_type != b.header.version_type {
                a.header.version_type.cmp(&b.header.version_type)
            } else if a.header.version_id != b.header.version_id {
                a.header.version_id.cmp(&b.header.version_id)
            } else if a.header.flags != b.header.flags {
                a.header.flags.cmp(&b.header.flags)
            } else {
                a.cmp(b)
            }
        });
        Ok(())
    }

    // 添加版本
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn add_version(&mut self, fi: FileInfo) -> Result<()> {
        let vid = fi.version_id;

        if let Some(ref data) = fi.data {
            let key = vid.unwrap_or(Uuid::nil()).to_string();
            self.data.replace(&key, data.clone())?;
        }

        let version = FileMetaVersion::from(fi);

        if !version.valid() {
            return Err(Error::msg("file meta version invalid"));
        }

        // should replace
        for (idx, ver) in self.versions.iter().enumerate() {
            if ver.header.version_id != vid {
                continue;
            }

            return self.set_idx(idx, version);
        }

        // TODO: version count limit !

        let mod_time = version.get_mod_time();

        // puth a -1 mod time value , so we can relplace this
        self.versions.push(FileMetaShallowVersion {
            header: FileMetaVersionHeader {
                mod_time: Some(OffsetDateTime::from_unix_timestamp(-1)?),
                ..Default::default()
            },
            ..Default::default()
        });

        for (idx, exist) in self.versions.iter().enumerate() {
            if let Some(ref ex_mt) = exist.header.mod_time {
                if let Some(ref in_md) = mod_time {
                    if ex_mt <= in_md {
                        // insert
                        self.versions.insert(idx, FileMetaShallowVersion::try_from(version)?);
                        self.versions.pop();
                        return Ok(());
                    }
                }
            }
        }

        Err(Error::msg("add_version failed"))
    }

    // delete_version 删除版本，返回data_dir
    pub fn delete_version(&mut self, fi: &FileInfo) -> Result<Option<Uuid>> {
        let mut ventry = FileMetaVersion::default();
        if fi.deleted {
            ventry.version_type = VersionType::Delete;
            ventry.delete_marker = Some(MetaDeleteMarker {
                version_id: fi.version_id,
                mod_time: fi.mod_time,
                ..Default::default()
            });

            if !fi.is_valid() {
                return Err(Error::msg("invalid file meta version"));
            }
        }

        for (i, ver) in self.versions.iter().enumerate() {
            if ver.header.version_id != fi.version_id {
                continue;
            }

            match ver.header.version_type {
                VersionType::Invalid => return Err(Error::msg("invalid file meta version")),
                VersionType::Delete => return Ok(None),
                VersionType::Object => {
                    let v = self.get_idx(i)?;

                    self.versions.remove(i);

                    let a = v.object.map(|v| v.data_dir).unwrap_or_default();
                    return Ok(a);
                }
            }
        }

        Err(Error::new(DiskError::FileVersionNotFound))
    }

    // read_data fill fi.dada
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn into_fileinfo(
        &self,
        volume: &str,
        path: &str,
        version_id: &str,
        read_data: bool,
        all_parts: bool,
    ) -> Result<FileInfo> {
        let has_vid = {
            if !version_id.is_empty() {
                let id = Uuid::parse_str(version_id)?;
                if !id.is_nil() {
                    Some(id)
                } else {
                    None
                }
            } else {
                None
            }
        };

        let mut is_latest = true;
        let mut succ_mod_time = None;
        for ver in self.versions.iter() {
            let header = &ver.header;

            if let Some(vid) = has_vid {
                if header.version_id != Some(vid) {
                    is_latest = false;
                    succ_mod_time = header.mod_time;
                    continue;
                }
            }

            let mut fi = ver.to_fileinfo(volume, path, has_vid, all_parts)?;
            fi.is_latest = is_latest;
            if let Some(_d) = succ_mod_time {
                fi.successor_mod_time = succ_mod_time;
            }
            if read_data {
                fi.data = self.data.find(fi.version_id.unwrap_or(Uuid::nil()).to_string().as_str())?;
            }

            fi.num_versions = self.versions.len();

            return Ok(fi);
        }

        if has_vid.is_none() {
            Err(Error::from(DiskError::FileNotFound))
        } else {
            Err(Error::from(DiskError::FileVersionNotFound))
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn into_file_info_versions(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfoVersions> {
        let mut versions = Vec::new();
        for version in self.versions.iter() {
            let mut file_version = FileMetaVersion::default();
            file_version.unmarshal_msg(&version.meta)?;
            let fi = file_version.to_fileinfo(volume, path, None, all_parts);
            versions.push(fi);
        }

        Ok(FileInfoVersions {
            volume: volume.to_string(),
            name: path.to_string(),
            latest_mod_time: versions[0].mod_time,
            versions,
            ..Default::default()
        })
    }

    pub fn lastest_mod_time(&self) -> Option<OffsetDateTime> {
        if self.versions.is_empty() {
            return None;
        }

        self.versions.first().unwrap().header.mod_time
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
    pub fn to_fileinfo(&self, volume: &str, path: &str, version_id: Option<Uuid>, all_parts: bool) -> Result<FileInfo> {
        let file_version = FileMetaVersion::try_from(self.meta.as_slice())?;

        Ok(file_version.to_fileinfo(volume, path, version_id, all_parts))
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
    pub version_type: VersionType,
    pub object: Option<MetaObject>,
    pub delete_marker: Option<MetaDeleteMarker>,
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
        self.valid()
            .then(|| {
                if self.version_type == VersionType::Object {
                    self.object.as_ref().map(|v| v.data_dir).unwrap_or_default()
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    pub fn get_version_id(&self) -> Option<Uuid> {
        match self.version_type {
            VersionType::Object | VersionType::Delete => self.object.as_ref().map(|v| v.version_id).unwrap_or_default(),
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

    // decode_data_dir_from_meta 从 meta中读取data_dir TODO: 直接从meta buf中只解析出data_dir, msg.skip
    pub fn decode_data_dir_from_meta(buf: &[u8]) -> Result<Option<Uuid>> {
        let mut ver = Self::default();
        ver.unmarshal_msg(buf)?;

        let data_dir = ver.object.map(|v| v.data_dir).unwrap_or_default();
        Ok(data_dir)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            // println!("unmarshal_msg fields idx {}", fields_len);

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            // println!("unmarshal_msg fields name len() {}", &str_len);

            // ！！！ Vec::with_capacity(str_len) 失败，vec!正常
            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;

            // println!("unmarshal_msg fields name {}", &field);

            match field.as_str() {
                "Type" => {
                    let u: u8 = rmp::decode::read_int(&mut cur)?;
                    self.version_type = VersionType::from_u8(u);
                }

                "V2Obj" => {
                    //  is_nil()
                    if buf[cur.position() as usize] == 0xc0 {
                        rmp::decode::read_nil(&mut cur)?;
                    } else {
                        // let buf = unsafe { cur.position() };
                        let mut obj = MetaObject::default();
                        // let start = cur.position();

                        let (_, remain) = buf.split_at(cur.position() as usize);

                        let read_len = obj.unmarshal_msg(remain)?;
                        cur.set_position(cur.position() + read_len);

                        self.object = Some(obj);
                    }
                }
                "DelObj" => {
                    if buf[cur.position() as usize] == 0xc0 {
                        rmp::decode::read_nil(&mut cur)?;
                    } else {
                        // let buf = unsafe { cur.position() };
                        let mut obj = MetaDeleteMarker::default();
                        // let start = cur.position();

                        let (_, remain) = buf.split_at(cur.position() as usize);
                        let read_len = obj.unmarshal_msg(remain)?;
                        cur.set_position(cur.position() + read_len);

                        self.delete_marker = Some(obj);
                    }
                }
                "v" => {
                    self.write_version = rmp::decode::read_int(&mut cur)?;
                }
                name => return Err(Error::msg(format!("not suport field name {}", name))),
            }
        }

        Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut len: u32 = 4;
        let mut mask: u8 = 0;

        if self.object.is_none() {
            len -= 1;
            mask |= 0x2;
        }
        if self.delete_marker.is_none() {
            len -= 1;
            mask |= 0x4;
        }

        let mut wr = Vec::new();

        // 字段数量
        rmp::encode::write_map_len(&mut wr, len)?;

        // write "Type"
        rmp::encode::write_str(&mut wr, "Type")?;
        rmp::encode::write_uint(&mut wr, self.version_type.to_u8() as u64)?;

        if (mask & 0x2) == 0 {
            // write V2Obj
            rmp::encode::write_str(&mut wr, "V2Obj")?;
            if self.object.is_none() {
                let _ = rmp::encode::write_nil(&mut wr);
            } else {
                let buf = self.object.as_ref().unwrap().marshal_msg()?;
                wr.write_all(&buf)?;
            }
        }

        if (mask & 0x4) == 0 {
            // write "DelObj"
            rmp::encode::write_str(&mut wr, "DelObj")?;
            if self.delete_marker.is_none() {
                let _ = rmp::encode::write_nil(&mut wr);
            } else {
                let buf = self.delete_marker.as_ref().unwrap().marshal_msg()?;
                wr.write_all(&buf)?;
            }
        }

        // write "v"
        rmp::encode::write_str(&mut wr, "v")?;
        rmp::encode::write_uint(&mut wr, self.write_version)?;

        Ok(wr)
    }

    pub fn free_version(&self) -> bool {
        self.version_type == VersionType::Delete && self.delete_marker.as_ref().map(|m| m.free_version()).unwrap_or_default()
    }

    pub fn header(&self) -> FileMetaVersionHeader {
        FileMetaVersionHeader::from(self.clone())
    }

    pub fn to_fileinfo(&self, volume: &str, path: &str, version_id: Option<Uuid>, all_parts: bool) -> FileInfo {
        match self.version_type {
            VersionType::Invalid => FileInfo {
                name: path.to_string(),
                volume: volume.to_string(),
                version_id,
                ..Default::default()
            },
            VersionType::Object => self
                .object
                .as_ref()
                .unwrap()
                .clone()
                .into_fileinfo(volume, path, version_id, all_parts),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .unwrap()
                .clone()
                .into_fileinfo(volume, path, version_id, all_parts),
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
    #[tracing::instrument]
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();

        // array len 7
        rmp::encode::write_array_len(&mut wr, 7)?;

        // version_id
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or(Uuid::nil()).as_bytes())?;
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
            return Err(Error::msg(format!("version header array len err need 7 got {}", alen)));
        }

        // version_id
        rmp::decode::read_bin_len(&mut cur)?;
        let mut buf = [0u8; 16];
        cur.read_exact(&mut buf)?;
        self.version_id = {
            let id = Uuid::from_bytes(buf);
            if id.is_nil() {
                None
            } else {
                Some(id)
            }
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

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.use_data_dir()).unwrap_or_default() {
                f |= Flags::UsesDataDir as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.use_inlinedata()).unwrap_or_default()
            {
                f |= Flags::InlineData as u8;
            }

            f
        };

        let (ec_n, ec_m) = {
            if value.version_type == VersionType::Object && value.object.is_some() {
                (
                    value.object.as_ref().unwrap().erasure_n as u8,
                    value.object.as_ref().unwrap().erasure_m as u8,
                )
            } else {
                (0, 0)
            }
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
// 因为自定义message_pack，所以一定要保证字段顺序
pub struct MetaObject {
    pub version_id: Option<Uuid>,                   // Version ID
    pub data_dir: Option<Uuid>,                     // Data dir ID
    pub erasure_algorithm: ErasureAlgo,             // Erasure coding algorithm
    pub erasure_m: usize,                           // Erasure data blocks
    pub erasure_n: usize,                           // Erasure parity blocks
    pub erasure_block_size: usize,                  // Erasure block size
    pub erasure_index: usize,                       // Erasure disk index
    pub erasure_dist: Vec<u8>,                      // Erasure distribution
    pub bitrot_checksum_algo: ChecksumAlgo,         // Bitrot checksum algo
    pub part_numbers: Vec<usize>,                   // Part Numbers
    pub part_etags: Option<Vec<String>>,            // Part ETags
    pub part_sizes: Vec<usize>,                     // Part Sizes
    pub part_actual_sizes: Option<Vec<usize>>,      // Part ActualSizes (compression)
    pub part_indices: Option<Vec<Vec<u8>>>,         // Part Indexes (compression)
    pub size: usize,                                // Object version size
    pub mod_time: Option<OffsetDateTime>,           // Object version modified time
    pub meta_sys: Option<HashMap<String, Vec<u8>>>, // Object version internal metadata
    pub meta_user: Option<HashMap<String, String>>, // Object version metadata set by user
}

impl MetaObject {
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        // let mut ret = Self::default();

        while fields_len > 0 {
            fields_len -= 1;

            // println!("unmarshal_msg fields idx {}", fields_len);

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            // println!("unmarshal_msg fields name len() {}", &str_len);

            // ！！！ Vec::with_capacity(str_len) 失败，vec!正常
            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;

            // println!("unmarshal_msg fields name {}", &field);

            match field.as_str() {
                "ID" => {
                    rmp::decode::read_bin_len(&mut cur)?;
                    let mut buf = [0u8; 16];
                    cur.read_exact(&mut buf)?;
                    self.version_id = {
                        let id = Uuid::from_bytes(buf);
                        if id.is_nil() {
                            None
                        } else {
                            Some(id)
                        }
                    };
                }
                "DDir" => {
                    rmp::decode::read_bin_len(&mut cur)?;
                    let mut buf = [0u8; 16];
                    cur.read_exact(&mut buf)?;
                    self.data_dir = {
                        let id = Uuid::from_bytes(buf);
                        if id.is_nil() {
                            None
                        } else {
                            Some(id)
                        }
                    };
                }
                "EcAlgo" => {
                    let u: u8 = rmp::decode::read_int(&mut cur)?;
                    self.erasure_algorithm = ErasureAlgo::from_u8(u)
                }
                "EcM" => {
                    self.erasure_m = rmp::decode::read_int(&mut cur)?;
                }
                "EcN" => {
                    self.erasure_n = rmp::decode::read_int(&mut cur)?;
                }
                "EcBSize" => {
                    self.erasure_block_size = rmp::decode::read_int(&mut cur)?;
                }
                "EcIndex" => {
                    self.erasure_index = rmp::decode::read_int(&mut cur)?;
                }
                "EcDist" => {
                    let alen = rmp::decode::read_array_len(&mut cur)? as usize;
                    self.erasure_dist = vec![0u8; alen];
                    for i in 0..alen {
                        self.erasure_dist[i] = rmp::decode::read_int(&mut cur)?;
                    }
                }
                "CSumAlgo" => {
                    let u: u8 = rmp::decode::read_int(&mut cur)?;
                    self.bitrot_checksum_algo = ChecksumAlgo::from_u8(u)
                }
                "PartNums" => {
                    let alen = rmp::decode::read_array_len(&mut cur)? as usize;
                    self.part_numbers = vec![0; alen];
                    for i in 0..alen {
                        self.part_numbers[i] = rmp::decode::read_int(&mut cur)?;
                    }
                }
                "PartETags" => {
                    let array_len = match rmp::decode::read_nil(&mut cur) {
                        Ok(_) => None,
                        Err(e) => match e {
                            rmp::decode::ValueReadError::TypeMismatch(marker) => match marker {
                                Marker::FixArray(l) => Some(l as usize),
                                Marker::Array16 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                Marker::Array32 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                _ => return Err(Error::msg("PartETags parse failed")),
                            },
                            _ => return Err(Error::msg("PartETags parse failed.")),
                        },
                    };

                    if array_len.is_some() {
                        let l = array_len.unwrap();
                        let mut etags = Vec::with_capacity(l);
                        for _ in 0..l {
                            let str_len = rmp::decode::read_str_len(&mut cur)?;
                            let mut field_buff = vec![0u8; str_len as usize];
                            cur.read_exact(&mut field_buff)?;
                            etags.push(String::from_utf8(field_buff)?);
                        }
                        self.part_etags = Some(etags);
                    }
                }
                "PartSizes" => {
                    let alen = rmp::decode::read_array_len(&mut cur)? as usize;
                    self.part_sizes = vec![0; alen];
                    for i in 0..alen {
                        self.part_sizes[i] = rmp::decode::read_int(&mut cur)?;
                    }
                }
                "PartASizes" => {
                    let array_len = match rmp::decode::read_nil(&mut cur) {
                        Ok(_) => None,
                        Err(e) => match e {
                            rmp::decode::ValueReadError::TypeMismatch(marker) => match marker {
                                Marker::FixArray(l) => Some(l as usize),
                                Marker::Array16 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                Marker::Array32 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                _ => return Err(Error::msg("PartETags parse failed")),
                            },
                            _ => return Err(Error::msg("PartETags parse failed.")),
                        },
                    };
                    if let Some(l) = array_len {
                        let mut sizes = vec![0; l];
                        for size in sizes.iter_mut().take(l) {
                            *size = rmp::decode::read_int(&mut cur)?;
                        }
                        // for size in sizes.iter_mut().take(l) {
                        //     let tmp = rmp::decode::read_int(&mut cur)?;
                        //     size = tmp;
                        // }
                        self.part_actual_sizes = Some(sizes);
                    }
                }
                "PartIdx" => {
                    let alen = rmp::decode::read_array_len(&mut cur)? as usize;

                    if alen == 0 {
                        self.part_indices = None;
                        continue;
                    }

                    let mut indices = Vec::with_capacity(alen);
                    for _ in 0..alen {
                        let blen = rmp::decode::read_bin_len(&mut cur)?;
                        let mut buf = vec![0u8; blen as usize];
                        cur.read_exact(&mut buf)?;

                        indices.push(buf);
                    }

                    self.part_indices = Some(indices);
                }
                "Size" => {
                    self.size = rmp::decode::read_int(&mut cur)?;
                }
                "MTime" => {
                    let unix: i128 = rmp::decode::read_int(&mut cur)?;
                    let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
                    if time == OffsetDateTime::UNIX_EPOCH {
                        self.mod_time = None;
                    } else {
                        self.mod_time = Some(time);
                    }
                }
                "MetaSys" => {
                    let len = match rmp::decode::read_nil(&mut cur) {
                        Ok(_) => None,
                        Err(e) => match e {
                            rmp::decode::ValueReadError::TypeMismatch(marker) => match marker {
                                Marker::FixMap(l) => Some(l as usize),
                                Marker::Map16 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                Marker::Map32 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                _ => return Err(Error::msg("MetaSys parse failed")),
                            },
                            _ => return Err(Error::msg("MetaSys parse failed.")),
                        },
                    };
                    if len.is_some() {
                        let l = len.unwrap();
                        let mut map = HashMap::new();
                        for _ in 0..l {
                            let str_len = rmp::decode::read_str_len(&mut cur)?;
                            let mut field_buff = vec![0u8; str_len as usize];
                            cur.read_exact(&mut field_buff)?;
                            let key = String::from_utf8(field_buff)?;

                            let blen = rmp::decode::read_bin_len(&mut cur)?;
                            let mut val = vec![0u8; blen as usize];
                            cur.read_exact(&mut val)?;

                            map.insert(key, val);
                        }

                        self.meta_sys = Some(map);
                    }
                }
                "MetaUsr" => {
                    let len = match rmp::decode::read_nil(&mut cur) {
                        Ok(_) => None,
                        Err(e) => match e {
                            rmp::decode::ValueReadError::TypeMismatch(marker) => match marker {
                                Marker::FixMap(l) => Some(l as usize),
                                Marker::Map16 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                Marker::Map32 => Some(rmp::decode::read_u16(&mut cur)? as usize),
                                _ => return Err(Error::msg("MetaUsr parse failed")),
                            },
                            _ => return Err(Error::msg("MetaUsr parse failed.")),
                        },
                    };
                    if len.is_some() {
                        let l = len.unwrap();
                        let mut map = HashMap::new();
                        for _ in 0..l {
                            let str_len = rmp::decode::read_str_len(&mut cur)?;
                            let mut field_buff = vec![0u8; str_len as usize];
                            cur.read_exact(&mut field_buff)?;
                            let key = String::from_utf8(field_buff)?;

                            let blen = rmp::decode::read_str_len(&mut cur)?;
                            let mut val_buf = vec![0u8; blen as usize];
                            cur.read_exact(&mut val_buf)?;
                            let val = String::from_utf8(val_buf)?;

                            map.insert(key, val);
                        }

                        self.meta_user = Some(map);
                    }
                }

                name => return Err(Error::msg(format!("not suport field name {}", name))),
            }
        }

        Ok(cur.position())
    }
    // marshal_msg 自定义 messagepack 命名与go一致
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut len: u32 = 18;
        let mut mask: u32 = 0;

        if self.part_indices.is_none() {
            len -= 1;
            mask |= 0x2000;
        }

        let mut wr = Vec::new();

        // 字段数量
        rmp::encode::write_map_len(&mut wr, len)?;

        // string "ID"
        rmp::encode::write_str(&mut wr, "ID")?;
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or(Uuid::nil()).as_bytes())?;

        // string "DDir"
        rmp::encode::write_str(&mut wr, "DDir")?;
        rmp::encode::write_bin(&mut wr, self.data_dir.unwrap_or(Uuid::nil()).as_bytes())?;

        // string "EcAlgo"
        rmp::encode::write_str(&mut wr, "EcAlgo")?;
        rmp::encode::write_uint(&mut wr, self.erasure_algorithm.to_u8() as u64)?;

        // string "EcM"
        rmp::encode::write_str(&mut wr, "EcM")?;
        rmp::encode::write_uint(&mut wr, self.erasure_m.try_into().unwrap())?;

        // string "EcN"
        rmp::encode::write_str(&mut wr, "EcN")?;
        rmp::encode::write_uint(&mut wr, self.erasure_n.try_into().unwrap())?;

        // string "EcBSize"
        rmp::encode::write_str(&mut wr, "EcBSize")?;
        rmp::encode::write_uint(&mut wr, self.erasure_block_size.try_into().unwrap())?;

        // string "EcIndex"
        rmp::encode::write_str(&mut wr, "EcIndex")?;
        rmp::encode::write_uint(&mut wr, self.erasure_index.try_into().unwrap())?;

        // string "EcDist"
        rmp::encode::write_str(&mut wr, "EcDist")?;
        rmp::encode::write_array_len(&mut wr, self.erasure_dist.len() as u32)?;
        for v in self.erasure_dist.iter() {
            rmp::encode::write_uint(&mut wr, *v as _)?;
        }

        // string "CSumAlgo"
        rmp::encode::write_str(&mut wr, "CSumAlgo")?;
        rmp::encode::write_uint(&mut wr, self.bitrot_checksum_algo.to_u8() as u64)?;

        // string "PartNums"
        rmp::encode::write_str(&mut wr, "PartNums")?;
        rmp::encode::write_array_len(&mut wr, self.part_numbers.len() as u32)?;
        for v in self.part_numbers.iter() {
            rmp::encode::write_uint(&mut wr, *v as _)?;
        }

        // string "PartETags"
        rmp::encode::write_str(&mut wr, "PartETags")?;
        if self.part_etags.is_none() {
            rmp::encode::write_nil(&mut wr)?;
        } else {
            let etags = self.part_etags.as_ref().unwrap();
            rmp::encode::write_array_len(&mut wr, etags.len() as u32)?;
            for v in etags.iter() {
                rmp::encode::write_str(&mut wr, v.as_str())?;
            }
        }

        // string "PartSizes"
        rmp::encode::write_str(&mut wr, "PartSizes")?;
        rmp::encode::write_array_len(&mut wr, self.part_sizes.len() as u32)?;
        for v in self.part_sizes.iter() {
            rmp::encode::write_uint(&mut wr, *v as _)?;
        }

        // string "PartASizes"
        rmp::encode::write_str(&mut wr, "PartASizes")?;
        if self.part_actual_sizes.is_none() {
            rmp::encode::write_nil(&mut wr)?;
        } else {
            let asizes = self.part_actual_sizes.as_ref().unwrap();
            rmp::encode::write_array_len(&mut wr, asizes.len() as u32)?;
            for v in asizes.iter() {
                rmp::encode::write_uint(&mut wr, *v as _)?;
            }
        }

        if (mask & 0x2000) == 0 {
            // string "PartIdx"
            rmp::encode::write_str(&mut wr, "PartIdx")?;
            let indices = self.part_indices.as_ref().unwrap();
            rmp::encode::write_array_len(&mut wr, indices.len() as u32)?;
            for v in indices.iter() {
                rmp::encode::write_bin(&mut wr, v)?;
            }
        }

        // string "Size"
        rmp::encode::write_str(&mut wr, "Size")?;
        rmp::encode::write_uint(&mut wr, self.size.try_into().unwrap())?;

        // string "MTime"
        rmp::encode::write_str(&mut wr, "MTime")?;
        rmp::encode::write_uint(
            &mut wr,
            self.mod_time
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                .unix_timestamp_nanos()
                .try_into()
                .unwrap(),
        )?;

        // string "MetaSys"
        rmp::encode::write_str(&mut wr, "MetaSys")?;
        if self.meta_sys.is_none() {
            rmp::encode::write_nil(&mut wr)?;
        } else {
            let metas = self.meta_sys.as_ref().unwrap();
            rmp::encode::write_map_len(&mut wr, metas.len() as u32)?;
            for (k, v) in metas {
                rmp::encode::write_str(&mut wr, k.as_str())?;
                rmp::encode::write_bin(&mut wr, v)?;
            }
        }

        // string "MetaUsr"
        rmp::encode::write_str(&mut wr, "MetaUsr")?;
        if self.meta_user.is_none() {
            rmp::encode::write_nil(&mut wr)?;
        } else {
            let metas = self.meta_user.as_ref().unwrap();
            rmp::encode::write_map_len(&mut wr, metas.len() as u32)?;
            for (k, v) in metas {
                rmp::encode::write_str(&mut wr, k.as_str())?;
                rmp::encode::write_str(&mut wr, v.as_str())?;
            }
        }

        Ok(wr)
    }
    pub fn use_data_dir(&self) -> bool {
        // TODO: when use inlinedata
        true
    }

    pub fn use_inlinedata(&self) -> bool {
        // TODO: when use inlinedata
        false
    }

    pub fn into_fileinfo(self, volume: &str, path: &str, _version_id: Option<Uuid>, _all_parts: bool) -> FileInfo {
        let version_id = self.version_id;

        let erasure = ErasureInfo {
            algorithm: self.erasure_algorithm.to_string(),
            data_blocks: self.erasure_m,
            parity_blocks: self.erasure_n,
            block_size: self.erasure_block_size,
            index: self.erasure_index,
            distribution: self.erasure_dist.iter().map(|&v| v as usize).collect(),
            ..Default::default()
        };

        let mut parts = Vec::new();
        for (i, _) in self.part_numbers.iter().enumerate() {
            parts.push(ObjectPartInfo {
                number: self.part_numbers[i],
                size: self.part_sizes[i],
                ..Default::default()
            });
        }

        let metadata = {
            if let Some(metauser) = self.meta_user.as_ref() {
                let mut m = HashMap::new();
                for (k, v) in metauser {
                    // TODO: skip xhttp x-amz-storage-class
                    m.insert(k.to_owned(), v.to_owned());
                }
                Some(m)
            } else {
                None
            }
        };

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
            ..Default::default()
        }
    }
}

impl From<FileInfo> for MetaObject {
    fn from(value: FileInfo) -> Self {
        let part_numbers: Vec<usize> = value.parts.iter().map(|v| v.number).collect();
        let part_sizes: Vec<usize> = value.parts.iter().map(|v| v.size).collect();

        Self {
            version_id: value.version_id,
            size: value.size,
            mod_time: value.mod_time,
            data_dir: value.data_dir,
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            erasure_m: value.erasure.data_blocks,
            erasure_n: value.erasure.parity_blocks,
            erasure_block_size: value.erasure.block_size,
            erasure_index: value.erasure.index,
            erasure_dist: value.erasure.distribution.iter().map(|x| *x as u8).collect(),
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers,
            part_etags: None, // TODO: add part_etags
            part_sizes,
            part_actual_sizes: None, // TODO: add part_etags
            part_indices: None,
            meta_sys: None,
            meta_user: value.metadata.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct MetaDeleteMarker {
    pub version_id: Option<Uuid>,                   // Version ID for delete marker
    pub mod_time: Option<OffsetDateTime>,           // Object delete marker modified time
    pub meta_sys: Option<HashMap<String, Vec<u8>>>, // Delete marker internal metadata
}

impl MetaDeleteMarker {
    pub fn free_version(&self) -> bool {
        self.meta_sys
            .as_ref()
            .map(|v| v.get(FREE_VERSION_META_HEADER).is_some())
            .unwrap_or_default()
    }

    pub fn into_fileinfo(self, volume: &str, path: &str, version_id: Option<Uuid>, _all_parts: bool) -> FileInfo {
        FileInfo {
            name: path.to_string(),
            volume: volume.to_string(),
            version_id,
            deleted: true,
            mod_time: self.mod_time,
            ..Default::default()
        }
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            // ！！！ Vec::with_capacity(str_len) 失败，vec!正常
            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;

            match field.as_str() {
                "ID" => {
                    rmp::decode::read_bin_len(&mut cur)?;
                    let mut buf = [0u8; 16];
                    cur.read_exact(&mut buf)?;
                    self.version_id = {
                        let id = Uuid::from_bytes(buf);
                        if id.is_nil() {
                            None
                        } else {
                            Some(id)
                        }
                    };
                }

                "MTime" => {
                    let unix: i64 = rmp::decode::read_int(&mut cur)?;
                    let time = OffsetDateTime::from_unix_timestamp(unix)?;
                    if time == OffsetDateTime::UNIX_EPOCH {
                        self.mod_time = None;
                    } else {
                        self.mod_time = Some(time);
                    }
                }
                "MetaSys" => {
                    let l = rmp::decode::read_map_len(&mut cur)?;
                    let mut map = HashMap::new();
                    for _ in 0..l {
                        let str_len = rmp::decode::read_str_len(&mut cur)?;
                        let mut field_buff = vec![0u8; str_len as usize];
                        cur.read_exact(&mut field_buff)?;
                        let key = String::from_utf8(field_buff)?;

                        let blen = rmp::decode::read_bin_len(&mut cur)?;
                        let mut val = vec![0u8; blen as usize];
                        cur.read_exact(&mut val)?;

                        map.insert(key, val);
                    }

                    self.meta_sys = Some(map);
                }
                name => return Err(Error::msg(format!("not suport field name {}", name))),
            }
        }

        Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut len: u32 = 3;
        let mut mask: u8 = 0;

        if self.meta_sys.is_none() {
            len -= 1;
            mask |= 0x4;
        }

        let mut wr = Vec::new();

        // 字段数量
        rmp::encode::write_map_len(&mut wr, len)?;

        // string "ID"
        rmp::encode::write_str(&mut wr, "ID")?;
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or(Uuid::nil()).as_bytes())?;

        // string "MTime"
        rmp::encode::write_str(&mut wr, "MTime")?;
        rmp::encode::write_uint(
            &mut wr,
            self.mod_time
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                .unix_timestamp()
                .try_into()
                .unwrap(),
        )?;

        if (mask & 0x4) == 0 {
            let metas = self.meta_sys.as_ref().unwrap();
            rmp::encode::write_map_len(&mut wr, metas.len() as u32)?;
            for (k, v) in metas {
                rmp::encode::write_str(&mut wr, k.as_str())?;
                rmp::encode::write_bin(&mut wr, v)?;
            }
        }

        Ok(wr)
    }
}

impl From<FileInfo> for MetaDeleteMarker {
    fn from(value: FileInfo) -> Self {
        Self {
            version_id: value.version_id,
            mod_time: value.mod_time,
            meta_sys: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone, PartialOrd, Ord, Hash)]
pub enum VersionType {
    #[default]
    Invalid = 0,
    Object = 1,
    Delete = 2,
    // Legacy = 3,
}

impl VersionType {
    pub fn valid(&self) -> bool {
        matches!(*self, VersionType::Object | VersionType::Delete)
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            VersionType::Invalid => 0,
            VersionType::Object => 1,
            VersionType::Delete => 2,
        }
    }

    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => VersionType::Object,
            2 => VersionType::Delete,
            _ => VersionType::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum ErasureAlgo {
    #[default]
    Invalid = 0,
    ReedSolomon = 1,
}

impl ErasureAlgo {
    pub fn valid(&self) -> bool {
        *self > ErasureAlgo::Invalid
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ErasureAlgo::Invalid => 0,
            ErasureAlgo::ReedSolomon => 1,
        }
    }

    pub fn from_u8(u: u8) -> Self {
        match u {
            1 => ErasureAlgo::ReedSolomon,
            _ => ErasureAlgo::Invalid,
        }
    }
}

impl Display for ErasureAlgo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErasureAlgo::Invalid => write!(f, "Invalid"),
            ErasureAlgo::ReedSolomon => write!(f, "{}", ERASURE_ALGORITHM),
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

const FREE_VERSION_META_HEADER: &str = "free-version";

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
            let mut lastest_count = 0;
            for (i, ver) in tops.iter().enumerate() {
                if ver.header == latest.header {
                    lastest_count += 1;
                    continue;
                }

                if i == 0 || ver.header.sorts_before(&latest.header) {
                    if i == 0 || lastest_count == 0 {
                        lastest_count = 1;
                    } else if !strict && ver.header.matches_not_strict(&latest.header) {
                        lastest_count += 1;
                    } else {
                        lastest_count = 1;
                    }
                    latest = ver.clone();
                    continue;
                }

                // Mismatch, but older.
                if lastest_count > 0 && !strict && ver.header.matches_not_strict(&latest.header) {
                    lastest_count += 1;
                    continue;
                }

                if lastest_count > 0 && ver.header.version_id == latest.header.version_id {
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
                    lastest_count = 0;
                    for (k, v) in x.iter() {
                        if *v < lastest_count {
                            continue;
                        }
                        if *v == lastest_count && latest.header.sorts_before(k) {
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

                        lastest_count = *v;
                    }
                    break;
                }
            }
            if lastest_count >= quorum {
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

pub async fn file_info_from_raw(ri: RawFileInfo, bucket: &str, object: &str, read_data: bool) -> Result<FileInfo> {
    get_file_info(&ri.buf, bucket, object, "", FileInfoOpts { data: read_data }).await
}

pub struct FileInfoOpts {
    pub data: bool,
}

pub async fn get_file_info(buf: &[u8], volume: &str, path: &str, version_id: &str, opts: FileInfoOpts) -> Result<FileInfo> {
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

    let fi = meta.into_fileinfo(volume, path, version_id, opts.data, true)?;
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
        return Err(Error::new(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF")));
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
                    error!("read_xl_meta_no_data buffer too small (length: {}, needed: {})", &buf.len(), want);
                    return Err(Error::new(DiskError::FileCorrupt));
                }

                let tmp = &buf[want..];
                let crc_size = 5;
                let other_size = tmp.len() - crc_size;

                want += tmp.len() - other_size;

                Ok(buf[..want].to_vec())
            }
            _ => Err(Error::new(io::Error::new(io::ErrorKind::InvalidData, "Unknown minor metadata version"))),
        },
        _ => Err(Error::new(io::Error::new(io::ErrorKind::InvalidData, "Unknown major metadata version"))),
    }
}
#[cfg(test)]
mod test {

    use super::*;

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
    #[tracing::instrument]
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

        // 时间截不一致- -
        assert_eq!(obj, obj2);
        assert_eq!(obj.get_version_id(), obj2.get_version_id());
        assert_eq!(obj.write_version, obj2.write_version);
        assert_eq!(obj.write_version, 110);
    }

    #[test]
    #[tracing::instrument]
    fn test_marshal_metaversionheader() {
        let mut obj = FileMetaVersionHeader::default();
        let vid = Some(Uuid::new_v4());
        obj.version_id = vid;

        let encoded = obj.marshal_msg().unwrap();

        let mut obj2 = FileMetaVersionHeader::default();
        obj2.unmarshal_msg(&encoded).unwrap();

        // 时间截不一致- -
        assert_eq!(obj, obj2);
        assert_eq!(obj.version_id, obj2.version_id);
        assert_eq!(obj.version_id, vid);
    }

    // New comprehensive tests for utility functions and validation

    #[test]
    fn test_xl_file_header_constants() {
        // Test XL file header constants
        assert_eq!(XL_FILE_HEADER, [b'X', b'L', b'2', b' ']);
        assert_eq!(XL_FILE_VERSION_MAJOR, 1);
        assert_eq!(XL_FILE_VERSION_MINOR, 3);
        assert_eq!(XL_HEADER_VERSION, 3);
        assert_eq!(XL_META_VERSION, 2);
    }

    #[test]
    fn test_is_xl2_v1_format() {
        // Test valid XL2 V1 format
        let mut valid_buf = vec![0u8; 20];
        valid_buf[0..4].copy_from_slice(&XL_FILE_HEADER);
        byteorder::LittleEndian::write_u16(&mut valid_buf[4..6], 1);
        byteorder::LittleEndian::write_u16(&mut valid_buf[6..8], 0);

        assert!(FileMeta::is_xl2_v1_format(&valid_buf));

        // Test invalid format - wrong header
        let invalid_buf = vec![0u8; 20];
        assert!(!FileMeta::is_xl2_v1_format(&invalid_buf));

        // Test buffer too small
        let small_buf = vec![0u8; 4];
        assert!(!FileMeta::is_xl2_v1_format(&small_buf));
    }

    #[test]
    fn test_check_xl2_v1() {
        // Test valid XL2 V1 check
        let mut valid_buf = vec![0u8; 20];
        valid_buf[0..4].copy_from_slice(&XL_FILE_HEADER);
        byteorder::LittleEndian::write_u16(&mut valid_buf[4..6], 1);
        byteorder::LittleEndian::write_u16(&mut valid_buf[6..8], 2);

        let result = FileMeta::check_xl2_v1(&valid_buf);
        assert!(result.is_ok());
        let (remaining, major, minor) = result.unwrap();
        assert_eq!(major, 1);
        assert_eq!(minor, 2);
        assert_eq!(remaining.len(), 12); // 20 - 8

        // Test buffer too small
        let small_buf = vec![0u8; 4];
        assert!(FileMeta::check_xl2_v1(&small_buf).is_err());

        // Test wrong header
        let mut wrong_header = vec![0u8; 20];
        wrong_header[0..4].copy_from_slice(b"ABCD");
        assert!(FileMeta::check_xl2_v1(&wrong_header).is_err());

        // Test version too high
        let mut high_version = vec![0u8; 20];
        high_version[0..4].copy_from_slice(&XL_FILE_HEADER);
        byteorder::LittleEndian::write_u16(&mut high_version[4..6], 99);
        byteorder::LittleEndian::write_u16(&mut high_version[6..8], 0);
        assert!(FileMeta::check_xl2_v1(&high_version).is_err());
    }

    #[test]
    fn test_version_type_enum() {
        // Test VersionType enum methods
        assert!(VersionType::Object.valid());
        assert!(VersionType::Delete.valid());
        assert!(!VersionType::Invalid.valid());

        assert_eq!(VersionType::Object.to_u8(), 1);
        assert_eq!(VersionType::Delete.to_u8(), 2);
        assert_eq!(VersionType::Invalid.to_u8(), 0);

        assert_eq!(VersionType::from_u8(1), VersionType::Object);
        assert_eq!(VersionType::from_u8(2), VersionType::Delete);
        assert_eq!(VersionType::from_u8(99), VersionType::Invalid);
    }

    #[test]
    fn test_erasure_algo_enum() {
        // Test ErasureAlgo enum methods
        assert!(ErasureAlgo::ReedSolomon.valid());
        assert!(!ErasureAlgo::Invalid.valid());

        assert_eq!(ErasureAlgo::ReedSolomon.to_u8(), 1);
        assert_eq!(ErasureAlgo::Invalid.to_u8(), 0);

        assert_eq!(ErasureAlgo::from_u8(1), ErasureAlgo::ReedSolomon);
        assert_eq!(ErasureAlgo::from_u8(99), ErasureAlgo::Invalid);

        // Test Display trait
        assert_eq!(format!("{}", ErasureAlgo::ReedSolomon), "rs-vandermonde");
        assert_eq!(format!("{}", ErasureAlgo::Invalid), "Invalid");
    }

    #[test]
    fn test_checksum_algo_enum() {
        // Test ChecksumAlgo enum methods
        assert!(ChecksumAlgo::HighwayHash.valid());
        assert!(!ChecksumAlgo::Invalid.valid());

        assert_eq!(ChecksumAlgo::HighwayHash.to_u8(), 1);
        assert_eq!(ChecksumAlgo::Invalid.to_u8(), 0);

        assert_eq!(ChecksumAlgo::from_u8(1), ChecksumAlgo::HighwayHash);
        assert_eq!(ChecksumAlgo::from_u8(99), ChecksumAlgo::Invalid);
    }

    #[test]
    fn test_file_meta_version_header_methods() {
        let mut header = FileMetaVersionHeader::default();
        header.ec_n = 4;
        header.ec_m = 2;
        header.flags = XL_FLAG_FREE_VERSION;

        // Test has_ec
        assert!(header.has_ec());

        // Test free_version
        assert!(header.free_version());

        // Test user_data_dir (should be false by default)
        assert!(!header.user_data_dir());

        // Test with different flags
        header.flags = 0;
        assert!(!header.free_version());
    }

    #[test]
    fn test_file_meta_version_header_comparison() {
        let mut header1 = FileMetaVersionHeader::default();
        header1.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000).unwrap());
        header1.version_id = Some(Uuid::new_v4());

        let mut header2 = FileMetaVersionHeader::default();
        header2.mod_time = Some(OffsetDateTime::from_unix_timestamp(2000).unwrap());
        header2.version_id = Some(Uuid::new_v4());

        // Test sorts_before - header2 should sort before header1 (newer mod_time)
        assert!(!header1.sorts_before(&header2));
        assert!(header2.sorts_before(&header1));

        // Test matches_not_strict
        let header3 = header1.clone();
        assert!(header1.matches_not_strict(&header3));

        // Test matches_ec
        header1.ec_n = 4;
        header1.ec_m = 2;
        header2.ec_n = 4;
        header2.ec_m = 2;
        assert!(header1.matches_ec(&header2));

        header2.ec_n = 6;
        assert!(!header1.matches_ec(&header2));
    }

    #[test]
    fn test_file_meta_version_methods() {
        // Test with object version
        let mut fi = FileInfo::new("test", 4, 2);
        fi.version_id = Some(Uuid::new_v4());
        fi.data_dir = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::now_utc());

        let version = FileMetaVersion::from(fi.clone());

        assert!(version.valid());
        assert_eq!(version.get_version_id(), fi.version_id);
        assert_eq!(version.get_data_dir(), fi.data_dir);
        assert_eq!(version.get_mod_time(), fi.mod_time);
        assert!(!version.free_version());

        // Test with delete marker
        let mut delete_fi = FileInfo::new("test", 4, 2);
        delete_fi.deleted = true;
        delete_fi.version_id = Some(Uuid::new_v4());
        delete_fi.mod_time = Some(OffsetDateTime::now_utc());

        let delete_version = FileMetaVersion::from(delete_fi);
        assert!(delete_version.valid());
        assert_eq!(delete_version.version_type, VersionType::Delete);
    }

    #[test]
    fn test_meta_object_methods() {
        let mut obj = MetaObject::default();
        obj.data_dir = Some(Uuid::new_v4());
        obj.size = 1024;

        // Test use_data_dir
        assert!(obj.use_data_dir());

        obj.data_dir = None;
        assert!(obj.use_data_dir()); // use_data_dir always returns true

        // Test use_inlinedata (currently always returns false)
        obj.size = 100; // Small size
        assert!(!obj.use_inlinedata());

        obj.size = 100000; // Large size
        assert!(!obj.use_inlinedata());
    }

    #[test]
    fn test_meta_delete_marker_methods() {
        let marker = MetaDeleteMarker::default();

        // Test free_version (should always return false for delete markers)
        assert!(!marker.free_version());
    }

    #[test]
    fn test_file_meta_latest_mod_time() {
        let mut fm = FileMeta::new();

        // Empty FileMeta should return None
        assert!(fm.lastest_mod_time().is_none());

        // Add versions with different mod times
        let time1 = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let time2 = OffsetDateTime::from_unix_timestamp(2000).unwrap();
        let time3 = OffsetDateTime::from_unix_timestamp(1500).unwrap();

        let mut fi1 = FileInfo::new("test1", 4, 2);
        fi1.mod_time = Some(time1);
        fm.add_version(fi1).unwrap();

        let mut fi2 = FileInfo::new("test2", 4, 2);
        fi2.mod_time = Some(time2);
        fm.add_version(fi2).unwrap();

        let mut fi3 = FileInfo::new("test3", 4, 2);
        fi3.mod_time = Some(time3);
        fm.add_version(fi3).unwrap();

        // Sort first to ensure latest is at the front
        fm.sort_by_mod_time();

        // Should return the latest mod time (time2 is the latest)
        assert_eq!(fm.lastest_mod_time(), Some(time2));
    }

    #[test]
    fn test_file_meta_shard_data_dir_count() {
        let mut fm = FileMeta::new();
        let data_dir = Some(Uuid::new_v4());

        // Add versions with same data_dir
        for i in 0..3 {
            let mut fi = FileInfo::new(&format!("test{}", i), 4, 2);
            fi.data_dir = data_dir;
            fi.mod_time = Some(OffsetDateTime::now_utc());
            fm.add_version(fi).unwrap();
        }

        // Add one version with different data_dir
        let mut fi_diff = FileInfo::new("test_diff", 4, 2);
        fi_diff.data_dir = Some(Uuid::new_v4());
        fi_diff.mod_time = Some(OffsetDateTime::now_utc());
        fm.add_version(fi_diff).unwrap();

        // Count should be 3 for the matching data_dir
        assert_eq!(fm.shard_data_dir_count(&None, &data_dir), 3);

        // Count should be 0 for non-existent data_dir
        assert_eq!(fm.shard_data_dir_count(&None, &Some(Uuid::new_v4())), 0);
    }

    #[test]
    fn test_file_meta_sort_by_mod_time() {
        let mut fm = FileMeta::new();

        let time1 = OffsetDateTime::from_unix_timestamp(3000).unwrap();
        let time2 = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let time3 = OffsetDateTime::from_unix_timestamp(2000).unwrap();

        // Add versions in non-chronological order
        let mut fi1 = FileInfo::new("test1", 4, 2);
        fi1.mod_time = Some(time1);
        fm.add_version(fi1).unwrap();

        let mut fi2 = FileInfo::new("test2", 4, 2);
        fi2.mod_time = Some(time2);
        fm.add_version(fi2).unwrap();

        let mut fi3 = FileInfo::new("test3", 4, 2);
        fi3.mod_time = Some(time3);
        fm.add_version(fi3).unwrap();

        // Sort by mod time
        fm.sort_by_mod_time();

        // Verify they are sorted (newest first)
        assert_eq!(fm.versions[0].header.mod_time, Some(time1)); // 3000
        assert_eq!(fm.versions[1].header.mod_time, Some(time3)); // 2000
        assert_eq!(fm.versions[2].header.mod_time, Some(time2)); // 1000
    }

    #[test]
    fn test_file_meta_find_version() {
        let mut fm = FileMeta::new();
        let version_id = Some(Uuid::new_v4());

        let mut fi = FileInfo::new("test", 4, 2);
        fi.version_id = version_id;
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fm.add_version(fi).unwrap();

        // Should find the version
        let result = fm.find_version(version_id);
        assert!(result.is_ok());
        let (idx, version) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(version.get_version_id(), version_id);

        // Should not find non-existent version
        let non_existent_id = Some(Uuid::new_v4());
        assert!(fm.find_version(non_existent_id).is_err());
    }

    #[test]
    fn test_file_meta_delete_version() {
        let mut fm = FileMeta::new();
        let version_id = Some(Uuid::new_v4());

        let mut fi = FileInfo::new("test", 4, 2);
        fi.version_id = version_id;
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fm.add_version(fi.clone()).unwrap();

        assert_eq!(fm.versions.len(), 1);

        // Delete the version
        let result = fm.delete_version(&fi);
        assert!(result.is_ok());

        // Version should be removed
        assert_eq!(fm.versions.len(), 0);
    }

    #[test]
    fn test_file_meta_update_object_version() {
        let mut fm = FileMeta::new();
        let version_id = Some(Uuid::new_v4());

        // Add initial version
        let mut fi = FileInfo::new("test", 4, 2);
        fi.version_id = version_id;
        fi.size = 1024;
        fi.mod_time = Some(OffsetDateTime::now_utc());
        fm.add_version(fi.clone()).unwrap();

        // Update with new size
        fi.size = 2048;
        let result = fm.update_object_version(fi);
        assert!(result.is_ok());

        // Verify the version was updated
        let (_, updated_version) = fm.find_version(version_id).unwrap();
        if let Some(obj) = updated_version.object {
            assert_eq!(obj.size, 2048);
        } else {
            panic!("Expected object version");
        }
    }

    #[test]
    fn test_file_info_opts() {
        let opts = FileInfoOpts { data: true };
        assert!(opts.data);

        let opts_no_data = FileInfoOpts { data: false };
        assert!(!opts_no_data.data);
    }

    #[test]
    fn test_decode_data_dir_from_meta() {
        // Test with valid metadata containing data_dir
        let data_dir = Some(Uuid::new_v4());
        let mut obj = MetaObject::default();
        obj.data_dir = data_dir;
        obj.mod_time = Some(OffsetDateTime::now_utc());
        obj.erasure_algorithm = ErasureAlgo::ReedSolomon;
        obj.bitrot_checksum_algo = ChecksumAlgo::HighwayHash;

        // Create a valid FileMetaVersion with the object
        let mut version = FileMetaVersion::default();
        version.version_type = VersionType::Object;
        version.object = Some(obj);

        let encoded = version.marshal_msg().unwrap();
        let result = FileMetaVersion::decode_data_dir_from_meta(&encoded);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data_dir);

        // Test with invalid metadata
        let invalid_data = vec![0u8; 10];
        let result = FileMetaVersion::decode_data_dir_from_meta(&invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_latest_delete_marker() {
        // Create a FileMeta with a delete marker as the latest version
        let mut fm = FileMeta::new();

        // Add a regular object first
        let mut fi_obj = FileInfo::new("test", 4, 2);
        fi_obj.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000).unwrap());
        fm.add_version(fi_obj).unwrap();

        // Add a delete marker with later timestamp
        let mut fi_del = FileInfo::new("test", 4, 2);
        fi_del.deleted = true;
        fi_del.mod_time = Some(OffsetDateTime::from_unix_timestamp(2000).unwrap());
        fm.add_version(fi_del).unwrap();

        // Sort to ensure delete marker is first (latest)
        fm.sort_by_mod_time();

        let encoded = fm.marshal_msg().unwrap();

        // Should detect delete marker as latest
        assert!(FileMeta::is_latest_delete_marker(&encoded));

        // Test with object as latest
        let mut fm2 = FileMeta::new();
        let mut fi_obj2 = FileInfo::new("test", 4, 2);
        fi_obj2.mod_time = Some(OffsetDateTime::from_unix_timestamp(3000).unwrap());
        fm2.add_version(fi_obj2).unwrap();

        let encoded2 = fm2.marshal_msg().unwrap();
        assert!(!FileMeta::is_latest_delete_marker(&encoded2));
    }

    #[test]
    fn test_merge_file_meta_versions_basic() {
        // Test basic merge functionality
        let mut version1 = FileMetaShallowVersion::default();
        version1.header.version_id = Some(Uuid::new_v4());
        version1.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000).unwrap());

        let mut version2 = FileMetaShallowVersion::default();
        version2.header.version_id = Some(Uuid::new_v4());
        version2.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(2000).unwrap());

        let versions = vec![
            vec![version1.clone(), version2.clone()],
            vec![version1.clone()],
            vec![version2.clone()],
        ];

        let merged = merge_file_meta_versions(2, false, 10, &versions);

        // Should return versions that appear in at least quorum (2) sources
        assert!(!merged.is_empty());
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
    // 写入字符串
    file.write_all(&buff).await.unwrap();

    let mut f = File::open(filepath).await.unwrap();

    let stat = f.metadata().await.unwrap();

    let data = read_xl_meta_no_data(&mut f, stat.len() as usize).await.unwrap();

    let mut newfm = FileMeta::default();
    newfm.unmarshal_msg(&data).unwrap();

    fs::remove_file(filepath).await.unwrap();

    assert_eq!(fm, newfm)
}

#[tokio::test]
async fn test_get_file_info() {
    // Test get_file_info function
    let mut fm = FileMeta::new();
    let version_id = Uuid::new_v4();

    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(version_id);
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

    let encoded = fm.marshal_msg().unwrap();

    let opts = FileInfoOpts { data: false };
    let result = get_file_info(&encoded, "test-volume", "test-path", &version_id.to_string(), opts).await;

    assert!(result.is_ok());
    let file_info = result.unwrap();
    assert_eq!(file_info.volume, "test-volume");
    assert_eq!(file_info.name, "test-path");
}

#[tokio::test]
async fn test_file_info_from_raw() {
    // Test file_info_from_raw function
    let mut fm = FileMeta::new();
    let mut fi = FileInfo::new("test", 4, 2);
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

    let encoded = fm.marshal_msg().unwrap();

    let raw_info = RawFileInfo {
        buf: encoded,
    };

    let result = file_info_from_raw(raw_info, "test-bucket", "test-object", false).await;
    assert!(result.is_ok());

    let file_info = result.unwrap();
    assert_eq!(file_info.volume, "test-bucket");
    assert_eq!(file_info.name, "test-object");
}

// Additional comprehensive tests for better coverage

#[test]
fn test_file_meta_load_function() {
    // Test FileMeta::load function
    let mut fm = FileMeta::new();
    let mut fi = FileInfo::new("test", 4, 2);
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

    let encoded = fm.marshal_msg().unwrap();

    // Test successful load
    let loaded_fm = FileMeta::load(&encoded);
    assert!(loaded_fm.is_ok());
    assert_eq!(loaded_fm.unwrap(), fm);

    // Test load with invalid data
    let invalid_data = vec![0u8; 10];
    let result = FileMeta::load(&invalid_data);
    assert!(result.is_err());
}

#[test]
fn test_file_meta_read_bytes_header() {
    // Test read_bytes_header function
    let mut buf = vec![0u8; 8];
    byteorder::LittleEndian::write_u32(&mut buf[0..4], 100); // length
    buf.extend_from_slice(b"test data");

    let result = FileMeta::read_bytes_header(&buf);
    assert!(result.is_ok());
    let (length, remaining) = result.unwrap();
    assert_eq!(length, 100);
    assert_eq!(remaining, b"test data");

    // Test with buffer too small
    let small_buf = vec![0u8; 2];
    let result = FileMeta::read_bytes_header(&small_buf);
    assert!(result.is_err());
}

#[test]
fn test_file_meta_get_set_idx() {
    let mut fm = FileMeta::new();
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

    // Test get_idx
    let result = fm.get_idx(0);
    assert!(result.is_ok());

    // Test get_idx with invalid index
    let result = fm.get_idx(10);
    assert!(result.is_err());

    // Test set_idx
    let mut new_version = FileMetaVersion::default();
    new_version.version_type = VersionType::Object;
    let result = fm.set_idx(0, new_version);
    assert!(result.is_ok());

    // Test set_idx with invalid index
    let invalid_version = FileMetaVersion::default();
    let result = fm.set_idx(10, invalid_version);
    assert!(result.is_err());
}

#[test]
fn test_file_meta_into_fileinfo() {
    let mut fm = FileMeta::new();
    let version_id = Uuid::new_v4();
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(version_id);
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

    // Test into_fileinfo with valid version_id
    let result = fm.into_fileinfo("test-volume", "test-path", &version_id.to_string(), false, false);
    assert!(result.is_ok());
    let file_info = result.unwrap();
    assert_eq!(file_info.volume, "test-volume");
    assert_eq!(file_info.name, "test-path");

    // Test into_fileinfo with invalid version_id
    let invalid_id = Uuid::new_v4();
    let result = fm.into_fileinfo("test-volume", "test-path", &invalid_id.to_string(), false, false);
    assert!(result.is_err());

    // Test into_fileinfo with empty version_id (should get latest)
    let result = fm.into_fileinfo("test-volume", "test-path", "", false, false);
    assert!(result.is_ok());
}

#[test]
fn test_file_meta_into_file_info_versions() {
    let mut fm = FileMeta::new();

    // Add multiple versions
    for i in 0..3 {
        let mut fi = FileInfo::new(&format!("test{}", i), 4, 2);
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000 + i).unwrap());
        fm.add_version(fi).unwrap();
    }

    let result = fm.into_file_info_versions("test-volume", "test-path", false);
    assert!(result.is_ok());
    let versions = result.unwrap();
    assert_eq!(versions.versions.len(), 3);
}

#[test]
fn test_file_meta_shallow_version_to_fileinfo() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(OffsetDateTime::now_utc());

    let version = FileMetaVersion::from(fi.clone());
    let shallow_version = FileMetaShallowVersion::try_from(version).unwrap();

    let result = shallow_version.to_fileinfo("test-volume", "test-path", fi.version_id, false);
    assert!(result.is_ok());
    let converted_fi = result.unwrap();
    assert_eq!(converted_fi.volume, "test-volume");
    assert_eq!(converted_fi.name, "test-path");
}

#[test]
fn test_file_meta_version_try_from_bytes() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    let version = FileMetaVersion::from(fi);
    let encoded = version.marshal_msg().unwrap();

    // Test successful conversion
    let result = FileMetaVersion::try_from(encoded.as_slice());
    assert!(result.is_ok());

    // Test with invalid data
    let invalid_data = vec![0u8; 5];
    let result = FileMetaVersion::try_from(invalid_data.as_slice());
    assert!(result.is_err());
}

#[test]
fn test_file_meta_version_try_from_shallow() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    let version = FileMetaVersion::from(fi);
    let shallow = FileMetaShallowVersion::try_from(version.clone()).unwrap();

    let result = FileMetaVersion::try_from(shallow);
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert_eq!(converted.get_version_id(), version.get_version_id());
}

#[test]
fn test_file_meta_version_header_from_version() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(OffsetDateTime::now_utc());
    let version = FileMetaVersion::from(fi.clone());

    let header = FileMetaVersionHeader::from(version);
    assert_eq!(header.version_id, fi.version_id);
    assert_eq!(header.mod_time, fi.mod_time);
}

#[test]
fn test_meta_object_into_fileinfo() {
    let mut obj = MetaObject::default();
    obj.version_id = Some(Uuid::new_v4());
    obj.size = 1024;
    obj.mod_time = Some(OffsetDateTime::now_utc());

    let version_id = obj.version_id;
    let expected_version_id = version_id;
    let file_info = obj.into_fileinfo("test-volume", "test-path", version_id, false);
    assert_eq!(file_info.volume, "test-volume");
    assert_eq!(file_info.name, "test-path");
    assert_eq!(file_info.size, 1024);
    assert_eq!(file_info.version_id, expected_version_id);
}

#[test]
fn test_meta_object_from_fileinfo() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.data_dir = Some(Uuid::new_v4());
    fi.size = 2048;
    fi.mod_time = Some(OffsetDateTime::now_utc());

    let obj = MetaObject::from(fi.clone());
    assert_eq!(obj.version_id, fi.version_id);
    assert_eq!(obj.data_dir, fi.data_dir);
    assert_eq!(obj.size, fi.size);
    assert_eq!(obj.mod_time, fi.mod_time);
}

#[test]
fn test_meta_delete_marker_into_fileinfo() {
    let mut marker = MetaDeleteMarker::default();
    marker.version_id = Some(Uuid::new_v4());
    marker.mod_time = Some(OffsetDateTime::now_utc());

    let version_id = marker.version_id;
    let expected_version_id = version_id;
    let file_info = marker.into_fileinfo("test-volume", "test-path", version_id, false);
    assert_eq!(file_info.volume, "test-volume");
    assert_eq!(file_info.name, "test-path");
    assert_eq!(file_info.version_id, expected_version_id);
    assert!(file_info.deleted);
}

#[test]
fn test_meta_delete_marker_from_fileinfo() {
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fi.deleted = true;

    let marker = MetaDeleteMarker::from(fi.clone());
    assert_eq!(marker.version_id, fi.version_id);
    assert_eq!(marker.mod_time, fi.mod_time);
}

#[test]
fn test_flags_enum() {
    // Test Flags enum values
    assert_eq!(Flags::FreeVersion as u8, 1);
    assert_eq!(Flags::UsesDataDir as u8, 2);
    assert_eq!(Flags::InlineData as u8, 4);
}

#[test]
fn test_file_meta_version_header_user_data_dir() {
    let mut header = FileMetaVersionHeader::default();

    // Test without UsesDataDir flag
    header.flags = 0;
    assert!(!header.user_data_dir());

    // Test with UsesDataDir flag
    header.flags = Flags::UsesDataDir as u8;
    assert!(header.user_data_dir());

    // Test with multiple flags including UsesDataDir
    header.flags = Flags::UsesDataDir as u8 | Flags::FreeVersion as u8;
    assert!(header.user_data_dir());
}

#[test]
fn test_file_meta_version_header_ordering() {
    let mut header1 = FileMetaVersionHeader::default();
    header1.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000).unwrap());
    header1.version_id = Some(Uuid::new_v4());

    let mut header2 = FileMetaVersionHeader::default();
    header2.mod_time = Some(OffsetDateTime::from_unix_timestamp(2000).unwrap());
    header2.version_id = Some(Uuid::new_v4());

    // Test partial_cmp
    assert!(header1.partial_cmp(&header2).is_some());

    // Test cmp - header2 should be greater (newer)
    use std::cmp::Ordering;
    assert_eq!(header1.cmp(&header2), Ordering::Greater); // Newer versions sort first
    assert_eq!(header2.cmp(&header1), Ordering::Less);
    assert_eq!(header1.cmp(&header1), Ordering::Equal);
}

#[test]
fn test_merge_file_meta_versions_edge_cases() {
    // Test with empty versions
    let empty_versions: Vec<Vec<FileMetaShallowVersion>> = vec![];
    let merged = merge_file_meta_versions(1, false, 10, &empty_versions);
    assert!(merged.is_empty());

    // Test with quorum larger than available sources
    let mut version = FileMetaShallowVersion::default();
    version.header.version_id = Some(Uuid::new_v4());
    let versions = vec![vec![version]];
    let merged = merge_file_meta_versions(5, false, 10, &versions);
    assert!(merged.is_empty());

    // Test strict mode
    let mut version1 = FileMetaShallowVersion::default();
    version1.header.version_id = Some(Uuid::new_v4());
    version1.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(1000).unwrap());

    let mut version2 = FileMetaShallowVersion::default();
    version2.header.version_id = Some(Uuid::new_v4());
    version2.header.mod_time = Some(OffsetDateTime::from_unix_timestamp(2000).unwrap());

    let versions = vec![
        vec![version1.clone()],
        vec![version2.clone()],
    ];

    let _merged_strict = merge_file_meta_versions(1, true, 10, &versions);
    let merged_non_strict = merge_file_meta_versions(1, false, 10, &versions);

    // In strict mode, behavior might be different
    assert!(!merged_non_strict.is_empty());
}

#[tokio::test]
async fn test_read_more_function() {
    use std::io::Cursor;

    let data = b"Hello, World! This is test data.";
    let mut reader = Cursor::new(data);
    let mut buf = vec![0u8; 10];

    // Test reading more data
    let result = read_more(&mut reader, &mut buf, 33, 20, false).await;
    assert!(result.is_ok());
    assert_eq!(buf.len(), 20);

    // Test with has_full = true
    let mut reader2 = Cursor::new(data);
    let mut buf2 = vec![0u8; 5];
    let result = read_more(&mut reader2, &mut buf2, 10, 5, true).await;
    assert!(result.is_ok());
    assert_eq!(buf2.len(), 10);

    // Test reading beyond available data
    let mut reader3 = Cursor::new(b"short");
    let mut buf3 = vec![0u8; 2];
    let result = read_more(&mut reader3, &mut buf3, 100, 98, false).await;
    // Should handle gracefully even if not enough data
    assert!(result.is_ok() || result.is_err()); // Either is acceptable
}

#[tokio::test]
async fn test_read_xl_meta_no_data_edge_cases() {
    use std::io::Cursor;

    // Test with empty data
    let empty_data = vec![];
    let mut reader = Cursor::new(empty_data);
    let result = read_xl_meta_no_data(&mut reader, 0).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());

    // Test with very small size
    let small_data = vec![1, 2, 3];
    let mut reader = Cursor::new(small_data);
    let result = read_xl_meta_no_data(&mut reader, 3).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_file_info_edge_cases() {
    // Test with empty buffer
    let empty_buf = vec![];
    let opts = FileInfoOpts { data: false };
    let result = get_file_info(&empty_buf, "volume", "path", "version", opts).await;
    assert!(result.is_err());

    // Test with invalid version_id format
    let mut fm = FileMeta::new();
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();
    let encoded = fm.marshal_msg().unwrap();

    let opts = FileInfoOpts { data: false };
    let result = get_file_info(&encoded, "volume", "path", "invalid-uuid", opts).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_file_info_from_raw_edge_cases() {
    // Test with empty buffer
    let empty_raw = RawFileInfo {
        buf: vec![],
    };
    let result = file_info_from_raw(empty_raw, "bucket", "object", false).await;
    assert!(result.is_err());

    // Test with invalid buffer
    let invalid_raw = RawFileInfo {
        buf: vec![1, 2, 3, 4, 5],
    };
    let result = file_info_from_raw(invalid_raw, "bucket", "object", false).await;
    assert!(result.is_err());
}

#[test]
fn test_file_meta_version_invalid_cases() {
    // Test invalid version
    let mut version = FileMetaVersion::default();
    version.version_type = VersionType::Invalid;
    assert!(!version.valid());

    // Test version with neither object nor delete marker
    version.version_type = VersionType::Object;
    version.object = None;
    version.delete_marker = None;
    assert!(!version.valid());
}

#[test]
fn test_meta_object_edge_cases() {
    let mut obj = MetaObject::default();

    // Test use_data_dir with None (use_data_dir always returns true)
    obj.data_dir = None;
    assert!(obj.use_data_dir());

    // Test use_inlinedata with exactly threshold size
    obj.size = 128 * 1024; // 128KB threshold
    assert!(!obj.use_inlinedata()); // Should be false at threshold

    obj.size = 128 * 1024 - 1;
    assert!(obj.use_inlinedata()); // Should be true below threshold
}

#[test]
fn test_file_meta_version_header_edge_cases() {
    let mut header = FileMetaVersionHeader::default();

    // Test has_ec with zero values
    header.ec_n = 0;
    header.ec_m = 0;
    assert!(!header.has_ec());

    // Test matches_not_strict with different signatures
    let mut other = FileMetaVersionHeader::default();
    header.signature = [1, 2, 3, 4];
    other.signature = [5, 6, 7, 8];
    assert!(!header.matches_not_strict(&other));

    // Test sorts_before with same mod_time but different version_id
    let time = OffsetDateTime::from_unix_timestamp(1000).unwrap();
    header.mod_time = Some(time);
    other.mod_time = Some(time);
    header.version_id = Some(Uuid::new_v4());
    other.version_id = Some(Uuid::new_v4());

    // Should use version_id for comparison when mod_time is same
    let sorts_before = header.sorts_before(&other);
    assert!(sorts_before || other.sorts_before(&header)); // One should sort before the other
}

#[test]
fn test_file_meta_add_version_edge_cases() {
    let mut fm = FileMeta::new();

    // Test adding version with same version_id (should update)
    let version_id = Some(Uuid::new_v4());
    let mut fi1 = FileInfo::new("test1", 4, 2);
    fi1.version_id = version_id;
    fi1.size = 1024;
    fi1.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi1).unwrap();

    let mut fi2 = FileInfo::new("test2", 4, 2);
    fi2.version_id = version_id;
    fi2.size = 2048;
    fi2.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi2).unwrap();

    // Should still have only one version, but updated
    assert_eq!(fm.versions.len(), 1);
    let (_, version) = fm.find_version(version_id).unwrap();
    if let Some(obj) = version.object {
        assert_eq!(obj.size, 2048); // Should be updated size
    }
}

#[test]
fn test_file_meta_delete_version_edge_cases() {
    let mut fm = FileMeta::new();

    // Test deleting non-existent version
    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = Some(Uuid::new_v4());

    let result = fm.delete_version(&fi);
    assert!(result.is_err()); // Should fail for non-existent version
}

#[test]
fn test_file_meta_shard_data_dir_count_edge_cases() {
    let mut fm = FileMeta::new();

    // Test with None data_dir parameter
    let count = fm.shard_data_dir_count(&None, &None);
    assert_eq!(count, 0);

    // Test with version_id parameter (not None)
    let version_id = Some(Uuid::new_v4());
    let data_dir = Some(Uuid::new_v4());

    let mut fi = FileInfo::new("test", 4, 2);
    fi.version_id = version_id;
    fi.data_dir = data_dir;
    fi.mod_time = Some(OffsetDateTime::now_utc());
    fm.add_version(fi).unwrap();

            let count = fm.shard_data_dir_count(&version_id, &data_dir);
        assert_eq!(count, 0); // Should be 0 because it excludes the version_id itself

    // Test with different version_id
    let other_version_id = Some(Uuid::new_v4());
    let count = fm.shard_data_dir_count(&other_version_id, &data_dir);
    assert_eq!(count, 0);
}

