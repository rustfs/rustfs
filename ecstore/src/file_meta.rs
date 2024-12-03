use byteorder::ByteOrder;
use rmp::Marker;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::io::{Read, Write};
use std::{collections::HashMap, io::Cursor};
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;
use xxhash_rust::xxh64;

use crate::disk::FileInfoVersions;
use crate::file_meta_inline::InlineData;
use crate::store_api::RawFileInfo;
use crate::{
    disk::error::DiskError,
    error::{Error, Result},
    store_api::{ErasureInfo, FileInfo, ObjectPartInfo, ERASURE_ALGORITHM},
};

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

    pub fn is_xl_format(buf: &[u8]) -> bool {
        !matches!(Self::read_xl_file_header(buf), Err(_e))
    }

    pub fn load(buf: &[u8]) -> Result<FileMeta> {
        let mut xl = FileMeta::default();
        xl.unmarshal_msg(buf)?;

        Ok(xl)
    }

    // read_xl_file_header 读xl文件头，返回后续内容，版本信息
    #[tracing::instrument]
    pub fn read_xl_file_header(buf: &[u8]) -> Result<(&[u8], u16, u16)> {
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
    #[tracing::instrument]
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let i = buf.len() as u64;

        // check version, buf = buf[8..]
        let (buf, _, _) = Self::read_xl_file_header(buf)?;

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
            let (versions_len, _, meta_ver, read_size) = Self::decode_head_ver(meta)?;

            let (_, meta) = meta.split_at(read_size as usize);

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

    // decode_head_ver 解析 meta 头，返回 (versions数量，xl_header_version, xl_meta_version, 已读数据长度)
    #[tracing::instrument]
    fn decode_head_ver(buf: &[u8]) -> Result<(usize, u8, u8, u64)> {
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

        Ok((versions_len, header_ver, meta_ver, cur.position()))
    }

    #[tracing::instrument]
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

        let pre_mod_time = self.versions[idx].header.mod_time.clone();

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

        self.versions.reverse();

        for (i, v) in self.versions.iter().enumerate() {
            warn!("sort {} {:?}", i, v);
        }
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
                if header.version_id == Some(vid) {
                    is_latest = false;
                    succ_mod_time = header.mod_time;
                    continue;
                }
            }

            let mut fi = ver.into_fileinfo(volume, path, has_vid, all_parts)?;
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
            let fi = file_version.into_fileinfo(volume, path, None, all_parts);
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
    pub fn into_fileinfo(&self, volume: &str, path: &str, version_id: Option<Uuid>, all_parts: bool) -> Result<FileInfo> {
        let file_version = FileMetaVersion::try_from(self.meta.as_slice())?;

        Ok(file_version.into_fileinfo(volume, path, version_id, all_parts))
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

    #[tracing::instrument]
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

    pub fn into_fileinfo(self, volume: &str, path: &str, version_id: Option<Uuid>, all_parts: bool) -> FileInfo {
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Ord, Hash)]
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
        if self.version_id.is_none() {
            return self.version_id == o.version_id && self.version_type == o.version_type && self.mod_time == o.mod_time;
        }

        self.version_id == o.version_id && self.version_type == o.version_type
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
    #[tracing::instrument]
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
        match self.mod_time.partial_cmp(&other.mod_time) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }

        match self.version_type.partial_cmp(&other.version_type) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.signature.partial_cmp(&other.signature) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.version_id.partial_cmp(&other.version_id) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.flags.partial_cmp(&other.flags)
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
    #[tracing::instrument]
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

async fn get_file_info(buf: &[u8], volume: &str, path: &str, version_id: &str, opts: FileInfoOpts) -> Result<FileInfo> {
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

        // println!("fm:{:?}", &fm);

        let buff = fm.marshal_msg().unwrap();

        let mut newfm = FileMeta::default();
        newfm.unmarshal_msg(&buff).unwrap();

        // println!("newone:{:?}", newone);

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
}
