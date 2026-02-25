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

use super::*;

impl FileMeta {
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
}
