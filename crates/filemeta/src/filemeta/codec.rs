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

    #[hotpath::measure(impl_type = "FileMeta")]
    pub fn load(buf: &[u8]) -> Result<FileMeta> {
        let mut xl = FileMeta::default();
        xl.unmarshal_msg(buf)?;
        Ok(xl)
    }

    /// Read (major, minor, header_ver, meta_ver) from xl.meta without full parse.
    /// Returns Err if format is not feat (e.g. legacy  uses rmp_serde in meta block).
    pub fn read_format_versions(buf: &[u8]) -> Result<(u16, u16, u8, u8)> {
        let (buf, major, minor) = Self::check_xl2_v1(buf)?;
        if buf.len() < 5 {
            return Err(Error::FileCorrupt);
        }
        let (mut size_buf, buf) = buf.split_at(5);
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).map_err(|_| Error::FileCorrupt)?;
        if buf.len() < bin_len as usize {
            return Err(Error::FileCorrupt);
        }
        let (meta, _) = buf.split_at(bin_len as usize);
        let (_, header_ver, meta_ver, _) = Self::decode_xl_headers(meta)?;
        Ok((major, minor, header_ver, meta_ver))
    }

    pub fn check_xl2_v1(buf: &[u8]) -> Result<(&[u8], u16, u16)> {
        // A file too short to hold the XL2 magic, or one that carries the
        // wrong magic, is not merely unreadable — it is affirmative evidence
        // of a torn or foreign write. Classify it as FileCorrupt so quorum
        // and listing code can distinguish deterministic damage from
        // transient IO faults (issue #5716).
        if buf.len() < 8 {
            return Err(Error::FileCorrupt);
        }

        if buf[0..4] != XL_FILE_HEADER {
            return Err(Error::FileCorrupt);
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

        if buf.len() < 5 {
            return Err(Error::FileCorrupt);
        }

        let (mut size_buf, buf) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).map_err(|_| Error::FileCorrupt)?;

        if buf.len() < bin_len as usize {
            return Err(Error::FileCorrupt);
        }
        let (meta, buf) = buf.split_at(bin_len as usize);

        if buf.len() < 5 {
            return Err(Error::FileCorrupt);
        }
        let (mut crc_buf, inline_data) = buf.split_at(5);

        // crc check
        let crc = rmp::decode::read_u32(&mut crc_buf).map_err(|_| Error::FileCorrupt)?;
        let meta_crc = xxh64::xxh64(meta, XXHASH_SEED) as u32;

        if crc != meta_crc {
            error!(
                event = "filemeta_xl_crc_mismatch",
                component = "filemeta",
                expected_crc = meta_crc,
                actual_crc = crc,
                "xl.meta payload failed its CRC check"
            );
            // Error::FileCorrupt, not a generic error, for the same reason
            // check_xl2_v1 classifies a bad magic as FileCorrupt: heal
            // classification (should_heal_object_on_disk) recognises
            // corruption only by the DiskError::FileCorrupt variant this
            // converts to. As a generic error the drive is skipped,
            // heal_object reports ok, and on-disk bitrot is never repaired.
            return Err(Error::FileCorrupt);
        }

        Ok((meta, inline_data))
    }

    // Fixed u32
    pub fn read_bytes_header(buf: &[u8]) -> Result<(u32, &[u8])> {
        if buf.len() < 5 {
            return Err(Error::FileCorrupt);
        }

        let (mut size_buf, remaining) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).map_err(|_| Error::FileCorrupt)?;

        Ok((bin_len, remaining))
    }

    #[inline]
    pub(super) fn parse_validated_xl_envelope(buf: &[u8]) -> Result<(&[u8], &[u8])> {
        // check version, buf = buf[8..]
        let (buf, _, _) = Self::check_xl2_v1(buf)?;

        // These bytes have already been read. Invalid framing is deterministic
        // metadata damage; preserve FileCorrupt so quorum-backed heal can repair it.
        if buf.len() < 5 {
            error!(
                "insufficient data for metadata length prefix: expected at least 5 bytes, got {}",
                buf.len()
            );
            return Err(Error::FileCorrupt);
        }

        let (mut size_buf, buf) = buf.split_at(5);

        // Get meta data, buf = crc + data
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).map_err(|e| {
            error!("failed to read binary length for metadata: {}", e);
            Error::FileCorrupt
        })?;

        if buf.len() < bin_len as usize {
            error!("insufficient data for metadata: expected {} bytes, got {} bytes", bin_len, buf.len());
            return Err(Error::FileCorrupt);
        }
        let (meta, buf) = buf.split_at(bin_len as usize);

        if buf.len() < 5 {
            error!("insufficient data for CRC: expected 5 bytes, got {} bytes", buf.len());
            return Err(Error::FileCorrupt);
        }
        let (mut crc_buf, buf) = buf.split_at(5);

        // crc check
        let crc = rmp::decode::read_u32(&mut crc_buf).map_err(|e| {
            error!("failed to read CRC value: {}", e);
            Error::FileCorrupt
        })?;
        let meta_crc = xxh64::xxh64(meta, XXHASH_SEED) as u32;

        if crc != meta_crc {
            error!(
                event = "filemeta_xl_crc_mismatch",
                component = "filemeta",
                expected_crc = meta_crc,
                actual_crc = crc,
                "xl.meta payload failed its CRC check"
            );
            // See is_indexed_meta: the FileCorrupt variant is what makes heal
            // classify this drive as needing metadata repair.
            return Err(Error::FileCorrupt);
        }

        if !buf.is_empty() {
            InlineData::validate_slice(buf).map_err(|e| {
                error!("data validation failed: {}", e);
                // Validation only examines bytes already read. Classify damage
                // as repairable corruption only for formats we understand.
                if InlineData::version_ok_slice(buf) && Self::decode_xl_headers(meta).is_ok() {
                    Error::FileCorrupt
                } else {
                    e
                }
            })?;
        }

        Ok((meta, buf))
    }

    #[inline]
    pub(super) fn read_next_raw_slices<'a>(meta: &'a [u8], cur: &mut Cursor<&'a [u8]>) -> Result<(&'a [u8], &'a [u8])> {
        let bin_len = rmp::decode::read_bin_len(cur).map_err(|e| {
            error!("failed to read binary length for version header: {}", e);
            Error::other(format!("failed to read binary length for version header: {e}"))
        })? as usize;

        let start = cur.position() as usize;
        let remaining = meta.len().saturating_sub(start);
        if bin_len > remaining {
            error!("corrupt xl.meta: version header length {} exceeds remaining {} bytes", bin_len, remaining);
            return Err(Error::other("corrupt xl.meta: version header length exceeds metadata size"));
        }
        let end = start + bin_len;
        let header_raw = &meta[start..end];
        cur.set_position(end as u64);

        let bin_len = rmp::decode::read_bin_len(cur).map_err(|e| {
            error!("failed to read binary length for version metadata: {}", e);
            Error::other(format!("failed to read binary length for version metadata: {e}"))
        })? as usize;

        let start = cur.position() as usize;
        let remaining = meta.len().saturating_sub(start);
        if bin_len > remaining {
            error!(
                "corrupt xl.meta: version metadata length {} exceeds remaining {} bytes",
                bin_len, remaining
            );
            return Err(Error::other("corrupt xl.meta: version metadata length exceeds metadata size"));
        }
        let end = start + bin_len;
        cur.set_position(end as u64);

        Ok((header_raw, &meta[start..end]))
    }

    #[inline]
    pub(super) fn read_next_shallow_slice<'a>(
        meta: &'a [u8],
        cur: &mut Cursor<&'a [u8]>,
        header_ver: u8,
    ) -> Result<(FileMetaVersionHeader, &'a [u8])> {
        let (header_raw, ver_meta) = Self::read_next_raw_slices(meta, cur)?;
        let mut header = FileMetaVersionHeader::default();
        header.unmarshal_v(header_ver, header_raw).map_err(|e| {
            error!("failed to unmarshal version header: {}", e);
            e
        })?;
        Ok((header, ver_meta))
    }

    #[hotpath::measure(impl_type = "FileMeta")]
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let i = buf.len() as u64;
        let (meta, inline_buf) = Self::parse_validated_xl_envelope(buf)?;

        if !inline_buf.is_empty() {
            self.data.update(inline_buf);
        }

        // Parse meta
        if !meta.is_empty() {
            let (versions_len, header_ver, meta_ver, meta) = Self::decode_validated_xl_headers(meta)?;

            self.meta_ver = meta_ver;
            self.versions = Vec::with_capacity(versions_len);

            let mut cur: Cursor<&[u8]> = Cursor::new(meta);
            for _ in 0..versions_len {
                let (header, ver_meta) = Self::read_next_shallow_slice(meta, &mut cur, header_ver)?;
                self.versions.push(FileMetaShallowVersion {
                    header,
                    meta: ver_meta.to_vec(),
                });
            }
        }

        Ok(i)
    }

    pub(crate) fn matches_buf_strict(&self, buf: &[u8]) -> bool {
        let Ok((meta, _inline_buf)) = Self::parse_validated_xl_envelope(buf) else {
            return false;
        };

        if meta.is_empty() {
            return self.versions.is_empty();
        }

        let Ok((versions_len, header_ver, _meta_ver, meta)) = Self::decode_validated_xl_headers(meta) else {
            return false;
        };

        if versions_len != self.versions.len() {
            return false;
        }

        let mut cur: Cursor<&[u8]> = Cursor::new(meta);
        for s_version in &self.versions {
            let Ok((header_raw, _ver_meta)) = Self::read_next_raw_slices(meta, &mut cur) else {
                return false;
            };
            if header_ver == XL_HEADER_VERSION
                && let Some(matched) = Self::header_matches_canonical_v3_raw_strict(&s_version.header, header_raw)
            {
                if !matched {
                    return false;
                }
                continue;
            }
            let mut o_header = FileMetaVersionHeader::default();
            if o_header.unmarshal_v(header_ver, header_raw).is_err() {
                return false;
            }
            if !Self::headers_match_strict(&s_version.header, &o_header) {
                return false;
            }
        }

        true
    }

    #[inline]
    fn header_matches_canonical_v3_raw_strict(s_header: &FileMetaVersionHeader, buf: &[u8]) -> Option<bool> {
        if buf.len() != 38 {
            return None;
        }
        let fields = parse_canonical_v3_header_fields(buf)?;
        if !ec_fields_match_strict(s_header.ec_m, s_header.ec_n, fields.ec_m, fields.ec_n)
            || s_header.signature != fields.signature
            || s_header.flags != fields.flags
            || s_header.version_type != fields.version_type
            || s_header.version_id != Some(Uuid::from_bytes(fields.uuid_bytes))
        {
            return Some(false);
        }

        let time_ok = if fields.unix_nanos == 0 {
            s_header.mod_time.is_none()
        } else {
            s_header
                .mod_time
                .is_some_and(|t| t.unix_timestamp_nanos() == i128::from(fields.unix_nanos))
        };
        Some(time_ok)
    }

    #[inline]
    pub(crate) fn headers_match_strict(s_header: &FileMetaVersionHeader, o_header: &FileMetaVersionHeader) -> bool {
        let FileMetaVersionHeader {
            version_id,
            mod_time,
            signature,
            ec_n,
            ec_m,
            version_type,
            flags,
        } = s_header;
        *version_id == o_header.version_id
            && *mod_time == o_header.mod_time
            && *signature == o_header.signature
            && *version_type == o_header.version_type
            && *flags == o_header.flags
            && ec_fields_match_strict(*ec_m, *ec_n, o_header.ec_m, o_header.ec_n)
    }

    #[inline]
    pub(crate) fn borrow_cached(&self) -> BorrowedFileMeta<'_> {
        BorrowedFileMeta::from_file_meta(self)
    }

    pub(crate) fn decode_borrowed(buf: &[u8]) -> Result<BorrowedFileMeta<'_>> {
        Self::decode_borrowed_with_hint(buf, None)
    }

    pub(crate) fn decode_borrowed_with_hint<'a>(
        buf: &'a [u8],
        hint: Option<&BorrowedFileMeta<'a>>,
    ) -> Result<BorrowedFileMeta<'a>> {
        let (meta, inline_buf) = Self::parse_validated_xl_envelope(buf)?;
        if meta.is_empty() {
            return Ok(BorrowedFileMeta {
                header_ver: 0,
                meta_ver: 0,
                inline_buf,
                versions: BorrowedVersions::Empty,
            });
        }
        let (versions_len, header_ver, meta_ver, meta) = Self::decode_validated_xl_headers(meta)?;
        Self::decode_borrowed_body_with_hint(versions_len, header_ver, meta_ver, meta, inline_buf, hint)
    }

    fn decode_borrowed_body_with_hint<'a>(
        versions_len: usize,
        header_ver: u8,
        meta_ver: u8,
        meta: &'a [u8],
        inline_buf: &'a [u8],
        hint: Option<&BorrowedFileMeta<'a>>,
    ) -> Result<BorrowedFileMeta<'a>> {
        let hint_slice = hint
            .filter(|h| h.header_ver == header_ver && h.versions.as_slice().len() == versions_len)
            .map(|h| h.versions.as_slice());
        let mut cur: Cursor<&'a [u8]> = Cursor::new(meta);
        let decode_idx = |idx: usize, cur: &mut Cursor<&'a [u8]>| -> Result<BorrowedVersionEntry<'a>> {
            let (header_raw, ver_meta) = Self::read_next_raw_slices(meta, cur)?;
            if let Some(h_slice) = hint_slice {
                let (ref h_hdr, h_raw, _) = h_slice[idx];
                if !h_raw.is_empty() && header_raw == h_raw {
                    return Ok((h_hdr.clone(), header_raw, ver_meta));
                }
            }
            let mut header = FileMetaVersionHeader::default();
            header.unmarshal_v(header_ver, header_raw).map_err(|e| {
                error!("failed to unmarshal version header: {}", e);
                e
            })?;
            Ok((header, header_raw, ver_meta))
        };
        let versions = match versions_len {
            0 => BorrowedVersions::Empty,
            1 => BorrowedVersions::Single([decode_idx(0, &mut cur)?]),
            n => {
                let mut list = Vec::with_capacity(n);
                for idx in 0..n {
                    list.push(decode_idx(idx, &mut cur)?);
                }
                BorrowedVersions::Multi(list)
            }
        };
        Ok(BorrowedFileMeta {
            header_ver,
            meta_ver,
            inline_buf,
            versions,
        })
    }

    pub(crate) fn validate_and_compare_borrowed<'a>(
        buf: &'a [u8],
        selected: &mut BorrowedFileMeta<'a>,
        fallback_hint: Option<&BorrowedFileMeta<'a>>,
        same_name: bool,
        strict: bool,
    ) -> Result<(Option<bool>, Option<BorrowedFileMeta<'a>>)> {
        if same_name {
            let (meta, inline_buf) = Self::parse_validated_xl_envelope(buf)?;
            if meta.is_empty() {
                if selected.versions.as_slice().is_empty() {
                    selected.header_ver = 0;
                    selected.meta_ver = 0;
                    selected.inline_buf = inline_buf;
                    return Ok((Some(true), None));
                }
                return Ok((
                    None,
                    Some(BorrowedFileMeta {
                        header_ver: 0,
                        meta_ver: 0,
                        inline_buf,
                        versions: BorrowedVersions::Empty,
                    }),
                ));
            }
            let (versions_len, header_ver, meta_ver, meta) = Self::decode_validated_xl_headers(meta)?;
            if header_ver == selected.header_ver && versions_len == selected.versions.as_slice().len() {
                let mut cur: Cursor<&'a [u8]> = Cursor::new(meta);
                let mut all_raw_equal = true;
                for (_, sel_raw, _) in selected.versions.as_slice() {
                    let (hdr_raw, _) = Self::read_next_raw_slices(meta, &mut cur)?;
                    if sel_raw.is_empty() || hdr_raw != *sel_raw {
                        all_raw_equal = false;
                    }
                }
                if all_raw_equal {
                    let mut cur: Cursor<&'a [u8]> = Cursor::new(meta);
                    for (_, sel_raw, sel_meta) in selected.versions.as_mut_slice() {
                        let (hdr_raw, ver_meta) = Self::read_next_raw_slices(meta, &mut cur)?;
                        *sel_raw = hdr_raw;
                        *sel_meta = ver_meta;
                    }
                    selected.meta_ver = meta_ver;
                    selected.inline_buf = inline_buf;
                    return Ok((Some(true), None));
                }
            }
            let candidate = Self::decode_borrowed_body_with_hint(
                versions_len,
                header_ver,
                meta_ver,
                meta,
                inline_buf,
                fallback_hint.or(Some(selected)),
            )?;
            let outcome = reconcile_headers(
                candidate.versions.as_slice().iter().map(|(h, _, _)| h),
                selected.versions.as_slice().iter().map(|(h, _, _)| h),
                true,
                strict,
            );
            return match outcome {
                Some(true) => {
                    let prev = std::mem::replace(selected, candidate);
                    Ok((Some(true), Some(prev)))
                }
                Some(false) => Ok((Some(false), Some(candidate))),
                None => Ok((None, Some(candidate))),
            };
        }

        let candidate = Self::decode_borrowed_with_hint(buf, fallback_hint.or(Some(selected)))?;
        Ok((None, Some(candidate)))
    }

    #[inline]
    pub(super) fn decode_validated_xl_headers(meta: &[u8]) -> Result<(usize, u8, u8, &[u8])> {
        let (versions_len, header_ver, meta_ver, meta) = Self::decode_xl_headers(meta).map_err(|e| {
            error!("failed to decode XL headers: {}", e);
            e
        })?;

        // `versions_len` is decoded from a potentially corrupted buffer.
        // Every version contributes at least two msgpack bin headers to
        // `meta`, so a count larger than the remaining bytes is corrupt;
        // reject it before sizing any allocation from it (rustfs/rustfs#2715).
        if versions_len > meta.len() {
            error!(
                "corrupt xl.meta: version count {} exceeds remaining metadata size {}",
                versions_len,
                meta.len()
            );
            return Err(Error::other(format!(
                "corrupt xl.meta: version count {versions_len} exceeds metadata size {}",
                meta.len()
            )));
        }

        Ok((versions_len, header_ver, meta_ver, meta))
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
            let header_buf = buf
                .get(start..end)
                .ok_or_else(|| Error::other("corrupt xl.meta: version header segment out of range"))?;

            cur.set_position(end as u64);

            let bin_len = rmp::decode::read_bin_len(&mut cur)? as usize;
            let start = cur.position() as usize;
            let end = start + bin_len;
            let ver_meta_buf = buf
                .get(start..end)
                .ok_or_else(|| Error::other("corrupt xl.meta: version metadata segment out of range"))?;

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
        if let Some((versions, hdr_v, _meta_v, meta)) = header {
            if versions == 0 {
                return false;
            }

            let mut is_delete_marker = true;

            let _ = Self::decode_versions(meta, versions, |_: usize, hdr: &[u8], _: &[u8]| {
                let mut header = FileMetaVersionHeader::default();
                if header.unmarshal_v(hdr_v, hdr).is_err() {
                    return Err(Error::DoneForNow);
                }

                if header.free_version() {
                    return Ok(());
                }
                is_delete_marker = header.version_type == VersionType::Delete;

                Err(Error::DoneForNow)
            });

            is_delete_marker
        } else {
            false
        }
    }

    #[hotpath::measure(impl_type = "FileMeta")]
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr =
            Vec::with_capacity(24 + self.versions.iter().map(|v| 48 + v.meta.len()).sum::<usize>() + self.data.as_slice().len());

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

        rmp::encode::write_uint(&mut wr, XL_HEADER_VERSION as u64)?;
        rmp::encode::write_uint(&mut wr, XL_META_VERSION as u64)?;

        rmp::encode::write_sint(&mut wr, self.versions.len() as i64)?;

        let mut hbuf = [0u8; 48];
        for ver in self.versions.iter() {
            let mut hcur = Cursor::new(&mut hbuf[..]);
            ver.header.encode_to(&mut hcur)?;
            let hlen = hcur.position() as usize;
            rmp::encode::write_bin(&mut wr, &hbuf[..hlen])?;

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

pub(crate) type BorrowedVersionEntry<'a> = (FileMetaVersionHeader, &'a [u8], &'a [u8]);

pub(crate) enum BorrowedVersions<'a> {
    Empty,
    Single([BorrowedVersionEntry<'a>; 1]),
    Multi(Vec<BorrowedVersionEntry<'a>>),
}

impl<'a> BorrowedVersions<'a> {
    #[inline]
    pub(crate) fn as_slice(&self) -> &[BorrowedVersionEntry<'a>] {
        match self {
            Self::Empty => &[],
            Self::Single(one) => one.as_slice(),
            Self::Multi(many) => many.as_slice(),
        }
    }

    #[inline]
    pub(crate) fn as_mut_slice(&mut self) -> &mut [BorrowedVersionEntry<'a>] {
        match self {
            Self::Empty => &mut [],
            Self::Single(one) => one.as_mut_slice(),
            Self::Multi(many) => many.as_mut_slice(),
        }
    }
}

pub(crate) struct BorrowedFileMeta<'a> {
    pub(crate) header_ver: u8,
    pub(crate) meta_ver: u8,
    pub(crate) inline_buf: &'a [u8],
    pub(crate) versions: BorrowedVersions<'a>,
}

impl<'a> BorrowedFileMeta<'a> {
    pub(crate) fn from_file_meta(fm: &'a FileMeta) -> Self {
        let versions = match fm.versions.as_slice() {
            [] => BorrowedVersions::Empty,
            [v0] => BorrowedVersions::Single([(v0.header.clone(), &[], v0.meta.as_slice())]),
            many => BorrowedVersions::Multi(many.iter().map(|v| (v.header.clone(), &[][..], v.meta.as_slice())).collect()),
        };
        Self {
            header_ver: 0,
            meta_ver: fm.meta_ver,
            inline_buf: fm.data.as_slice(),
            versions,
        }
    }

    pub(crate) fn as_borrowed_stream(&self) -> Vec<(&FileMetaVersionHeader, &'a [u8])> {
        self.versions.as_slice().iter().map(|(h, _, m)| (h, *m)).collect()
    }

    /// Builds a borrowed `(header, ver_meta)` stream directly from a pre-validated `xl.meta` buffer
    /// whose raw version headers match `self`, borrowing `&FileMetaVersionHeader` from `self` without
    /// allocating an intermediate `BorrowedFileMeta` or re-running CRC validation.
    pub(crate) fn stream_from_prevalidated_matching_buf<'b>(
        &'b self,
        buf: &'b [u8],
    ) -> Option<Vec<(&'b FileMetaVersionHeader, &'b [u8])>> {
        if buf.len() < 18 {
            return None;
        }
        let mut size_buf = &buf[8..13];
        let bin_len = rmp::decode::read_bin_len(&mut size_buf).ok()? as usize;
        let meta = buf.get(13..13 + bin_len)?;
        if meta.is_empty() {
            return self.versions.as_slice().is_empty().then(Vec::new);
        }
        let (versions_len, header_ver, _, meta) = FileMeta::decode_xl_headers(meta).ok()?;
        let sel_slice = self.versions.as_slice();
        if versions_len == 0 && sel_slice.is_empty() {
            return Some(Vec::new());
        }
        if header_ver != self.header_ver || versions_len != sel_slice.len() {
            return None;
        }
        let mut cur = Cursor::new(meta);
        let mut stream = Vec::with_capacity(versions_len);
        for (sel_hdr, sel_raw, _) in sel_slice {
            let (hdr_raw, ver_meta) = FileMeta::read_next_raw_slices(meta, &mut cur).ok()?;
            if sel_raw.is_empty() || hdr_raw != *sel_raw {
                return None;
            }
            stream.push((sel_hdr, ver_meta));
        }
        Some(stream)
    }

    pub(crate) fn into_versions(self) -> Vec<FileMetaShallowVersion> {
        self.versions
            .as_slice()
            .iter()
            .map(|(header, _, meta)| FileMetaShallowVersion {
                header: header.clone(),
                meta: meta.to_vec(),
            })
            .collect()
    }

    pub(crate) fn into_file_meta(self) -> FileMeta {
        let mut fm = FileMeta {
            meta_ver: self.meta_ver,
            ..Default::default()
        };
        if !self.inline_buf.is_empty() {
            fm.data.update(self.inline_buf);
        }
        fm.versions = self.into_versions();
        fm
    }

    pub(crate) fn compare_cached(&mut self, fm: &'a FileMeta, same_name: bool, strict: bool) -> (Option<bool>, Option<Self>) {
        let outcome = reconcile_headers(
            fm.versions.iter().map(|v| &v.header),
            self.versions.as_slice().iter().map(|(h, _, _)| h),
            same_name,
            strict,
        );
        if outcome == Some(true) {
            let prev = std::mem::replace(self, Self::from_file_meta(fm));
            (Some(true), Some(prev))
        } else {
            (outcome, None)
        }
    }
}

/// Reconciles candidate version headers against selected headers: returns `None` on
/// disagreement, `Some(true)` when the candidate replaces `selected`, or `Some(false)` when `selected` is kept.
#[inline]
pub(crate) fn reconcile_headers<'a>(
    cand_headers: impl ExactSizeIterator<Item = &'a FileMetaVersionHeader>,
    sel_headers: impl ExactSizeIterator<Item = &'a FileMetaVersionHeader>,
    same_name: bool,
    strict: bool,
) -> Option<bool> {
    if !same_name || cand_headers.len() != sel_headers.len() {
        return None;
    }
    let mut prefer_self = None;
    for (s_header, o_header) in cand_headers.zip(sel_headers) {
        if FileMeta::headers_match_strict(s_header, o_header) {
            continue;
        } else if !strict && s_header.matches_not_strict(o_header) {
            if prefer_self.is_none() {
                prefer_self = Some(s_header.sorts_before(o_header));
            }
        } else {
            return None;
        }
    }
    Some(prefer_self.unwrap_or(true))
}
