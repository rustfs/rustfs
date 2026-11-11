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

//! Segment files: preallocated, written append-only by one writer, sealed
//! with a footer and a trailer, read concurrently by position.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::aligned::{AlignedBuf, BLOCK_SIZE, round_up};
use crate::error::{KvError, KvResult};
use crate::format::{
    EntryMeta, FOOTER_RECORD_FIXED, Trailer, check_key, crc32c, decode_entry_header, decode_footer, decode_trailer,
    encode_entry_header, encode_footer, encode_trailer, entry_footprint, footer_blocks, footer_len,
};

pub type SegmentId = u64;

const BLOCK: u64 = BLOCK_SIZE as u64;

/// File name of a segment: its id in 16 hex digits.
pub fn segment_file_name(id: SegmentId) -> String {
    format!("{id:016x}.seg")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterOptions {
    /// Size of every segment file; a multiple of the block size.
    pub segment_bytes: u64,
    /// Open segment files with `O_DIRECT` (Linux), bypassing the page cache.
    pub direct_io: bool,
    /// `fdatasync` a segment when it is sealed. Entries are never synced
    /// one by one; a crash loses the unsynced tail of open segments.
    pub sync_on_seal: bool,
}

fn open_options(direct_io: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(target_os = "linux")]
    if direct_io {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = direct_io;
    options
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            let read = std::os::windows::fs::FileExt::seek_read(file, &mut buf[done..], offset + done as u64)?;
            if read == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            done += read;
        }
        Ok(())
    }
}

fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            done += std::os::windows::fs::FileExt::seek_write(file, &buf[done..], offset + done as u64)?;
        }
        Ok(())
    }
}

fn preallocate(file: &File, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        match rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len) {
            Ok(()) => return Ok(()),
            // Filesystems without fallocate fall back to a sparse file.
            Err(err) if err == rustix::io::Errno::OPNOTSUPP => {}
            Err(err) => return Err(err.into()),
        }
    }
    file.set_len(len)
}

/// A segment opened for reading. Readers hold it (through an `Arc`) for the
/// duration of a read, so the file can be unlinked while reads finish.
#[derive(Debug)]
pub struct SegmentFile {
    id: SegmentId,
    path: PathBuf,
    file: File,
}

impl SegmentFile {
    pub fn open(path: &Path, id: SegmentId, direct_io: bool) -> KvResult<Self> {
        let file = open_options(direct_io).read(true).open(path)?;
        Ok(Self {
            id,
            path: path.to_owned(),
            file,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the value of the entry at `offset` and verifies it against
    /// `value_crc`. Returns a block-aligned buffer whose first `value_len`
    /// bytes are the value.
    pub fn read_value(&self, offset: u64, value_len: u64, value_crc: u32) -> KvResult<AlignedBuf> {
        let mut buf = AlignedBuf::zeroed(usize::try_from(value_len).map_err(|_| KvError::Corrupt("value length".into()))?);
        if value_len > 0 {
            read_exact_at(&self.file, &mut buf, offset + BLOCK)?;
        }
        if crc32c(&buf[..value_len as usize]) != value_crc {
            return Err(KvError::ChecksumMismatch);
        }
        Ok(buf)
    }
}

/// The single writer of an open segment.
#[derive(Debug)]
pub struct SegmentWriter {
    id: SegmentId,
    path: PathBuf,
    file: File,
    options: WriterOptions,
    offset: u64,
    entries: Vec<(u64, EntryMeta)>,
    footer_len: usize,
    sealed: bool,
}

impl SegmentWriter {
    /// Creates and preallocates a new segment file in `dir`.
    pub fn create(dir: &Path, id: SegmentId, options: WriterOptions) -> KvResult<Self> {
        if !options.segment_bytes.is_multiple_of(BLOCK) || options.segment_bytes < 4 * BLOCK {
            return Err(KvError::Corrupt(format!(
                "segment size {} is not a multiple of {BLOCK} of at least four blocks",
                options.segment_bytes
            )));
        }
        let path = dir.join(segment_file_name(id));
        let file = open_options(options.direct_io)
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        preallocate(&file, options.segment_bytes)?;
        Ok(Self {
            id,
            path,
            file,
            options,
            offset: 0,
            entries: Vec::new(),
            footer_len: footer_len(std::iter::empty()),
            sealed: false,
        })
    }

    /// Reopens an unsealed segment after recovery, positioned after the
    /// entries the scan found, so it can be sealed (or appended to).
    pub fn resume(scanned: &ScannedSegment, path: &Path, options: WriterOptions) -> KvResult<Self> {
        if scanned.sealed {
            return Err(KvError::Sealed);
        }
        let file = open_options(options.direct_io).read(true).write(true).open(path)?;
        Ok(Self {
            id: scanned.id,
            path: path.to_owned(),
            file,
            options,
            offset: scanned.end_offset,
            footer_len: footer_len(scanned.entries.iter().map(|(_, meta)| meta.key.as_str())),
            entries: scanned.entries.clone(),
            sealed: false,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes appended so far.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn entries(&self) -> &[(u64, EntryMeta)] {
        &self.entries
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Whether an entry with this key and value length still fits, leaving
    /// room for the footer and the trailer.
    pub fn fits(&self, key: &str, value_len: u64) -> bool {
        let footer = round_up(self.footer_len + FOOTER_RECORD_FIXED + key.len()) as u64;
        self.offset + entry_footprint(value_len) + footer + BLOCK <= self.options.segment_bytes
    }

    /// Whether an entry of this size can ever fit in an empty segment.
    pub fn fits_empty(key: &str, value_len: u64, segment_bytes: u64) -> bool {
        let footer = round_up(footer_len(std::iter::once(key))) as u64;
        entry_footprint(value_len) + footer + BLOCK <= segment_bytes
    }

    /// Appends an entry; returns its offset, or `None` when the segment is
    /// full and must be sealed.
    pub fn append(&mut self, key: &str, value: &[u8], write_time_ms: u64) -> KvResult<Option<u64>> {
        self.append_entry(key, value, write_time_ms, false)
    }

    /// Appends a tombstone, a persisted delete of `key`; returns its offset,
    /// or `None` when the segment is full.
    pub fn append_tombstone(&mut self, key: &str, write_time_ms: u64) -> KvResult<Option<u64>> {
        self.append_entry(key, &[], write_time_ms, true)
    }

    fn append_entry(&mut self, key: &str, value: &[u8], write_time_ms: u64, tombstone: bool) -> KvResult<Option<u64>> {
        if self.sealed {
            return Err(KvError::Sealed);
        }
        check_key(key)?;
        let value_len = value.len() as u64;
        if !Self::fits_empty(key, value_len, self.options.segment_bytes) {
            return Err(KvError::ValueTooLarge {
                len: value_len,
                segment_bytes: self.options.segment_bytes,
            });
        }
        if !self.fits(key, value_len) {
            return Ok(None);
        }
        let meta = EntryMeta {
            key: key.to_owned(),
            value_len,
            value_crc: crc32c(value),
            write_time_ms,
            tombstone,
        };
        let footprint = entry_footprint(value_len) as usize;
        let mut buf = AlignedBuf::zeroed(footprint);
        encode_entry_header(&meta, &mut buf[..BLOCK_SIZE]);
        buf[BLOCK_SIZE..BLOCK_SIZE + value.len()].copy_from_slice(value);
        write_all_at(&self.file, &buf, self.offset)?;

        let offset = self.offset;
        self.offset += footprint as u64;
        self.footer_len += FOOTER_RECORD_FIXED + key.len();
        self.entries.push((offset, meta));
        Ok(Some(offset))
    }

    /// Writes the footer and the trailer; the segment is then immutable.
    pub fn seal(&mut self, sealed_time_ms: u64) -> KvResult<()> {
        if self.sealed {
            return Ok(());
        }
        let footer = encode_footer(&self.entries);
        let mut buf = AlignedBuf::zeroed(footer_blocks(footer.len()));
        buf[..footer.len()].copy_from_slice(&footer);
        write_all_at(&self.file, &buf, self.offset)?;

        let trailer = Trailer {
            footer_offset: self.offset,
            footer_len: footer.len() as u64,
            entry_count: self.entries.len() as u32,
            sealed_time_ms,
        };
        let mut block = AlignedBuf::zeroed(BLOCK_SIZE);
        encode_trailer(&trailer, &mut block);
        write_all_at(&self.file, &block, self.options.segment_bytes - BLOCK)?;
        if self.options.sync_on_seal {
            self.file.sync_data()?;
        }
        self.sealed = true;
        Ok(())
    }
}

/// What recovery found in a segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedSegment {
    pub id: SegmentId,
    pub sealed: bool,
    pub sealed_time_ms: Option<u64>,
    /// Entries in write order with their offsets.
    pub entries: Vec<(u64, EntryMeta)>,
    /// Where the next entry (or the footer) goes.
    pub end_offset: u64,
}

/// Reads a segment's entries: from the footer when the segment is sealed,
/// otherwise by scanning entry headers from the start and stopping at the
/// first torn or corrupt entry (whose value fails its checksum).
pub fn scan_segment(path: &Path, id: SegmentId, direct_io: bool) -> KvResult<ScannedSegment> {
    let file = open_options(direct_io).read(true).open(path)?;
    let len = file.metadata()?.len();
    if !len.is_multiple_of(BLOCK) || len < 4 * BLOCK {
        return Err(KvError::Corrupt(format!("segment length {len}")));
    }
    let mut block = AlignedBuf::zeroed(BLOCK_SIZE);
    read_exact_at(&file, &mut block, len - BLOCK)?;
    if let Some(trailer) = decode_trailer(&block)
        && trailer.footer_offset + trailer.footer_len <= len - BLOCK
    {
        let mut footer = AlignedBuf::zeroed(footer_blocks(trailer.footer_len as usize));
        read_exact_at(&file, &mut footer, trailer.footer_offset)?;
        let entries = decode_footer(&footer[..trailer.footer_len as usize])?;
        if entries.len() == trailer.entry_count as usize {
            return Ok(ScannedSegment {
                id,
                sealed: true,
                sealed_time_ms: Some(trailer.sealed_time_ms),
                entries,
                end_offset: trailer.footer_offset,
            });
        }
    }

    // Open (or damaged) segment: walk the entry headers.
    let mut entries = Vec::new();
    let mut offset = 0;
    let limit = len - BLOCK;
    while offset + BLOCK <= limit {
        read_exact_at(&file, &mut block, offset)?;
        let Ok(meta) = decode_entry_header(&block) else {
            break;
        };
        let footprint = entry_footprint(meta.value_len);
        if offset + footprint > limit {
            break;
        }
        let mut value = AlignedBuf::zeroed(meta.value_len as usize);
        if meta.value_len > 0 {
            read_exact_at(&file, &mut value, offset + BLOCK)?;
        }
        if crc32c(&value[..meta.value_len as usize]) != meta.value_crc {
            break;
        }
        entries.push((offset, meta));
        offset += footprint;
    }
    Ok(ScannedSegment {
        id,
        sealed: false,
        sealed_time_ms: None,
        entries,
        end_offset: offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(segment_bytes: u64) -> WriterOptions {
        WriterOptions {
            segment_bytes,
            direct_io: false,
            sync_on_seal: false,
        }
    }

    fn value(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
    }

    #[test]
    fn appended_entries_read_back_and_survive_sealing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = SegmentWriter::create(dir.path(), 1, options(1 << 20)).expect("create");
        let sizes = [0usize, 1, 4096, 5000, 100_000];
        let mut written = Vec::new();
        for (i, size) in sizes.iter().enumerate() {
            let key = format!("page-{i}");
            let data = value(i as u8, *size);
            let offset = writer.append(&key, &data, 1000 + i as u64).expect("append").expect("fits");
            written.push((key, data, offset));
        }
        writer.seal(2000).expect("seal");

        let file = SegmentFile::open(writer.path(), 1, false).expect("open");
        for (key, data, offset) in &written {
            let meta = &writer.entries().iter().find(|(o, _)| o == offset).expect("entry").1;
            assert_eq!(&meta.key, key);
            let read = file.read_value(*offset, meta.value_len, meta.value_crc).expect("read");
            assert_eq!(&read[..data.len()], &data[..]);
        }

        let scanned = scan_segment(writer.path(), 1, false).expect("scan");
        assert!(scanned.sealed);
        assert_eq!(scanned.sealed_time_ms, Some(2000));
        assert_eq!(scanned.entries, writer.entries());
    }

    #[test]
    fn a_full_segment_refuses_entries_until_rotated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = SegmentWriter::create(dir.path(), 2, options(64 * 1024)).expect("create");
        let data = value(1, 9000);
        let mut appended = 0;
        while writer.append(&format!("k{appended}"), &data, 1).expect("append").is_some() {
            appended += 1;
        }
        // 64 KiB = 16 blocks: entries of 4 blocks, footer and trailer reserved.
        assert_eq!(appended, 3);
        writer.seal(2).expect("seal");
        assert!(matches!(writer.append("late", b"x", 3), Err(KvError::Sealed)));

        let too_big = value(2, 64 * 1024);
        let mut fresh = SegmentWriter::create(dir.path(), 3, options(64 * 1024)).expect("create");
        assert!(matches!(fresh.append("big", &too_big, 1), Err(KvError::ValueTooLarge { .. })));
        assert!(matches!(fresh.append("", b"x", 1), Err(KvError::KeyTooLong { .. })));
    }

    #[test]
    fn an_open_segment_is_recovered_by_scanning_and_stops_at_a_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = SegmentWriter::create(dir.path(), 4, options(1 << 20)).expect("create");
        let mut offsets = Vec::new();
        for i in 0..4u8 {
            offsets.push(
                writer
                    .append(&format!("k{i}"), &value(i, 10_000), u64::from(i))
                    .expect("append")
                    .expect("fits"),
            );
        }
        // Simulate a crash with the last value only partly written.
        let file = OpenOptions::new().write(true).open(writer.path()).expect("open");
        write_all_at(&file, &[0xAA; 100], offsets[3] + BLOCK + 50).expect("tear");
        drop(writer);

        let scanned = scan_segment(&dir.path().join(segment_file_name(4)), 4, false).expect("scan");
        assert!(!scanned.sealed);
        assert_eq!(scanned.entries.len(), 3, "the torn entry and anything after it are dropped");
        assert_eq!(scanned.end_offset, offsets[3]);

        // Recovery seals what survived; later scans use the footer.
        let path = dir.path().join(segment_file_name(4));
        let mut resumed = SegmentWriter::resume(&scanned, &path, options(1 << 20)).expect("resume");
        resumed.seal(99).expect("seal");
        let rescanned = scan_segment(&path, 4, false).expect("rescan");
        assert!(rescanned.sealed);
        assert_eq!(rescanned.entries, scanned.entries);
    }

    #[test]
    fn a_damaged_value_reads_as_a_checksum_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = SegmentWriter::create(dir.path(), 5, options(1 << 20)).expect("create");
        let offset = writer.append("k", &value(9, 20_000), 1).expect("append").expect("fits");
        let meta = writer.entries()[0].1.clone();
        let file = OpenOptions::new().write(true).open(writer.path()).expect("open");
        write_all_at(&file, &[0x55], offset + BLOCK + 12_345).expect("flip");

        let segment = SegmentFile::open(writer.path(), 5, false).expect("open");
        assert!(matches!(
            segment.read_value(offset, meta.value_len, meta.value_crc),
            Err(KvError::ChecksumMismatch)
        ));
    }

    #[test]
    fn an_empty_or_garbage_file_scans_as_empty_or_corrupt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = SegmentWriter::create(dir.path(), 6, options(64 * 1024)).expect("create");
        let scanned = scan_segment(writer.path(), 6, false).expect("scan");
        assert!(!scanned.sealed);
        assert!(scanned.entries.is_empty());

        let short = dir.path().join("short.seg");
        std::fs::write(&short, b"junk").expect("write");
        assert!(matches!(scan_segment(&short, 7, false), Err(KvError::Corrupt(_))));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_io_round_trips_where_the_filesystem_supports_it() {
        let dir = tempfile::tempdir_in(std::env::var_os("KVCACHE_DIRECT_IO_DIR").unwrap_or_else(|| ".".into())).expect("tempdir");
        let direct = WriterOptions {
            segment_bytes: 1 << 20,
            direct_io: true,
            sync_on_seal: true,
        };
        let mut writer = match SegmentWriter::create(dir.path(), 8, direct) {
            Ok(writer) => writer,
            // tmpfs and some overlay filesystems reject O_DIRECT.
            Err(KvError::Io(err)) if err.raw_os_error() == Some(libc::EINVAL) => return,
            Err(err) => panic!("create: {err}"),
        };
        let data = value(3, 300_000);
        let offset = writer.append("direct", &data, 1).expect("append").expect("fits");
        writer.seal(2).expect("seal");
        let meta = writer.entries()[0].1.clone();
        let file = SegmentFile::open(writer.path(), 8, true).expect("open");
        let read = file.read_value(offset, meta.value_len, meta.value_crc).expect("read");
        assert_eq!(&read[..data.len()], &data[..]);
        assert!(scan_segment(writer.path(), 8, true).expect("scan").sealed);
    }
}
