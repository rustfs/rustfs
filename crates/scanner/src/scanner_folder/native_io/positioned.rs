// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Reopen an indexed directory at the last entry committed by the walker.

use std::ffi::OsStr;
use std::fs::{File, FileType, Metadata};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use rustix::fs::{Dir, FileType as NativeType, Mode, OFlags, SeekFrom};
use sha2::{Digest, Sha256};

use super::{DirEntry, DirectoryAnchor, EntrySource, PositionedEntry};

// Darwin's dirent does not expose Linux d_off. Use the kernel buffer's start
// position and verify the consumed prefix of that buffer on a fresh handle.
#[cfg(target_os = "macos")]
const MAX_ANCHOR_PREFIX: u32 = 4096;
#[cfg(target_os = "linux")]
const MAX_ANCHOR_PREFIX: u32 = 1;

fn changed() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "scanner directory position no longer matches its checkpoint")
}

fn resume_error(error: rustix::io::Errno) -> io::Error {
    use rustix::io::Errno;
    match error {
        Errno::SPIPE | Errno::OPNOTSUPP => io::ErrorKind::Unsupported.into(),
        Errno::INVAL | Errno::OVERFLOW | Errno::STALE => changed(),
        other => other.into(),
    }
}

fn hash_record(prefix: &mut Sha256, record: &rustix::fs::DirEntry) {
    prefix.update(record.ino().to_le_bytes());
    // Names cannot contain NUL. Include the terminator to delimit records.
    prefix.update(record.file_name().to_bytes_with_nul());
}

fn identity(meta: &Metadata) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(std::env::consts::OS.as_bytes());
    for value in [
        meta.dev().to_le_bytes(),
        meta.ino().to_le_bytes(),
        meta.ctime().to_le_bytes(),
        meta.ctime_nsec().to_le_bytes(),
        meta.mtime().to_le_bytes(),
        meta.mtime_nsec().to_le_bytes(),
    ] {
        digest.update(value);
    }
    digest.finalize().into()
}

pub(super) struct Reader {
    dir: Dir,
    parent: Arc<Path>,
    directory_type: FileType,
    regular_type: Option<FileType>,
    link_type: Option<FileType>,
    identity: [u8; 32],
    next_cookie: Option<u64>,
    #[cfg(target_os = "macos")]
    page: DarwinPage,
}

#[cfg(target_os = "macos")]
struct DarwinPage {
    position: File,
    start: Option<u64>,
    count: u32,
    prefix: Sha256,
}

impl Reader {
    pub(super) fn open(path: &Path, resume: Option<&DirectoryAnchor>) -> io::Result<Self> {
        let fd = File::from(rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )?);
        let meta = fd.metadata()?;
        let identity = identity(&meta);
        let cookie = resume.map_or(0, |anchor| anchor.cookie);
        if let Some(anchor) = resume {
            if anchor.directory != identity || !(1..=MAX_ANCHOR_PREFIX).contains(&anchor.entries) {
                return Err(changed());
            }
            if rustix::fs::seek(&fd, SeekFrom::Start(cookie)).map_err(resume_error)? != cookie {
                return Err(changed());
            }
        }
        #[cfg(target_os = "macos")]
        let page = DarwinPage {
            position: fd.try_clone()?,
            start: Some(cookie),
            count: 0,
            prefix: Sha256::new(),
        };
        let mut reader = Self {
            dir: Dir::new(fd)?,
            parent: Arc::from(path),
            directory_type: meta.file_type(),
            regular_type: None,
            link_type: None,
            identity,
            next_cookie: Some(cookie),
            #[cfg(target_os = "macos")]
            page,
        };
        if let Some(expected) = resume {
            // A fresh libc stream can choose a different buffer size. Validate
            // exactly the saved prefix, regardless of its new buffer boundaries.
            let mut prefix = Sha256::new();
            for _ in 0..expected.entries {
                let record = reader.dir.read().transpose().map_err(resume_error)?.ok_or_else(changed)?;
                hash_record(&mut prefix, &record);
                reader.anchor(&record);
            }
            if prefix.finalize()[..] != expected.prefix {
                return Err(changed());
            }
        }
        Ok(reader)
    }

    fn anchor(&mut self, record: &rustix::fs::DirEntry) -> Option<DirectoryAnchor> {
        #[cfg(target_os = "linux")]
        let mut single = Sha256::new();
        #[cfg(target_os = "linux")]
        let (cookie, entries, prefix) = {
            let cookie = self.next_cookie;
            self.next_cookie = u64::try_from(record.offset()).ok();
            (cookie?, 1, &mut single)
        };
        #[cfg(target_os = "macos")]
        let (cookie, entries, prefix) = {
            let after = rustix::fs::tell(&self.page.position).ok();
            if after != self.next_cookie {
                self.page.start = self.next_cookie;
                self.next_cookie = after;
                self.page.count = 0;
                self.page.prefix = Sha256::new();
            }
            self.page.count = self.page.count.saturating_add(1);
            if self.page.count > MAX_ANCHOR_PREFIX {
                return None;
            }
            (self.page.start?, self.page.count, &mut self.page.prefix)
        };
        hash_record(prefix, record);
        Some(DirectoryAnchor {
            directory: self.identity,
            cookie,
            entries,
            prefix: prefix.clone().finalize().into(),
        })
    }

    fn file_type(&mut self, kind: NativeType, name: &OsStr) -> (Option<FileType>, u64) {
        // std::fs::FileType has no public constructor. Reuse the observed type
        // only when the kernel dirent identified the same class. DT_UNKNOWN
        // and special files retain the per-entry symlink_metadata fallback.
        let cached = match kind {
            NativeType::Directory => return (Some(self.directory_type), 0),
            NativeType::RegularFile => &mut self.regular_type,
            NativeType::Symlink => &mut self.link_type,
            _ => return (None, 1),
        };
        if cached.is_none() {
            let Ok(meta) = std::fs::symlink_metadata(self.parent.join(name)) else {
                // The caller will retry classification using the entry path.
                return (None, 2);
            };
            let ty = meta.file_type();
            if (ty.is_file() && kind == NativeType::RegularFile) || (ty.is_symlink() && kind == NativeType::Symlink) {
                *cached = Some(ty);
            }
            return (Some(ty), 1);
        }
        (*cached, 0)
    }

    pub(super) fn next(&mut self, first_in_batch: bool) -> Option<io::Result<DirEntry>> {
        loop {
            let raw = match self.dir.read()? {
                Ok(record) => record,
                Err(error) => return Some(Err(error.into())),
            };
            let anchor = self.anchor(&raw);
            let bytes = raw.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            let name = OsStr::from_bytes(bytes);
            let (file_type, type_ops) = self.file_type(raw.file_type(), name);
            return Some(Ok(DirEntry {
                source: EntrySource::Positioned(Arc::new(PositionedEntry {
                    parent: self.parent.clone(),
                    name: name.into(),
                    anchor,
                })),
                file_type,
                disk_ops: u64::from(first_in_batch) + type_ops,
            }));
        }
    }
}
