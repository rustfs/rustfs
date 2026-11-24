use crate::NixlError;
use memmap2::MmapRaw;
use nixl_sys::{MemType, MemoryRegion, NixlDescriptor};
use rustix::fs::{MemfdFlags, OFlags, SealFlags, fcntl_add_seals, fcntl_getfl, memfd_create};
use std::fs::File;
use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

/// Peer transfers can outlive a client's control request. Local I/O reaches
/// the same pages through the kernel without creating Rust references to them.
pub(crate) struct PassiveBuffer {
    map: MmapRaw,
    file: File,
}

impl std::fmt::Debug for PassiveBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PassiveBuffer").field("len", &self.len()).finish()
    }
}

enum FileOperation {
    Read,
    Write,
}

fn io_error(op: &'static str) -> impl FnOnce(io::Error) -> NixlError {
    move |source| NixlError::Os { op, source }
}

impl PassiveBuffer {
    pub(crate) fn new(len: usize) -> Result<Self, NixlError> {
        if len == 0 || len > isize::MAX as usize {
            return Err(NixlError::Token("invalid passive buffer length".to_owned()));
        }
        let create = || -> io::Result<Self> {
            let file = File::from(memfd_create("rustfs-nixl", MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)?);
            file.set_len(len as u64)?;
            fcntl_add_seals(&file, SealFlags::GROW | SealFlags::SHRINK | SealFlags::SEAL)?;
            let map = MmapRaw::map_raw(&file)?;
            Ok(Self { map, file })
        };
        create().map_err(io_error("create passive buffer"))
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn addr(&self) -> usize {
        self.map.as_ptr() as usize
    }

    fn check(&self, offset: usize, len: usize) -> Result<(), NixlError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len()) {
            return Err(NixlError::Token("range outside passive buffer".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn read_at(&self, offset: usize, bytes: &mut [u8]) -> Result<(), NixlError> {
        self.check(offset, bytes.len())?;
        self.file
            .read_exact_at(bytes, offset as u64)
            .map_err(io_error("read passive buffer"))
    }

    pub(crate) fn write_at(&self, offset: usize, bytes: &[u8]) -> Result<(), NixlError> {
        self.check(offset, bytes.len())?;
        self.file
            .write_all_at(bytes, offset as u64)
            .map_err(io_error("write passive buffer"))
    }

    pub(crate) fn read_file_at(&self, file: &File, file_offset: u64, window_offset: usize, len: usize) -> Result<(), NixlError> {
        self.file_io_at(file, file_offset, window_offset, len, FileOperation::Read)
    }

    pub(crate) fn write_file_at(&self, file: &File, file_offset: u64, window_offset: usize, len: usize) -> Result<(), NixlError> {
        self.file_io_at(file, file_offset, window_offset, len, FileOperation::Write)
    }

    fn file_io_at(
        &self,
        file: &File,
        file_offset: u64,
        window_offset: usize,
        len: usize,
        operation: FileOperation,
    ) -> Result<(), NixlError> {
        self.check(window_offset, len)?;
        // Linux pread64/pwrite64 use a signed 64-bit file offset.
        let invalid_offset = || NixlError::Token("passive file offset exceeds the platform limit".to_owned());
        let mut position = i64::try_from(file_offset).map_err(|_| invalid_offset())?;
        position.checked_add(len as i64).ok_or_else(invalid_offset)?;
        if len != 0 && matches!(operation, FileOperation::Write) {
            // On Linux pwrite on an append-mode fd silently ignores its offset.
            let flags = fcntl_getfl(file).map_err(|e| io_error("read file status flags")(e.into()))?;
            if flags.contains(OFlags::APPEND) {
                return Err(io_error("write passive buffer to append-mode file")(ErrorKind::InvalidInput.into()));
            }
        }
        let mut done = 0;
        while done < len {
            // SAFETY: the checked range is in the live, fixed-size mapping.
            // The kernel accesses its raw pages; no reference to them is made.
            // Both the borrowed fd and the mapping stay alive for the syscall.
            let result = unsafe {
                let ptr = self.map.as_mut_ptr().add(window_offset + done).cast();
                match operation {
                    FileOperation::Read => libc::pread64(file.as_raw_fd(), ptr, len - done, position),
                    FileOperation::Write => libc::pwrite64(file.as_raw_fd(), ptr, len - done, position),
                }
            };
            if result < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(io_error("passive file I/O")(source));
            }
            if result == 0 {
                let kind = match operation {
                    FileOperation::Read => ErrorKind::UnexpectedEof,
                    FileOperation::Write => ErrorKind::WriteZero,
                };
                return Err(io_error("passive file I/O")(kind.into()));
            }
            // A successful syscall transfers at most the requested byte count.
            done += result as usize;
            position += result as i64;
        }
        Ok(())
    }
}

impl MemoryRegion for PassiveBuffer {
    unsafe fn as_ptr(&self) -> *const u8 {
        self.map.as_ptr()
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl NixlDescriptor for PassiveBuffer {
    fn mem_type(&self) -> MemType {
        MemType::Dram
    }

    fn device_id(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passive_io_preserves_file_offsets_and_checks_ranges() {
        let buffer = PassiveBuffer::new(16).expect("create passive mapping");
        let file = File::from(memfd_create("passive-io-test", MemfdFlags::CLOEXEC).expect("create test file"));
        file.write_all_at(b"0123456789", 0).expect("seed file");
        buffer.read_file_at(&file, 3, 2, 4).expect("file to window");
        let mut bytes = [0; 7];
        buffer.read_at(0, &mut bytes).expect("read window");
        assert_eq!(bytes, [0, 0, b'3', b'4', b'5', b'6', 0]);
        buffer.write_at(1, b"DATA").expect("write window");
        buffer.write_file_at(&file, 5, 1, 4).expect("window to file");
        let mut file_bytes = [0; 10];
        file.read_exact_at(&mut file_bytes, 0).expect("read output");
        assert_eq!(&file_bytes, b"01234DATA9");
        assert_eq!(file.metadata().expect("file length").len(), 10);
        assert!(buffer.file.set_len(15).is_err());
        assert!(buffer.file.set_len(17).is_err());
        buffer.read_at(16, &mut []).expect("empty end range");
        assert!(buffer.read_at(16, &mut [0]).is_err());
        assert!(buffer.write_at(usize::MAX, b"x").is_err());
        assert!(buffer.read_file_at(&file, u64::MAX, 0, 2).is_err());
        assert!(buffer.write_file_at(&file, 0, 15, 2).is_err());
        assert!(matches!(
            buffer.read_file_at(&file, 9, 0, 2),
            Err(NixlError::Os { source, .. }) if source.kind() == ErrorKind::UnexpectedEof
        ));
        let flags = fcntl_getfl(&file).expect("read output flags");
        rustix::fs::fcntl_setfl(&file, flags | OFlags::APPEND).expect("set append mode");
        assert!(matches!(
            buffer.write_file_at(&file, 5, 0, 1),
            Err(NixlError::Os { source, .. }) if source.kind() == ErrorKind::InvalidInput
        ));
        assert_eq!(file.metadata().expect("unchanged length").len(), 10);
    }
}
