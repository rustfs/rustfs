//! Page-aligned host memory handed to NIXL for registration. The unsafe
//! surface of the crate lives here: disjoint range views and the
//! memory-region trait the binding needs.

#![allow(unsafe_code)]

use crate::NixlError;
use memmap2::{MmapMut, MmapRaw};
use nixl_sys::{MemType, MemoryRegion, NixlDescriptor};

/// Page-aligned, zero-filled host memory registered with the agent. An
/// anonymous mapping, so the pool stays outside the global allocator; held as
/// `MmapRaw` because chunks write through `&self`.
pub(crate) struct HostBuffer {
    map: MmapRaw,
}

impl std::fmt::Debug for HostBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostBuffer").field("len", &self.len()).finish()
    }
}

impl HostBuffer {
    pub(crate) fn new(len: usize) -> Result<Self, NixlError> {
        let map = MmapMut::map_anon(len.max(1)).map_err(|source| NixlError::Os {
            op: "staging allocation",
            source,
        })?;
        Ok(Self { map: map.into() })
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn addr(&self) -> usize {
        self.map.as_ptr() as usize
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut u8 {
        self.map.as_mut_ptr()
    }

    pub(crate) fn check(&self, offset: usize, len: usize) -> Result<(), NixlError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len()) {
            return Err(NixlError::Token("staging range outside the buffer".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn slice(&self, offset: usize, len: usize) -> Result<&[u8], NixlError> {
        self.check(offset, len)?;
        // SAFETY: the range is inside the mapping; callers own disjoint ranges.
        Ok(unsafe { std::slice::from_raw_parts(self.map.as_ptr().add(offset), len) })
    }

    #[allow(clippy::mut_from_ref, reason = "chunk ranges are disjoint and owned by one chunk at a time")]
    pub(crate) fn slice_mut(&self, offset: usize, len: usize) -> Result<&mut [u8], NixlError> {
        self.check(offset, len)?;
        // SAFETY: as above; the caller holds the only handle to this range.
        Ok(unsafe { std::slice::from_raw_parts_mut(self.map.as_mut_ptr().add(offset), len) })
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) -> Result<(), NixlError> {
        self.slice_mut(offset, data.len())?.copy_from_slice(data);
        Ok(())
    }
}

impl MemoryRegion for HostBuffer {
    unsafe fn as_ptr(&self) -> *const u8 {
        self.map.as_ptr()
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl NixlDescriptor for HostBuffer {
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
    fn buffer_is_page_aligned_zeroed_and_bounds_checked() {
        let buffer = HostBuffer::new(3 * 4096 + 5).expect("map");
        assert_eq!(buffer.len(), 3 * 4096 + 5);
        assert_eq!(buffer.addr() % 4096, 0);
        assert!(buffer.slice(0, buffer.len()).expect("whole").iter().all(|&b| b == 0));

        buffer.write(4096, b"chunk").expect("write");
        assert_eq!(buffer.slice(4096, 5).expect("read"), b"chunk");
        assert!(buffer.slice(buffer.len() - 1, 2).is_err());
        assert!(buffer.write(usize::MAX, b"x").is_err());

        assert_eq!(HostBuffer::new(0).expect("empty").len(), 1);
    }
}
