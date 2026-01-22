---
id: ptr-04
original_id: G.UNS.PTR.01
level: G
impact: HIGH
clippy: cast_ptr_alignment
---

# Do Not Dereference Pointers Cast to Misaligned Types

## Summary

When casting a pointer to a different type, ensure the resulting pointer is properly aligned for the target type.

## Rationale

Misaligned pointer dereferences are undefined behavior on most architectures. Even on architectures that support unaligned access, it may cause performance penalties or subtle bugs.

## Bad Example

```rust
// DON'T: Cast without checking alignment
fn bad_cast(bytes: &[u8]) -> u32 {
    // BAD: bytes might not be aligned for u32
    let ptr = bytes.as_ptr() as *const u32;
    unsafe { *ptr }  // UB if misaligned!
}

// DON'T: Assume struct layout
#[repr(C)]
struct Header {
    flags: u8,
    value: u32,  // Aligned at offset 4 in the struct
}

fn bad_field_access(bytes: &[u8]) -> u32 {
    let header = bytes.as_ptr() as *const Header;
    // Even if bytes is 4-byte aligned, this might fail
    // if Header has different alignment than expected
    unsafe { (*header).value }
}
```

## Good Example

```rust
// DO: Use read_unaligned for potentially misaligned data
fn good_cast(bytes: &[u8]) -> u32 {
    assert!(bytes.len() >= 4);
    let ptr = bytes.as_ptr() as *const u32;
    // SAFETY: We're reading 4 bytes, alignment doesn't matter for read_unaligned
    unsafe { ptr.read_unaligned() }
}

// DO: Check alignment before cast
fn good_aligned_cast(bytes: &[u8]) -> Option<&u32> {
    if bytes.len() >= 4 && bytes.as_ptr() as usize % std::mem::align_of::<u32>() == 0 {
        // SAFETY: Checked length and alignment
        Some(unsafe { &*(bytes.as_ptr() as *const u32) })
    } else {
        None
    }
}

// DO: Use from_ne_bytes for portable byte conversion
fn good_from_bytes(bytes: &[u8]) -> u32 {
    u32::from_ne_bytes(bytes[..4].try_into().unwrap())
}

// DO: Use bytemuck for safe transmutation
// use bytemuck::{Pod, Zeroable};
// let value: u32 = bytemuck::pod_read_unaligned(bytes);

// DO: Use align_to for splitting at alignment boundaries
fn process_aligned(bytes: &[u8]) {
    let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u32>() };
    // prefix and suffix are unaligned portions
    // aligned is a &[u32] that's properly aligned
}
```

## Alignment Check Helpers

```rust
fn is_aligned<T>(ptr: *const u8) -> bool {
    ptr as usize % std::mem::align_of::<T>() == 0
}

/// Align a pointer up to the next aligned address
fn align_up<T>(ptr: *const u8) -> *const u8 {
    let align = std::mem::align_of::<T>();
    let addr = ptr as usize;
    let aligned = (addr + align - 1) & !(align - 1);
    aligned as *const u8
}
```

## Architecture Notes

| Arch | Misaligned Access |
|------|-------------------|
| x86/x64 | Works but slower |
| ARM | UB, may trap or give wrong results |
| RISC-V | UB, may trap |
| WASM | UB |

## Checklist

- [ ] Is my pointer cast changing alignment requirements?
- [ ] Is the source pointer guaranteed to be aligned?
- [ ] Should I use read_unaligned instead?
- [ ] Can I use safe conversion methods (from_ne_bytes)?

## Related Rules

- `mem-01`: Choose appropriate data layout
- `ffi-13`: Ensure consistent data layout
