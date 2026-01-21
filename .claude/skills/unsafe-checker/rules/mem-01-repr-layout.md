---
id: mem-01
original_id: P.UNS.MEM.01
level: P
impact: HIGH
---

# Choose Appropriate Data Layout for Struct/Tuple/Enum

## Summary

Use `#[repr(...)]` attributes to control data layout when interfacing with C, doing memory mapping, or needing specific guarantees.

## Rationale

Rust's default layout is unspecified and may change between compiler versions. For FFI, persistence, or low-level memory operations, you need predictable layout.

## Repr Attributes

| Attribute | Use Case |
|-----------|----------|
| `#[repr(C)]` | C-compatible layout, stable field order |
| `#[repr(transparent)]` | Single-field struct with same layout as field |
| `#[repr(packed)]` | No padding (alignment = 1), careful with references! |
| `#[repr(align(N))]` | Minimum alignment of N bytes |
| `#[repr(u8)]`, `#[repr(i32)]`, etc. | Enum discriminant type |

## Bad Example

```rust
// DON'T: Assume Rust struct layout matches C
struct BadFFI {
    a: u8,
    b: u32,
    c: u8,
}
// Rust may reorder fields or add different padding than C

// DON'T: Use packed without understanding the risks
#[repr(packed)]
struct Dangerous {
    a: u8,
    b: u32,
}

fn bad_ref(d: &Dangerous) -> &u32 {
    &d.b  // UB: Creates unaligned reference!
}
```

## Good Example

```rust
// DO: Use repr(C) for FFI
#[repr(C)]
struct GoodFFI {
    a: u8,
    b: u32,
    c: u8,
}
// Guaranteed: a at 0, padding 1-3, b at 4, c at 8, padding 9-11

// DO: Use repr(transparent) for newtypes
#[repr(transparent)]
struct Wrapper(u32);
// Guaranteed same layout as u32, can be transmuted

// DO: Use repr(packed) carefully, access via copy
#[repr(C, packed)]
struct PackedData {
    header: u8,
    value: u32,
}

impl PackedData {
    fn value(&self) -> u32 {
        // Copy out the value to avoid unaligned reference
        let ptr = std::ptr::addr_of!(self.value);
        // SAFETY: Reading unaligned is OK with read_unaligned
        unsafe { ptr.read_unaligned() }
    }
}

// DO: Use align for SIMD or cache line alignment
#[repr(C, align(64))]
struct CacheAligned {
    data: [u8; 64],
}

// DO: Specify enum discriminant for FFI
#[repr(u8)]
enum Status {
    Ok = 0,
    Error = 1,
    Unknown = 255,
}
```

## Layout Guarantees

```rust
use std::mem::{size_of, align_of};

#[repr(C)]
struct Example {
    a: u8,   // offset 0, size 1
    // padding: 3 bytes
    b: u32,  // offset 4, size 4
    c: u8,   // offset 8, size 1
    // padding: 3 bytes
}

assert_eq!(size_of::<Example>(), 12);
assert_eq!(align_of::<Example>(), 4);

// repr(Rust) might reorder to: b, a, c -> size 8
```

## Checklist

- [ ] Is this type used in FFI? → Use `#[repr(C)]`
- [ ] Is this a newtype wrapper? → Consider `#[repr(transparent)]`
- [ ] Do I need specific alignment? → Use `#[repr(align(N))]`
- [ ] Am I using packed? → Never create references to packed fields

## Related Rules

- `ffi-13`: Ensure consistent data layout for custom types
- `ffi-14`: Types in FFI should have stable layout
- `ptr-04`: Alignment considerations
