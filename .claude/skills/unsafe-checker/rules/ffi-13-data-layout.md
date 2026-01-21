---
id: ffi-13
original_id: P.UNS.FFI.13
level: P
impact: HIGH
---

# Ensure Consistent Data Layout for Custom Types

## Summary

Types shared between Rust and C must have `#[repr(C)]` to ensure the memory layout matches what C expects.

## Rationale

- Rust's default layout is unspecified and may change
- C has specific, standardized layout rules
- Mismatched layouts cause memory corruption

## Bad Example

```rust
// DON'T: Rust layout for FFI types
struct BadStruct {
    a: u8,
    b: u32,
    c: u8,
}
// Rust may reorder to: b, a, c (for better packing)
// C expects: a, padding, b, c, padding

extern "C" {
    fn use_struct(s: *const BadStruct);  // Layout mismatch!
}

// DON'T: Assume Rust enum layout matches C
enum BadEnum {
    A,
    B(i32),
    C { x: u8, y: u8 },
}
// Rust enum layout is complex and not C-compatible
```

## Good Example

```rust
// DO: Use repr(C) for FFI structs
#[repr(C)]
struct GoodStruct {
    a: u8,      // offset 0
    // 3 bytes padding
    b: u32,     // offset 4
    c: u8,      // offset 8
    // 3 bytes padding
}
// Total size: 12, align: 4

// DO: Use repr(C) for enums with explicit discriminant
#[repr(C)]
enum GoodEnum {
    A = 0,
    B = 1,
    C = 2,
}
// Equivalent to C: enum { A = 0, B = 1, C = 2 };

// DO: For complex enums, use tagged unions
#[repr(C)]
struct TaggedUnion {
    tag: GoodEnum,
    data: GoodUnionData,
}

#[repr(C)]
union GoodUnionData {
    a: (),         // For GoodEnum::A
    b: i32,        // For GoodEnum::B
    c: [u8; 2],    // For GoodEnum::C
}

// DO: Verify layout at compile time
const _: () = {
    assert!(std::mem::size_of::<GoodStruct>() == 12);
    assert!(std::mem::align_of::<GoodStruct>() == 4);
};
```

## Layout Verification

```rust
use std::mem::{size_of, align_of, offset_of};

#[repr(C)]
struct Verified {
    a: u8,
    b: u32,
    c: u8,
}

// Compile-time layout verification
const _: () = {
    assert!(size_of::<Verified>() == 12);
    assert!(align_of::<Verified>() == 4);
    // offset_of! requires nightly or crate
    // assert!(offset_of!(Verified, a) == 0);
    // assert!(offset_of!(Verified, b) == 4);
    // assert!(offset_of!(Verified, c) == 8);
};

// Runtime verification
#[test]
fn verify_layout() {
    assert_eq!(size_of::<Verified>(), 12);
    assert_eq!(align_of::<Verified>(), 4);

    let v = Verified { a: 0, b: 0, c: 0 };
    let base = &v as *const _ as usize;

    assert_eq!(&v.a as *const _ as usize - base, 0);
    assert_eq!(&v.b as *const _ as usize - base, 4);
    assert_eq!(&v.c as *const _ as usize - base, 8);
}
```

## repr Options

| Attribute | Effect |
|-----------|--------|
| `#[repr(C)]` | C-compatible layout |
| `#[repr(C, packed)]` | C layout, no padding |
| `#[repr(C, align(N))]` | C layout, minimum align N |
| `#[repr(transparent)]` | Same layout as single field |
| `#[repr(u8)]` etc. | Enum discriminant type |

## Checklist

- [ ] Is every FFI struct marked `#[repr(C)]`?
- [ ] Is every FFI enum using explicit discriminants?
- [ ] Have I verified the layout matches the C header?
- [ ] Have I added compile-time assertions?

## Related Rules

- `mem-01`: Choose appropriate data layout
- `ffi-14`: Types in FFI should have stable layout
