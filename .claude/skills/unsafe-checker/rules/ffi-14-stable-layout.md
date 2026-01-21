---
id: ffi-14
original_id: P.UNS.FFI.14
level: P
impact: HIGH
---

# Types Used in FFI Should Have Stable Layout

## Summary

FFI types should not change layout between versions. Use `#[repr(C)]` and avoid types with unstable layout like generic `std` types.

## Rationale

- ABI compatibility requires stable layout
- Dynamic libraries may be loaded with different compiler versions
- Layout changes break binary compatibility

## Bad Example

```rust
// DON'T: Use Rust std types with unstable layout in FFI
extern "C" {
    // Vec layout is not stable!
    fn bad_vec(v: Vec<i32>);

    // String layout is not stable!
    fn bad_string(s: String);

    // HashMap layout varies between versions
    fn bad_map(m: std::collections::HashMap<i32, i32>);
}

// DON'T: Use Rust-specific types in C structs
#[repr(C)]
struct BadMixed {
    id: i32,
    data: Vec<u8>,  // Vec is not C-compatible!
}

// DON'T: Use Option with non-null optimization assumptions
#[repr(C)]
struct BadOption {
    value: Option<std::num::NonZeroU32>,  // Layout may change!
}
```

## Good Example

```rust
use std::os::raw::{c_int, c_char, c_void};

// DO: Use C-compatible types
#[repr(C)]
struct GoodStruct {
    id: c_int,
    name: *const c_char,  // C-style string
    data: *const c_void,  // Generic pointer
    data_len: usize,
}

// DO: Use explicit struct for what Vec would provide
#[repr(C)]
struct GoodBuffer {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl GoodBuffer {
    fn from_vec(mut v: Vec<u8>) -> Self {
        let buf = Self {
            ptr: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        std::mem::forget(v);
        buf
    }

    /// # Safety
    /// Must have been created by from_vec()
    unsafe fn into_vec(self) -> Vec<u8> {
        Vec::from_raw_parts(self.ptr, self.len, self.cap)
    }
}

// DO: Use fixed-size arrays for bounded data
#[repr(C)]
struct FixedName {
    name: [c_char; 64],
    name_len: usize,
}

// DO: Define your own stable option type
#[repr(C)]
struct OptionalU32 {
    has_value: bool,
    value: u32,
}

impl From<Option<u32>> for OptionalU32 {
    fn from(opt: Option<u32>) -> Self {
        match opt {
            Some(v) => Self { has_value: true, value: v },
            None => Self { has_value: false, value: 0 },
        }
    }
}
```

## Stable Types for FFI

| Use Instead Of | Stable Type |
|----------------|-------------|
| `Vec<T>` | `*mut T` + `len` + `cap` |
| `String` | `*const c_char` or `*mut c_char` + `len` |
| `&[T]` | `*const T` + `len` |
| `Option<T>` | Custom tagged struct |
| `Result<T, E>` | Error code + out parameter |
| `Box<T>` | `*mut T` |
| `bool` | `c_int` or explicit `u8` |

## Checklist

- [ ] Am I using only C-compatible primitive types?
- [ ] Am I avoiding std collection types in FFI signatures?
- [ ] Have I created stable wrappers for Rust types?
- [ ] Is the layout documented for other languages?

## Related Rules

- `ffi-13`: Ensure consistent data layout
- `ffi-05`: Use portable type aliases
