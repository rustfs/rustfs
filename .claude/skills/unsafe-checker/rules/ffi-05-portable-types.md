---
id: ffi-05
original_id: P.UNS.FFI.05
level: P
impact: HIGH
---

# Use Portable Type Aliases from std or libc

## Summary

Use type aliases from `std::os::raw` or the `libc` crate for C-compatible types. Don't assume sizes of C types.

## Rationale

- C types have platform-dependent sizes (`int` is not always 32 bits)
- `long` is 32 bits on Windows, 64 bits on Unix
- Using Rust primitives directly causes portability bugs

## Bad Example

```rust
// DON'T: Use Rust types directly for C interop
extern "C" {
    fn c_function(x: i32, y: i64) -> i32;  // Might not match C types!
}

// DON'T: Assume sizes
#[repr(C)]
struct BadStruct {
    count: i32,   // C 'int' might not be 32 bits
    size: i64,    // C 'long' varies by platform!
    ptr: usize,   // size_t? intptr_t? Different!
}
```

## Good Example

```rust
use std::os::raw::{c_int, c_long, c_char, c_void};

// DO: Use std::os::raw types
extern "C" {
    fn c_function(x: c_int, y: c_long) -> c_int;
}

// DO: Use libc for more types
use libc::{size_t, ssize_t, off_t, pid_t, time_t};

extern "C" {
    fn read(fd: c_int, buf: *mut c_void, count: size_t) -> ssize_t;
    fn lseek(fd: c_int, offset: off_t, whence: c_int) -> off_t;
    fn getpid() -> pid_t;
}

// DO: Match C struct layout
#[repr(C)]
struct GoodStruct {
    count: c_int,
    size: c_long,
    data: *mut c_void,
}

// DO: Use isize/usize for pointer-sized integers
#[repr(C)]
struct PointerSized {
    offset: isize,     // intptr_t equivalent
    size: usize,       // size_t in pointer arithmetic
}
```

## Type Mapping Reference

| C Type | Rust Type | Notes |
|--------|-----------|-------|
| `char` | `c_char` | May be signed or unsigned! |
| `signed char` | `i8` | |
| `unsigned char` | `u8` | |
| `short` | `c_short` | Usually i16 |
| `int` | `c_int` | Usually i32 |
| `long` | `c_long` | 32 or 64 bits! |
| `long long` | `c_longlong` | Usually i64 |
| `size_t` | `usize` or `libc::size_t` | |
| `ssize_t` | `isize` or `libc::ssize_t` | |
| `float` | `c_float` / `f32` | |
| `double` | `c_double` / `f64` | |
| `void*` | `*mut c_void` | |
| `const void*` | `*const c_void` | |

## Platform Differences

```rust
#[cfg(target_pointer_width = "64")]
type PtrDiff = i64;

#[cfg(target_pointer_width = "32")]
type PtrDiff = i32;

// Better: use isize
let diff: isize = ptr1 as isize - ptr2 as isize;
```

## Checklist

- [ ] Am I using std::os::raw or libc types for FFI?
- [ ] Have I avoided assuming c_long is 64 bits?
- [ ] Am I using size_t/usize for sizes?
- [ ] Have I tested on multiple platforms?

## Related Rules

- `ffi-13`: Ensure consistent data layout
- `ffi-14`: Types in FFI should have stable layout
